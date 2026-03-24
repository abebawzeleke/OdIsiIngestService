using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace OdIsiIngestService
{
  public class Worker : BackgroundService
  {
    private readonly ILogger<Worker> _logger;
    private readonly OdIsiConfig _cfg;

    private readonly string _logDir = @"C:\ProgramData\OilTank";
    private readonly string _logFile;

    // ── Stats ──
    private long _bytesRead;
    private long _framesFound;
    private long _jsonParsed;
    private long _measurements;
    private long _framesSkipped;
    private DateTime _lastHeartbeatUtc = DateTime.MinValue;
    private int _diagnosticFramesLogged = 0;

    // ── Throttle tracking ──
    private readonly ConcurrentDictionary<int, DateTime> _lastLiveWriteByChannel = new();
    private readonly ConcurrentDictionary<int, DateTime> _lastHistoryBucketByChannel = new();

    private TimeSpan LiveWriteInterval =>
        TimeSpan.FromSeconds(_cfg.LiveWriteIntervalSeconds <= 0 ? 1 : _cfg.LiveWriteIntervalSeconds);

    // ── Per-channel latest frame (writer always gets newest data) ──
    private readonly ConcurrentDictionary<int, MeasurementFrame> _latestFrameByChannel = new();

    // ── Reader-side throttle: accept at most one frame per channel per interval ──
    private readonly ConcurrentDictionary<int, DateTime> _lastAcceptedByChannel = new();

    // ── Signal channel: writer wakes up when any channel has new data ──
    private readonly Channel<int> _writerSignal = Channel.CreateBounded<int>(
        new BoundedChannelOptions(64)
        {
          FullMode = BoundedChannelFullMode.DropOldest,
          SingleReader = true,
          SingleWriter = false
        });

    // ── JSON framing state (byte buffer instead of StringBuilder to avoid LOH string allocations) ──
    private bool _inJson = false;
    private bool _inString = false;
    private bool _escape = false;
    private int _depth = 0;
    private byte[] _frameBuf = new byte[512 * 1024];
    private int _frameBufLen = 0;
    private const int MaxFrameSize = 2_000_000;

    public Worker(ILogger<Worker> logger, OdIsiConfig cfg)
    {
      _logger = logger;
      _cfg = cfg;
      _logFile = Path.Combine(_logDir, "ingest-debug.log");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
      EnsureLogFolder();

      // Hint GC to compact the LOH periodically to reclaim fragmented memory
      GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;

      LogBoth($"START build={GetType().Assembly.GetName().Version} host={_cfg.Host} port={_cfg.Port}");

      await StartDebugHttpServer(stoppingToken);

      // Launch the SQL writer as a background task
      var writerTask = Task.Run(() => SqlWriterLoopAsync(stoppingToken), stoppingToken);

      // TCP reader loop — persistent connection with TCP backpressure.
      //
      // Instead of start/stop cycling (which depends on controller latency),
      // we keep a single streaming session and use TCP flow control:
      //
      //  1. Connect + send "measurements start" once
      //  2. READ phase:  read until every channel has a frame
      //  3. PAUSE phase: stop calling ReadAsync for the write interval.
      //     Our 256KB receive buffer fills in ~12ms, TCP advertises window=0,
      //     the controller's write() blocks → it stops generating data.
      //  4. RESUME: call ReadAsync again. TCP window opens, fresh data flows.
      //     Discard the ~256KB of stale buffered data, then collect fresh frames.
      //  5. goto 2
      //
      // This is a transport-layer guarantee — no timing heuristics, no burst
      // windows, and works regardless of the controller's application logic.
      while (!stoppingToken.IsCancellationRequested)
      {
        try
        {
          LogBoth($"CONNECTING host={_cfg.Host} port={_cfg.Port}");

          using var client = new TcpClient();
          client.ReceiveBufferSize = 256 * 1024; // 256KB OS-level TCP receive buffer

          using (var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken))
          {
            cts.CancelAfter(TimeSpan.FromSeconds(10));
            await client.ConnectAsync(_cfg.Host, _cfg.Port, cts.Token);
          }

          using var net = client.GetStream();

          LogBoth($"CONNECTED to ODiSI {_cfg.Host}:{_cfg.Port}");

          // Send start ONCE for the lifetime of this connection
          await TrySendStartCommandAsync(net, "{\"command\":\"measurements start\"}\n", stoppingToken);

          // 64KB read buffer (each frame is ~400KB so bigger buffer = fewer reads)
          var readBuf = new byte[65536];

          while (!stoppingToken.IsCancellationRequested)
          {
            // ── READ PHASE: collect one frame per channel ──
            var channelsReceived = new HashSet<int>();
            var readStart = DateTime.UtcNow;

            // Safety timeout: if we can't get all channels in 30s, something is wrong
            using var readCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            readCts.CancelAfter(TimeSpan.FromSeconds(30));

            try
            {
              while (!readCts.Token.IsCancellationRequested)
              {
                int n = await net.ReadAsync(readBuf.AsMemory(0, readBuf.Length), readCts.Token);

                if (n == 0)
                {
                  LogBoth("ODiSI closed connection -> reconnecting");
                  goto Reconnect;
                }

                _bytesRead += n;

                for (int i = 0; i < n; i++)
                {
                  if (TryExtractJsonFrame(readBuf[i]))
                  {
                    _framesFound++;
                    int? ch = ParseAndEnqueueFrame();
                    if (ch.HasValue)
                      channelsReceived.Add(ch.Value);
                  }
                }

                HeartbeatIfNeeded();

                if (channelsReceived.Count >= _cfg.ChannelCount)
                  break;
              }
            }
            catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
            {
              LogBoth($"Read phase timeout: got {channelsReceived.Count}/{_cfg.ChannelCount} channels in 30s");
            }

            var readElapsed = DateTime.UtcNow - readStart;
            LogBoth($"Read phase: {channelsReceived.Count}/{_cfg.ChannelCount} channels in {readElapsed.TotalMilliseconds:F0}ms");

            // ── PAUSE PHASE: stop reading → TCP backpressure throttles the controller ──
            // The controller's send buffer + our receive buffer (~512KB total) fills up,
            // TCP window goes to 0, and the controller's write() blocks. No data is
            // generated or buffered on the controller during this time.
            var sleepTime = LiveWriteInterval - readElapsed;
            if (sleepTime > TimeSpan.FromMilliseconds(100))
              await Task.Delay(sleepTime, stoppingToken);

            // ── RESUME: drain stale buffered data before next read phase ──
            // After the pause, up to ~256KB of data sits in our receive buffer
            // (sent just before the TCP window closed). Drain it so the next
            // read phase gets fresh frames.
            await DrainStaleDataAsync(net, readBuf, stoppingToken);
          }
        }
        catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
        {
          LogBoth($"Timed out connecting to ODiSI {_cfg.Host}:{_cfg.Port} after 10s");
        }
        catch (OperationCanceledException)
        {
          break;
        }
        catch (Exception ex)
        {
          LogBoth("ERROR tcp reader loop: " + ex);
        }

        Reconnect:

        try
        {
          await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
        }
        catch
        {
          break;
        }
      }

      LogBoth("STOP — waiting for writer to finish");

      _writerSignal.Writer.TryComplete();
      try { await writerTask; } catch { }

      LogBoth("STOP complete");
    }

    // ══════════════════════════════════════════════════════════════
    //  TCP-side: parse JSON frame and store latest per channel.
    //  This NEVER touches SQL — runs entirely in the read loop.
    //
    //  Key optimization: uses Utf8JsonReader on the raw byte buffer
    //  for a cheap channel+messageType scan. Only frames that pass
    //  the reader-side throttle are fully parsed (avoiding ~1.2MB
    //  of string allocations per skipped frame).
    // ══════════════════════════════════════════════════════════════

    /// <summary>
    /// Returns the channel number if the frame was accepted, null otherwise.
    /// </summary>
    private int? ParseAndEnqueueFrame()
    {
      var bytes = _frameBuf.AsMemory(0, _frameBufLen);

      // Diagnostic logging for first 10 frames (always parse fully)
      if (_diagnosticFramesLogged < 10)
      {
        _diagnosticFramesLogged++;
        LogDiagnosticFrameFromBytes(bytes);
      }

      // ── Cheap scan: extract messageType and channel via Utf8JsonReader (zero alloc) ──
      string? messageType = null;
      int? channel = null;

      try
      {
        var reader = new Utf8JsonReader(bytes.Span);

        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
          return null;

        while (reader.Read())
        {
          if (reader.TokenType == JsonTokenType.EndObject)
            break;

          if (reader.TokenType != JsonTokenType.PropertyName)
            continue;

          bool isChannel = reader.ValueTextEquals("channel"u8);
          bool isMsgType = reader.ValueTextEquals("message type"u8) ||
                           reader.ValueTextEquals("message_type"u8) ||
                           reader.ValueTextEquals("messageType"u8);

          if (!reader.Read()) break; // advance to value

          if (isChannel && reader.TokenType == JsonTokenType.Number)
            channel = reader.GetInt32();
          else if (isMsgType && reader.TokenType == JsonTokenType.String)
            messageType = reader.GetString();
          else if (reader.TokenType is JsonTokenType.StartObject or JsonTokenType.StartArray)
            reader.TrySkip(); // skip nested structures without allocating

          // Once we have both, stop — avoids reading the huge "data" array
          if (channel.HasValue && messageType != null)
            break;
        }
      }
      catch
      {
        return null; // malformed JSON
      }

      Interlocked.Increment(ref _jsonParsed);

      // Filter non-measurement messages
      if (!string.IsNullOrWhiteSpace(messageType) &&
          !string.Equals(messageType, "measurement", StringComparison.OrdinalIgnoreCase))
        return null;

      if (!channel.HasValue || channel.Value < 1 || channel.Value > _cfg.ChannelCount)
        return null;

      // ── Reader-side throttle: skip if this channel was recently accepted ──
      if (!ShouldAcceptFrame(channel.Value))
      {
        Interlocked.Increment(ref _framesSkipped);
        return null;
      }

      // Only NOW do we allocate: parse full document from bytes (no string conversion)
      using var doc = JsonDocument.Parse(bytes);
      var root = doc.RootElement;

      if (!root.TryGetProperty("data", out var data) || data.ValueKind != JsonValueKind.Array)
        return null;

      int gaugeCount = data.GetArrayLength();
      string vectorArrayJson = data.GetRawText();

      Interlocked.Increment(ref _measurements);
      _lastAcceptedByChannel[channel.Value] = DateTime.UtcNow;

      _latestFrameByChannel[channel.Value] = new MeasurementFrame
      {
        Channel = channel.Value,
        GaugeCount = gaugeCount,
        DataJson = vectorArrayJson,
        ReceivedUtc = DateTime.UtcNow
      };

      _writerSignal.Writer.TryWrite(channel.Value);
      return channel.Value;
    }

    /// <summary>
    /// Reader-side throttle: accept a frame only if enough time has elapsed
    /// since the last accepted frame for this channel. Uses a slightly shorter
    /// window than the live write interval so data is ready before the writer needs it.
    /// </summary>
    private bool ShouldAcceptFrame(int channel)
    {
      if (!_lastAcceptedByChannel.TryGetValue(channel, out var last))
        return true; // never accepted — need data

      var elapsed = DateTime.UtcNow - last;
      var threshold = LiveWriteInterval - TimeSpan.FromMilliseconds(500);
      if (threshold < TimeSpan.FromMilliseconds(500))
        threshold = TimeSpan.FromMilliseconds(500);

      return elapsed >= threshold;
    }

    // ══════════════════════════════════════════════════════════════
    //  SQL Writer: runs on a separate task, never blocks TCP reads.
    //  Uses a persistent SQL connection with auto-reconnect.
    // ══════════════════════════════════════════════════════════════

    private async Task SqlWriterLoopAsync(CancellationToken ct)
    {
      LogBoth("SQL writer started");

      SqlConnection? conn = null;

      try
      {
        await foreach (var channelSignal in _writerSignal.Reader.ReadAllAsync(ct))
        {
          // Grab the latest frame for this channel
          if (!_latestFrameByChannel.TryGetValue(channelSignal, out var frame))
            continue;

          var ts = frame.ReceivedUtc;
          var channel = frame.Channel;

          bool needsLive = !_lastLiveWriteByChannel.TryGetValue(channel, out var lastLive)
                           || DateTime.UtcNow - lastLive >= LiveWriteInterval;
          bool needsHistory = !_lastHistoryBucketByChannel.TryGetValue(channel, out var lastBucket)
                              || lastBucket != GetHistoryBucketStartUtc(ts);

          if (!needsLive && !needsHistory)
            continue;

          try
          {
            // Reuse connection or reconnect
            conn = await EnsureConnectionAsync(conn, ct);

            if (needsLive)
              await UpsertLiveVectorAsync(conn, ts, channel, frame.GaugeCount, frame.DataJson, ct);

            if (needsHistory)
              await InsertChannelHistoryIfNeededAsync(conn, ts, channel, frame.GaugeCount, frame.DataJson, ct);
          }
          catch (SqlException ex)
          {
            _logger.LogError(ex, "SQL error for Channel={Channel}, will reconnect", channel);
            FileLog(ex.ToString());
            TryDisposeConnection(ref conn);
          }
          catch (Exception ex)
          {
            _logger.LogError(ex, "Writer error for Channel={Channel}", channel);
            FileLog(ex.ToString());
            TryDisposeConnection(ref conn);
          }
        }
      }
      catch (OperationCanceledException) { }
      catch (Exception ex)
      {
        LogBoth("ERROR SQL writer loop crashed: " + ex);
      }
      finally
      {
        TryDisposeConnection(ref conn);
        LogBoth("SQL writer stopped");
      }
    }

    private async Task<SqlConnection> EnsureConnectionAsync(SqlConnection? conn, CancellationToken ct)
    {
      if (conn?.State == System.Data.ConnectionState.Open)
        return conn;

      TryDisposeConnection(ref conn!);

      var newConn = new SqlConnection(_cfg.SqlConnectionString);
      await newConn.OpenAsync(ct);
      return newConn;
    }

    private void TryDisposeConnection(ref SqlConnection? conn)
    {
      try { conn?.Dispose(); } catch { }
      conn = null;
    }

    // ══════════════════════════════════════════════════════════════
    //  SQL operations (same upsert/insert logic as before)
    // ══════════════════════════════════════════════════════════════

    private async Task UpsertLiveVectorAsync(
        SqlConnection conn,
        DateTime ts,
        int channel,
        int gaugeCount,
        string json,
        CancellationToken ct)
    {
      string upsertSql = $@"
UPDATE {_cfg.LiveTableName}
   SET TimestampUtc = @ts,
       GaugeCount   = @gc,
       DataJson     = @json
 WHERE LiveKey = @lk AND Channel = @ch;

IF @@ROWCOUNT = 0
  INSERT INTO {_cfg.LiveTableName}
         (LiveKey, TimestampUtc, Channel, GaugeCount, DataJson)
  VALUES (@lk, @ts, @ch, @gc, @json);";

      try
      {
        await using var cmd = new SqlCommand(upsertSql, conn);
        cmd.Parameters.AddWithValue("@lk", channel);
        cmd.Parameters.AddWithValue("@ts", ts);
        cmd.Parameters.AddWithValue("@ch", channel);
        cmd.Parameters.AddWithValue("@gc", gaugeCount);
        cmd.Parameters.AddWithValue("@json", json);

        await cmd.ExecuteNonQueryAsync(ct);
        _lastLiveWriteByChannel[channel] = DateTime.UtcNow;

        _logger.LogDebug("Upserted live row: Channel={Channel}, GaugeCount={GaugeCount}", channel, gaugeCount);
      }
      catch (Exception ex)
      {
        _logger.LogError(ex, "Failed to upsert live vector for Channel={Channel}", channel);
        throw; // Let writer loop handle reconnect
      }
    }

    private DateTime GetHistoryBucketStartUtc(DateTime tsUtc)
    {
      tsUtc = DateTime.SpecifyKind(tsUtc, DateTimeKind.Utc);

      int intervalSeconds = _cfg.HistoryWriteIntervalSeconds <= 0
          ? 60
          : _cfg.HistoryWriteIntervalSeconds;

      int totalSeconds = tsUtc.Hour * 3600 + tsUtc.Minute * 60 + tsUtc.Second;
      int bucketSeconds = (totalSeconds / intervalSeconds) * intervalSeconds;

      return tsUtc.Date.AddSeconds(bucketSeconds);
    }

    private async Task InsertChannelHistoryIfNeededAsync(
        SqlConnection conn,
        DateTime ts,
        int channel,
        int gaugeCount,
        string json,
        CancellationToken ct)
    {
      var bucketStart = GetHistoryBucketStartUtc(ts);

      string insertSql = $@"
IF NOT EXISTS (
    SELECT 1
    FROM {_cfg.HistoryTableName}
    WHERE BucketStartUtc = @bucketStart
      AND Channel = @ch
)
BEGIN
    INSERT INTO {_cfg.HistoryTableName}
    (BucketStartUtc, TimestampUtc, Channel, GaugeCount, DataJson, CreatedUtc)
    VALUES
    (@bucketStart, @ts, @ch, @gc, @json, @created);
END";

      try
      {
        await using var cmd = new SqlCommand(insertSql, conn);
        cmd.Parameters.AddWithValue("@bucketStart", bucketStart);
        cmd.Parameters.AddWithValue("@ts", ts);
        cmd.Parameters.AddWithValue("@ch", channel);
        cmd.Parameters.AddWithValue("@gc", gaugeCount);
        cmd.Parameters.AddWithValue("@json", json);
        cmd.Parameters.AddWithValue("@created", DateTime.UtcNow);

        await cmd.ExecuteNonQueryAsync(ct);

        _lastHistoryBucketByChannel[channel] = bucketStart;

        _logger.LogInformation(
            "Saved history row for Channel={Channel}, BucketStart={BucketStart}",
            channel, bucketStart);
      }
      catch (Exception ex)
      {
        _logger.LogError(
            ex,
            "Failed to save history row for Channel={Channel}, BucketStart={BucketStart}",
            channel, bucketStart);
        throw; // Let writer loop handle reconnect
      }
    }

    // ══════════════════════════════════════════════════════════════
    //  JSON frame extraction — byte-based (no StringBuilder/string)
    //
    //  Accumulates raw bytes into _frameBuf. When a complete JSON
    //  object/array is detected (depth returns to 0), returns true.
    //  The caller reads the frame from _frameBuf[0.._frameBufLen].
    // ══════════════════════════════════════════════════════════════

    private bool TryExtractJsonFrame(byte b)
    {
      if (!_inJson)
      {
        if (b == (byte)'{' || b == (byte)'[')
        {
          _inJson = true;
          _depth = 1;
          _inString = false;
          _escape = false;
          _frameBufLen = 0;
          AppendToFrameBuf(b);
        }
        return false;
      }

      AppendToFrameBuf(b);

      if (_escape)
      {
        _escape = false;
        return false;
      }

      if (b == (byte)'\\')
      {
        if (_inString) _escape = true;
        return false;
      }

      if (b == (byte)'"')
      {
        _inString = !_inString;
        return false;
      }

      if (_inString) return false;

      if (b == (byte)'{' || b == (byte)'[') _depth++;
      else if (b == (byte)'}' || b == (byte)']') _depth--;

      if (_depth == 0)
      {
        _inJson = false;
        return _frameBufLen >= 2;
      }

      if (_frameBufLen > MaxFrameSize)
      {
        _inJson = false;
        _frameBufLen = 0;
      }

      return false;
    }

    private void AppendToFrameBuf(byte b)
    {
      if (_frameBufLen >= _frameBuf.Length)
      {
        // Double the buffer (rare — only for unexpectedly large frames)
        var newBuf = new byte[_frameBuf.Length * 2];
        Buffer.BlockCopy(_frameBuf, 0, newBuf, 0, _frameBufLen);
        _frameBuf = newBuf;
      }
      _frameBuf[_frameBufLen++] = b;
    }

    /// <summary>
    /// After the pause phase, drain stale bytes sitting in the TCP receive
    /// buffer so the next read phase gets fresh frames. Uses DataAvailable
    /// to read only what's already buffered (no blocking).
    /// </summary>
    private async Task DrainStaleDataAsync(NetworkStream net, byte[] buf, CancellationToken ct)
    {
      long drained = 0;

      try
      {
        // Read whatever is already in the OS receive buffer (non-blocking check)
        while (net.DataAvailable)
        {
          // Short timeout — we're just draining what's buffered, not waiting for new data
          using var drainCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
          drainCts.CancelAfter(200);

          int n = await net.ReadAsync(buf.AsMemory(0, buf.Length), drainCts.Token);
          if (n == 0) break;
          drained += n;
        }
      }
      catch (OperationCanceledException) when (!ct.IsCancellationRequested)
      {
        // Drain timeout — fine
      }
      catch
      {
        // Connection issue — next read will trigger reconnect
      }

      // Reset JSON framing state so partial frames from the stale data
      // don't corrupt the next read phase
      _inJson = false;
      _frameBufLen = 0;
      _depth = 0;
      _inString = false;
      _escape = false;

      if (drained > 0)
        LogBoth($"Drained {drained} stale bytes after pause");
    }

    // ══════════════════════════════════════════════════════════════
    //  Debug HTTP server (unchanged)
    // ══════════════════════════════════════════════════════════════

    private async Task StartDebugHttpServer(CancellationToken ct)
    {
      var listener = new HttpListener();
      listener.Prefixes.Add("http://*:8081/");
      listener.Start();

      _ = Task.Run(async () =>
      {
        while (!ct.IsCancellationRequested)
        {
          HttpListenerContext? ctx = null;
          try
          {
            ctx = await listener.GetContextAsync();

            if (ctx.Request.Url?.AbsolutePath == "/api/odisi/live")
            {
              var rows = new List<object>();

              await using var conn = new SqlConnection(_cfg.SqlConnectionString);
              await conn.OpenAsync(ct);

              var cmd = new SqlCommand(
                  $"SELECT Channel, TimestampUtc, GaugeCount FROM {_cfg.LiveTableName} ORDER BY Channel",
                  conn);

              await using var reader = await cmd.ExecuteReaderAsync(ct);
              while (await reader.ReadAsync(ct))
              {
                rows.Add(new
                {
                  Channel = reader.GetInt32(0),
                  TimestampUtc = reader.GetDateTime(1),
                  GaugeCount = reader.GetInt32(2)
                });
              }

              var jsonResp = JsonSerializer.Serialize(rows);
              var buffer = Encoding.UTF8.GetBytes(jsonResp);

              ctx.Response.ContentType = "application/json";
              await ctx.Response.OutputStream.WriteAsync(buffer, 0, buffer.Length, ct);
              ctx.Response.Close();
            }
            else
            {
              ctx.Response.StatusCode = 404;
              ctx.Response.Close();
            }
          }
          catch (Exception ex)
          {
            _logger.LogWarning(ex, "Debug HTTP server request failed.");
            try
            {
              ctx?.Response?.Close();
            }
            catch { }
          }
        }
      }, ct);
    }

    // ══════════════════════════════════════════════════════════════
    //  Helpers
    // ══════════════════════════════════════════════════════════════

    private async Task<bool> TrySendStartCommandAsync(NetworkStream net, string commandText, CancellationToken ct)
    {
      try
      {
        var cmdBytes = Encoding.UTF8.GetBytes(commandText);
        await net.WriteAsync(cmdBytes, 0, cmdBytes.Length, ct);
        await net.FlushAsync(ct);

        LogBoth($"Sent ODiSI command: {commandText.Replace("\r", "\\r").Replace("\n", "\\n")}");
        return true;
      }
      catch (Exception ex)
      {
        _logger.LogError(ex, "Failed sending ODiSI command: {Command}", commandText);
        return false;
      }
    }

    private void HeartbeatIfNeeded()
    {
      var now = DateTime.UtcNow;
      if (now - _lastHeartbeatUtc < TimeSpan.FromSeconds(10)) return;
      _lastHeartbeatUtc = now;

      LogBoth($"HB bytes={_bytesRead} frames={_framesFound} parsed={_jsonParsed} " +
              $"meas={_measurements} skipped={_framesSkipped}");
    }

    private void LogDiagnosticFrameFromBytes(ReadOnlyMemory<byte> bytes)
    {
      try
      {
        using var doc = JsonDocument.Parse(bytes);
        var root = doc.RootElement;

        string preview = bytes.Length > 1000
            ? Encoding.UTF8.GetString(bytes.Span[..1000])
            : Encoding.UTF8.GetString(bytes.Span);

        bool hasData = root.TryGetProperty("data", out var dbgData) && dbgData.ValueKind == JsonValueKind.Array;
        int? dbgChannel = TryGetInt(root, "channel");
        string? dbgMessageType = GetMessageType(root);

        _logger.LogWarning(
            "ODiSI diagnostic frame #{FrameNo}: messageType={MessageType}, hasData={HasData}, channel={Channel}, preview={Preview}",
            _diagnosticFramesLogged,
            dbgMessageType ?? "<none>",
            hasData,
            dbgChannel,
            preview);
      }
      catch (Exception ex)
      {
        _logger.LogWarning(ex, "Failed to log diagnostic frame #{FrameNo}", _diagnosticFramesLogged);
      }
    }

    private static string? GetMessageType(JsonElement root)
    {
      if (root.TryGetProperty("message type", out var mt1) && mt1.ValueKind == JsonValueKind.String)
        return mt1.GetString();
      if (root.TryGetProperty("message_type", out var mt2) && mt2.ValueKind == JsonValueKind.String)
        return mt2.GetString();
      if (root.TryGetProperty("messageType", out var mt3) && mt3.ValueKind == JsonValueKind.String)
        return mt3.GetString();
      return null;
    }

    private static int? TryGetInt(JsonElement root, string name)
    {
      if (root.TryGetProperty(name, out var el) && el.ValueKind == JsonValueKind.Number)
        return el.GetInt32();
      return null;
    }

    private void EnsureLogFolder()
    {
      try { Directory.CreateDirectory(_logDir); }
      catch { }
    }

    private void LogBoth(string msg)
    {
      _logger.LogInformation(msg);
      FileLog(msg);
    }

    private void FileLog(string msg)
    {
      try
      {
        File.AppendAllText(_logFile, $"{DateTime.UtcNow:O} {msg}{Environment.NewLine}");
      }
      catch
      {
        // never throw from logging
      }
    }

    // ══════════════════════════════════════════════════════════════
    //  Data transfer object for per-channel latest frame
    // ══════════════════════════════════════════════════════════════

    private sealed class MeasurementFrame
    {
      public int Channel;
      public int GaugeCount;
      public string DataJson = "";
      public DateTime ReceivedUtc;
    }
  }
}
