using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
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
    private DateTime _lastHeartbeatUtc = DateTime.MinValue;
    private int _diagnosticFramesLogged = 0;

    // ── Throttle tracking ──
    private readonly ConcurrentDictionary<int, DateTime> _lastLiveWriteByChannel = new();
    private readonly ConcurrentDictionary<int, DateTime> _lastHistoryBucketByChannel = new();

    private TimeSpan LiveWriteInterval =>
        TimeSpan.FromSeconds(_cfg.LiveWriteIntervalSeconds <= 0 ? 1 : _cfg.LiveWriteIntervalSeconds);

    // ── Per-channel latest frame (writer always gets newest data) ──
    private readonly ConcurrentDictionary<int, MeasurementFrame> _latestFrameByChannel = new();

    // ── Signal channel: writer wakes up when any channel has new data ──
    private readonly Channel<int> _writerSignal = Channel.CreateBounded<int>(
        new BoundedChannelOptions(64)
        {
          FullMode = BoundedChannelFullMode.DropOldest,
          SingleReader = true,
          SingleWriter = false
        });

    // ── JSON framing state ──
    private bool _inJson = false;
    private bool _inString = false;
    private bool _escape = false;
    private int _depth = 0;
    private readonly StringBuilder _jsonSb = new(512 * 1024);

    public Worker(ILogger<Worker> logger, OdIsiConfig cfg)
    {
      _logger = logger;
      _cfg = cfg;
      _logFile = Path.Combine(_logDir, "ingest-debug.log");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
      EnsureLogFolder();

      LogBoth($"START build={GetType().Assembly.GetName().Version} host={_cfg.Host} port={_cfg.Port}");

      await StartDebugHttpServer(stoppingToken);

      // Launch the SQL writer as a background task
      var writerTask = Task.Run(() => SqlWriterLoopAsync(stoppingToken), stoppingToken);

      // TCP reader loop (main loop — never blocks on SQL)
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

          await TrySendStartCommandAsync(net, "{\"command\":\"measurements start\"}\n", stoppingToken);

          // 64KB read buffer (was 8KB — each frame is ~400KB so bigger buffer = fewer reads)
          var readBuf = new byte[65536];

          while (!stoppingToken.IsCancellationRequested)
          {
            int n = await net.ReadAsync(readBuf.AsMemory(0, readBuf.Length), stoppingToken);

            if (n == 0)
            {
              LogBoth("ODiSI closed connection -> reconnecting");
              break;
            }

            _bytesRead += n;

            // Extract JSON frames and queue them — NO SQL here
            for (int i = 0; i < n; i++)
            {
              if (TryExtractJsonFrame((char)readBuf[i], out var frame))
              {
                _framesFound++;
                ParseAndEnqueueFrame(frame);
              }
            }

            HeartbeatIfNeeded();
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
    // ══════════════════════════════════════════════════════════════

    private void ParseAndEnqueueFrame(string json)
    {
      JsonDocument doc;
      try
      {
        doc = JsonDocument.Parse(json);
        Interlocked.Increment(ref _jsonParsed);
      }
      catch (Exception ex)
      {
        var preview = json.Length > 200 ? json[..200] : json;
        _logger.LogWarning(ex, "Invalid JSON from ODiSI (skipping). Preview: {Preview}", preview);
        return;
      }

      using (doc)
      {
        var root = doc.RootElement;

        if (_diagnosticFramesLogged < 10)
        {
          _diagnosticFramesLogged++;
          LogDiagnosticFrame(root, json);
        }

        // Check message type
        string? messageType = GetMessageType(root);
        if (!string.IsNullOrWhiteSpace(messageType) &&
            !string.Equals(messageType, "measurement", StringComparison.OrdinalIgnoreCase))
        {
          return;
        }

        if (!root.TryGetProperty("data", out var data) || data.ValueKind != JsonValueKind.Array)
          return;

        int? channel = TryGetInt(root, "channel");
        if (!channel.HasValue || channel.Value < 1 || channel.Value > _cfg.ChannelCount)
        {
          _logger.LogWarning("Invalid channel value: {Channel}", channel);
          return;
        }

        int gaugeCount = data.GetArrayLength();
        string vectorArrayJson = data.GetRawText();

        Interlocked.Increment(ref _measurements);

        // Store the latest frame for this channel (overwrites previous if writer hasn't consumed it yet)
        var mf = new MeasurementFrame
        {
          Channel = channel.Value,
          GaugeCount = gaugeCount,
          DataJson = vectorArrayJson,
          ReceivedUtc = DateTime.UtcNow
        };

        _latestFrameByChannel[channel.Value] = mf;

        // Signal the writer that channel N has new data
        _writerSignal.Writer.TryWrite(channel.Value);
      }
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
    //  JSON frame extraction (unchanged)
    // ══════════════════════════════════════════════════════════════

    private bool TryExtractJsonFrame(char ch, out string json)
    {
      json = "";

      if (!_inJson)
      {
        if (ch == '{' || ch == '[')
        {
          _inJson = true;
          _depth = 1;
          _inString = false;
          _escape = false;
          _jsonSb.Clear();
          _jsonSb.Append(ch);
        }
        return false;
      }

      _jsonSb.Append(ch);

      if (_escape)
      {
        _escape = false;
        return false;
      }

      if (ch == '\\')
      {
        if (_inString) _escape = true;
        return false;
      }

      if (ch == '"')
      {
        _inString = !_inString;
        return false;
      }

      if (_inString) return false;

      if (ch == '{' || ch == '[') _depth++;
      else if (ch == '}' || ch == ']') _depth--;

      if (_depth == 0)
      {
        json = _jsonSb.ToString();

        _inJson = false;
        _jsonSb.Clear();
        _depth = 0;
        _inString = false;
        _escape = false;

        return json.Length >= 2;
      }

      if (_jsonSb.Length > 2_000_000)
      {
        _inJson = false;
        _jsonSb.Clear();
        _depth = 0;
        _inString = false;
        _escape = false;
      }

      return false;
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

      LogBoth($"HB bytes={_bytesRead} frames={_framesFound} parsed={_jsonParsed} meas={_measurements}");
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

    private void LogDiagnosticFrame(JsonElement root, string json)
    {
      string preview = json.Length > 1000 ? json[..1000] : json;

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

    private static int GetInt(JsonElement root, string name) =>
        root.TryGetProperty(name, out var el) && el.ValueKind == JsonValueKind.Number ? el.GetInt32() : 0;

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
