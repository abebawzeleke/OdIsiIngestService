using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
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

    private long _bytesRead;
    private long _framesFound;
    private long _jsonParsed;
    private long _measurements;
    private DateTime _lastHeartbeatUtc = DateTime.MinValue;
    private int _diagnosticFramesLogged = 0;

    private readonly ConcurrentDictionary<int, DateTime> _lastLiveWriteByChannel = new();
    private readonly ConcurrentDictionary<int, DateTime> _lastHistoryBucketByChannel = new();

    private TimeSpan LiveWriteInterval =>
        TimeSpan.FromSeconds(_cfg.LiveWriteIntervalSeconds <= 0 ? 1 : _cfg.LiveWriteIntervalSeconds);

    // JSON framing state copied from the old working oil-tank ingest path
    private bool _inJson = false;
    private bool _inString = false;
    private bool _escape = false;
    private int _depth = 0;
    private readonly StringBuilder _jsonSb = new(256 * 1024);

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

      while (!stoppingToken.IsCancellationRequested)
      {
        try
        {
          LogBoth($"CONNECTING host={_cfg.Host} port={_cfg.Port}");

          using var client = new TcpClient();

          using (var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken))
          {
            cts.CancelAfter(TimeSpan.FromSeconds(10));
            await client.ConnectAsync(_cfg.Host, _cfg.Port, cts.Token);
          }

          using var net = client.GetStream();

          LogBoth($"CONNECTED to ODiSI {_cfg.Host}:{_cfg.Port}");

          // Try the most likely ODiSI command variant for this firmware.
          await TrySendStartCommandAsync(net, "{\"command\":\"measurements start\"}\n", stoppingToken);

          var readBuf = new byte[8192];

          while (!stoppingToken.IsCancellationRequested)
          {
            int n = await net.ReadAsync(readBuf, 0, readBuf.Length, stoppingToken);

            if (n == 0)
            {
              LogBoth("ODiSI closed connection -> reconnecting");
              break;
            }

            _bytesRead += n;

            for (int i = 0; i < n; i++)
            {
              if (TryExtractJsonFrame((char)readBuf[i], out var frame))
              {
                _framesFound++;
                await ProcessVectorMessageAsync(frame, stoppingToken);
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
          LogBoth("ERROR ingest loop crashed: " + ex);
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

      LogBoth("STOP");
    }

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

    private async Task ProcessVectorMessageAsync(string json, CancellationToken ct)
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

          string preview = json.Length > 1000 ? json[..1000] : json;

          bool hasData = root.TryGetProperty("data", out var dbgData) && dbgData.ValueKind == JsonValueKind.Array;
          int? dbgChannel = TryGetInt(root, "channel");

          string? dbgMessageType = null;
          if (root.TryGetProperty("message type", out var dbgMt1) && dbgMt1.ValueKind == JsonValueKind.String)
            dbgMessageType = dbgMt1.GetString();
          else if (root.TryGetProperty("message_type", out var dbgMt2) && dbgMt2.ValueKind == JsonValueKind.String)
            dbgMessageType = dbgMt2.GetString();
          else if (root.TryGetProperty("messageType", out var dbgMt3) && dbgMt3.ValueKind == JsonValueKind.String)
            dbgMessageType = dbgMt3.GetString();

          _logger.LogWarning(
              "ODiSI diagnostic frame #{FrameNo}: messageType={MessageType}, hasData={HasData}, channel={Channel}, preview={Preview}",
              _diagnosticFramesLogged,
              dbgMessageType ?? "<none>",
              hasData,
              dbgChannel,
              preview);
        }

        // Accept vendor variations of message type if present, but do not require it.
        // Some ODiSI payloads only contain { "channel": N, "data": [...] } with no message type.
        string? messageType = null;

        if (root.TryGetProperty("message type", out var mt1) && mt1.ValueKind == JsonValueKind.String)
          messageType = mt1.GetString();
        else if (root.TryGetProperty("message_type", out var mt2) && mt2.ValueKind == JsonValueKind.String)
          messageType = mt2.GetString();
        else if (root.TryGetProperty("messageType", out var mt3) && mt3.ValueKind == JsonValueKind.String)
          messageType = mt3.GetString();

        // If a message type is provided and it is not "measurement", skip it.
        // If message type is missing, still allow the frame as long as it has data[].
        if (!string.IsNullOrWhiteSpace(messageType) &&
            !string.Equals(messageType, "measurement", StringComparison.OrdinalIgnoreCase))
        {
          return;
        }

        if (!root.TryGetProperty("data", out var data) || data.ValueKind != JsonValueKind.Array)
          return;

        DateTime ts = TryGetTimestampUtc(root) ?? DateTime.UtcNow;

        int? channel = TryGetInt(root, "channel");
        if (!channel.HasValue || channel.Value < 1 || channel.Value > _cfg.ChannelCount)
        {
          _logger.LogWarning("Invalid channel value: {Channel}", channel);
          return;
        }

        int gaugeCount = data.GetArrayLength();

        // Store just the vector array JSON: [1,2,3,...]
        string vectorArrayJson = data.GetRawText();

        Interlocked.Increment(ref _measurements);
        _logger.LogDebug("Accepted measurement frame for Channel={Channel}, GaugeCount={GaugeCount}", channel.Value, gaugeCount);

        // Share one SQL connection for both live upsert and history insert
        bool needsLive = !_lastLiveWriteByChannel.TryGetValue(channel.Value, out var lastLive)
                         || DateTime.UtcNow - lastLive >= LiveWriteInterval;
        bool needsHistory = !_lastHistoryBucketByChannel.TryGetValue(channel.Value, out var lastBucket)
                            || lastBucket != GetHistoryBucketStartUtc(ts);

        if (needsLive || needsHistory)
        {
          try
          {
            await using var conn = new SqlConnection(_cfg.SqlConnectionString);
            await conn.OpenAsync(ct);

            if (needsLive)
              await UpsertLiveVectorAsync(conn, ts, channel.Value, gaugeCount, vectorArrayJson, ct);

            if (needsHistory)
              await InsertChannelHistoryIfNeededAsync(conn, ts, channel.Value, gaugeCount, vectorArrayJson, ct);
          }
          catch (Exception ex)
          {
            _logger.LogError(ex, "SQL connection error for Channel={Channel}", channel.Value);
            FileLog(ex.ToString());
          }
        }
      }
    }

    /// <summary>
    /// Upserts one row per channel into OdIsiLiveVector.
    /// LiveKey = Channel, so we maintain exactly 4 live rows.
    /// Uses a single atomic statement to avoid race conditions.
    /// </summary>
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
        cmd.Parameters.AddWithValue("@lk", channel);  // LiveKey = Channel
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
        FileLog(ex.ToString());
      }
    }

    private DateTime GetHistoryBucketStartUtc(DateTime tsUtc)
    {
      tsUtc = DateTime.SpecifyKind(tsUtc, DateTimeKind.Utc);

      int intervalMinutes = _cfg.HistoryWriteIntervalMinutes <= 0
          ? 1
          : _cfg.HistoryWriteIntervalMinutes;

      int bucketMinute = (tsUtc.Minute / intervalMinutes) * intervalMinutes;

      return new DateTime(
          tsUtc.Year,
          tsUtc.Month,
          tsUtc.Day,
          tsUtc.Hour,
          bucketMinute,
          0,
          DateTimeKind.Utc);
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
        FileLog(ex.ToString());
      }
    }

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

              var json = JsonSerializer.Serialize(rows);
              var buffer = Encoding.UTF8.GetBytes(json);

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

    private void HeartbeatIfNeeded()
    {
      var now = DateTime.UtcNow;
      if (now - _lastHeartbeatUtc < TimeSpan.FromSeconds(10)) return;
      _lastHeartbeatUtc = now;

      LogBoth($"HB bytes={_bytesRead} frames={_framesFound} parsed={_jsonParsed} meas={_measurements}");
    }

    private static int GetInt(JsonElement root, string name) =>
        root.TryGetProperty(name, out var el) && el.ValueKind == JsonValueKind.Number ? el.GetInt32() : 0;

    private static int? TryGetInt(JsonElement root, string name)
    {
      if (root.TryGetProperty(name, out var el) && el.ValueKind == JsonValueKind.Number)
        return el.GetInt32();
      return null;
    }

    private static DateTime? TryGetTimestampUtc(JsonElement root)
    {
      int year = GetInt(root, "year");
      if (year <= 0) return null;

      int month = GetInt(root, "month");
      int day = GetInt(root, "day");
      int hour = GetInt(root, "hours");
      int min = GetInt(root, "minutes");
      int sec = GetInt(root, "seconds");
      int ms = GetInt(root, "milliseconds");

      try
      {
        return new DateTime(year, month, day, hour, min, sec, ms, DateTimeKind.Utc);
      }
      catch
      {
        return null;
      }
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
  }
}