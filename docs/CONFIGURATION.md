# How to Change the History Save Interval

The OdIsiIngestService saves a snapshot of each channel's data to `OdIsiChannelHistory` at a configurable interval. No code changes or rebuild required.

## Steps

### 1. Open appsettings.json

Located at:
```
C:\Users\axon4d-user\source\repos\OdIsiIngestService\appsettings.json
```

Or wherever the service is deployed (check the published output folder).

### 2. Add or update the interval setting

Set `ODISI_HISTORY_WRITE_INTERVAL_MINUTES` to your desired value:

```json
{
  "ODISI_HOST": "100.107.133.125",
  "ODISI_PORT": "50000",
  "ODISI_HISTORY_WRITE_INTERVAL_MINUTES": "1",
  "ConnectionStrings": {
    "OilTankDb": "Server=tcp:localhost,1433;Database=OIL_TANK;Integrated Security=True;Encrypt=False;TrustServerCertificate=True;MultipleActiveResultSets=True;"
  }
}
```

Common values:
- `"1"` — every minute (5,760 rows/day)
- `"5"` — every 5 minutes (1,152 rows/day)
- `"10"` — every 10 minutes (576 rows/day)
- `"15"` — every 15 minutes (384 rows/day)

### 3. Restart the service

**If running as a Windows Service:**
```
sc stop OdIsiIngestService
sc start OdIsiIngestService
```

**If running from Visual Studio:**
Just stop and re-run (Ctrl+F5).

### 4. Verify

Check the console or Event Viewer for log entries like:
```
Saved history row for Channel=1, BucketStart=03/14/2026 18:16:00
```

The `BucketStart` timestamps should align to your chosen interval.

## Other configurable settings

All of these work the same way — edit `appsettings.json`, restart the service:

| Setting | Default | Description |
|---|---|---|
| `ODISI_HOST` | `100.107.133.125` | ODiSI instrument IP address |
| `ODISI_PORT` | `50000` | OMSP TCP port |
| `ODISI_LIVE_WRITE_INTERVAL_SECONDS` | `1` | How often each channel's live row is updated |
| `ODISI_HISTORY_WRITE_INTERVAL_MINUTES` | `1` | How often a history snapshot is saved per channel |
| `ODISI_CHANNEL_COUNT` | `4` | Number of active channels to accept |
| `ODISI_CABLE_LENGTH_METERS` | `80` | Fiber cable length |
| `ODISI_LIVE_TABLE_NAME` | `dbo.OdIsiLiveVector` | SQL table for live data |
| `ODISI_HISTORY_TABLE_NAME` | `dbo.OdIsiChannelHistory` | SQL table for history data |
