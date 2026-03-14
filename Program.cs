using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.EventLog;
using OdIsiIngestService;
using System;
using System.Diagnostics;


var host = Host.CreateDefaultBuilder(args)

    // 1) Run as Windows Service
    .UseWindowsService(options =>
    {
      options.ServiceName = "OdIsiIngestService";
    })

    // 2) Configuration (critical for Windows Service correctness)
    .ConfigureAppConfiguration((hostingContext, config) =>
    {
      // When running as a service, default path is System32 � fix it
      config.SetBasePath(AppContext.BaseDirectory);

      config.Sources.Clear();

      config.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
      config.AddJsonFile(
          $"appsettings.{hostingContext.HostingEnvironment.EnvironmentName}.json",
          optional: true,
          reloadOnChange: true
      );

      // Environment variables override JSON
      config.AddEnvironmentVariables();
    })

    // 3) Logging (Event Viewer + Console)
    .ConfigureLogging(logging =>
    {
      logging.ClearProviders();

      logging.AddConsole();

      logging.AddEventLog(new EventLogSettings
      {
        SourceName = "OilTank ODiSI Ingest",
        LogName = "Application"
      });
    })

    // 4) Dependency Injection
    .ConfigureServices((hostContext, services) =>
    {
      var cfg = hostContext.Configuration;

      var hostIp = cfg["ODISI_HOST"] ?? "100.107.133.125";

      var port = int.TryParse(cfg["ODISI_PORT"], out var p)
          ? p
          : 50000;

      var conn =
          cfg.GetConnectionString("OilTankDb")
          ?? cfg["OILTANK_CONN"]
          ?? "Server=localhost;Database=OIL_TANK;User Id=sa;Password=***;TrustServerCertificate=True;";

      services.AddSingleton(new OdIsiConfig
      {
        Host = hostIp,
        Port = port,
        SqlConnectionString = conn,

        LiveWriteIntervalSeconds =
            int.TryParse(cfg["ODISI_LIVE_WRITE_INTERVAL_SECONDS"], out var liveSec) ? liveSec : 1,

        HistoryWriteIntervalSeconds =
            int.TryParse(cfg["ODISI_HISTORY_WRITE_INTERVAL_SECONDS"], out var historySec) ? historySec : 60,

        ChannelCount =
            int.TryParse(cfg["ODISI_CHANNEL_COUNT"], out var channelCount) ? channelCount : 4,

        CableLengthMeters =
            int.TryParse(cfg["ODISI_CABLE_LENGTH_METERS"], out var cableLength) ? cableLength : 80,

        LiveTableName =
            cfg["ODISI_LIVE_TABLE_NAME"] ?? "dbo.OdIsiLiveVector",

        HistoryTableName =
            cfg["ODISI_HISTORY_TABLE_NAME"] ?? "dbo.OdIsiChannelHistory"
      });

      // Your ingestion loop
      services.AddHostedService<Worker>();
    })

    .Build();

await host.RunAsync();
