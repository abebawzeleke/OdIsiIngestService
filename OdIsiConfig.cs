namespace OdIsiIngestService;

public sealed class OdIsiConfig
{
  public string Host { get; set; } = "100.107.133.125";

  public int Port { get; set; } = 50000;

  public string SqlConnectionString { get; set; } = "";

  public int LiveWriteIntervalSeconds { get; set; } = 1;

  public int HistoryWriteIntervalMinutes { get; set; } = 1;

  public int ChannelCount { get; set; } = 4;

  public int CableLengthMeters { get; set; } = 80;

  public string LiveTableName { get; set; } = "OdIsiLiveVector";

  public string HistoryTableName { get; set; } = "OdIsiChannelHistory";
}