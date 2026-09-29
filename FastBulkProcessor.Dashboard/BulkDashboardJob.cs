namespace FastBulkProcessor.Dashboard;

public enum BulkDashboardJobStatus
{
    Running,
    Succeeded,
    Failed,
    Canceled
}

public sealed record BulkDashboardJob(
    Guid Id,
    string Name,
    BulkDashboardJobStatus Status,
    DateTimeOffset StartedAt,
    DateTimeOffset? CompletedAt,
    long ProcessedRowCount,
    int CompletedBatchCount,
    double ElapsedSeconds,
    double RowsPerSecond,
    string? Error);

public interface IBulkDashboardJobs
{
    IReadOnlyList<BulkDashboardJob> GetJobs();
}
