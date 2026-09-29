using System.Diagnostics;

namespace FastBulkProcessor.Dashboard;

internal sealed class BulkDashboardJobRegistry : IBulkDashboardJobs
{
    private const int MaximumCompletedJobs = 100;
    private readonly object _gate = new();
    private readonly Dictionary<Guid, JobState> _jobs = new();
    private readonly LinkedList<Guid> _newestFirst = new();

    public Guid Start(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var job = new JobState(Guid.NewGuid(), name, DateTimeOffset.UtcNow,
            Stopwatch.GetTimestamp());

        lock (_gate)
        {
            _jobs.Add(job.Id, job);
            _newestFirst.AddFirst(job.Id);
            TrimCompletedJobs();
        }

        return job.Id;
    }

    public void CompleteBatch(Guid id, int rowCount)
    {
        lock (_gate)
        {
            var job = _jobs[id];
            if (job.Status != BulkDashboardJobStatus.Running)
            {
                throw new InvalidOperationException("Cannot update a completed dashboard job.");
            }

            job.ProcessedRowCount += rowCount;
            job.CompletedBatchCount++;
        }
    }

    public void Finish(Guid id, BulkDashboardJobStatus status, Exception? error = null)
    {
        lock (_gate)
        {
            var job = _jobs[id];
            job.Status = status;
            job.CompletedAt = DateTimeOffset.UtcNow;
            job.CompletedTimestamp = Stopwatch.GetTimestamp();
            var message = error is null ? null :
                $"{error.GetType().Name}: {error.Message}";
            job.Error = message is null ? null :
                message[..Math.Min(500, message.Length)];
            TrimCompletedJobs();
        }
    }

    public IReadOnlyList<BulkDashboardJob> GetJobs()
    {
        lock (_gate)
        {
            return _newestFirst.Select(id =>
            {
                var job = _jobs[id];
                var elapsed = Stopwatch.GetElapsedTime(
                    job.StartedTimestamp,
                    job.CompletedTimestamp ?? Stopwatch.GetTimestamp());
                return new BulkDashboardJob(
                    job.Id,
                    job.Name,
                    job.Status,
                    job.StartedAt,
                    job.CompletedAt,
                    job.ProcessedRowCount,
                    job.CompletedBatchCount,
                    elapsed.TotalSeconds,
                    elapsed.TotalSeconds > 0
                        ? job.ProcessedRowCount / elapsed.TotalSeconds
                        : 0,
                    job.Error);
            }).ToArray();
        }
    }

    private void TrimCompletedJobs()
    {
        var completedCount = _jobs.Values.Count(job =>
            job.Status != BulkDashboardJobStatus.Running);
        var node = _newestFirst.Last;

        while (completedCount > MaximumCompletedJobs && node is not null)
        {
            var previous = node.Previous;
            if (_jobs[node.Value].Status != BulkDashboardJobStatus.Running)
            {
                _jobs.Remove(node.Value);
                _newestFirst.Remove(node);
                completedCount--;
            }

            node = previous;
        }
    }

    private sealed class JobState(
        Guid id,
        string name,
        DateTimeOffset startedAt,
        long startedTimestamp)
    {
        public Guid Id { get; } = id;
        public string Name { get; } = name;
        public DateTimeOffset StartedAt { get; } = startedAt;
        public long StartedTimestamp { get; } = startedTimestamp;
        public BulkDashboardJobStatus Status { get; set; } =
            BulkDashboardJobStatus.Running;
        public DateTimeOffset? CompletedAt { get; set; }
        public long? CompletedTimestamp { get; set; }
        public long ProcessedRowCount { get; set; }
        public int CompletedBatchCount { get; set; }
        public string? Error { get; set; }
    }
}
