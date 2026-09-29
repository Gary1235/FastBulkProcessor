using System.Diagnostics.Metrics;

namespace FastBulkProcessor;

internal static class BulkMetrics
{
    private static readonly Meter Meter = new("FastBulkProcessor");

    internal static readonly Counter<long> WrittenRows = Meter.CreateCounter<long>(
        "fastbulkprocessor.write.rows", "rows");

    internal static readonly Counter<long> WrittenBatches = Meter.CreateCounter<long>(
        "fastbulkprocessor.write.batches", "batches");

    internal static readonly Histogram<double> BatchDuration =
        Meter.CreateHistogram<double>(
            "fastbulkprocessor.write.batch.duration", "ms");

    internal static readonly Counter<long> BatchFailures = Meter.CreateCounter<long>(
        "fastbulkprocessor.write.batch.failures", "batches");
}
