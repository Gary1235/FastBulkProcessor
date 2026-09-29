using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;

namespace FastBulkProcessor.Tests;

public sealed class BulkWriterMetricsTests
{
    [Fact]
    public async Task WriteAsync_成功批次_發出筆數批次與耗時指標()
    {
        var currentOperation = new AsyncLocal<bool>();
        var rows = new List<long>();
        var batches = new List<long>();
        var durations = new List<double>();
        using var listener = CreateListener(currentOperation,
            (name, value) =>
            {
                if (name == "fastbulkprocessor.write.rows") rows.Add(value);
                if (name == "fastbulkprocessor.write.batches") batches.Add(value);
            },
            (name, value) =>
            {
                if (name == "fastbulkprocessor.write.batch.duration")
                    durations.Add(value);
            });
        var writer = new BulkWriter<int>(new SessionFactory(new Session()));

        currentOperation.Value = true;
        await writer.WriteAsync(ReadBatchesAsync([1, 2], [3, 4, 5]));
        currentOperation.Value = false;

        Assert.Equal([2, 3], rows);
        Assert.Equal([1, 1], batches);
        Assert.Equal(2, durations.Count);
        Assert.All(durations, duration => Assert.True(duration >= 0));
    }

    [Fact]
    public async Task WriteAsync_批次失敗_只計入失敗次數()
    {
        var currentOperation = new AsyncLocal<bool>();
        var failures = new List<long>();
        var rows = new List<long>();
        using var listener = CreateListener(currentOperation,
            (name, value) =>
            {
                if (name == "fastbulkprocessor.write.batch.failures") failures.Add(value);
                if (name == "fastbulkprocessor.write.rows") rows.Add(value);
            },
            (_, _) => { });
        var writer = new BulkWriter<int>(
            new SessionFactory(new Session(fail: true)));

        currentOperation.Value = true;
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.WriteAsync(ReadBatchesAsync([1, 2])));
        currentOperation.Value = false;

        Assert.Equal([1], failures);
        Assert.Empty(rows);
    }

    private static MeterListener CreateListener(
        AsyncLocal<bool> currentOperation,
        Action<string, long> onLong,
        Action<string, double> onDouble)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name == "FastBulkProcessor")
                    meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            if (currentOperation.Value) onLong(instrument.Name, value);
        });
        listener.SetMeasurementEventCallback<double>((instrument, value, _, _) =>
        {
            if (currentOperation.Value) onDouble(instrument.Name, value);
        });
        listener.Start();
        return listener;
    }

    private static async IAsyncEnumerable<IReadOnlyList<int>> ReadBatchesAsync(
        params IReadOnlyList<int>[] batches)
    {
        foreach (var batch in batches)
        {
            yield return batch;
            await Task.Yield();
        }
    }

    private sealed class SessionFactory(Session session) : IBulkWriteSessionFactory<int>
    {
        public ValueTask<IBulkWriteSession<int>> OpenAsync(
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult<IBulkWriteSession<int>>(session);
    }

    private sealed class Session(bool fail = false) : IBulkWriteSession<int>
    {
        public ValueTask WriteBatchAsync(IReadOnlyList<int> batch,
            CancellationToken cancellationToken = default) =>
            fail ? ValueTask.FromException(new InvalidOperationException("failure"))
                : ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
