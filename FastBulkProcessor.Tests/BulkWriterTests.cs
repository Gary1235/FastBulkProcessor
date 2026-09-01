using System.Runtime.CompilerServices;

namespace FastBulkProcessor.Tests;

public class BulkWriterTests
{
    /// <summary>
    /// 驗證第一批資料寫入完成前不會預先讀取下一批，並在全部寫入後回傳正確的批次與資料筆數。
    /// </summary>
    [Fact]
    public async Task WriteAsync_第一批尚未寫入完成時_不預讀下一批並維持背壓()
    {
        var source = new CountingBatchSource();
        var session = new BlockingBulkWriteSession();
        var sessionFactory = new TrackingBulkWriteSessionFactory(session);
        var sut = new BulkWriter<int>(sessionFactory);

        var writeTask = sut.WriteAsync(source.ReadAsync()).AsTask();
        await session.FirstWriteStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(1, source.ReadCount);
        Assert.Single(session.WrittenBatches);

        session.AllowFirstWriteToComplete.SetResult();
        var result = await writeTask;

        Assert.Equal(1, sessionFactory.OpenCount);
        Assert.Equal(2, result.WrittenBatchCount);
        Assert.Equal(5, result.WrittenRowCount);
        Assert.Equal(2, source.ReadCount);
        Assert.True(session.IsDisposed);
    }

    /// <summary>
    /// 驗證任一批次寫入失敗時立即停止，不預讀下一批、原樣傳遞例外，並釋放寫入 session。
    /// </summary>
    [Fact]
    public async Task WriteAsync_第一批寫入失敗時_立即停止並釋放Session()
    {
        var source = new CountingBatchSource();
        var expectedException = new InvalidOperationException("Bulk write failed.");
        var session = new FailingBulkWriteSession(expectedException);
        var sessionFactory = new TrackingBulkWriteSessionFactory(session);
        var sut = new BulkWriter<int>(sessionFactory);

        var actualException = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await sut.WriteAsync(source.ReadAsync()));

        Assert.Same(expectedException, actualException);
        Assert.Equal(1, sessionFactory.OpenCount);
        Assert.Equal(1, session.WriteAttemptCount);
        Assert.Equal(1, source.ReadCount);
        Assert.True(session.IsDisposed);
    }

    private sealed class TrackingBulkWriteSessionFactory(
        IBulkWriteSession<int> session) : IBulkWriteSessionFactory<int>
    {
        public int OpenCount { get; private set; }

        public ValueTask<IBulkWriteSession<int>> OpenAsync(
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            OpenCount++;
            return ValueTask.FromResult<IBulkWriteSession<int>>(session);
        }
    }

    private sealed class FailingBulkWriteSession(Exception exception)
        : IBulkWriteSession<int>
    {
        public int WriteAttemptCount { get; private set; }
        public bool IsDisposed { get; private set; }

        public ValueTask WriteBatchAsync(
            IReadOnlyList<int> batch,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            WriteAttemptCount++;
            return ValueTask.FromException(exception);
        }

        public ValueTask DisposeAsync()
        {
            IsDisposed = true;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class BlockingBulkWriteSession : IBulkWriteSession<int>
    {
        public TaskCompletionSource FirstWriteStarted { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource AllowFirstWriteToComplete { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public List<IReadOnlyList<int>> WrittenBatches { get; } = [];
        public bool IsDisposed { get; private set; }

        public async ValueTask WriteBatchAsync(
            IReadOnlyList<int> batch,
            CancellationToken cancellationToken = default)
        {
            WrittenBatches.Add(batch);

            if (WrittenBatches.Count == 1)
            {
                FirstWriteStarted.SetResult();
                await AllowFirstWriteToComplete.Task.WaitAsync(cancellationToken);
            }
        }

        public ValueTask DisposeAsync()
        {
            IsDisposed = true;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class CountingBatchSource
    {
        public int ReadCount { get; private set; }

        public async IAsyncEnumerable<IReadOnlyList<int>> ReadAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ReadCount++;
            yield return new[] { 1, 2, 3 };
            await Task.Yield();

            cancellationToken.ThrowIfCancellationRequested();
            ReadCount++;
            yield return new[] { 4, 5 };
        }
    }
}
