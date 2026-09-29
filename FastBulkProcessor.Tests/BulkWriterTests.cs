using System.Runtime.CompilerServices;

namespace FastBulkProcessor.Tests;

public class BulkWriterTests
{
    [Fact]
    public void Constructor_SessionFactory為Null_擲出參數例外()
    {
        var exception = Assert.Throws<ArgumentNullException>(
            () => new BulkWriter<int>(null!));

        Assert.Equal("sessionFactory", exception.ParamName);
    }

    [Fact]
    public async Task WriteAsync_Batches為Null_不開啟Session並擲出參數例外()
    {
        var sessionFactory = new TrackingBulkWriteSessionFactory(
            new CompletedBulkWriteSession());
        var sut = new BulkWriter<int>(sessionFactory);

        var exception = await Assert.ThrowsAsync<ArgumentNullException>(
            async () => await sut.WriteAsync(null!));

        Assert.Equal("batches", exception.ParamName);
        Assert.Equal(0, sessionFactory.OpenCount);
    }

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

    [Fact]
    public async Task WriteAsync_成功寫入剛好十萬筆_輸出一筆區間統計()
    {
        var session = new CompletedBulkWriteSession();
        var sessionFactory = new TrackingBulkWriteSessionFactory(session);
        var sut = new BulkWriter<int>(sessionFactory);
        using var output = new StringWriter();
        var originalOutput = Console.Out;

        try
        {
            Console.SetOut(output);
            await sut.WriteAsync(ReadSingleBatchAsync(100_000));
        }
        finally
        {
            Console.SetOut(originalOutput);
        }

        Assert.Matches(
            @"^\[FastBulkProcessor\] CompletedRows=100000; ElapsedMs=\d+(\.\d+)?; RowsPerSecond=\d+(\.\d+)?\r?\n$",
            output.ToString());
    }

    [Fact]
    public async Task WriteAsync_成功結束但不足十萬筆_輸出實際筆數的區間統計()
    {
        var session = new CompletedBulkWriteSession();
        var sessionFactory = new TrackingBulkWriteSessionFactory(session);
        var sut = new BulkWriter<int>(sessionFactory);
        using var output = new StringWriter();
        var originalOutput = Console.Out;

        try
        {
            Console.SetOut(output);
            await sut.WriteAsync(ReadSingleBatchAsync(99_999));
        }
        finally
        {
            Console.SetOut(originalOutput);
        }

        Assert.Matches(
            @"^\[FastBulkProcessor\] CompletedRows=99999; ElapsedMs=\d+(\.\d+)?; RowsPerSecond=\d+(\.\d+)?\r?\n$",
            output.ToString());
    }

    [Fact]
    public async Task WriteAsync_批次跨過十萬筆門檻_輸出實際完成筆數的區間統計()
    {
        var session = new CompletedBulkWriteSession();
        var sessionFactory = new TrackingBulkWriteSessionFactory(session);
        var sut = new BulkWriter<int>(sessionFactory);
        using var output = new StringWriter();
        var originalOutput = Console.Out;

        try
        {
            Console.SetOut(output);
            await sut.WriteAsync(ReadBatchesAsync(
                Enumerable.Range(1, 99_000).ToArray(),
                Enumerable.Range(99_001, 2_000).ToArray()));
        }
        finally
        {
            Console.SetOut(originalOutput);
        }

        Assert.Matches(
            @"^\[FastBulkProcessor\] CompletedRows=101000; ElapsedMs=\d+(\.\d+)?; RowsPerSecond=\d+(\.\d+)?\r?\n$",
            output.ToString());
    }

    [Fact]
    public async Task WriteAsync_完成多個區間與最後剩餘資料_分別輸出各區間統計()
    {
        var session = new CompletedBulkWriteSession();
        var sessionFactory = new TrackingBulkWriteSessionFactory(session);
        var sut = new BulkWriter<int>(sessionFactory);
        using var output = new StringWriter();
        var originalOutput = Console.Out;

        try
        {
            Console.SetOut(output);
            await sut.WriteAsync(ReadBatchesAsync(
                Enumerable.Range(1, 100_000).ToArray(),
                Enumerable.Range(100_001, 100_000).ToArray(),
                Enumerable.Range(200_001, 50_000).ToArray()));
        }
        finally
        {
            Console.SetOut(originalOutput);
        }

        var lines = output.ToString()
            .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

        Assert.Collection(
            lines,
            line => Assert.Matches(@"^\[FastBulkProcessor\] CompletedRows=100000; ElapsedMs=\d+(\.\d+)?; RowsPerSecond=\d+(\.\d+)?$", line),
            line => Assert.Matches(@"^\[FastBulkProcessor\] CompletedRows=100000; ElapsedMs=\d+(\.\d+)?; RowsPerSecond=\d+(\.\d+)?$", line),
            line => Assert.Matches(@"^\[FastBulkProcessor\] CompletedRows=50000; ElapsedMs=\d+(\.\d+)?; RowsPerSecond=\d+(\.\d+)?$", line));
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

    private sealed class CompletedBulkWriteSession : IBulkWriteSession<int>
    {
        public ValueTask WriteBatchAsync(
            IReadOnlyList<int> batch,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private static async IAsyncEnumerable<IReadOnlyList<int>> ReadSingleBatchAsync(
        int count,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        yield return Enumerable.Range(1, count).ToArray();
        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<IReadOnlyList<int>> ReadBatchesAsync(
        params IReadOnlyList<int>[] batches)
    {
        foreach (var batch in batches)
        {
            yield return batch;
            await Task.CompletedTask;
        }
    }
}
