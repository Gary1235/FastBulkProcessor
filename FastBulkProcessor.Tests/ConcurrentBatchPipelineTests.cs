using System.Runtime.CompilerServices;

namespace FastBulkProcessor.Tests;

/// <summary>
/// 驗證並行批次 Pipeline 能重疊執行資料準備，同時維持原始提交順序。
/// </summary>
public sealed class ConcurrentBatchPipelineTests
{
    /// <summary>
    /// 驗證第一批仍在準備時第二批可以先完成，但提交端必須等待第一批並依來源順序處理。
    /// </summary>
    [Fact]
    public async Task RunAsync_後續批次先完成準備_仍依來源批次順序提交()
    {
        var firstBatchRelease = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var secondBatchPrepared = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var committedBatchStarts = new List<int>();
        var source = new CountingSource(Enumerable.Range(1, 8));
        var options = new ConcurrentBatchPipelineOptions(
            batchSize: 2,
            sourceBufferCapacity: 2,
            preparedBufferCapacity: 2,
            maxPrepareConcurrency: 2);
        var sut = new ConcurrentBatchPipeline<int, IReadOnlyList<int>>(options);

        var runTask = sut.RunAsync(
            source.ReadAsync(),
            async (batch, cancellationToken) =>
            {
                if (batch[0] == 1)
                {
                    await firstBatchRelease.Task.WaitAsync(cancellationToken);
                }

                if (batch[0] == 3)
                {
                    secondBatchPrepared.SetResult();
                }

                return batch;
            },
            (batch, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                committedBatchStarts.Add(batch[0]);
                return ValueTask.CompletedTask;
            });

        await secondBatchPrepared.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Empty(committedBatchStarts);
        Assert.True(source.ReadCount >= 4);

        firstBatchRelease.SetResult();
        await runTask;

        Assert.Equal([1, 3, 5, 7], committedBatchStarts);
        Assert.Equal(8, source.ReadCount);
    }

    /// <summary>
    /// 驗證同時執行的 Prepare delegate 數量不會超過應用端設定的上限。
    /// </summary>
    [Fact]
    public async Task RunAsync_多批資料等待準備_同時執行數不超過設定值()
    {
        var twoPreparesStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var allowPrepareToComplete = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var activePrepareCount = 0;
        var maximumPrepareCount = 0;
        var options = new ConcurrentBatchPipelineOptions(1, 2, 2, 2);
        var sut = new ConcurrentBatchPipeline<int, int>(options);

        var runTask = sut.RunAsync(
            ReadValuesAsync(1, 2, 3, 4),
            async (batch, cancellationToken) =>
            {
                var active = Interlocked.Increment(ref activePrepareCount);
                UpdateMaximum(ref maximumPrepareCount, active);

                if (active == 2)
                {
                    twoPreparesStarted.TrySetResult();
                }

                try
                {
                    await allowPrepareToComplete.Task.WaitAsync(cancellationToken);
                    return batch[0];
                }
                finally
                {
                    Interlocked.Decrement(ref activePrepareCount);
                }
            },
            (_, _) => ValueTask.CompletedTask);

        await twoPreparesStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(2, Volatile.Read(ref maximumPrepareCount));

        allowPrepareToComplete.SetResult();
        await runTask;

        Assert.Equal(2, maximumPrepareCount);
    }

    /// <summary>
    /// 驗證來源串流失敗時，Pipeline 將 Reader 的原始例外傳回應用端。
    /// </summary>
    [Fact]
    public async Task RunAsync_來源串流失敗_傳回原始例外()
    {
        var expectedException = new InvalidOperationException("來源讀取失敗。");
        var options = new ConcurrentBatchPipelineOptions(1, 1, 1, 1);
        var sut = new ConcurrentBatchPipeline<int, int>(options);

        var actualException = await Assert.ThrowsAsync<InvalidOperationException>(
            () => sut.RunAsync(
                ReadThenFailAsync(expectedException),
                (batch, _) => ValueTask.FromResult(batch[0]),
                (_, _) => ValueTask.CompletedTask));

        Assert.Same(expectedException, actualException);
    }

    /// <summary>
    /// 驗證來源 Buffer 已滿且 Prepare 正在等待時，Reader 不會繼續無限制讀取資料。
    /// </summary>
    [Fact]
    public async Task RunAsync_來源Buffer已滿_停止繼續讀取直到下游消費()
    {
        var firstPrepareStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var allowPrepareToComplete = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var source = new SignalingCountingSource(Enumerable.Range(1, 5));
        var options = new ConcurrentBatchPipelineOptions(1, 1, 1, 1);
        var sut = new ConcurrentBatchPipeline<int, int>(options);

        var runTask = sut.RunAsync(
            source.ReadAsync(),
            async (batch, cancellationToken) =>
            {
                if (batch[0] == 1)
                {
                    firstPrepareStarted.SetResult();
                    await allowPrepareToComplete.Task.WaitAsync(cancellationToken);
                }

                return batch[0];
            },
            (_, _) => ValueTask.CompletedTask);

        await firstPrepareStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await source.FourthRead.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(4, source.ReadCount);

        allowPrepareToComplete.SetResult();
        await runTask;

        Assert.Equal(5, source.ReadCount);
    }

    /// <summary>
    /// 驗證取消 Pipeline 時會停止 Prepare 並釋放來源非同步列舉器。
    /// </summary>
    [Fact]
    public async Task RunAsync_外部取消_停止處理並釋放來源列舉器()
    {
        using var cancellation = new CancellationTokenSource();
        var prepareStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var source = new DisposableInfiniteSource();
        var options = new ConcurrentBatchPipelineOptions(1, 1, 1, 1);
        var sut = new ConcurrentBatchPipeline<int, int>(options);

        var runTask = sut.RunAsync(
            source.ReadAsync(),
            async (batch, cancellationToken) =>
            {
                prepareStarted.SetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                return batch[0];
            },
            (_, _) => ValueTask.CompletedTask,
            cancellation.Token);

        await prepareStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => runTask);
        Assert.True(source.IsDisposed);
    }

    /// <summary>
    /// 驗證 Prepare delegate 失敗時傳回原始例外，且所有尚未提交的批次都立即停止。
    /// </summary>
    [Fact]
    public async Task RunAsync_Prepare失敗_傳回原始例外並停止所有尚未提交批次()
    {
        var expectedException = new InvalidOperationException("Mapping 失敗。");
        var committedValues = new List<int>();
        var options = new ConcurrentBatchPipelineOptions(1, 2, 2, 2);
        var sut = new ConcurrentBatchPipeline<int, int>(options);

        var actualException = await Assert.ThrowsAsync<InvalidOperationException>(
            () => sut.RunAsync(
                ReadValuesAsync(1, 2, 3, 4),
                (batch, _) => batch[0] == 2
                    ? ValueTask.FromException<int>(expectedException)
                    : ValueTask.FromResult(batch[0]),
                (value, _) =>
                {
                    committedValues.Add(value);
                    return ValueTask.CompletedTask;
                }));

        Assert.Same(expectedException, actualException);
        Assert.Empty(committedValues);
    }

    /// <summary>
    /// 驗證 Commit 失敗時會取消並等待其他背景 Prepare 結束，再將原始例外傳回應用端。
    /// </summary>
    [Fact]
    public async Task RunAsync_Commit失敗_停止所有背景Prepare後傳回原始例外()
    {
        var expectedException = new InvalidOperationException("交易提交失敗。");
        var secondPrepareStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var secondPrepareStopped = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var options = new ConcurrentBatchPipelineOptions(1, 1, 1, 2);
        var sut = new ConcurrentBatchPipeline<int, int>(options);

        var actualException = await Assert.ThrowsAsync<InvalidOperationException>(
            () => sut.RunAsync(
                ReadValuesAsync(1, 2),
                async (batch, cancellationToken) =>
                {
                    if (batch[0] == 1)
                    {
                        await secondPrepareStarted.Task.WaitAsync(cancellationToken);
                        return batch[0];
                    }

                    secondPrepareStarted.SetResult();

                    try
                    {
                        await Task.Delay(
                            Timeout.InfiniteTimeSpan,
                            cancellationToken);
                        return batch[0];
                    }
                    finally
                    {
                        secondPrepareStopped.SetResult();
                    }
                },
                (_, _) => ValueTask.FromException(expectedException)));

        Assert.Same(expectedException, actualException);
        Assert.True(secondPrepareStopped.Task.IsCompletedSuccessfully);
    }

    /// <summary>
    /// 驗證後序批次先發生 Prepare 例外時，立即取消仍在等待的前序批次並傳回後序原始例外。
    /// </summary>
    [Fact]
    public async Task RunAsync_後序Prepare先失敗_立即取消前序Prepare並傳回原始例外()
    {
        using var cleanupCancellation = new CancellationTokenSource();
        var expectedException = new InvalidOperationException("第二批 Mapping 失敗。");
        var firstPrepareStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var firstPrepareStopped = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var secondPrepareFailed = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var options = new ConcurrentBatchPipelineOptions(1, 1, 1, 2);
        var sut = new ConcurrentBatchPipeline<int, int>(options);

        var runTask = sut.RunAsync(
            ReadValuesAsync(1, 2),
            async (batch, cancellationToken) =>
            {
                if (batch[0] == 1)
                {
                    firstPrepareStarted.SetResult();

                    try
                    {
                        await Task.Delay(
                            Timeout.InfiniteTimeSpan,
                            cancellationToken);
                        return batch[0];
                    }
                    finally
                    {
                        firstPrepareStopped.SetResult();
                    }
                }

                await firstPrepareStarted.Task.WaitAsync(cancellationToken);
                secondPrepareFailed.SetResult();
                throw expectedException;
            },
            (_, _) => ValueTask.CompletedTask,
            cleanupCancellation.Token);

        await secondPrepareFailed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        try
        {
            var completedTask = await Task.WhenAny(
                runTask,
                Task.Delay(TimeSpan.FromSeconds(1)));

            Assert.Same(runTask, completedTask);

            var actualException = await Assert.ThrowsAsync<InvalidOperationException>(
                () => runTask);

            Assert.Same(expectedException, actualException);
            Assert.True(firstPrepareStopped.Task.IsCompletedSuccessfully);
        }
        finally
        {
            await cleanupCancellation.CancelAsync();

            try
            {
                await runTask;
            }
            catch
            {
            }
        }
    }

    /// <summary>
    /// 驗證 Commit 失敗取消背景 Prepare 後，必須等待該 Prepare 完成非同步清理才能結束 Pipeline。
    /// </summary>
    [Fact]
    public async Task RunAsync_Commit失敗且背景Prepare正在清理_等待全部Prepare結束後才返回()
    {
        var expectedException = new InvalidOperationException("第一批 Commit 失敗。");
        var secondPrepareStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var secondPrepareCancellationObserved = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var allowSecondPrepareToStop = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var secondPrepareStopped = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var options = new ConcurrentBatchPipelineOptions(1, 1, 1, 2);
        var sut = new ConcurrentBatchPipeline<int, int>(options);

        var runTask = sut.RunAsync(
            ReadValuesAsync(1, 2),
            async (batch, cancellationToken) =>
            {
                if (batch[0] == 1)
                {
                    await secondPrepareStarted.Task.WaitAsync(cancellationToken);
                    return batch[0];
                }

                secondPrepareStarted.SetResult();

                try
                {
                    await Task.Delay(
                        Timeout.InfiniteTimeSpan,
                        cancellationToken);
                    return batch[0];
                }
                catch (OperationCanceledException)
                {
                    secondPrepareCancellationObserved.SetResult();
                    await allowSecondPrepareToStop.Task;
                    throw;
                }
                finally
                {
                    secondPrepareStopped.SetResult();
                }
            },
            (_, _) => ValueTask.FromException(expectedException));

        await secondPrepareCancellationObserved.Task.WaitAsync(
            TimeSpan.FromSeconds(5));

        Assert.False(runTask.IsCompleted);

        allowSecondPrepareToStop.SetResult();

        var actualException = await Assert.ThrowsAsync<InvalidOperationException>(
            () => runTask);

        Assert.Same(expectedException, actualException);
        Assert.True(secondPrepareStopped.Task.IsCompletedSuccessfully);
    }

    /// <summary>
    /// 驗證批次大小、緩衝容量或 Prepare 併發數小於一時立即拒絕設定。
    /// </summary>
    /// <param name="batchSize">測試用批次大小。</param>
    /// <param name="sourceBufferCapacity">測試用來源緩衝容量。</param>
    /// <param name="preparedBufferCapacity">測試用準備完成緩衝容量。</param>
    /// <param name="maxPrepareConcurrency">測試用 Prepare 最大併發數。</param>
    /// <param name="expectedParameterName">預期的無效參數名稱。</param>
    [Theory]
    [InlineData(0, 1, 1, 1, "batchSize")]
    [InlineData(1, 0, 1, 1, "sourceBufferCapacity")]
    [InlineData(1, 1, 0, 1, "preparedBufferCapacity")]
    [InlineData(1, 1, 1, 0, "maxPrepareConcurrency")]
    public void Constructor_設定值小於一_拋出參數超出範圍例外(
        int batchSize,
        int sourceBufferCapacity,
        int preparedBufferCapacity,
        int maxPrepareConcurrency,
        string expectedParameterName)
    {
        var exception = Assert.Throws<ArgumentOutOfRangeException>(
            () => new ConcurrentBatchPipelineOptions(
                batchSize,
                sourceBufferCapacity,
                preparedBufferCapacity,
                maxPrepareConcurrency));

        Assert.Equal(expectedParameterName, exception.ParamName);
    }

    private static void UpdateMaximum(ref int maximum, int candidate)
    {
        var current = Volatile.Read(ref maximum);

        while (candidate > current)
        {
            var original = Interlocked.CompareExchange(
                ref maximum,
                candidate,
                current);

            if (original == current)
            {
                return;
            }

            current = original;
        }
    }

    private static async IAsyncEnumerable<int> ReadValuesAsync(
        params int[] values)
    {
        foreach (var value in values)
        {
            yield return value;
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<int> ReadThenFailAsync(
        Exception exception)
    {
        yield return 1;
        await Task.Yield();
        throw exception;
    }

    private sealed class CountingSource(IEnumerable<int> values)
    {
        public int ReadCount { get; private set; }

        public async IAsyncEnumerable<int> ReadAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var value in values)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ReadCount++;
                yield return value;
                await Task.Yield();
            }
        }
    }

    private sealed class SignalingCountingSource(IEnumerable<int> values)
    {
        public int ReadCount { get; private set; }

        public TaskCompletionSource FourthRead { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async IAsyncEnumerable<int> ReadAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var value in values)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ReadCount++;

                if (ReadCount == 4)
                {
                    FourthRead.TrySetResult();
                }

                yield return value;
                await Task.Yield();
            }
        }
    }

    private sealed class DisposableInfiniteSource
    {
        public bool IsDisposed { get; private set; }

        public async IAsyncEnumerable<int> ReadAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            try
            {
                var value = 0;

                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    yield return value++;
                    await Task.Yield();
                }
            }
            finally
            {
                IsDisposed = true;
            }
        }
    }
}
