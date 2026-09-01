using System.Threading.Channels;
using System.Runtime.ExceptionServices;

namespace FastBulkProcessor;

/// <summary>
/// 定義並行批次 Pipeline 的批次大小、緩衝容量與資料準備併發數。
/// </summary>
public sealed class ConcurrentBatchPipelineOptions
{
    /// <summary>
    /// 建立由應用端決定處理規模的並行批次 Pipeline 設定。
    /// </summary>
    /// <param name="batchSize">每個來源批次包含的最大資料筆數。</param>
    /// <param name="sourceBufferCapacity">允許預讀並等待資料準備的來源批次數。</param>
    /// <param name="preparedBufferCapacity">允許完成準備並等待提交的批次數。</param>
    /// <param name="maxPrepareConcurrency">同時執行應用端 Prepare delegate 的最大批次數。</param>
    /// <exception cref="ArgumentOutOfRangeException">任一設定值小於一時擲出。</exception>
    public ConcurrentBatchPipelineOptions(
        int batchSize,
        int sourceBufferCapacity,
        int preparedBufferCapacity,
        int maxPrepareConcurrency)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(sourceBufferCapacity, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(preparedBufferCapacity, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxPrepareConcurrency, 1);

        BatchSize = batchSize;
        SourceBufferCapacity = sourceBufferCapacity;
        PreparedBufferCapacity = preparedBufferCapacity;
        MaxPrepareConcurrency = maxPrepareConcurrency;
    }

    /// <summary>
    /// 取得每個來源批次包含的最大資料筆數。
    /// </summary>
    public int BatchSize { get; }

    /// <summary>
    /// 取得允許預讀並等待資料準備的來源批次數。
    /// </summary>
    public int SourceBufferCapacity { get; }

    /// <summary>
    /// 取得允許完成準備並等待提交的批次數。
    /// </summary>
    public int PreparedBufferCapacity { get; }

    /// <summary>
    /// 取得同時執行應用端 Prepare delegate 的最大批次數。
    /// </summary>
    public int MaxPrepareConcurrency { get; }
}

/// <summary>
/// 將來源串流分批後有限度地並行呼叫應用端 Prepare delegate，
/// 並依來源批次順序逐批呼叫應用端 Commit delegate。
/// </summary>
/// <typeparam name="TSource">來源串流每筆資料的型別。</typeparam>
/// <typeparam name="TPrepared">應用端完成 Mapping 與規則處理後的批次型別。</typeparam>
public sealed class ConcurrentBatchPipeline<TSource, TPrepared>
{
    private readonly ConcurrentBatchPipelineOptions _options;

    /// <summary>
    /// 建立使用指定批次與併發設定的 Pipeline。
    /// </summary>
    /// <param name="options">由應用端提供的 Pipeline 設定。</param>
    /// <exception cref="ArgumentNullException">當設定為空值時擲出。</exception>
    public ConcurrentBatchPipeline(ConcurrentBatchPipelineOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    /// <summary>
    /// 背景預讀來源資料、並行準備批次，並依原始順序逐批提交結果。
    /// </summary>
    /// <param name="source">應用端提供的來源資料串流。</param>
    /// <param name="prepareAsync">應用端提供的 Mapping 與 BusinessRule 批次處理委派。</param>
    /// <param name="commitAsync">應用端提供的循序交易寫入委派。</param>
    /// <param name="cancellationToken">用來取消整條 Pipeline 的權杖。</param>
    /// <exception cref="ArgumentNullException">任一來源或委派為空值時擲出。</exception>
    public async Task RunAsync(
        IAsyncEnumerable<TSource> source,
        Func<IReadOnlyList<TSource>, CancellationToken, ValueTask<TPrepared>> prepareAsync,
        Func<TPrepared, CancellationToken, ValueTask> commitAsync,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(prepareAsync);
        ArgumentNullException.ThrowIfNull(commitAsync);

        using var pipelineCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var sourceChannel = Channel.CreateBounded<IReadOnlyList<TSource>>(
            CreateChannelOptions(_options.SourceBufferCapacity));
        var preparedTaskChannel = Channel.CreateBounded<Task<TPrepared>>(
            CreateChannelOptions(_options.PreparedBufferCapacity));
        using var prepareConcurrency = new SemaphoreSlim(
            _options.MaxPrepareConcurrency,
            _options.MaxPrepareConcurrency);
        var activePrepareTasks = new ActivePrepareTaskRegistry();
        var prepareFailure = new PrepareFailureState();

        var producerTask = Task.Run(
            () => ProduceBatchesAsync(
                source,
                sourceChannel.Writer,
                pipelineCancellation),
            CancellationToken.None);
        var schedulerTask = Task.Run(
            () => SchedulePrepareAsync(
                sourceChannel.Reader,
                preparedTaskChannel.Writer,
                prepareAsync,
                prepareConcurrency,
                activePrepareTasks,
                prepareFailure,
                pipelineCancellation),
            CancellationToken.None);

        try
        {
            await CommitInOrderAsync(
                    preparedTaskChannel.Reader,
                    commitAsync,
                    pipelineCancellation.Token)
                .ConfigureAwait(false);

            await Task.WhenAll(producerTask, schedulerTask)
                .ConfigureAwait(false);
            await Task.WhenAll(activePrepareTasks.Snapshot())
                .ConfigureAwait(false);
        }
        catch (Exception pipelineException)
        {
            pipelineCancellation.Cancel();
            sourceChannel.Writer.TryComplete();
            preparedTaskChannel.Writer.TryComplete();
            await ObserveCompletionAsync(producerTask, schedulerTask)
                .ConfigureAwait(false);
            await ObserveCompletionAsync(
                    activePrepareTasks.Snapshot())
                .ConfigureAwait(false);

            var originalException =
                prepareFailure.Exception ??
                GetNonCancellationException(producerTask) ??
                GetNonCancellationException(schedulerTask) ??
                pipelineException;

            ExceptionDispatchInfo.Capture(originalException).Throw();
            throw;
        }
    }

    private static BoundedChannelOptions CreateChannelOptions(int capacity) =>
        new(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = false
        };

    private async Task ProduceBatchesAsync(
        IAsyncEnumerable<TSource> source,
        ChannelWriter<IReadOnlyList<TSource>> writer,
        CancellationTokenSource pipelineCancellation)
    {
        Exception? completionException = null;

        try
        {
            var batchingReader = new BatchingReader<TSource>(
                source,
                _options.BatchSize);

            await foreach (var batch in batchingReader
                               .ReadAsync(pipelineCancellation.Token)
                               .ConfigureAwait(false))
            {
                await writer
                    .WriteAsync(batch, pipelineCancellation.Token)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception exception)
        {
            completionException = exception;
            pipelineCancellation.Cancel();
            throw;
        }
        finally
        {
            writer.TryComplete(completionException);
        }
    }

    private static async Task SchedulePrepareAsync(
        ChannelReader<IReadOnlyList<TSource>> sourceReader,
        ChannelWriter<Task<TPrepared>> preparedTaskWriter,
        Func<IReadOnlyList<TSource>, CancellationToken, ValueTask<TPrepared>> prepareAsync,
        SemaphoreSlim prepareConcurrency,
        ActivePrepareTaskRegistry activePrepareTasks,
        PrepareFailureState prepareFailure,
        CancellationTokenSource pipelineCancellation)
    {
        Exception? completionException = null;

        try
        {
            await foreach (var batch in sourceReader
                               .ReadAllAsync(pipelineCancellation.Token)
                               .ConfigureAwait(false))
            {
                await prepareConcurrency
                    .WaitAsync(pipelineCancellation.Token)
                    .ConfigureAwait(false);

                var prepareTask = Task.Run(
                    async () =>
                    {
                        try
                        {
                            return await prepareAsync(
                                    batch,
                                    pipelineCancellation.Token)
                                .ConfigureAwait(false);
                        }
                        catch (Exception exception)
                        {
                            if (!pipelineCancellation.IsCancellationRequested)
                            {
                                prepareFailure.TrySet(exception);
                            }

                            pipelineCancellation.Cancel();
                            throw;
                        }
                        finally
                        {
                            prepareConcurrency.Release();
                        }
                    },
                    CancellationToken.None);
                activePrepareTasks.Track(prepareTask);

                await preparedTaskWriter
                    .WriteAsync(prepareTask, pipelineCancellation.Token)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception exception)
        {
            completionException = exception;
            pipelineCancellation.Cancel();
            throw;
        }
        finally
        {
            preparedTaskWriter.TryComplete(completionException);
        }
    }

    private static async Task CommitInOrderAsync(
        ChannelReader<Task<TPrepared>> preparedTaskReader,
        Func<TPrepared, CancellationToken, ValueTask> commitAsync,
        CancellationToken cancellationToken)
    {
        await foreach (var prepareTask in preparedTaskReader
                           .ReadAllAsync(cancellationToken)
                           .ConfigureAwait(false))
        {
            var preparedBatch = await prepareTask
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            await commitAsync(preparedBatch, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private static async Task ObserveCompletionAsync(params Task[] tasks)
    {
        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch
        {
        }
    }

    private static Exception? GetNonCancellationException(Task task)
    {
        if (!task.IsFaulted || task.Exception is null)
        {
            return null;
        }

        return task.Exception
            .Flatten()
            .InnerExceptions
            .FirstOrDefault(exception =>
                exception is not OperationCanceledException);
    }

    private sealed class PrepareFailureState
    {
        private Exception? _exception;

        public Exception? Exception => Volatile.Read(ref _exception);

        public void TrySet(Exception exception) =>
            Interlocked.CompareExchange(
                ref _exception,
                exception,
                comparand: null);
    }
}
