namespace FastBulkProcessor;

public sealed class BulkWorkflow<TSource, TPrepared>
{
    public BulkWorkflow(
        IAsyncEnumerable<TSource> source,
        Func<IReadOnlyList<TSource>, CancellationToken, ValueTask<TPrepared>> prepareAsync,
        Func<TPrepared, CancellationToken, ValueTask> commitAsync)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(prepareAsync);
        ArgumentNullException.ThrowIfNull(commitAsync);

        Source = source;
        PrepareAsync = prepareAsync;
        CommitAsync = commitAsync;
    }

    public IAsyncEnumerable<TSource> Source { get; }

    public Func<IReadOnlyList<TSource>, CancellationToken, ValueTask<TPrepared>>
        PrepareAsync { get; }

    public Func<TPrepared, CancellationToken, ValueTask> CommitAsync { get; }
}
