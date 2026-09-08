namespace FastBulkProcessor;

public static class BulkProcessor
{
    public static Task ExecuteAsync<TSource, TPrepared>(
        BulkWorkflow<TSource, TPrepared> workflow,
        BulkExecutionOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(workflow);
        ArgumentNullException.ThrowIfNull(options);

        var pipeline = new ConcurrentBatchPipeline<TSource, TPrepared>(
            new ConcurrentBatchPipelineOptions(
                options.BatchSize,
                options.SourceBufferCapacity,
                options.PreparedBufferCapacity,
                options.PreparationConcurrency));

        return pipeline.RunAsync(
            workflow.Source,
            workflow.PrepareAsync,
            workflow.CommitAsync,
            cancellationToken);
    }
}
