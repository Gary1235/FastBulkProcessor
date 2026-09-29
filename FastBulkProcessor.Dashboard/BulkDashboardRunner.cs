using FastBulkProcessor;

namespace FastBulkProcessor.Dashboard;

public sealed class BulkDashboardRunner
{
    private readonly BulkDashboardJobRegistry _jobs;

    internal BulkDashboardRunner(BulkDashboardJobRegistry jobs) => _jobs = jobs;

    public async Task RunWorkflowAsync<TSource, TPrepared>(
        string name,
        BulkWorkflow<TSource, TPrepared> workflow,
        BulkExecutionOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(workflow);
        ArgumentNullException.ThrowIfNull(options);
        var id = _jobs.Start(name);

        try
        {
            var trackedWorkflow = new BulkWorkflow<TSource, PreparedBatch<TPrepared>>(
                workflow.Source,
                async (batch, token) => new PreparedBatch<TPrepared>(
                    await workflow.PrepareAsync(batch, token).ConfigureAwait(false),
                    batch.Count),
                async (prepared, token) =>
                {
                    await workflow.CommitAsync(prepared.Value, token)
                        .ConfigureAwait(false);
                    _jobs.CompleteBatch(id, prepared.SourceRowCount);
                });

            await BulkProcessor.ExecuteAsync(trackedWorkflow, options,
                cancellationToken).ConfigureAwait(false);
            _jobs.Finish(id, BulkDashboardJobStatus.Succeeded);
        }
        catch (OperationCanceledException exception)
        {
            _jobs.Finish(id, BulkDashboardJobStatus.Canceled, exception);
            throw;
        }
        catch (Exception exception)
        {
            _jobs.Finish(id, BulkDashboardJobStatus.Failed, exception);
            throw;
        }
    }

    public async ValueTask<BulkWriteResult> RunWriteAsync<T>(
        string name,
        IAsyncEnumerable<IReadOnlyList<T>> batches,
        IBulkWriteSessionFactory<T> sessionFactory,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batches);
        ArgumentNullException.ThrowIfNull(sessionFactory);
        var id = _jobs.Start(name);

        try
        {
            var writer = new BulkWriter<T>(
                new TrackingSessionFactory<T>(sessionFactory,
                    rowCount => _jobs.CompleteBatch(id, rowCount)));
            var result = await writer.WriteAsync(batches, cancellationToken)
                .ConfigureAwait(false);
            _jobs.Finish(id, BulkDashboardJobStatus.Succeeded);
            return result;
        }
        catch (OperationCanceledException exception)
        {
            _jobs.Finish(id, BulkDashboardJobStatus.Canceled, exception);
            throw;
        }
        catch (Exception exception)
        {
            _jobs.Finish(id, BulkDashboardJobStatus.Failed, exception);
            throw;
        }
    }

    private sealed record PreparedBatch<T>(T Value, int SourceRowCount);

    private sealed class TrackingSessionFactory<T>(
        IBulkWriteSessionFactory<T> inner,
        Action<int> batchCompleted) : IBulkWriteSessionFactory<T>
    {
        public async ValueTask<IBulkWriteSession<T>> OpenAsync(
            CancellationToken cancellationToken = default) =>
            new TrackingSession<T>(
                await inner.OpenAsync(cancellationToken).ConfigureAwait(false),
                batchCompleted);
    }

    private sealed class TrackingSession<T>(
        IBulkWriteSession<T> inner,
        Action<int> batchCompleted) : IBulkWriteSession<T>
    {
        public async ValueTask WriteBatchAsync(
            IReadOnlyList<T> batch,
            CancellationToken cancellationToken = default)
        {
            await inner.WriteBatchAsync(batch, cancellationToken)
                .ConfigureAwait(false);
            batchCompleted(batch.Count);
        }

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}
