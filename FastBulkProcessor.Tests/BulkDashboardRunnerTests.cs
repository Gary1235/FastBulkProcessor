using FastBulkProcessor.Dashboard;
using Microsoft.Extensions.DependencyInjection;

namespace FastBulkProcessor.Tests;

public sealed class BulkDashboardRunnerTests
{
    [Fact]
    public async Task RunWorkflowAsync_依提交結果更新來源筆數與狀態()
    {
        var services = new ServiceCollection()
            .AddFastBulkProcessorDashboard()
            .BuildServiceProvider();
        var runner = services.GetRequiredService<BulkDashboardRunner>();
        var jobs = services.GetRequiredService<IBulkDashboardJobs>();
        var committed = new List<int>();
        var workflow = new BulkWorkflow<int, int>(
            ReadValuesAsync(1, 2, 3),
            (batch, _) => ValueTask.FromResult(batch.Sum()),
            (value, _) =>
            {
                committed.Add(value);
                return ValueTask.CompletedTask;
            });

        await runner.RunWorkflowAsync(
            "Import customers",
            workflow,
            new BulkExecutionOptions(2, 2));

        var job = Assert.Single(jobs.GetJobs());
        Assert.Equal("Import customers", job.Name);
        Assert.Equal(BulkDashboardJobStatus.Succeeded, job.Status);
        Assert.Equal(3, job.ProcessedRowCount);
        Assert.Equal(2, job.CompletedBatchCount);
        Assert.Equal([3, 3], committed);
        Assert.True(job.RowsPerSecond >= 0);
        Assert.True(job.ElapsedSeconds >= 0);
        Assert.NotNull(job.CompletedAt);
    }

    [Fact]
    public async Task RunWorkflowAsync_提交失敗_保留已完成進度與錯誤()
    {
        var services = new ServiceCollection()
            .AddFastBulkProcessorDashboard()
            .BuildServiceProvider();
        var runner = services.GetRequiredService<BulkDashboardRunner>();
        var jobs = services.GetRequiredService<IBulkDashboardJobs>();
        var failure = new InvalidOperationException("commit failed");
        var workflow = new BulkWorkflow<int, int>(
            ReadValuesAsync(1, 2),
            (batch, _) => ValueTask.FromResult(batch[0]),
            (value, _) => value == 2
                ? ValueTask.FromException(failure)
                : ValueTask.CompletedTask);

        var actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => runner.RunWorkflowAsync(
                "Failing import", workflow, new BulkExecutionOptions(1, 1)));

        Assert.Same(failure, actual);
        var job = Assert.Single(jobs.GetJobs());
        Assert.Equal(BulkDashboardJobStatus.Failed, job.Status);
        Assert.Equal(1, job.ProcessedRowCount);
        Assert.Equal(1, job.CompletedBatchCount);
        Assert.Contains("commit failed", job.Error);
    }

    [Fact]
    public async Task RunWorkflowAsync_執行期間可讀取進度且取消後標記狀態()
    {
        var services = new ServiceCollection()
            .AddFastBulkProcessorDashboard()
            .BuildServiceProvider();
        var runner = services.GetRequiredService<BulkDashboardRunner>();
        var jobs = services.GetRequiredService<IBulkDashboardJobs>();
        using var cancellation = new CancellationTokenSource();
        var secondCommitStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var workflow = new BulkWorkflow<int, int>(
            ReadValuesAsync(1, 2),
            (batch, _) => ValueTask.FromResult(batch[0]),
            async (value, token) =>
            {
                if (value == 1)
                {
                    return;
                }

                secondCommitStarted.SetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
            });

        var task = runner.RunWorkflowAsync("Cancelable", workflow,
            new BulkExecutionOptions(1, 1), cancellation.Token);
        await secondCommitStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var running = Assert.Single(jobs.GetJobs());
        Assert.Equal(BulkDashboardJobStatus.Running, running.Status);
        Assert.Equal(1, running.ProcessedRowCount);

        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
        Assert.Equal(BulkDashboardJobStatus.Canceled,
            Assert.Single(jobs.GetJobs()).Status);
    }

    [Fact]
    public async Task RunWriteAsync_僅在批次寫入成功後增加寫入筆數()
    {
        var services = new ServiceCollection()
            .AddFastBulkProcessorDashboard()
            .BuildServiceProvider();
        var runner = services.GetRequiredService<BulkDashboardRunner>();
        var jobs = services.GetRequiredService<IBulkDashboardJobs>();
        var session = new FailingSecondSession();

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await runner.RunWriteAsync("Write rows", ReadBatchesAsync(),
                new SingleSessionFactory(session)));

        var job = Assert.Single(jobs.GetJobs());
        Assert.Equal(BulkDashboardJobStatus.Failed, job.Status);
        Assert.Equal(2, job.ProcessedRowCount);
        Assert.Equal(1, job.CompletedBatchCount);
        Assert.True(session.IsDisposed);
    }

    [Fact]
    public async Task RunWorkflowAsync_完成作業超過上限_只保留最近一百筆()
    {
        var services = new ServiceCollection()
            .AddFastBulkProcessorDashboard()
            .BuildServiceProvider();
        var runner = services.GetRequiredService<BulkDashboardRunner>();
        var jobs = services.GetRequiredService<IBulkDashboardJobs>();
        var workflow = new BulkWorkflow<int, int>(
            ReadValuesAsync(),
            (batch, _) => ValueTask.FromResult(batch[0]),
            (_, _) => ValueTask.CompletedTask);

        for (var index = 0; index < 101; index++)
        {
            await runner.RunWorkflowAsync($"Job {index}", workflow,
                new BulkExecutionOptions(1, 1));
        }

        var retained = jobs.GetJobs();
        Assert.Equal(100, retained.Count);
        Assert.Equal("Job 100", retained[0].Name);
        Assert.Equal("Job 1", retained[^1].Name);
    }

    private static async IAsyncEnumerable<int> ReadValuesAsync(params int[] values)
    {
        foreach (var value in values)
        {
            yield return value;
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<IReadOnlyList<int>> ReadBatchesAsync()
    {
        yield return new[] { 1, 2 };
        await Task.Yield();
        yield return new[] { 3 };
    }

    private sealed class SingleSessionFactory(FailingSecondSession session)
        : IBulkWriteSessionFactory<int>
    {
        public ValueTask<IBulkWriteSession<int>> OpenAsync(
            CancellationToken cancellationToken = default) =>
            ValueTask.FromResult<IBulkWriteSession<int>>(session);
    }

    private sealed class FailingSecondSession : IBulkWriteSession<int>
    {
        private int _writes;

        public bool IsDisposed { get; private set; }

        public ValueTask WriteBatchAsync(IReadOnlyList<int> batch,
            CancellationToken cancellationToken = default)
        {
            if (++_writes == 2)
            {
                throw new InvalidOperationException("second batch failed");
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            IsDisposed = true;
            return ValueTask.CompletedTask;
        }
    }
}
