namespace FastBulkProcessor.Tests;

public sealed class BulkProcessorTests
{
    [Fact]
    public async Task ExecuteAsync_執行Workflow並依批次順序Commit()
    {
        var committedValues = new List<int>();
        var workflow = new BulkWorkflow<int, int>(
            ReadValuesAsync(1, 2, 3, 4),
            (batch, _) => ValueTask.FromResult(batch.Sum()),
            (prepared, _) =>
            {
                committedValues.Add(prepared);
                return ValueTask.CompletedTask;
            });

        await BulkProcessor.ExecuteAsync(
            workflow,
            new BulkExecutionOptions(
                batchSize: 2,
                preparationConcurrency: 2));

        Assert.Equal([3, 7], committedValues);
    }

    [Fact]
    public void BulkExecutionOptions_批次大小無效時_拋出例外()
    {
        var exception = Assert.Throws<ArgumentOutOfRangeException>(
            () => new BulkExecutionOptions(
                batchSize: 0,
                preparationConcurrency: 1));

        Assert.Equal("batchSize", exception.ParamName);
    }

    private static async IAsyncEnumerable<int> ReadValuesAsync(params int[] values)
    {
        foreach (var value in values)
        {
            yield return value;
            await Task.Yield();
        }
    }
}
