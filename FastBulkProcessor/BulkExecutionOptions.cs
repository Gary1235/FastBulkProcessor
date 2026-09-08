namespace FastBulkProcessor;

public sealed class BulkExecutionOptions
{
    public BulkExecutionOptions(
        int batchSize,
        int preparationConcurrency,
        int sourceBufferCapacity = 2,
        int preparedBufferCapacity = 2)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(preparationConcurrency, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(sourceBufferCapacity, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(preparedBufferCapacity, 1);

        BatchSize = batchSize;
        PreparationConcurrency = preparationConcurrency;
        SourceBufferCapacity = sourceBufferCapacity;
        PreparedBufferCapacity = preparedBufferCapacity;
    }

    public int BatchSize { get; }
    public int PreparationConcurrency { get; }
    public int SourceBufferCapacity { get; }
    public int PreparedBufferCapacity { get; }
}
