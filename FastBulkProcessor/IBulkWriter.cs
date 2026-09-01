namespace FastBulkProcessor;

/// <summary>
/// 定義依序接收資料批次並寫入目標儲存區的非同步寫入器。
/// </summary>
/// <typeparam name="T">每筆目標資料的型別。</typeparam>
public interface IBulkWriter<T>
{
    /// <summary>
    /// 依序寫入所有資料批次，並回傳完成後的批次及資料筆數統計。
    /// </summary>
    /// <param name="batches">要依序寫入的非同步資料批次串流。</param>
    /// <param name="cancellationToken">用來取消批次列舉與寫入作業的權杖。</param>
    /// <returns>完成後的技術統計結果。</returns>
    ValueTask<BulkWriteResult> WriteAsync(
        IAsyncEnumerable<IReadOnlyList<T>> batches,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// 使用單一寫入 session 依序處理資料批次，並在每批完成前維持來源背壓。
/// </summary>
/// <typeparam name="T">每筆目標資料的型別。</typeparam>
public sealed class BulkWriter<T> : IBulkWriter<T>
{
    private readonly IBulkWriteSessionFactory<T> _sessionFactory;

    /// <summary>
    /// 建立批次資料寫入器。
    /// </summary>
    /// <param name="sessionFactory">用來建立單次寫入作業 session 的工廠。</param>
    public BulkWriter(IBulkWriteSessionFactory<T> sessionFactory)
    {
        _sessionFactory = sessionFactory;
    }

    /// <summary>
    /// 依序寫入所有資料批次；目前批次完成前不會要求來源提供下一批資料。
    /// </summary>
    /// <param name="batches">要依序寫入的非同步資料批次串流。</param>
    /// <param name="cancellationToken">用來取消 session 建立、批次列舉及寫入作業的權杖。</param>
    /// <returns>完成後的技術統計結果。</returns>
    public async ValueTask<BulkWriteResult> WriteAsync(
        IAsyncEnumerable<IReadOnlyList<T>> batches,
        CancellationToken cancellationToken = default)
    {
        await using var session = await _sessionFactory
            .OpenAsync(cancellationToken)
            .ConfigureAwait(false);

        long writtenRowCount = 0;
        var writtenBatchCount = 0;

        await foreach (var batch in batches
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            await session
                .WriteBatchAsync(batch, cancellationToken)
                .ConfigureAwait(false);

            writtenRowCount += batch.Count;
            writtenBatchCount++;
        }

        return new BulkWriteResult(
            writtenRowCount,
            writtenBatchCount);
    }
}
