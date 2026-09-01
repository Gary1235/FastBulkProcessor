namespace FastBulkProcessor;

/// <summary>
/// 定義一次 Bulk Write 作業期間可重複使用的批次寫入 session。
/// </summary>
/// <typeparam name="T">每筆目標資料的型別。</typeparam>
public interface IBulkWriteSession<T> : IAsyncDisposable
{
    /// <summary>
    /// 將單一批次寫入目標儲存區。
    /// </summary>
    /// <param name="batch">要寫入且已通過應用端業務規則的目標資料。</param>
    /// <param name="cancellationToken">用來取消批次寫入作業的權杖。</param>
    ValueTask WriteBatchAsync(
        IReadOnlyList<T> batch,
        CancellationToken cancellationToken = default);
}
