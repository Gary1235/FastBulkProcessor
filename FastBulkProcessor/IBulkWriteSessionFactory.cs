namespace FastBulkProcessor;

/// <summary>
/// 定義如何為一次 Bulk Write 作業建立可重複使用的寫入 session。
/// </summary>
/// <typeparam name="T">每筆目標資料的型別。</typeparam>
public interface IBulkWriteSessionFactory<T>
{
    /// <summary>
    /// 建立並初始化一次 Bulk Write 作業使用的寫入 session。
    /// </summary>
    /// <param name="cancellationToken">用來取消 session 建立作業的權杖。</param>
    /// <returns>由呼叫端負責非同步釋放的寫入 session。</returns>
    ValueTask<IBulkWriteSession<T>> OpenAsync(
        CancellationToken cancellationToken = default);
}
