namespace FastBulkProcessor;

/// <summary>
/// 表示一次批次資料寫入作業完成後的技術統計結果。
/// </summary>
/// <param name="WrittenRowCount">成功交由寫入 session 處理的資料總筆數。</param>
/// <param name="WrittenBatchCount">成功交由寫入 session 處理的批次總數。</param>
public sealed record BulkWriteResult(
    long WrittenRowCount,
    int WrittenBatchCount);
