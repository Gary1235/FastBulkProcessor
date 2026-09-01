using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace FastBulkProcessor;

/// <summary>
/// 定義 SqlBulkCopy 寫入 session 與具體 SQL Client 之間的可測試介面。
/// </summary>
internal interface ISqlBulkCopyAdapter : IAsyncDisposable
{
    /// <summary>
    /// 加入來源 ordinal 與資料庫目的欄位名稱的對應。
    /// </summary>
    void AddColumnMapping(int sourceOrdinal, string destinationName);

    /// <summary>
    /// 將資料讀取器提供的所有資料列寫入目的資料表。
    /// </summary>
    ValueTask WriteToServerAsync(
        DbDataReader dataReader,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// 使用欄位 mapping 將每個目標資料批次交給 SqlBulkCopy adapter 寫入。
/// </summary>
/// <typeparam name="T">每筆目標資料的型別。</typeparam>
internal sealed class SqlBulkWriteSession<T> : IBulkWriteSession<T>
{
    private readonly ISqlBulkCopyAdapter _bulkCopy;
    private readonly IReadOnlyList<BulkColumn<T>> _columns;

    /// <summary>
    /// 建立 SQL Server Bulk Write session，並依欄位順序註冊所有目的欄位 mapping。
    /// </summary>
    public SqlBulkWriteSession(
        ISqlBulkCopyAdapter bulkCopy,
        IReadOnlyList<BulkColumn<T>> columns)
    {
        _bulkCopy = bulkCopy;
        _columns = columns;

        for (var ordinal = 0; ordinal < columns.Count; ordinal++)
        {
            bulkCopy.AddColumnMapping(
                ordinal,
                columns[ordinal].DestinationName);
        }
    }

    /// <summary>
    /// 將單一目標資料批次包裝成 DbDataReader，並交給 SqlBulkCopy 寫入。
    /// </summary>
    public async ValueTask WriteBatchAsync(
        IReadOnlyList<T> batch,
        CancellationToken cancellationToken = default)
    {
        using var dataReader = new BatchDataReader<T>(batch, _columns);

        await _bulkCopy
            .WriteToServerAsync(dataReader, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// 釋放底層 SqlBulkCopy adapter 及其相關資源。
    /// </summary>
    public ValueTask DisposeAsync() => _bulkCopy.DisposeAsync();
}

/// <summary>
/// 將可測試介面轉接至真正的 Microsoft.Data.SqlClient.SqlBulkCopy。
/// </summary>
internal sealed class SqlBulkCopyAdapter(
    SqlBulkCopy bulkCopy,
    SqlConnection? connection = null) : ISqlBulkCopyAdapter
{
    /// <inheritdoc />
    public void AddColumnMapping(int sourceOrdinal, string destinationName) =>
        bulkCopy.ColumnMappings.Add(sourceOrdinal, destinationName);

    /// <inheritdoc />
    public ValueTask WriteToServerAsync(
        DbDataReader dataReader,
        CancellationToken cancellationToken = default)
    {
        return new ValueTask(
            bulkCopy.WriteToServerAsync(dataReader, cancellationToken));
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        try
        {
            bulkCopy.Close();
        }
        finally
        {
            if (connection is not null)
            {
                await connection.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}
