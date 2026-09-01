using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;

namespace FastBulkProcessor;

/// <summary>
/// 使用 ADO.NET 逐筆讀取並映射資料，避免在開始處理前將所有資料載入記憶體。
/// </summary>
/// <typeparam name="T">每筆資料完成映射後的型別。</typeparam>
public sealed class AdoStreamingReader<T> : IStreamingReader<T>
{
    private readonly Func<DbConnection> _connectionFactory;
    private readonly Func<DbConnection, DbCommand> _commandFactory;
    private readonly Func<DbDataReader, T> _mapper;

    /// <summary>
    /// 建立 ADO.NET 非同步串流讀取器。
    /// </summary>
    /// <param name="connectionFactory">由應用端提供，用來建立來源資料庫連線的工廠。</param>
    /// <param name="commandFactory">使用已開啟連線建立查詢命令的工廠。</param>
    /// <param name="mapper">將目前資料列映射成指定型別的函式。</param>
    /// <exception cref="ArgumentNullException">當任一必要參數為空值時擲出。</exception>
    public AdoStreamingReader(
        Func<DbConnection> connectionFactory,
        Func<DbConnection, DbCommand> commandFactory,
        Func<DbDataReader, T> mapper)
    {
        ArgumentNullException.ThrowIfNull(connectionFactory);
        ArgumentNullException.ThrowIfNull(commandFactory);
        ArgumentNullException.ThrowIfNull(mapper);

        _connectionFactory = connectionFactory;
        _commandFactory = commandFactory;
        _mapper = mapper;
    }

    /// <summary>
    /// 開啟來源資料庫連線，並以 ADO.NET 非同步方式逐筆讀取及映射資料。
    /// </summary>
    /// <param name="cancellationToken">用來取消開啟連線、執行命令及讀取資料的權杖。</param>
    /// <returns>可逐筆列舉的非同步資料串流。</returns>
    public async IAsyncEnumerable<T> ReadAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var connection = _connectionFactory();
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        await using var command = _commandFactory(connection);
        await using var dataReader = await command
            .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
            .ConfigureAwait(false);

        while (await dataReader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            yield return _mapper(dataReader);
        }
    }
}
