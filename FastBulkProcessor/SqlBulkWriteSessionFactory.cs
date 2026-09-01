using Microsoft.Data.SqlClient;

namespace FastBulkProcessor;

/// <summary>
/// 定義建立並設定底層 SqlBulkCopy 資源的可測試介面。
/// </summary>
internal interface ISqlBulkCopyResourceFactory
{
    /// <summary>
    /// 建立已開啟連線並完成目的資料表、逾時及寫入選項設定的 SqlBulkCopy adapter。
    /// </summary>
    ValueTask<ISqlBulkCopyAdapter> OpenAsync(
        string destinationTableName,
        int bulkCopyTimeoutSeconds,
        SqlBulkCopyOptions options,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// 使用應用端提供的 SQL Server 連線工廠，建立一次 Bulk Write 作業共用的寫入 session。
/// </summary>
/// <typeparam name="T">每筆目標資料的型別。</typeparam>
public sealed class SqlBulkWriteSessionFactory<T> : IBulkWriteSessionFactory<T>
{
    private readonly ISqlBulkCopyResourceFactory _resourceFactory;
    private readonly string _destinationTableName;
    private readonly IReadOnlyList<BulkColumn<T>> _columns;
    private readonly SqlBulkCopyOptions _options;
    private readonly int _bulkCopyTimeoutSeconds;

    /// <summary>
    /// 建立 SQL Server Bulk Write session 工廠。
    /// </summary>
    /// <param name="connectionFactory">由應用端提供，用來建立目的資料庫連線的工廠。</param>
    /// <param name="destinationTableName">SqlBulkCopy 寫入的目的資料表名稱。</param>
    /// <param name="columns">目標資料與資料庫目的欄位的 mapping。</param>
    /// <param name="options">SqlBulkCopy 寫入選項；預設使用 TableLock 並讓每次批次寫入擁有獨立交易。</param>
    /// <param name="bulkCopyTimeoutSeconds">每次 SqlBulkCopy 寫入允許的逾時秒數。</param>
    /// <exception cref="ArgumentNullException">當 SQL Connection 工廠為空值時擲出。</exception>
    /// <exception cref="ArgumentException">當目的資料表名稱未提供有效內容時擲出。</exception>
    /// <exception cref="ArgumentNullException">當欄位 mapping 集合為空值時擲出。</exception>
    /// <exception cref="ArgumentException">當欄位 mapping 集合沒有任何欄位時擲出。</exception>
    /// <exception cref="ArgumentException">當多個 mapping 指向相同目的欄位時擲出。</exception>
    /// <exception cref="ArgumentOutOfRangeException">當 BulkCopy 逾時秒數為負數時擲出。</exception>
    public SqlBulkWriteSessionFactory(
        Func<SqlConnection> connectionFactory,
        string destinationTableName,
        IReadOnlyList<BulkColumn<T>> columns,
        SqlBulkCopyOptions options =
            SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction,
        int bulkCopyTimeoutSeconds = 30)
        : this(
            new SqlBulkCopyResourceFactory(connectionFactory),
            destinationTableName,
            columns,
            options,
            bulkCopyTimeoutSeconds)
    {
    }

    /// <summary>
    /// 使用應用端已開啟的 SQL Connection 與 Transaction 建立 Bulk Write session 工廠。
    /// Library 只使用外部資源，不會提交、回滾、關閉或釋放它們。
    /// </summary>
    /// <param name="connection">應用端已開啟且持有的目的資料庫連線。</param>
    /// <param name="transaction">應用端建立並負責提交或回滾的交易。</param>
    /// <param name="destinationTableName">SqlBulkCopy 寫入的目的資料表名稱。</param>
    /// <param name="columns">目標資料與資料庫目的欄位的 mapping。</param>
    /// <param name="options">SqlBulkCopy 寫入選項；外部交易模式不得包含 UseInternalTransaction。</param>
    /// <param name="bulkCopyTimeoutSeconds">每次 SqlBulkCopy 寫入允許的逾時秒數。</param>
    /// <exception cref="ArgumentNullException">當 Connection 或 Transaction 為空值時擲出。</exception>
    /// <exception cref="ArgumentException">當 Transaction 不屬於指定 Connection 時擲出。</exception>
    /// <exception cref="ArgumentException">當 options 包含 UseInternalTransaction 時擲出。</exception>
    public SqlBulkWriteSessionFactory(
        SqlConnection connection,
        SqlTransaction transaction,
        string destinationTableName,
        IReadOnlyList<BulkColumn<T>> columns,
        SqlBulkCopyOptions options = SqlBulkCopyOptions.TableLock,
        int bulkCopyTimeoutSeconds = 30)
        : this(
            new ExternalSqlBulkCopyResourceFactory(connection, transaction),
            destinationTableName,
            columns,
            options,
            bulkCopyTimeoutSeconds)
    {
        if ((options & SqlBulkCopyOptions.UseInternalTransaction) != 0)
        {
            throw new ArgumentException(
                "外部交易模式不得同時使用 UseInternalTransaction。",
                nameof(options));
        }
    }

    /// <summary>
    /// 建立使用指定底層資源工廠的 SQL Server Bulk Write session 工廠。
    /// </summary>
    internal SqlBulkWriteSessionFactory(
        ISqlBulkCopyResourceFactory resourceFactory,
        string destinationTableName,
        IReadOnlyList<BulkColumn<T>> columns,
        SqlBulkCopyOptions options,
        int bulkCopyTimeoutSeconds)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationTableName);
        ArgumentNullException.ThrowIfNull(columns);

        if (columns.Count == 0)
        {
            throw new ArgumentException(
                "欄位 mapping 集合至少必須包含一個欄位。",
                nameof(columns));
        }

        var destinationNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var column in columns)
        {
            if (!destinationNames.Add(column.DestinationName))
            {
                throw new ArgumentException(
                    $"目的欄位 '{column.DestinationName}' 不可重複 mapping。",
                    nameof(columns));
            }
        }

        ArgumentOutOfRangeException.ThrowIfNegative(bulkCopyTimeoutSeconds);

        _resourceFactory = resourceFactory;
        _destinationTableName = destinationTableName;
        _columns = columns.ToArray();
        _options = options;
        _bulkCopyTimeoutSeconds = bulkCopyTimeoutSeconds;
    }

    /// <summary>
    /// 開啟目的資料庫連線、建立 SqlBulkCopy，並回傳可供所有批次共用的寫入 session。
    /// </summary>
    /// <param name="cancellationToken">用來取消連線開啟及 session 建立作業的權杖。</param>
    /// <returns>由 BulkWriter 負責非同步釋放的 SQL Server 寫入 session。</returns>
    public async ValueTask<IBulkWriteSession<T>> OpenAsync(
        CancellationToken cancellationToken = default)
    {
        var adapter = await _resourceFactory
            .OpenAsync(
                _destinationTableName,
                _bulkCopyTimeoutSeconds,
                _options,
                cancellationToken)
            .ConfigureAwait(false);

        try
        {
            return new SqlBulkWriteSession<T>(adapter, _columns);
        }
        catch
        {
            await adapter.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}

/// <summary>
/// 使用真正的 Microsoft.Data.SqlClient 元件建立已初始化的 SqlBulkCopy adapter。
/// </summary>
internal sealed class SqlBulkCopyResourceFactory : ISqlBulkCopyResourceFactory
{
    private readonly Func<SqlConnection> _connectionFactory;

    /// <summary>
    /// 建立使用應用端 SQL Connection 工廠的 SqlBulkCopy 資源工廠。
    /// </summary>
    /// <param name="connectionFactory">用來建立目的資料庫連線的工廠。</param>
    /// <exception cref="ArgumentNullException">當 SQL Connection 工廠為空值時擲出。</exception>
    public SqlBulkCopyResourceFactory(Func<SqlConnection> connectionFactory)
    {
        ArgumentNullException.ThrowIfNull(connectionFactory);
        _connectionFactory = connectionFactory;
    }

    /// <inheritdoc />
    public async ValueTask<ISqlBulkCopyAdapter> OpenAsync(
        string destinationTableName,
        int bulkCopyTimeoutSeconds,
        SqlBulkCopyOptions options,
        CancellationToken cancellationToken = default)
    {
        var connection = _connectionFactory();

        try
        {
            await connection
                .OpenAsync(cancellationToken)
                .ConfigureAwait(false);

            var bulkCopy = new SqlBulkCopy(
                connection,
                options,
                externalTransaction: null)
            {
                DestinationTableName = destinationTableName,
                BulkCopyTimeout = bulkCopyTimeoutSeconds,
                EnableStreaming = true
            };

            return new SqlBulkCopyAdapter(bulkCopy, connection);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}

/// <summary>
/// 使用應用端持有的已開啟連線及交易建立 SqlBulkCopy，且不取得外部資源的所有權。
/// </summary>
internal sealed class ExternalSqlBulkCopyResourceFactory
    : ISqlBulkCopyResourceFactory
{
    private readonly SqlConnection _connection;
    private readonly SqlTransaction _transaction;

    /// <summary>
    /// 建立不持有 Connection 與 Transaction 所有權的 SqlBulkCopy 資源工廠。
    /// </summary>
    public ExternalSqlBulkCopyResourceFactory(
        SqlConnection connection,
        SqlTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);

        if (!ReferenceEquals(transaction.Connection, connection))
        {
            throw new ArgumentException(
                "Transaction 必須屬於指定的 Connection。",
                nameof(transaction));
        }

        _connection = connection;
        _transaction = transaction;
    }

    /// <inheritdoc />
    public ValueTask<ISqlBulkCopyAdapter> OpenAsync(
        string destinationTableName,
        int bulkCopyTimeoutSeconds,
        SqlBulkCopyOptions options,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_connection.State != System.Data.ConnectionState.Open)
        {
            throw new InvalidOperationException(
                "外部交易使用的 SQL Connection 必須保持開啟。");
        }

        if (!ReferenceEquals(_transaction.Connection, _connection))
        {
            throw new InvalidOperationException(
                "外部 Transaction 已完成或不再屬於指定的 Connection。");
        }

        if ((options & SqlBulkCopyOptions.UseInternalTransaction) != 0)
        {
            throw new ArgumentException(
                "外部交易模式不得同時使用 UseInternalTransaction。",
                nameof(options));
        }

        var bulkCopy = new SqlBulkCopy(
            _connection,
            options,
            _transaction)
        {
            DestinationTableName = destinationTableName,
            BulkCopyTimeout = bulkCopyTimeoutSeconds,
            EnableStreaming = true
        };

        return ValueTask.FromResult<ISqlBulkCopyAdapter>(
            new SqlBulkCopyAdapter(bulkCopy));
    }
}
