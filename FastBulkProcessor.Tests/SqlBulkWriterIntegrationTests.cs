using System.Runtime.CompilerServices;
using System.Data;
using Microsoft.Data.SqlClient;

namespace FastBulkProcessor.Tests;

/// <summary>
/// 驗證完整批次寫入流程能與真實 SQL Server 正確整合。
/// </summary>
public sealed class SqlBulkWriterIntegrationTests
{
    private const string ConnectionStringEnvironmentVariable =
        "FAST_BULK_PROCESSOR_SQL_CONNECTION_STRING";

    /// <summary>
    /// 驗證應用端提供的五筆有效資料會分成兩批寫入，並能從 SQL Server 完整查回。
    /// </summary>
    [SqlServerFact]
    [Trait("Category", "Integration")]
    public async Task WriteAsync_五筆有效資料分成兩批_資料與統計皆正確()
    {
        var connectionString = GetConnectionString();
        var tableName = $"[dbo].[FastBulkProcessorIntegration_{Guid.NewGuid():N}]";
        var tableCreated = false;

        try
        {
            await ExecuteNonQueryAsync(
                connectionString,
                $"""
                CREATE TABLE {tableName}
                (
                    [CustomerId] int NOT NULL,
                    [CustomerName] nvarchar(100) NULL,
                    [OrgId] int NULL
                );
                """);
            tableCreated = true;

            var columns = new[]
            {
                BulkColumn<CustomerTarget>.Create(
                    "CustomerId",
                    target => target.CustomerId),
                BulkColumn<CustomerTarget>.Create(
                    "CustomerName",
                    target => target.CustomerName),
                BulkColumn<CustomerTarget>.Create(
                    "OrgId",
                    target => target.OrgId)
            };
            var sessionFactory = new SqlBulkWriteSessionFactory<CustomerTarget>(
                () => new SqlConnection(connectionString),
                tableName,
                columns);
            var writer = new BulkWriter<CustomerTarget>(sessionFactory);
            var batches = new BatchingReader<CustomerTarget>(
                ReadAcceptedTargetsAsync(),
                batchSize: 3);

            var result = await writer.WriteAsync(batches.ReadAsync());
            var actualRows = await ReadRowsAsync(connectionString, tableName);

            Assert.Equal(5, result.WrittenRowCount);
            Assert.Equal(2, result.WrittenBatchCount);
            Assert.Equal(
                new[]
                {
                    new CustomerTarget(1, "王小明", 10),
                    new CustomerTarget(2, "陳小華", null),
                    new CustomerTarget(3, null, 20),
                    new CustomerTarget(4, "林大同", 20),
                    new CustomerTarget(5, "張美玲", 30)
                },
                actualRows);
        }
        finally
        {
            if (tableCreated)
            {
                await ExecuteNonQueryAsync(
                    connectionString,
                    $"DROP TABLE IF EXISTS {tableName};");
            }
        }
    }

    /// <summary>
    /// 驗證使用應用端外部交易寫入後，Library 不會關閉連線或提交交易，
    /// 且應用端回滾後資料不會留在目的資料表。
    /// </summary>
    [SqlServerFact]
    [Trait("Category", "Integration")]
    public async Task WriteAsync_使用外部交易後由應用端回滾_資料不存在且連線保持開啟()
    {
        var connectionString = GetConnectionString();
        var tableName = $"[dbo].[FastBulkProcessorTransaction_{Guid.NewGuid():N}]";
        var tableCreated = false;

        try
        {
            await ExecuteNonQueryAsync(
                connectionString,
                $"CREATE TABLE {tableName} ([CustomerId] int NOT NULL);");
            tableCreated = true;

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await using var transaction =
                (SqlTransaction)await connection.BeginTransactionAsync();
            var columns = new[]
            {
                BulkColumn<CustomerTarget>.Create(
                    "CustomerId",
                    target => target.CustomerId)
            };
            var sessionFactory = new SqlBulkWriteSessionFactory<CustomerTarget>(
                connection,
                transaction,
                tableName,
                columns);
            var writer = new BulkWriter<CustomerTarget>(sessionFactory);
            IReadOnlyList<CustomerTarget>[] batches =
            [
                [
                    new CustomerTarget(101, "交易測試一", null),
                    new CustomerTarget(102, "交易測試二", null)
                ]
            ];

            var result = await writer.WriteAsync(ReadBatchesAsync(batches));

            Assert.Equal(ConnectionState.Open, connection.State);
            Assert.Same(connection, transaction.Connection);
            Assert.Equal(2, await ReadCountAsync(connection, transaction, tableName));
            Assert.Equal(2, result.WrittenRowCount);
            Assert.Equal(1, result.WrittenBatchCount);

            await transaction.RollbackAsync();

            Assert.Equal(0, await ReadCountAsync(connectionString, tableName));
        }
        finally
        {
            if (tableCreated)
            {
                await ExecuteNonQueryAsync(
                    connectionString,
                    $"DROP TABLE IF EXISTS {tableName};");
            }
        }
    }

    /// <summary>
    /// 驗證外部交易模式不得同時指定 UseInternalTransaction，避免 SqlBulkCopy 使用衝突的交易策略。
    /// </summary>
    [SqlServerFact]
    [Trait("Category", "Integration")]
    public async Task Constructor_外部交易搭配內部交易選項_拋出參數例外()
    {
        var connectionString = GetConnectionString();
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var transaction =
            (SqlTransaction)await connection.BeginTransactionAsync();
        var columns = new[]
        {
            BulkColumn<CustomerTarget>.Create(
                "CustomerId",
                target => target.CustomerId)
        };

        var exception = Assert.Throws<ArgumentException>(
            () => new SqlBulkWriteSessionFactory<CustomerTarget>(
                connection,
                transaction,
                "dbo.Customers",
                columns,
                SqlBulkCopyOptions.TableLock |
                SqlBulkCopyOptions.UseInternalTransaction));

        Assert.Equal("options", exception.ParamName);
        Assert.Equal(ConnectionState.Open, connection.State);
        Assert.Same(connection, transaction.Connection);
    }

    /// <summary>
    /// 驗證外部 Transaction 不屬於指定 Connection 時立即拒絕建立工廠。
    /// </summary>
    [SqlServerFact]
    [Trait("Category", "Integration")]
    public async Task Constructor_外部交易不屬於指定連線_拋出參數例外()
    {
        var connectionString = GetConnectionString();
        await using var transactionConnection = new SqlConnection(connectionString);
        await using var otherConnection = new SqlConnection(connectionString);
        await transactionConnection.OpenAsync();
        await otherConnection.OpenAsync();
        await using var transaction =
            (SqlTransaction)await transactionConnection.BeginTransactionAsync();
        var columns = new[]
        {
            BulkColumn<CustomerTarget>.Create(
                "CustomerId",
                target => target.CustomerId)
        };

        var exception = Assert.Throws<ArgumentException>(
            () => new SqlBulkWriteSessionFactory<CustomerTarget>(
                otherConnection,
                transaction,
                "dbo.Customers",
                columns));

        Assert.Equal("transaction", exception.ParamName);
        Assert.Equal(ConnectionState.Open, transactionConnection.State);
        Assert.Equal(ConnectionState.Open, otherConnection.State);
        Assert.Same(transactionConnection, transaction.Connection);
    }

    private static string GetConnectionString()
    {
        var connectionString = Environment.GetEnvironmentVariable(
            ConnectionStringEnvironmentVariable,
            EnvironmentVariableTarget.Process);

        if (string.IsNullOrWhiteSpace(connectionString) && OperatingSystem.IsWindows())
        {
            connectionString = Environment.GetEnvironmentVariable(
                ConnectionStringEnvironmentVariable,
                EnvironmentVariableTarget.User);
        }

        return connectionString ?? throw new InvalidOperationException(
            $"未設定環境變數 {ConnectionStringEnvironmentVariable}。");
    }

    private static async Task ExecuteNonQueryAsync(
        string connectionString,
        string commandText)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = commandText;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<IReadOnlyList<CustomerTarget>> ReadRowsAsync(
        string connectionString,
        string tableName)
    {
        var rows = new List<CustomerTarget>();
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText =
            $"SELECT [CustomerId], [CustomerName], [OrgId] FROM {tableName} ORDER BY [CustomerId];";

        await using var reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            rows.Add(new CustomerTarget(
                reader.GetInt32(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetInt32(2)));
        }

        return rows;
    }

    private static async Task<int> ReadCountAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string tableName)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"SELECT COUNT(*) FROM {tableName};";
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }

    private static async Task<int> ReadCountAsync(
        string connectionString,
        string tableName)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {tableName};";
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }

    private static async IAsyncEnumerable<IReadOnlyList<CustomerTarget>> ReadBatchesAsync(
        IEnumerable<IReadOnlyList<CustomerTarget>> batches)
    {
        foreach (var batch in batches)
        {
            yield return batch;
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<CustomerTarget> ReadAcceptedTargetsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var targets = new[]
        {
            new CustomerTarget(1, "王小明", 10),
            new CustomerTarget(2, "陳小華", null),
            new CustomerTarget(3, null, 20),
            new CustomerTarget(4, "林大同", 20),
            new CustomerTarget(5, "張美玲", 30)
        };

        foreach (var target in targets)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return target;
            await Task.Yield();
        }
    }

    private sealed record CustomerTarget(
        int CustomerId,
        string? CustomerName,
        int? OrgId);
}
