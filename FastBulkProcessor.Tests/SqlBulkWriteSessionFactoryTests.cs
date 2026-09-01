using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace FastBulkProcessor.Tests;

public class SqlBulkWriteSessionFactoryTests
{
    /// <summary>
    /// 驗證建立 SQL Bulk Write session 時，目的資料表、逾時與寫入選項只設定一次，
    /// 並在 session 結束時釋放底層 SqlBulkCopy 資源。
    /// </summary>
    [Fact]
    public async Task OpenAsync_建立寫入Session時_套用SqlBulkCopy設定並共用單一資源()
    {
        var adapter = new TrackingSqlBulkCopyAdapter();
        var resourceFactory = new TrackingSqlBulkCopyResourceFactory(adapter);
        BulkColumn<TargetCustomer>[] columns =
        [
            BulkColumn<TargetCustomer>.Create("CustomerId", customer => customer.Id)
        ];
        var sut = new SqlBulkWriteSessionFactory<TargetCustomer>(
            resourceFactory,
            destinationTableName: "dbo.Customers",
            columns,
            SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction,
            bulkCopyTimeoutSeconds: 120);

        await using (var session = await sut.OpenAsync())
        {
            Assert.IsType<SqlBulkWriteSession<TargetCustomer>>(session);
        }

        Assert.Equal(1, resourceFactory.OpenCount);
        Assert.Equal("dbo.Customers", resourceFactory.DestinationTableName);
        Assert.Equal(120, resourceFactory.BulkCopyTimeoutSeconds);
        Assert.Equal(
            SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.UseInternalTransaction,
            resourceFactory.Options);
        Assert.True(adapter.IsDisposed);
    }

    /// <summary>
    /// 驗證目的資料表名稱未提供有效內容時立即拒絕建立工廠，避免延遲到資料寫入時才失敗。
    /// </summary>
    /// <param name="destinationTableName">要驗證的無效目的資料表名稱。</param>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Constructor_目的資料表名稱無效時_拋出參數例外(
        string? destinationTableName)
    {
        var adapter = new TrackingSqlBulkCopyAdapter();
        var resourceFactory = new TrackingSqlBulkCopyResourceFactory(adapter);
        BulkColumn<TargetCustomer>[] columns =
        [
            BulkColumn<TargetCustomer>.Create("CustomerId", customer => customer.Id)
        ];

        var exception = Assert.ThrowsAny<ArgumentException>(
            () => new SqlBulkWriteSessionFactory<TargetCustomer>(
                resourceFactory,
                destinationTableName!,
                columns,
                SqlBulkCopyOptions.Default,
                bulkCopyTimeoutSeconds: 30));

        Assert.Equal("destinationTableName", exception.ParamName);
    }

    /// <summary>
    /// 驗證欄位 mapping 集合為空值時立即拒絕建立工廠，避免開始連線後才發現無法寫入欄位。
    /// </summary>
    [Fact]
    public void Constructor_欄位Mapping集合為空值時_拋出參數空值例外()
    {
        var adapter = new TrackingSqlBulkCopyAdapter();
        var resourceFactory = new TrackingSqlBulkCopyResourceFactory(adapter);

        var exception = Assert.Throws<ArgumentNullException>(
            () => new SqlBulkWriteSessionFactory<TargetCustomer>(
                resourceFactory,
                destinationTableName: "dbo.Customers",
                columns: null!,
                SqlBulkCopyOptions.Default,
                bulkCopyTimeoutSeconds: 30));

        Assert.Equal("columns", exception.ParamName);
    }

    /// <summary>
    /// 驗證欄位 mapping 集合沒有任何欄位時立即拒絕建立工廠，避免執行無法寫入資料的 SqlBulkCopy。
    /// </summary>
    [Fact]
    public void Constructor_欄位Mapping集合為空集合時_拋出參數例外()
    {
        var adapter = new TrackingSqlBulkCopyAdapter();
        var resourceFactory = new TrackingSqlBulkCopyResourceFactory(adapter);

        var exception = Assert.Throws<ArgumentException>(
            () => new SqlBulkWriteSessionFactory<TargetCustomer>(
                resourceFactory,
                destinationTableName: "dbo.Customers",
                columns: [],
                SqlBulkCopyOptions.Default,
                bulkCopyTimeoutSeconds: 30));

        Assert.Equal("columns", exception.ParamName);
    }

    /// <summary>
    /// 驗證 BulkCopy 逾時秒數為負數時立即拒絕建立工廠；零仍保留為不限制逾時的有效設定。
    /// </summary>
    /// <param name="bulkCopyTimeoutSeconds">要驗證的無效負數逾時秒數。</param>
    [Theory]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    public void Constructor_BulkCopy逾時秒數為負數時_拋出參數超出範圍例外(
        int bulkCopyTimeoutSeconds)
    {
        var adapter = new TrackingSqlBulkCopyAdapter();
        var resourceFactory = new TrackingSqlBulkCopyResourceFactory(adapter);
        BulkColumn<TargetCustomer>[] columns =
        [
            BulkColumn<TargetCustomer>.Create("CustomerId", customer => customer.Id)
        ];

        var exception = Assert.Throws<ArgumentOutOfRangeException>(
            () => new SqlBulkWriteSessionFactory<TargetCustomer>(
                resourceFactory,
                destinationTableName: "dbo.Customers",
                columns,
                SqlBulkCopyOptions.Default,
                bulkCopyTimeoutSeconds));

        Assert.Equal("bulkCopyTimeoutSeconds", exception.ParamName);
    }

    /// <summary>
    /// 驗證應用端未提供 SQL Connection 工廠時立即拒絕建立 session 工廠。
    /// </summary>
    [Fact]
    public void Constructor_Connection工廠為空值時_拋出參數空值例外()
    {
        BulkColumn<TargetCustomer>[] columns =
        [
            BulkColumn<TargetCustomer>.Create("CustomerId", customer => customer.Id)
        ];

        var exception = Assert.Throws<ArgumentNullException>(
            () => new SqlBulkWriteSessionFactory<TargetCustomer>(
                connectionFactory: null!,
                destinationTableName: "dbo.Customers",
                columns));

        Assert.Equal("connectionFactory", exception.ParamName);
    }

    /// <summary>
    /// 驗證多個欄位 mapping 指向大小寫不同但相同的目的欄位時，立即拒絕建立 session 工廠。
    /// </summary>
    [Fact]
    public void Constructor_目的欄位Mapping重複時_拋出參數例外()
    {
        var adapter = new TrackingSqlBulkCopyAdapter();
        var resourceFactory = new TrackingSqlBulkCopyResourceFactory(adapter);
        BulkColumn<TargetCustomer>[] columns =
        [
            BulkColumn<TargetCustomer>.Create("CustomerId", customer => customer.Id),
            BulkColumn<TargetCustomer>.Create("customerid", customer => customer.Id)
        ];

        var exception = Assert.Throws<ArgumentException>(
            () => new SqlBulkWriteSessionFactory<TargetCustomer>(
                resourceFactory,
                destinationTableName: "dbo.Customers",
                columns,
                SqlBulkCopyOptions.Default,
                bulkCopyTimeoutSeconds: 30));

        Assert.Equal("columns", exception.ParamName);
    }

    /// <summary>
    /// 驗證 session 工廠會保存建構當下的欄位 mapping 快照，
    /// 不受應用端之後修改原始集合影響。
    /// </summary>
    [Fact]
    public async Task OpenAsync_原始欄位集合在建構後被修改時_仍使用建構當下的Mapping()
    {
        var adapter = new TrackingSqlBulkCopyAdapter();
        var resourceFactory = new TrackingSqlBulkCopyResourceFactory(adapter);
        var columns = new List<BulkColumn<TargetCustomer>>
        {
            BulkColumn<TargetCustomer>.Create("CustomerId", customer => customer.Id)
        };
        var sut = new SqlBulkWriteSessionFactory<TargetCustomer>(
            resourceFactory,
            destinationTableName: "dbo.Customers",
            columns,
            SqlBulkCopyOptions.Default,
            bulkCopyTimeoutSeconds: 30);

        columns.Clear();
        columns.Add(BulkColumn<TargetCustomer>.Create(
            "ChangedColumn",
            customer => customer.Id));

        await using (var session = await sut.OpenAsync())
        {
        }

        Assert.Equal([(0, "CustomerId")], adapter.ColumnMappings);
    }

    private sealed class TrackingSqlBulkCopyResourceFactory(
        ISqlBulkCopyAdapter adapter) : ISqlBulkCopyResourceFactory
    {
        public int OpenCount { get; private set; }
        public string? DestinationTableName { get; private set; }
        public int BulkCopyTimeoutSeconds { get; private set; }
        public SqlBulkCopyOptions Options { get; private set; }

        public ValueTask<ISqlBulkCopyAdapter> OpenAsync(
            string destinationTableName,
            int bulkCopyTimeoutSeconds,
            SqlBulkCopyOptions options,
            CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            OpenCount++;
            DestinationTableName = destinationTableName;
            BulkCopyTimeoutSeconds = bulkCopyTimeoutSeconds;
            Options = options;
            return ValueTask.FromResult(adapter);
        }
    }

    private sealed class TrackingSqlBulkCopyAdapter : ISqlBulkCopyAdapter
    {
        public List<(int SourceOrdinal, string DestinationName)> ColumnMappings { get; } = [];
        public bool IsDisposed { get; private set; }

        public void AddColumnMapping(int sourceOrdinal, string destinationName) =>
            ColumnMappings.Add((sourceOrdinal, destinationName));

        public ValueTask WriteToServerAsync(
            DbDataReader dataReader,
            CancellationToken cancellationToken = default) =>
            ValueTask.CompletedTask;

        public ValueTask DisposeAsync()
        {
            IsDisposed = true;
            return ValueTask.CompletedTask;
        }
    }

    private sealed record TargetCustomer(int Id);
}
