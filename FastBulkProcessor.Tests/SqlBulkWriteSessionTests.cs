using System.Data.Common;

namespace FastBulkProcessor.Tests;

public class SqlBulkWriteSessionTests
{
    /// <summary>
    /// 驗證 SQL Bulk Write session 會依欄位順序註冊目的欄位 mapping，
    /// 並將完整批次透過 DbDataReader 交給 SqlBulkCopy adapter 寫入。
    /// </summary>
    [Fact]
    public async Task WriteBatchAsync_收到目標資料批次時_套用欄位Mapping並交給SqlBulkCopy()
    {
        TargetCustomer[] batch =
        [
            new(Id: 1, Name: "Gary"),
            new(Id: 2, Name: null)
        ];
        BulkColumn<TargetCustomer>[] columns =
        [
            BulkColumn<TargetCustomer>.Create("CustomerId", customer => customer.Id),
            BulkColumn<TargetCustomer>.Create("CustomerName", customer => customer.Name)
        ];
        var adapter = new TrackingSqlBulkCopyAdapter();

        await using (var sut = new SqlBulkWriteSession<TargetCustomer>(adapter, columns))
        {
            await sut.WriteBatchAsync(batch);
        }

        Assert.Equal(
            [(0, "CustomerId"), (1, "CustomerName")],
            adapter.ColumnMappings);
        Assert.Collection(
            adapter.WrittenRows,
            row => Assert.Equal([1, "Gary"], row),
            row => Assert.Equal([2, DBNull.Value], row));
        Assert.Equal(1, adapter.WriteCount);
        Assert.True(adapter.IsDisposed);
    }

    private sealed class TrackingSqlBulkCopyAdapter : ISqlBulkCopyAdapter
    {
        public List<(int SourceOrdinal, string DestinationName)> ColumnMappings { get; } = [];
        public List<object[]> WrittenRows { get; } = [];
        public int WriteCount { get; private set; }
        public bool IsDisposed { get; private set; }

        public void AddColumnMapping(int sourceOrdinal, string destinationName) =>
            ColumnMappings.Add((sourceOrdinal, destinationName));

        public async ValueTask WriteToServerAsync(
            DbDataReader dataReader,
            CancellationToken cancellationToken = default)
        {
            WriteCount++;

            while (await dataReader.ReadAsync(cancellationToken))
            {
                var values = new object[dataReader.FieldCount];
                dataReader.GetValues(values);
                WrittenRows.Add(values);
            }
        }

        public ValueTask DisposeAsync()
        {
            IsDisposed = true;
            return ValueTask.CompletedTask;
        }
    }

    private sealed record TargetCustomer(int Id, string? Name);
}
