namespace FastBulkProcessor.Tests;

public class BatchDataReaderTests
{
    /// <summary>
    /// 驗證批次資料會依欄位 mapping 順序逐列輸出，並將目標資料中的空值轉換成資料庫空值。
    /// </summary>
    [Fact]
    public async Task ReadAsync_讀取批次資料時_依欄位順序輸出並將空值轉為DBNull()
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
        using var sut = new BatchDataReader<TargetCustomer>(batch, columns);

        Assert.Equal(2, sut.FieldCount);
        Assert.Equal("CustomerId", sut.GetName(0));
        Assert.Equal("CustomerName", sut.GetName(1));
        Assert.Equal(typeof(int), sut.GetFieldType(0));
        Assert.Equal(typeof(string), sut.GetFieldType(1));

        Assert.True(await sut.ReadAsync());
        Assert.Equal(1, sut.GetValue(0));
        Assert.Equal("Gary", sut.GetValue(1));

        Assert.True(await sut.ReadAsync());
        Assert.Equal(2, sut.GetValue(0));
        Assert.Same(DBNull.Value, sut.GetValue(1));
        Assert.True(sut.IsDBNull(1));

        Assert.False(await sut.ReadAsync());
    }

    /// <summary>
    /// 驗證 nullable value type 欄位會以基礎 CLR 型別描述 schema，
    /// 並在該筆資料沒有值時輸出 DBNull。
    /// </summary>
    [Fact]
    public async Task ReadAsync_Nullable值型別沒有值時_Schema使用基礎型別且資料輸出DBNull()
    {
        NullableTarget[] batch = [new(OrgId: null)];
        BulkColumn<NullableTarget>[] columns =
        [
            BulkColumn<NullableTarget>.Create("OrgId", target => target.OrgId)
        ];
        using var sut = new BatchDataReader<NullableTarget>(batch, columns);

        Assert.Equal(typeof(int), sut.GetFieldType(0));
        Assert.True(await sut.ReadAsync());
        Assert.Same(DBNull.Value, sut.GetValue(0));
        Assert.True(sut.IsDBNull(0));
    }

    private sealed record TargetCustomer(int Id, string? Name);

    private sealed record NullableTarget(int? OrgId);
}
