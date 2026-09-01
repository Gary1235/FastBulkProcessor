namespace FastBulkProcessor.Tests;

public class BulkColumnTests
{
    /// <summary>
    /// 驗證應用端可使用強型別取值函式，定義目標資料屬性與資料庫目的欄位的對應關係。
    /// </summary>
    [Fact]
    public void Create_提供目的欄位與強型別取值函式時_建立可讀取目標值的欄位對應()
    {
        var column = BulkColumn<TargetCustomer>.Create(
            "CustomerId",
            customer => customer.Id);
        var target = new TargetCustomer(Id: 42, Name: "Gary");

        var value = column.GetValue(target);

        Assert.Equal("CustomerId", column.DestinationName);
        Assert.Equal(typeof(int), column.DataType);
        Assert.Equal(42, value);
    }

    /// <summary>
    /// 驗證目的欄位名稱未提供有效內容時立即拒絕建立欄位 mapping。
    /// </summary>
    /// <param name="destinationName">要驗證的無效目的欄位名稱。</param>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Create_目的欄位名稱無效時_拋出參數例外(string? destinationName)
    {
        var exception = Assert.ThrowsAny<ArgumentException>(
            () => BulkColumn<TargetCustomer>.Create(
                destinationName!,
                customer => customer.Id));

        Assert.Equal("destinationName", exception.ParamName);
    }

    /// <summary>
    /// 驗證強型別取值函式為空值時立即拒絕建立欄位 mapping。
    /// </summary>
    [Fact]
    public void Create_取值函式為空值時_拋出參數空值例外()
    {
        var exception = Assert.Throws<ArgumentNullException>(
            () => BulkColumn<TargetCustomer>.Create<int>(
                "CustomerId",
                valueSelector: null!));

        Assert.Equal("valueSelector", exception.ParamName);
    }

    private sealed record TargetCustomer(int Id, string Name);
}
