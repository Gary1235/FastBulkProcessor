namespace FastBulkProcessor;

/// <summary>
/// 定義目標資料的取值方式，以及該值對應的資料庫目的欄位。
/// </summary>
/// <typeparam name="T">每筆目標資料的型別。</typeparam>
public sealed class BulkColumn<T>
{
    private readonly Func<T, object?> _valueSelector;

    private BulkColumn(
        string destinationName,
        Type dataType,
        Func<T, object?> valueSelector)
    {
        DestinationName = destinationName;
        DataType = dataType;
        _valueSelector = valueSelector;
    }

    /// <summary>
    /// 取得 SqlBulkCopy 寫入時使用的目的欄位名稱。
    /// </summary>
    public string DestinationName { get; }

    /// <summary>
    /// 取得欄位取值函式回傳的基礎資料型別；nullable value type 會回傳其底層型別。
    /// </summary>
    public Type DataType { get; }

    /// <summary>
    /// 使用強型別取值函式建立目標資料與資料庫目的欄位的對應關係。
    /// </summary>
    /// <typeparam name="TValue">目標欄位值的型別。</typeparam>
    /// <param name="destinationName">SqlBulkCopy 寫入時使用的目的欄位名稱。</param>
    /// <param name="valueSelector">從目標資料取得欄位值的函式。</param>
    /// <returns>可供 Bulk Writer 使用的欄位對應。</returns>
    /// <exception cref="ArgumentException">當目的欄位名稱未提供有效內容時擲出。</exception>
    /// <exception cref="ArgumentNullException">當取值函式為空值時擲出。</exception>
    public static BulkColumn<T> Create<TValue>(
        string destinationName,
        Func<T, TValue> valueSelector)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationName);
        ArgumentNullException.ThrowIfNull(valueSelector);

        return new BulkColumn<T>(
            destinationName,
            Nullable.GetUnderlyingType(typeof(TValue)) ?? typeof(TValue),
            target => valueSelector(target));
    }

    /// <summary>
    /// 從指定目標資料取得目前欄位要寫入的值。
    /// </summary>
    /// <param name="target">要取得欄位值的目標資料。</param>
    /// <returns>要傳入 Bulk Writer 的欄位值；資料庫空值可為空值。</returns>
    public object? GetValue(T target) => _valueSelector(target);
}
