using System.Runtime.CompilerServices;

namespace FastBulkProcessor;

/// <summary>
/// 定義以非同步串流逐筆讀取資料的介面，避免呼叫端必須先將所有資料載入記憶體。
/// </summary>
/// <typeparam name="T">每筆資料的型別。</typeparam>
public interface IStreamingReader<out T>
{
    /// <summary>
    /// 逐筆讀取資料，讓呼叫端可在來源尚未全部讀取完成前開始處理。
    /// </summary>
    /// <param name="cancellationToken">用來取消串流讀取作業的權杖。</param>
    /// <returns>可逐筆列舉的非同步資料串流。</returns>
    IAsyncEnumerable<T> ReadAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// 將非同步資料來源包裝成逐筆讀取器，讀取時不會預先將所有資料載入記憶體。
/// </summary>
/// <typeparam name="T">每筆資料的型別。</typeparam>
public sealed class StreamingReader<T> : IStreamingReader<T>
{
    private readonly Func<CancellationToken, IAsyncEnumerable<T>> _sourceFactory;

    /// <summary>
    /// 建立非同步串流讀取器。
    /// </summary>
    /// <param name="sourceFactory">依取消權杖建立非同步資料來源的委派。</param>
    /// <exception cref="ArgumentNullException">當資料來源委派為空值時擲出。</exception>
    public StreamingReader(Func<CancellationToken, IAsyncEnumerable<T>> sourceFactory)
    {
        ArgumentNullException.ThrowIfNull(sourceFactory);
        _sourceFactory = sourceFactory;
    }

    /// <summary>
    /// 逐筆讀取資料，讓呼叫端在取得第一筆後即可立即開始處理。
    /// </summary>
    /// <param name="cancellationToken">用來取消串流讀取作業的權杖。</param>
    /// <returns>可逐筆列舉的非同步資料串流。</returns>
    public async IAsyncEnumerable<T> ReadAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var source = _sourceFactory(cancellationToken);

        await foreach (var item in source
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            yield return item;
        }
    }
}
