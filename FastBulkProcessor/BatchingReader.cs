using System.Runtime.CompilerServices;

namespace FastBulkProcessor;

/// <summary>
/// 將應用端提供的目標資料串流依指定大小分批輸出，避免一次將所有資料載入記憶體。
/// </summary>
/// <typeparam name="T">每筆目標資料的型別。</typeparam>
public sealed class BatchingReader<T> : IStreamingReader<IReadOnlyList<T>>
{
    private readonly IAsyncEnumerable<T> _source;
    private readonly int _batchSize;

    /// <summary>
    /// 建立目標資料批次讀取器。
    /// </summary>
    /// <param name="source">由應用端提供且已通過業務規則的目標資料串流。</param>
    /// <param name="batchSize">每個完整批次包含的資料筆數。</param>
    /// <exception cref="ArgumentNullException">當目標資料串流為空值時擲出。</exception>
    /// <exception cref="ArgumentOutOfRangeException">當批次大小小於一時擲出。</exception>
    public BatchingReader(IAsyncEnumerable<T> source, int batchSize)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);

        _source = source;
        _batchSize = batchSize;
    }

    /// <summary>
    /// 逐筆讀取目標資料，並在資料累積到指定批次大小時立即輸出完整批次。
    /// </summary>
    /// <param name="cancellationToken">用來取消來源讀取及批次輸出的權杖。</param>
    /// <returns>可逐批列舉的非同步資料串流。</returns>
    public async IAsyncEnumerable<IReadOnlyList<T>> ReadAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var buffer = new List<T>(_batchSize);

        await foreach (var item in _source
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            buffer.Add(item);

            if (buffer.Count == _batchSize)
            {
                yield return buffer.ToArray();
                buffer.Clear();
            }
        }

        if (buffer.Count > 0)
        {
            yield return buffer.ToArray();
        }
    }
}
