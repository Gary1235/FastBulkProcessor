using System.Runtime.CompilerServices;

namespace FastBulkProcessor.Tests;

public class StreamingReaderTests
{
    /// <summary>
    /// 驗證大量資料尚未全部讀取完成前，呼叫端就能先取得第一筆資料，
    /// 且取得第一筆時不會預先載入後續資料。
    /// </summary>
    [Fact]
    public async Task ReadAsync_取得第一筆時_來源只讀取一筆()
    {
        var source = new CountingAsyncSource(totalCount: 500_000);
        var sut = new StreamingReader<int>(source.ReadAsync);

        await using var enumerator = sut.ReadAsync().GetAsyncEnumerator();

        var hasItem = await enumerator.MoveNextAsync();

        Assert.True(hasItem);
        Assert.Equal(1, enumerator.Current);
        Assert.Equal(1, source.ReadCount);
    }

    private sealed class CountingAsyncSource(int totalCount)
    {
        public int ReadCount { get; private set; }

        public async IAsyncEnumerable<int> ReadAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            for (var value = 1; value <= totalCount; value++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ReadCount++;
                yield return value;
                await Task.Yield();
            }
        }
    }
}
