using System.Runtime.CompilerServices;

namespace FastBulkProcessor.Tests;

public class BatchingReaderTests
{
    /// <summary>
    /// 驗證來源資料累積到指定批次大小時，立即輸出第一批資料，
    /// 且不會為了產生第一批而預先讀取下一筆資料。
    /// </summary>
    [Fact]
    public async Task ReadAsync_累積到批次大小時_立即輸出第一批且不預讀下一筆()
    {
        var source = new CountingSource(totalCount: 500_000);
        var sut = new BatchingReader<int>(
            source.ReadAsync(),
            batchSize: 3);

        await using var enumerator = sut.ReadAsync().GetAsyncEnumerator();

        var hasBatch = await enumerator.MoveNextAsync();

        Assert.True(hasBatch);
        Assert.Equal([1, 2, 3], enumerator.Current);
        Assert.Equal(3, source.ReadCount);
    }

    /// <summary>
    /// 驗證來源資料讀取完畢時，最後不足指定批次大小的剩餘資料仍會輸出為尾批。
    /// </summary>
    [Fact]
    public async Task ReadAsync_來源讀取完畢時_輸出最後不足批次大小的資料()
    {
        var source = new CountingSource(totalCount: 5);
        var sut = new BatchingReader<int>(
            source.ReadAsync(),
            batchSize: 3);
        var batches = new List<IReadOnlyList<int>>();

        await foreach (var batch in sut.ReadAsync())
        {
            batches.Add(batch);
        }

        Assert.Collection(
            batches,
            batch => Assert.Equal([1, 2, 3], batch),
            batch => Assert.Equal([4, 5], batch));
        Assert.Equal(5, source.ReadCount);
    }

    /// <summary>
    /// 驗證批次大小小於一時立即拒絕建立讀取器，避免資料無法正常分批。
    /// </summary>
    /// <param name="batchSize">要驗證的無效批次大小。</param>
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_批次大小小於一時_拋出參數超出範圍例外(int batchSize)
    {
        var source = new CountingSource(totalCount: 1);

        var exception = Assert.Throws<ArgumentOutOfRangeException>(
            () => new BatchingReader<int>(source.ReadAsync(), batchSize));

        Assert.Equal("batchSize", exception.ParamName);
    }

    /// <summary>
    /// 驗證目標資料串流為空值時立即拒絕建立讀取器，避免錯誤延遲到開始列舉時才發生。
    /// </summary>
    [Fact]
    public void Constructor_目標資料串流為空值時_拋出參數空值例外()
    {
        var exception = Assert.Throws<ArgumentNullException>(
            () => new BatchingReader<int>(source: null!, batchSize: 3));

        Assert.Equal("source", exception.ParamName);
    }

    /// <summary>
    /// 驗證資料筆數剛好是批次大小的倍數時，只輸出完整批次，不會額外輸出空的尾批。
    /// </summary>
    [Fact]
    public async Task ReadAsync_資料筆數為批次大小的倍數時_不輸出空尾批()
    {
        var source = new CountingSource(totalCount: 6);
        var sut = new BatchingReader<int>(
            source.ReadAsync(),
            batchSize: 3);
        var batches = new List<IReadOnlyList<int>>();

        await foreach (var batch in sut.ReadAsync())
        {
            batches.Add(batch);
        }

        Assert.Collection(
            batches,
            batch => Assert.Equal([1, 2, 3], batch),
            batch => Assert.Equal([4, 5, 6], batch));
        Assert.DoesNotContain(batches, batch => batch.Count == 0);
        Assert.Equal(6, source.ReadCount);
    }

    private sealed class CountingSource(int totalCount)
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
