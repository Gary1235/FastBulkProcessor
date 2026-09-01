namespace FastBulkProcessor.Tests;

/// <summary>
/// 驗證 Prepare task registry 只保留仍在執行的工作，不累積已完成批次及其結果。
/// </summary>
public sealed class ActivePrepareTaskRegistryTests
{
    /// <summary>
    /// 驗證未完成的 Prepare task 會被追蹤，完成後立即從活動集合移除。
    /// </summary>
    [Fact]
    public void Track_PrepareTask完成_從活動集合移除()
    {
        var pendingTask = new TaskCompletionSource<int>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var sut = new ActivePrepareTaskRegistry();

        sut.Track(pendingTask.Task);

        Assert.Equal(1, sut.Count);
        Assert.Contains(pendingTask.Task, sut.Snapshot());

        pendingTask.SetResult(42);

        Assert.True(SpinWait.SpinUntil(
            () => sut.Count == 0,
            TimeSpan.FromSeconds(1)));
        Assert.Equal(0, sut.Count);
        Assert.Empty(sut.Snapshot());
    }

    /// <summary>
    /// 驗證 Prepare task 發生例外後仍會完成例外觀察並從活動集合移除。
    /// </summary>
    [Fact]
    public void Track_PrepareTask失敗_觀察例外並從活動集合移除()
    {
        var expectedException = new InvalidOperationException("Mapping 失敗。");
        var sut = new ActivePrepareTaskRegistry();

        sut.Track(Task.FromException(expectedException));

        Assert.True(SpinWait.SpinUntil(
            () => sut.Count == 0,
            TimeSpan.FromSeconds(1)));
        Assert.Equal(0, sut.Count);
        Assert.Empty(sut.Snapshot());
    }
}
