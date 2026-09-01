using System.Collections.Concurrent;

namespace FastBulkProcessor;

/// <summary>
/// 追蹤仍在執行的 Prepare task，並在 task 完成後觀察例外及移除參考。
/// </summary>
internal sealed class ActivePrepareTaskRegistry
{
    private readonly ConcurrentDictionary<long, Task> _activeTasks = new();
    private long _nextTaskId;

    /// <summary>
    /// 取得目前仍未完成的 Prepare task 數量。
    /// </summary>
    internal int Count => _activeTasks.Count;

    /// <summary>
    /// 將 Prepare task 加入活動集合，並在完成、失敗或取消後自動移除。
    /// </summary>
    /// <param name="task">需要追蹤生命週期的 Prepare task。</param>
    internal void Track(Task task)
    {
        ArgumentNullException.ThrowIfNull(task);

        var taskId = Interlocked.Increment(ref _nextTaskId);

        if (!_activeTasks.TryAdd(taskId, task))
        {
            throw new InvalidOperationException("無法登記 Prepare task。");
        }

        _ = task.ContinueWith(
            static (completedTask, state) =>
            {
                var registration =
                    ((ActivePrepareTaskRegistry Registry, long TaskId))state!;

                if (completedTask.IsFaulted)
                {
                    _ = completedTask.Exception;
                }

                registration.Registry._activeTasks.TryRemove(
                    registration.TaskId,
                    out _);
            },
            (Registry: this, TaskId: taskId),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>
    /// 取得呼叫當下仍在執行的 Prepare task 快照，供 Pipeline 結束前等待。
    /// </summary>
    internal Task[] Snapshot() => _activeTasks.Values.ToArray();
}
