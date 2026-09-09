# FastBulkProcessor

以 ADO.NET 串流讀取、批次處理與 `SqlBulkCopy` 為核心的 .NET 資料搬移 Library。

它協助應用端在不一次載入全部資料的前提下，執行大量資料的 Mapping、規則處理與批次寫入；BusinessRule、Dictionary、Rejection 與 Checkpoint 仍由應用端負責。

## 特色

- `DbDataReader` 串流讀取，第一筆資料可立即處理。
- 可由應用端設定批次大小。
- 使用 `SqlBulkCopy` 寫入 SQL Server。
- 支援應用端外部 `SqlConnection` 與 `SqlTransaction`。
- `ConcurrentBatchPipeline` 可讓 Prepare/Mapping 有限度併發，Commit 保持來源順序。
- Bounded Buffer 與 Backpressure，避免無限制累積記憶體。
- 整合測試使用環境變數，不將連線字串提交到版控。

## 環境需求

- .NET 9 或 .NET 10。
- SQL Server。
- `Microsoft.Data.SqlClient` 7.0.2（由 Library 相依套件提供）。

## 引用 Library

目前適合使用 `ProjectReference`。假設應用端與本專案位於相鄰資料夾：

```powershell
dotnet add reference ..\FastBulkProcessor\FastBulkProcessor\FastBulkProcessor.csproj
```

或在應用端 `.csproj` 加入：

```xml
<ItemGroup>
  <ProjectReference Include="..\FastBulkProcessor\FastBulkProcessor\FastBulkProcessor.csproj" />
</ItemGroup>
```

應用端 Target Framework 必須為 `net9.0`、`net10.0`，或可相容的更高版本。

## 基本 Bulk 寫入

應用端先完成 Mapping 與 BusinessRule，只輸出可寫入的 `TTarget`。以下範例以 `CustomerTarget` 示意：

```csharp
using Microsoft.Data.SqlClient;
using FastBulkProcessor;

var columns = new[]
{
    BulkColumn<CustomerTarget>.Create(
        "CustomerId",
        target => target.CustomerId),
    BulkColumn<CustomerTarget>.Create(
        "CustomerName",
        target => target.CustomerName),
    BulkColumn<CustomerTarget>.Create(
        "OrgId",
        target => target.OrgId)
};

var acceptedTargets = GetAcceptedTargetsAsync(cancellationToken);
var batches = new BatchingReader<CustomerTarget>(
    acceptedTargets,
    batchSize: 2_000);
var sessionFactory = new SqlBulkWriteSessionFactory<CustomerTarget>(
    () => new SqlConnection(targetConnectionString),
    "[dbo].[Customer]",
    columns);
var writer = new BulkWriter<CustomerTarget>(sessionFactory);

var result = await writer.WriteAsync(
    batches.ReadAsync(cancellationToken),
    cancellationToken: cancellationToken);

Console.WriteLine(
    $"已寫入 {result.WrittenRowCount} 筆，{result.WrittenBatchCount} 批。");
```

`GetAcceptedTargetsAsync` 由應用端實作；不通過規則的資料應由應用端自行記錄 Rejection。

`BulkWriter.WriteAsync` 每寫入約 100,000 筆資料，以及作業結束時的剩餘資料，都會輸出一行進度到 `Console.Out`，內容包含該區間的完成筆數、耗時與每秒筆數。若應用程式需要集中式記錄、告警或更細緻的效能監控，仍應自行處理。

## 外部 Transaction

若單一來源批次需要保證 `Bulk insert + Rejection + Checkpoint` 原子提交，應用端建立並持有 Transaction：

```csharp
await using var connection = new SqlConnection(targetConnectionString);
await connection.OpenAsync(cancellationToken);
await using var transaction =
    (SqlTransaction)await connection.BeginTransactionAsync(cancellationToken);

var sessionFactory = new SqlBulkWriteSessionFactory<CustomerTarget>(
    connection,
    transaction,
    "[dbo].[Customer]",
    columns);
var writer = new BulkWriter<CustomerTarget>(sessionFactory);

await writer.WriteAsync(singleBatch, cancellationToken);

// 應用端：寫入 rejection、更新 checkpoint。
await SaveRejectionAndCheckpointAsync(transaction, cancellationToken);

await transaction.CommitAsync(cancellationToken);
```

外部 Transaction 模式下，Library 不會 Commit、Rollback、關閉或 Dispose 應用端提供的 Connection/Transaction。此模式不可同時使用 `SqlBulkCopyOptions.UseInternalTransaction`。

## 並行 Prepare Pipeline

以 `BulkWorkflow<TSource, TPrepared>` 定義來源、Prepare 與 Commit，再以 `BulkProcessor.ExecuteAsync` 執行，是並行 Prepare Pipeline 的建議入口。它會處理來源資料預讀、有限度並行 Prepare，以及循序 Commit。

```csharp
var workflow = new BulkWorkflow<SourceCustomer, PreparedBatch>(
    source: sourceReader.ReadAsync(cancellationToken),

    prepareAsync: async (sourceBatch, token) =>
    {
        // 應用端：Dictionary 補查、Mapping、BusinessRule。
        return await rule.PrepareAsync(sourceBatch, token);
    },

    commitAsync: async (preparedBatch, token) =>
    {
        // 應用端：每批建立 Transaction，完成 Bulk、Rejection、Checkpoint。
        await initializationWriter.CommitAsync(preparedBatch, token);
    });

var options = new BulkExecutionOptions(
    batchSize: 2_000,
    preparationConcurrency: 2,
    sourceBufferCapacity: 2,
    preparedBufferCapacity: 2);

await BulkProcessor.ExecuteAsync(
    workflow,
    options,
    cancellationToken: cancellationToken);
```

Prepare 可以並行，但 Commit 固定依來源批次順序執行。任一 Prepare 失敗時，Pipeline 會取消所有尚未提交的批次，等待背景 Prepare 結束後，再傳回原始例外。

若需要直接控制底層 Pipeline，可使用 `ConcurrentBatchPipeline<TSource, TPrepared>` 與 `ConcurrentBatchPipelineOptions`；一般應用程式不需要直接建立它們。

`prepareAsync` 若需要資料庫或 EF Core，請確保每個並行工作使用獨立且 thread-safe 的 Connection/DbContext；不可多個 Worker 共用同一個 `DbContext`。

## ADO.NET 串流讀取

`AdoStreamingReader<T>` 以 `DbDataReader` 和 `CommandBehavior.SequentialAccess` 執行來源資料串流：

```csharp
var sourceReader = new AdoStreamingReader<SourceCustomer>(
    () => new SqlConnection(sourceConnectionString),
    connection =>
    {
        var command = connection.CreateCommand();
        command.CommandText = """
            SELECT CustomerId, CustomerName, OrgCode
            FROM dbo.SourceCustomer
            ORDER BY CustomerId;
            """;
        return command;
    },
    reader => new SourceCustomer(
        reader.GetInt32(0),
        reader.GetString(1),
        reader.GetString(2)));
```

來源大量資料建議使用穩定排序與 keyset checkpoint，不使用 `OFFSET`。

## 整合測試環境變數

SQL Server 整合測試只讀取下列環境變數：

```text
FAST_BULK_PROCESSOR_SQL_CONNECTION_STRING
```

請將它設為具備測試資料庫權限的連線字串。不要把真實連線字串寫入 README、原始碼、`appsettings.json` 或 Git。

Windows PowerShell 設定範例：

```powershell
[Environment]::SetEnvironmentVariable(
    'FAST_BULK_PROCESSOR_SQL_CONNECTION_STRING',
    '<你的測試資料庫連線字串>',
    'User')
```

整合測試會建立並刪除 GUID 命名的暫時測試表，因此使用的帳號至少需具備建立資料表、Bulk 寫入、查詢及刪除資料表的測試資料庫權限。

## 執行測試

執行全部測試：

```powershell
dotnet test FastBulkProcessor.slnx
```

只執行 SQL Server 整合測試：

```powershell
dotnet test FastBulkProcessor.slnx --filter "Category=Integration"
```

未設定整合測試環境變數時，整合測試會略過；單元測試不受影響。

## 責任邊界

| Library 負責 | 應用端負責 |
| --- | --- |
| ADO 串流、批次、Backpressure | Dictionary、Mapping、BusinessRule |
| BulkCopy 與外部 Transaction 配合 | Rejection、Checkpoint、High-water mark |
| Prepare 併發與循序 Commit | 初始化鎖、重跑與冪等策略 |
| Cancellation、例外傳遞、資源釋放與基本寫入進度輸出 | 實際交易內容、集中式監控與告警 |
