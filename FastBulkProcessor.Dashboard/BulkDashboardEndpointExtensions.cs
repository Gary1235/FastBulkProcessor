using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace FastBulkProcessor.Dashboard;

public static class BulkDashboardEndpointExtensions
{
    public static IServiceCollection AddFastBulkProcessorDashboard(
        this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<BulkDashboardJobRegistry>();
        services.TryAddSingleton<IBulkDashboardJobs>(provider =>
            provider.GetRequiredService<BulkDashboardJobRegistry>());
        services.TryAddSingleton(provider => new BulkDashboardRunner(
            provider.GetRequiredService<BulkDashboardJobRegistry>()));
        return services;
    }

    /// <summary>
    /// Maps a read-only dashboard. The host must configure ASP.NET Core authentication
    /// and authorization; every dashboard endpoint requires an authenticated user.
    /// </summary>
    public static RouteGroupBuilder MapFastBulkProcessorDashboard(
        this IEndpointRouteBuilder endpoints,
        string pattern = "/bulk-dashboard")
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(pattern);
        var route = pattern.TrimEnd('/');
        if (route.Length == 0 || route[0] != '/')
        {
            throw new ArgumentException(
                "Dashboard route must be an absolute non-root path.",
                nameof(pattern));
        }

        var group = endpoints.MapGroup(route).RequireAuthorization();
        group.MapGet("", () => Results.Content(Page, "text/html; charset=utf-8"));
        group.MapGet("/jobs", (IBulkDashboardJobs jobs) =>
            Results.Json(jobs.GetJobs()));
        return group;
    }

    private const string Page = """
        <!doctype html>
        <html lang="zh-Hant">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>FastBulkProcessor Dashboard</title>
          <style>
            :root { font-family: system-ui, sans-serif; color: #1f2937; background: #f4f6f8; }
            * { box-sizing: border-box; }
            body { margin: 0; padding: clamp(16px, 2.5vw, 40px); }
            h1 { margin: 0 0 8px; font-size: clamp(1.35rem, 2.2vw, 1.9rem); overflow-wrap: anywhere; }
            p { margin: 0; color: #6b7280; line-height: 1.7; }
            .summary { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 16px; margin: 24px 0; }
            .summary div { background: white; border: 1px solid #e5e7eb; border-radius: 12px; padding: 18px 20px; box-shadow: 0 2px 6px #1f293705; }
            .summary strong { display: block; font-size: 1.75rem; margin-bottom: 4px; font-variant-numeric: tabular-nums; }
            .summary span { color: #6b7280; font-size: .85rem; }
            .card { background: white; border: 1px solid #e5e7eb; border-radius: 12px; overflow-x: auto; }
            table { width: 100%; border-collapse: collapse; }
            th, td { padding: 18px 20px; text-align: left; border-bottom: 1px solid #e5e7eb; white-space: nowrap; }
            td { font-variant-numeric: tabular-nums; }
            th { color: #6b7280; background: #f9fafb; font-size: .8rem; font-weight: 600; }
            tr:last-child td { border-bottom: 0; }
            .status { font-weight: 700; }
            .Running { color: #1d4ed8; }
            .Succeeded { color: #15803d; }
            .Failed { color: #b91c1c; }
            .Canceled { color: #6b7280; }
            .error { min-width: 100px; max-width: 300px; white-space: normal; overflow-wrap: anywhere; color: #b91c1c; }
            @media (max-width: 1024px) {
              th, td { padding: 14px 16px; }
              .summary { gap: 12px; }
            }
            @media (max-width: 640px) {
              .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); margin: 20px 0; }
              .summary div { padding: 14px 16px; }
              .card { background: transparent; border: 0; border-radius: 0; overflow: visible; }
              table, tbody { display: block; width: 100%; }
              thead { position: absolute; width: 1px; height: 1px; padding: 0; overflow: hidden; clip-path: inset(50%); }
              tr { display: block; background: white; border: 1px solid #e5e7eb; border-radius: 12px; margin-bottom: 12px; padding: 8px 16px; }
              td, tr:last-child td { display: grid; grid-template-columns: 6rem minmax(0, 1fr); gap: 12px; padding: 10px 0; border-bottom: 1px solid #f0f2f5; white-space: normal; overflow-wrap: anywhere; }
              td::before { content: attr(data-label); color: #6b7280; font-size: .8rem; font-weight: 400; }
              td:last-child, tr:last-child td:last-child { border-bottom: 0; }
              td.error { min-width: 0; max-width: none; }
              td[colspan] { display: block; }
              td[colspan]::before { content: none; }
            }
          </style>
        </head>
        <body>
          <h1>FastBulkProcessor Dashboard</h1>
          <p>作業狀態每 2 秒更新。筆數表示已成功完成的批次；Workflow 顯示來源筆數，Bulk Write 顯示寫入筆數。</p>
          <div class="summary">
            <div><strong id="running">0</strong><span>執行中</span></div>
            <div><strong id="succeeded">0</strong><span>已完成</span></div>
            <div><strong id="failed">0</strong><span>失敗</span></div>
            <div><strong id="canceled">0</strong><span>已取消</span></div>
          </div>
          <div class="card">
            <table aria-label="作業狀態">
              <thead><tr><th scope="col">作業</th><th scope="col">狀態</th><th scope="col">已處理筆數</th><th scope="col">批次</th><th scope="col">耗時</th><th scope="col">筆／秒</th><th scope="col">開始時間</th><th scope="col">錯誤</th></tr></thead>
              <tbody id="jobs"><tr><td colspan="8">載入中…</td></tr></tbody>
            </table>
          </div>
          <script>
            const body = document.getElementById('jobs');
            const statuses = ['Running', 'Succeeded', 'Failed', 'Canceled'];
            const labels = ['執行中', '已完成', '失敗', '已取消'];
            const columns = Array.from(document.querySelectorAll('thead th'), header => header.textContent);
            const api = location.pathname.replace(/\/$/, '') + '/jobs';
            async function refresh() {
              try {
                const response = await fetch(api, { cache: 'no-store' });
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
                const jobs = await response.json();
                for (const [key, index] of [['running', 0], ['succeeded', 1], ['failed', 2], ['canceled', 3]]) {
                  document.getElementById(key).textContent = jobs.filter(job => job.status === index).length;
                }
                body.replaceChildren();
                if (jobs.length === 0) {
                  const row = body.insertRow();
                  const cell = row.insertCell();
                  cell.colSpan = 8;
                  cell.textContent = '尚無作業';
                  return;
                }
                for (const job of jobs) {
                  const row = body.insertRow();
                  const cells = [
                    job.name,
                    labels[job.status] ?? String(job.status),
                    Number(job.processedRowCount).toLocaleString(),
                    Number(job.completedBatchCount).toLocaleString(),
                    `${Number(job.elapsedSeconds).toFixed(1)} 秒`,
                    Number(job.rowsPerSecond).toFixed(1),
                    new Date(job.startedAt).toLocaleString(),
                    job.error ?? ''
                  ];
                  for (const [index, value] of cells.entries()) {
                    const cell = row.insertCell();
                    cell.dataset.label = columns[index];
                    cell.textContent = value;
                  }
                  row.cells[1].className = `status ${statuses[job.status] ?? ''}`;
                  row.cells[7].className = 'error';
                }
              } catch (error) {
                body.replaceChildren();
                const row = body.insertRow();
                const cell = row.insertCell();
                cell.colSpan = 8;
                cell.textContent = `讀取作業失敗：${error.message}`;
              }
            }
            refresh();
            setInterval(refresh, 2000);
          </script>
        </body>
        </html>
        """;
}
