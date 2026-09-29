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
            body { max-width: 1180px; margin: 0 auto; padding: 32px 20px; }
            h1 { margin: 0 0 4px; font-size: 1.7rem; }
            p { color: #6b7280; }
            .summary { display: flex; gap: 12px; flex-wrap: wrap; margin: 20px 0; }
            .summary div { background: white; border: 1px solid #e5e7eb; border-radius: 10px; padding: 12px 18px; min-width: 110px; }
            .summary strong { display: block; font-size: 1.4rem; }
            .summary span { color: #6b7280; font-size: .85rem; }
            .card { background: white; border: 1px solid #e5e7eb; border-radius: 12px; overflow-x: auto; }
            table { width: 100%; border-collapse: collapse; min-width: 760px; }
            th, td { padding: 14px 16px; text-align: left; border-bottom: 1px solid #e5e7eb; }
            th { color: #6b7280; font-size: .8rem; text-transform: uppercase; }
            tr:last-child td { border-bottom: 0; }
            .status { font-weight: 700; }
            .Running { color: #1d4ed8; }
            .Succeeded { color: #15803d; }
            .Failed { color: #b91c1c; }
            .Canceled { color: #6b7280; }
            .error { max-width: 300px; overflow-wrap: anywhere; color: #b91c1c; }
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
            <table>
              <thead><tr><th>作業</th><th>狀態</th><th>已處理筆數</th><th>批次</th><th>耗時</th><th>筆／秒</th><th>開始時間</th><th>錯誤</th></tr></thead>
              <tbody id="jobs"><tr><td colspan="8">載入中…</td></tr></tbody>
            </table>
          </div>
          <script>
            const body = document.getElementById('jobs');
            const statuses = ['Running', 'Succeeded', 'Failed', 'Canceled'];
            const labels = ['執行中', '已完成', '失敗', '已取消'];
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
                  for (const value of cells) row.insertCell().textContent = value;
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
