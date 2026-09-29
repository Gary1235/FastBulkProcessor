using System.Net;
using System.Runtime.CompilerServices;
using System.Security.Claims;
using FastBulkProcessor.Dashboard;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;

namespace FastBulkProcessor.Dashboard.Sample;

public static class DashboardSampleApplication
{
    public static WebApplication Build(
        string[] args,
        Action<WebApplicationBuilder>? configureBuilder = null)
    {
        var builder = WebApplication.CreateBuilder(args);
        configureBuilder?.Invoke(builder);
        builder.Services.AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
            .AddCookie(options => options.LoginPath = "/demo/login");
        builder.Services.AddAuthorization();
        builder.Services.AddFastBulkProcessorDashboard();

        var app = builder.Build();
        app.Use(async (context, next) =>
        {
            if (context.Connection.RemoteIpAddress is { } address &&
                !IPAddress.IsLoopback(address))
            {
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                return;
            }

            await next(context);
        });
        app.UseAuthentication();
        app.UseAuthorization();

        app.MapGet("/", () => Results.Content("""
            <!doctype html>
            <html lang="zh-Hant">
            <head><meta charset="utf-8"><title>FastBulkProcessor Dashboard 範例</title></head>
            <body style="font: 16px system-ui; max-width: 640px; margin: 48px auto;">
              <h1>FastBulkProcessor Dashboard 範例</h1>
              <p>此範例只允許本機連線，登入後可執行 20 筆模擬資料的作業。</p>
              <p><a href="/demo/login">登入範例</a> · <a href="/bulk-dashboard">開啟儀表板</a></p>
              <form action="/demo/run" method="post"><button>執行示範作業</button></form>
            </body>
            </html>
            """, "text/html; charset=utf-8"));

        app.MapGet("/demo/login", async (HttpContext context) =>
        {
            var identity = new ClaimsIdentity(
                [new Claim(ClaimTypes.Name, "Local demo user")],
                CookieAuthenticationDefaults.AuthenticationScheme);
            await context.SignInAsync(
                CookieAuthenticationDefaults.AuthenticationScheme,
                new ClaimsPrincipal(identity));
            return Results.Redirect("/");
        });

        app.MapPost("/demo/run", async (
            BulkDashboardRunner runner,
            CancellationToken cancellationToken) =>
        {
            var workflow = new BulkWorkflow<int, int>(
                ReadValuesAsync(cancellationToken),
                (batch, _) => ValueTask.FromResult(batch.Count),
                async (_, token) => await Task.Delay(100, token));
            await runner.RunWorkflowAsync(
                "Sample import",
                workflow,
                new BulkExecutionOptions(5, 2),
                cancellationToken);
            return Results.Redirect("/bulk-dashboard");
        }).RequireAuthorization();

        app.MapFastBulkProcessorDashboard("/bulk-dashboard");
        return app;
    }

    private static async IAsyncEnumerable<int> ReadValuesAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        for (var value = 1; value <= 20; value++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return value;
            await Task.Delay(10, cancellationToken);
        }
    }
}
