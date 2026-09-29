using System.Net;
using System.Text.Json;
using FastBulkProcessor.Dashboard.Sample;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;

namespace FastBulkProcessor.Tests;

public sealed class DashboardSampleTests
{
    [Fact]
    public async Task Sample_匿名不可讀頁面_登入後執行作業並查到結果()
    {
        await using var app = DashboardSampleApplication.Build(
            ["--environment", "Development"], builder => builder.WebHost.ConfigureKestrel(options =>
                options.Listen(IPAddress.Loopback, 0)));
        await app.StartAsync();
        var address = app.Services.GetRequiredService<IServer>()
            .Features.Get<IServerAddressesFeature>()!.Addresses.Single();
        Assert.DoesNotContain(":0", address);
        using var client = new HttpClient(new HttpClientHandler
        {
            AllowAutoRedirect = false,
            UseCookies = true,
            UseProxy = false
        }) { BaseAddress = new Uri(address) };

        var anonymous = await client.GetAsync("/bulk-dashboard/");
        Assert.True(anonymous.StatusCode == HttpStatusCode.Redirect,
            $"{anonymous.StatusCode}: {await anonymous.Content.ReadAsStringAsync()}");
        var anonymousJobs = await client.GetAsync("/bulk-dashboard/jobs");
        Assert.Equal(HttpStatusCode.Redirect, anonymousJobs.StatusCode);

        var login = await client.GetAsync("/demo/login");
        Assert.Equal(HttpStatusCode.Redirect, login.StatusCode);

        var page = await client.GetAsync("/bulk-dashboard");
        Assert.Equal(HttpStatusCode.OK, page.StatusCode);
        Assert.Contains("FastBulkProcessor Dashboard",
            await page.Content.ReadAsStringAsync());

        var run = await client.PostAsync("/demo/run", content: null);
        Assert.Equal(HttpStatusCode.Redirect, run.StatusCode);
        var jobsResponse = await client.GetAsync("/bulk-dashboard/jobs");
        Assert.Equal(HttpStatusCode.OK, jobsResponse.StatusCode);
        using var jobs = JsonDocument.Parse(await jobsResponse.Content.ReadAsStringAsync());
        var job = Assert.Single(jobs.RootElement.EnumerateArray());
        Assert.Equal(1, job.GetProperty("status").GetInt32());
        Assert.Equal(20, job.GetProperty("processedRowCount").GetInt64());
    }
}
