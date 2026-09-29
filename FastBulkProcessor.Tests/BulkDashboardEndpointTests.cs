using FastBulkProcessor.Dashboard;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace FastBulkProcessor.Tests;

public sealed class BulkDashboardEndpointTests
{
    [Fact]
    public async Task MapFastBulkProcessorDashboard_提供頁面與作業API且兩者都需要授權()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddAuthorization();
        builder.Services.AddFastBulkProcessorDashboard();
        await using var app = builder.Build();
        app.MapFastBulkProcessorDashboard("/bulk-dashboard");

        var endpoints = ((IEndpointRouteBuilder)app).DataSources
            .SelectMany(source => source.Endpoints)
            .OfType<RouteEndpoint>()
            .ToArray();
        Assert.Equal(2, endpoints.Length);
        Assert.All(endpoints, endpoint =>
            Assert.NotEmpty(endpoint.Metadata.GetOrderedMetadata<IAuthorizeData>()));

        var page = Assert.Single(endpoints, endpoint =>
            endpoint.RoutePattern.RawText == "/bulk-dashboard/");
        var context = new DefaultHttpContext { RequestServices = app.Services };
        await using var body = new MemoryStream();
        context.Response.Body = body;

        await page.RequestDelegate!(context);

        body.Position = 0;
        using var reader = new StreamReader(body);
        var html = await reader.ReadToEndAsync();
        Assert.Contains("FastBulkProcessor Dashboard", html);
        Assert.Contains("/jobs", html);

        var api = Assert.Single(endpoints, endpoint =>
            endpoint.RoutePattern.RawText == "/bulk-dashboard/jobs");
        var apiContext = new DefaultHttpContext { RequestServices = app.Services };
        await using var apiBody = new MemoryStream();
        apiContext.Response.Body = apiBody;

        await api.RequestDelegate!(apiContext);

        apiBody.Position = 0;
        using var apiReader = new StreamReader(apiBody);
        Assert.Equal("[]", await apiReader.ReadToEndAsync());
    }
}
