using System.Globalization;
using System.Net;
using System.Text.Json;
using ActivityReportsAutomation.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Extensions.DurableTask;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace ActivityReportsAutomation.Functions;

public sealed class HttpStartActivityReports
{
    private readonly ILogger<HttpStartActivityReports> _logger;

    public HttpStartActivityReports(ILogger<HttpStartActivityReports> logger)
    {
        _logger = logger;
    }

    [Function("StartActivityReports")]
    public async Task<HttpResponseData> RunAsync(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "activity-reports")] HttpRequestData req,
        [DurableClient] DurableTaskClient client,
        FunctionContext context)
    {
        var payload = await JsonSerializer.DeserializeAsync<ActivityReportPayload>(req.Body, JsonOptions(), context.CancellationToken);
        if (payload is null)
        {
            return await CreateErrorResponseAsync(req, HttpStatusCode.BadRequest, "Payload is required.");
        }

        if (string.IsNullOrWhiteSpace(payload.MemberFirmId))
        {
            return await CreateErrorResponseAsync(req, HttpStatusCode.BadRequest, "MemberFirmId is required.");
        }

        if (!DateOnly.TryParse(payload.ReportingPeriod, CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
        {
            return await CreateErrorResponseAsync(req, HttpStatusCode.BadRequest, "ReportingPeriod must be a valid date.");
        }

        var request = new ActivityReportRequest(payload.MemberFirmId, payload.ReportingPeriod);
        var instanceId = await client.ScheduleNewOrchestrationInstanceAsync("ActivityReportsOrchestrator", request);

        _logger.LogInformation("Started ActivityReportsOrchestrator with ID = {InstanceId}", instanceId);

        return client.CreateCheckStatusResponse(req, instanceId);
    }

    private static async Task<HttpResponseData> CreateErrorResponseAsync(
        HttpRequestData req,
        HttpStatusCode statusCode,
        string message)
    {
        var response = req.CreateResponse(statusCode);
        await response.WriteStringAsync(message);
        return response;
    }

    private static JsonSerializerOptions JsonOptions() => new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private sealed record ActivityReportPayload(string MemberFirmId, string ReportingPeriod);
}
