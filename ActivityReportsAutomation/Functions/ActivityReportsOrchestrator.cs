using ActivityReportsAutomation.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Extensions.DurableTask;
using Microsoft.Extensions.Logging;

namespace ActivityReportsAutomation.Functions;

public sealed class ActivityReportsOrchestrator
{
    private readonly ILogger<ActivityReportsOrchestrator> _logger;

    public ActivityReportsOrchestrator(ILogger<ActivityReportsOrchestrator> logger)
    {
        _logger = logger;
    }

    [Function("ActivityReportsOrchestrator")]
    public async Task<ActivityReportResult> RunAsync(
        [OrchestrationTrigger] TaskOrchestrationContext context)
    {
        var request = context.GetInput<ActivityReportRequest>()
            ?? throw new InvalidOperationException("Orchestration input was missing.");

        _logger.LogInformation("Starting activity report orchestration for {MemberFirmId}.", request.MemberFirmId);

        var reportFile = await context.CallActivityAsync<ReportFileLocation>("CombineActivityReports", request);
        var sharePointLocation = await context.CallActivityAsync<string>("UploadActivityReportsToSharePoint", reportFile);

        return new ActivityReportResult(reportFile.BlobUri, reportFile.FileName, sharePointLocation);
    }
}
