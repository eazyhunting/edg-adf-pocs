using ActivityReportsAutomation.Models;
using ActivityReportsAutomation.Services;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace ActivityReportsAutomation.Activities;

public sealed class CombineActivityReports
{
    private readonly IReportStorageService _reportStorageService;
    private readonly ILogger<CombineActivityReports> _logger;

    public CombineActivityReports(
        IReportStorageService reportStorageService,
        ILogger<CombineActivityReports> logger)
    {
        _reportStorageService = reportStorageService;
        _logger = logger;
    }

    [Function("CombineActivityReports")]
    public async Task<ReportFileLocation> RunAsync(
        [ActivityTrigger] ActivityReportRequest request,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Combining activity report for member firm {MemberFirmId} with period {ReportingPeriod}",
            request.MemberFirmId,
            request.ReportingPeriod);

        return await _reportStorageService.BuildActivityReportAsync(
            request.MemberFirmId,
            request.ReportingPeriod,
            cancellationToken);
    }
}
