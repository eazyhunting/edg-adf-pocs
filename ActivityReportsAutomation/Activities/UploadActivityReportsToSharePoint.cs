using ActivityReportsAutomation.Models;
using ActivityReportsAutomation.Services;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace ActivityReportsAutomation.Activities;

public sealed class UploadActivityReportsToSharePoint
{
    private readonly ISharePointUploadService _sharePointUploadService;
    private readonly ILogger<UploadActivityReportsToSharePoint> _logger;

    public UploadActivityReportsToSharePoint(
        ISharePointUploadService sharePointUploadService,
        ILogger<UploadActivityReportsToSharePoint> logger)
    {
        _sharePointUploadService = sharePointUploadService;
        _logger = logger;
    }

    [Function("UploadActivityReportsToSharePoint")]
    public async Task<string> RunAsync(
        [ActivityTrigger] ReportFileLocation reportFile,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Uploading report {FileName} to SharePoint.", reportFile.FileName);
        return await _sharePointUploadService.UploadAsync(reportFile, cancellationToken);
    }
}
