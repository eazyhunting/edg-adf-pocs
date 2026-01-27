using ActivityReportsAutomation.Models;
using ActivityReportsAutomation.Options;
using Azure.Security.KeyVault.Secrets;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ActivityReportsAutomation.Services;

public interface ISharePointUploadService
{
    Task<string> UploadAsync(ReportFileLocation reportFile, CancellationToken cancellationToken);
}

public sealed class SharePointUploadService : ISharePointUploadService
{
    private readonly SecretClient _secretClient;
    private readonly ActivityReportsOptions _options;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<SharePointUploadService> _logger;

    public SharePointUploadService(
        SecretClient secretClient,
        IOptions<ActivityReportsOptions> options,
        IHttpClientFactory httpClientFactory,
        ILogger<SharePointUploadService> logger)
    {
        _secretClient = secretClient;
        _options = options.Value;
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    public async Task<string> UploadAsync(ReportFileLocation reportFile, CancellationToken cancellationToken)
    {
        var uploadUrlTemplate = await GetSecretAsync(_options.SharePointUploadUrlSecretName, cancellationToken);
        var accessToken = await GetSecretAsync(_options.SharePointAccessTokenSecretName, cancellationToken);

        var destinationUrl = uploadUrlTemplate.Contains("{fileName}", StringComparison.OrdinalIgnoreCase)
            ? uploadUrlTemplate.Replace("{fileName}", Uri.EscapeDataString(reportFile.FileName), StringComparison.OrdinalIgnoreCase)
            : $"{uploadUrlTemplate.TrimEnd('/')}/{Uri.EscapeDataString(reportFile.FileName)}";

        var httpClient = _httpClientFactory.CreateClient();
        httpClient.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", accessToken);

        var blobClient = new BlobClient(new Uri(reportFile.BlobUri), new Azure.Identity.DefaultAzureCredential());
        await using var blobStream = await blobClient.OpenReadAsync(cancellationToken: cancellationToken);

        using var request = new HttpRequestMessage(HttpMethod.Put, destinationUrl)
        {
            Content = new StreamContent(blobStream),
        };
        request.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");

        using var response = await httpClient.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            var content = await response.Content.ReadAsStringAsync(cancellationToken);
            _logger.LogError("SharePoint upload failed: {Status} {Body}", response.StatusCode, content);
            throw new InvalidOperationException($"SharePoint upload failed with status {response.StatusCode}.");
        }

        _logger.LogInformation("Uploaded report to SharePoint at {DestinationUrl}", destinationUrl);
        return destinationUrl;
    }

    private async Task<string> GetSecretAsync(string name, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new InvalidOperationException("Secret name cannot be empty.");
        }

        var response = await _secretClient.GetSecretAsync(name, cancellationToken: cancellationToken);
        return response.Value.Value;
    }
}
