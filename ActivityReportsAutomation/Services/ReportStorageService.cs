using System.Globalization;
using ActivityReportsAutomation.Models;
using ActivityReportsAutomation.Options;
using Azure.Security.KeyVault.Secrets;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using CsvHelper;
using CsvHelper.Configuration;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ActivityReportsAutomation.Services;

public interface IReportStorageService
{
    Task<ReportFileLocation> BuildActivityReportAsync(string memberFirmId, string reportingPeriod, CancellationToken cancellationToken);
}

public sealed class ReportStorageService : IReportStorageService
{
    private const int MaxSheetNameLength = 31;
    private readonly SecretClient _secretClient;
    private readonly ActivityReportsOptions _options;
    private readonly ILogger<ReportStorageService> _logger;

    public ReportStorageService(
        SecretClient secretClient,
        IOptions<ActivityReportsOptions> options,
        ILogger<ReportStorageService> logger)
    {
        _secretClient = secretClient;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<ReportFileLocation> BuildActivityReportAsync(
        string memberFirmId,
        string reportingPeriod,
        CancellationToken cancellationToken)
    {
        if (!DateOnly.TryParse(reportingPeriod, CultureInfo.InvariantCulture, DateTimeStyles.None, out var reportDate))
        {
            throw new ArgumentException("ReportingPeriod must be a valid date.", nameof(reportingPeriod));
        }

        var storageUrl = await GetSecretAsync(_options.StorageUrlSecretName, cancellationToken);
        var containerClient = new BlobContainerClient(new Uri(storageUrl), new Azure.Identity.DefaultAzureCredential());

        var sanitizedMemberFirmId = SanitizePathSegment(memberFirmId);
        var prefix = $"Reports/{sanitizedMemberFirmId}/{reportDate:yyyy}/{reportDate:MM}/{reportDate:dd}/";
        var csvBlobs = await ListCsvBlobsAsync(containerClient, prefix, cancellationToken);
        if (csvBlobs.Count == 0)
        {
            throw new InvalidOperationException("No CSV files were found for the specified member firm and reporting period.");
        }

        var fileName = $"ActivityReports_{_options.EnvironmentName}_{reportDate:yyyy}_{reportDate:MM}_{reportDate:dd}.xlsx";
        var blobPath = $"{prefix}{fileName}";
        var outputBlob = containerClient.GetBlobClient(blobPath);

        var tempFilePath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.xlsx");
        try
        {
            await BuildWorkbookAsync(csvBlobs, tempFilePath, cancellationToken);
            await UploadToStorageAsync(outputBlob, tempFilePath, cancellationToken);
        }
        finally
        {
            if (File.Exists(tempFilePath))
            {
                File.Delete(tempFilePath);
            }
        }

        _logger.LogInformation("Activity report created at {BlobUri}", outputBlob.Uri);
        return new ReportFileLocation(outputBlob.Uri.ToString(), fileName, blobPath);
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

    private static async Task<List<CsvBlob>> ListCsvBlobsAsync(
        BlobContainerClient containerClient,
        string prefix,
        CancellationToken cancellationToken)
    {
        var results = new List<CsvBlob>();
        await foreach (var blobItem in containerClient.GetBlobsAsync(prefix: prefix, cancellationToken: cancellationToken))
        {
            if (blobItem.Name.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
            {
                results.Add(new CsvBlob(blobItem.Name, containerClient.GetBlobClient(blobItem.Name)));
            }
        }

        results.Sort((left, right) => string.Compare(left.Name, right.Name, StringComparison.OrdinalIgnoreCase));
        return results;
    }

    private static async Task BuildWorkbookAsync(
        IReadOnlyList<CsvBlob> csvFiles,
        string outputPath,
        CancellationToken cancellationToken)
    {
        await using var output = new FileStream(outputPath, FileMode.Create, FileAccess.ReadWrite, FileShare.None);
        using var spreadsheet = SpreadsheetDocument.Create(output, SpreadsheetDocumentType.Workbook);
        var workbookPart = spreadsheet.AddWorkbookPart();
        workbookPart.Workbook = new Workbook();
        var sheets = workbookPart.Workbook.AppendChild(new Sheets());

        var usedSheetNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        uint sheetId = 1;
        foreach (var csvBlob in csvFiles)
        {
            var sheetName = GetUniqueSheetName(Path.GetFileNameWithoutExtension(csvBlob.Name), usedSheetNames);
            var worksheetPart = workbookPart.AddNewPart<WorksheetPart>();

            await WriteCsvToWorksheetAsync(csvBlob.BlobClient, worksheetPart, cancellationToken);

            var sheet = new Sheet
            {
                Id = workbookPart.GetIdOfPart(worksheetPart),
                SheetId = sheetId++,
                Name = sheetName,
            };
            sheets.Append(sheet);
        }

        workbookPart.Workbook.Save();
    }

    private static async Task WriteCsvToWorksheetAsync(
        BlobClient blobClient,
        WorksheetPart worksheetPart,
        CancellationToken cancellationToken)
    {
        await using var csvStream = await blobClient.OpenReadAsync(cancellationToken: cancellationToken);
        using var reader = new StreamReader(csvStream);
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = false,
            BadDataFound = null,
            MissingFieldFound = null,
        };

        using var csv = new CsvReader(reader, config);
        using var writer = OpenXmlWriter.Create(worksheetPart);
        writer.WriteStartElement(new Worksheet());
        writer.WriteStartElement(new SheetData());

        while (await csv.ReadAsync())
        {
            var rowValues = csv.Context.Record ?? Array.Empty<string>();
            writer.WriteStartElement(new Row());

            foreach (var cellValue in rowValues)
            {
                writer.WriteStartElement(new Cell { DataType = CellValues.InlineString });
                writer.WriteElement(new InlineString(new Text(cellValue ?? string.Empty)));
                writer.WriteEndElement();
            }

            writer.WriteEndElement();
        }

        writer.WriteEndElement();
        writer.WriteEndElement();
    }

    private static async Task UploadToStorageAsync(
        BlobClient outputBlobClient,
        string filePath,
        CancellationToken cancellationToken)
    {
        await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        var headers = new BlobHttpHeaders
        {
            ContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        };
        await outputBlobClient.UploadAsync(fileStream, new BlobUploadOptions { HttpHeaders = headers }, cancellationToken);
    }

    private static string GetUniqueSheetName(string name, HashSet<string> usedNames)
    {
        var sanitized = SanitizeSheetName(name);
        var candidate = sanitized;
        var suffix = 1;

        while (!usedNames.Add(candidate))
        {
            var trimmed = sanitized;
            var suffixText = $"_{suffix++}";
            if (trimmed.Length + suffixText.Length > MaxSheetNameLength)
            {
                trimmed = trimmed[..(MaxSheetNameLength - suffixText.Length)];
            }
            candidate = trimmed + suffixText;
        }

        return candidate;
    }

    private static string SanitizeSheetName(string name)
    {
        var invalidChars = new[] { '\\', '/', '*', '?', ':', '[', ']' };
        var sanitized = string.Join("_", name.Split(invalidChars, StringSplitOptions.RemoveEmptyEntries));
        if (string.IsNullOrWhiteSpace(sanitized))
        {
            sanitized = "Sheet";
        }

        return sanitized.Length > MaxSheetNameLength
            ? sanitized[..MaxSheetNameLength]
            : sanitized;
    }

    private static string SanitizePathSegment(string segment)
    {
        var invalidChars = Path.GetInvalidFileNameChars();
        var builder = new List<char>(segment.Length);
        foreach (var ch in segment)
        {
            if (ch == '/' || ch == '\\' || invalidChars.Contains(ch))
            {
                builder.Add('_');
                continue;
            }

            builder.Add(ch);
        }

        var sanitized = string.Concat(builder).Trim('_');
        return string.IsNullOrWhiteSpace(sanitized) ? "member" : sanitized;
    }

    private sealed record CsvBlob(string Name, BlobClient BlobClient);
}
