using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using CsvHelper;
using CsvHelper.Configuration;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;

namespace CombineCsvToSharePoint;

public static class CombineCsvToSharePointFunction
{
    private const int MaxSheetNameLength = 31;
    [FunctionName("CombineCsvToSharePoint")]
    public static async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "combine-csvs")] HttpRequest req,
        ILogger log,
        CancellationToken cancellationToken)
    {
        string requestBody;
        using (var reader = new StreamReader(req.Body, Encoding.UTF8))
        {
            requestBody = await reader.ReadToEndAsync();
        }

        CombineRequest? request;
        try
        {
            request = JsonSerializer.Deserialize<CombineRequest>(requestBody, JsonOptions());
        }
        catch (JsonException)
        {
            return new BadRequestObjectResult("Invalid JSON payload.");
        }

        if (request == null)
        {
            return new BadRequestObjectResult("Payload must include client_name and report_date.");
        }

        if (string.IsNullOrWhiteSpace(request.ClientName))
        {
            return new BadRequestObjectResult("Payload must include a non-empty 'client_name'.");
        }

        if (!DateOnly.TryParse(request.ReportDate, CultureInfo.InvariantCulture, DateTimeStyles.None, out var reportDate))
        {
            return new BadRequestObjectResult("Payload must include a valid 'report_date'.");
        }

        var containerClient = GetReportsContainerClient();
        var clientSegment = SanitizeBlobSegment(request.ClientName);
        var outputFilename = "ActivityReport.xlsx";
        var outputBlobClient = containerClient.GetBlobClient($"{clientSegment}/{outputFilename}");

        if (await outputBlobClient.ExistsAsync(cancellationToken))
        {
            return new OkObjectResult(new
            {
                status = "exists",
                output_filename = outputFilename,
                xlsx_url = outputBlobClient.Uri.ToString(),
            });
        }

        var csvBlobs = await ListCsvBlobsAsync(containerClient, clientSegment, cancellationToken);
        if (csvBlobs.Count == 0)
        {
            return new NotFoundObjectResult("No CSV files found for the specified client.");
        }

        var tempFilePath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.xlsx");

        try
        {
            await BuildWorkbookAsync(csvBlobs, tempFilePath, cancellationToken);
            await UploadToStorageAsync(outputBlobClient, tempFilePath, cancellationToken);

            return new OkObjectResult(new
            {
                status = "created",
                output_filename = outputFilename,
                xlsx_url = outputBlobClient.Uri.ToString(),
            });
        }
        catch (HttpRequestException ex)
        {
            log.LogError(ex, "Failed to download CSVs or upload to storage.");
            return new ObjectResult(ex.Message) { StatusCode = StatusCodes.Status502BadGateway };
        }
        catch (Exception ex)
        {
            log.LogError(ex, "Unhandled error while creating Excel file.");
            return new ObjectResult(ex.Message) { StatusCode = StatusCodes.Status500InternalServerError };
        }
        finally
        {
            if (File.Exists(tempFilePath))
            {
                File.Delete(tempFilePath);
            }
        }
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
        foreach (var csvFile in csvFiles)
        {
            var sheetName = GetUniqueSheetName(Path.GetFileNameWithoutExtension(csvFile.Name), usedSheetNames);
            var worksheetPart = workbookPart.AddNewPart<WorksheetPart>();

            await WriteCsvToWorksheetAsync(csvFile.BlobClient, worksheetPart, cancellationToken);

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
        var headers = new BlobHttpHeaders { ContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" };
        await outputBlobClient.UploadAsync(fileStream, new BlobUploadOptions { HttpHeaders = headers }, cancellationToken);
    }

    private static string GetRequiredEnv(string name)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new InvalidOperationException($"Missing required environment variable: {name}");
        }

        return value;
    }

    private static BlobContainerClient GetReportsContainerClient()
    {
        var connectionString = GetRequiredEnv("STORAGE_CONNECTION_STRING");
        var containerName = (Environment.GetEnvironmentVariable("REPORTS_CONTAINER") ?? "reports").Trim();
        if (string.IsNullOrWhiteSpace(containerName))
        {
            throw new InvalidOperationException("REPORTS_CONTAINER cannot be empty.");
        }

        return new BlobContainerClient(connectionString, containerName);
    }

    private static async Task<List<CsvBlob>> ListCsvBlobsAsync(
        BlobContainerClient containerClient,
        string clientSegment,
        CancellationToken cancellationToken)
    {
        var prefix = $"{clientSegment}/";
        var csvBlobs = new List<CsvBlob>();

        await foreach (var blobItem in containerClient.GetBlobsAsync(prefix: prefix, cancellationToken: cancellationToken))
        {
            if (!blobItem.Name.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            csvBlobs.Add(new CsvBlob(Path.GetFileName(blobItem.Name), containerClient.GetBlobClient(blobItem.Name)));
        }

        csvBlobs.Sort((left, right) => string.Compare(left.Name, right.Name, StringComparison.OrdinalIgnoreCase));
        return csvBlobs;
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

    private static string SanitizeBlobSegment(string segment)
    {
        var invalidChars = Path.GetInvalidFileNameChars();
        var builder = new StringBuilder(segment.Length);
        foreach (var ch in segment)
        {
            if (ch == '/' || ch == '\\' || invalidChars.Contains(ch))
            {
                builder.Append('_');
                continue;
            }

            builder.Append(ch);
        }

        var sanitized = builder.ToString().Trim('_');
        return string.IsNullOrWhiteSpace(sanitized) ? "client" : sanitized;
    }

    private static JsonSerializerOptions JsonOptions() => new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private sealed record CombineRequest(
        [property: JsonPropertyName("client_name")] string ClientName,
        [property: JsonPropertyName("report_date")] string ReportDate);

    private sealed record CsvBlob(string Name, BlobClient BlobClient);
}
