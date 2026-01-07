using System.Globalization;
using Azure.Storage.Blobs;
using CsvHelper;
using CsvHelper.Configuration;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs.Models;

namespace CsvMergeFunctionV2.Functions;

public class MergeCsvFunction
{
    private readonly BlobServiceClient _blobServiceClient;
    private readonly ILogger<MergeCsvFunction> _logger;

    public MergeCsvFunction(BlobServiceClient blobServiceClient, ILogger<MergeCsvFunction> logger)
    {
        _blobServiceClient = blobServiceClient;
        _logger = logger;
    }

    [Function("MergeCsv")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequestData request)
    {
        var mergeRequest = await MergeRequest.FromHttpRequestAsync(request);
        if (mergeRequest is null)
        {
            var badResponse = request.CreateResponse(System.Net.HttpStatusCode.BadRequest);
            await badResponse.WriteStringAsync("Provide 'blobUrl' and 'clientName' parameters. 'date' is optional.");
            return badResponse;
        }

        var containerName = mergeRequest.ContainerName.Trim().ToLowerInvariant();
        var folderPath = mergeRequest.ClientName.Trim().Trim('/');
        var prefix = string.IsNullOrWhiteSpace(folderPath) ? string.Empty : $"{folderPath}/";
        _logger.LogInformation("Merging CSV files for container {Container} and folder {Folder}.", containerName, folderPath);

        var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
        var blobs = containerClient.GetBlobsAsync(prefix: $"clients/{folderPath}");

        var tables = new List<(string WorksheetName, System.Data.DataTable Table)>();
        var csvFound = false;

        await foreach (var blobItem in blobs)
        {
            if (!blobItem.Name.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            csvFound = true;
            var blobClient = containerClient.GetBlobClient(blobItem.Name);
            await using var stream = await blobClient.OpenReadAsync();
            using var reader = new StreamReader(stream);
            var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                BadDataFound = null,
                MissingFieldFound = null
            };

            using var csv = new CsvReader(reader, csvConfig);
            using var csvDataReader = new CsvDataReader(csv);
            var table = new System.Data.DataTable();
            table.Load(csvDataReader);

            var worksheetName = GetWorksheetName(blobItem.Name);
            tables.Add((worksheetName, table));
        }

        if (!csvFound)
        {
            var notFoundResponse = request.CreateResponse(System.Net.HttpStatusCode.NotFound);
            await notFoundResponse.WriteStringAsync("No CSV files found for the provided folder path.");
            return notFoundResponse;
        }

        await using var outputStream = new MemoryStream();
        using (var document = SpreadsheetDocument.Create(outputStream, SpreadsheetDocumentType.Workbook))
        {
            var workbookPart = document.AddWorkbookPart();
            workbookPart.Workbook = new Workbook();
            var sheets = workbookPart.Workbook.AppendChild(new Sheets());
            uint sheetId = 1;

            foreach (var (worksheetName, table) in tables)
            {
                AppendWorksheet(workbookPart, sheets, worksheetName, table, sheetId++);
            }

            workbookPart.Workbook.Save();
        }
        outputStream.Position = 0;

        var folderLabel = string.IsNullOrWhiteSpace(folderPath) ? "root" : folderPath.Replace('/', '-');
        var outputBlobName = string.IsNullOrWhiteSpace(folderPath)
            ? $"clients/merged-{containerName}-{folderLabel}.xlsx"
            : $"clients/{folderPath}/merged-{containerName}-{folderLabel}.xlsx";
        var outputBytes = outputStream.ToArray();
        var outputBlobClient = containerClient.GetBlobClient(outputBlobName);
        await using (var uploadStream = new MemoryStream(outputBytes))
        {
            await outputBlobClient.UploadAsync(
                uploadStream,
                new BlobHttpHeaders
                {
                    ContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                });
        }

        var response = request.CreateResponse(System.Net.HttpStatusCode.OK);
        response.Headers.Add("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        response.Headers.Add("Content-Disposition", $"attachment; filename=merged-{containerName}-{folderLabel}.xlsx");
        await response.WriteBytesAsync(outputBytes);
        return response;
    }

    private static void AppendWorksheet(
        WorkbookPart workbookPart,
        Sheets sheets,
        string worksheetName,
        System.Data.DataTable table,
        uint sheetId)
    {
        var worksheetPart = workbookPart.AddNewPart<WorksheetPart>();
        var sheetData = new SheetData();
        worksheetPart.Worksheet = new Worksheet(sheetData);

        var headerRow = new Row();
        foreach (System.Data.DataColumn column in table.Columns)
        {
            headerRow.Append(CreateTextCell(column.ColumnName));
        }

        sheetData.Append(headerRow);

        foreach (System.Data.DataRow row in table.Rows)
        {
            var dataRow = new Row();
            foreach (System.Data.DataColumn column in table.Columns)
            {
                dataRow.Append(CreateTextCell(row[column]?.ToString() ?? string.Empty));
            }

            sheetData.Append(dataRow);
        }

        worksheetPart.Worksheet.Save();

        var sheet = new Sheet
        {
            Id = workbookPart.GetIdOfPart(worksheetPart),
            SheetId = sheetId,
            Name = worksheetName
        };
        sheets.Append(sheet);
    }

    private static Cell CreateTextCell(string text)
    {
        return new Cell
        {
            DataType = CellValues.String,
            CellValue = new CellValue(text)
        };
    }

    private static string GetWorksheetName(string blobName)
    {
        var fileName = Path.GetFileNameWithoutExtension(blobName);
        if (string.IsNullOrWhiteSpace(fileName))
        {
            return "Sheet";
        }

        var sanitized = string.Concat(fileName.Select(c => Path.GetInvalidFileNameChars().Contains(c) ? '_' : c));
        return sanitized.Length <= 31 ? sanitized : sanitized[..31];
    }

    private sealed record MergeRequest(string ContainerName, string ClientName)
    {
        public static async Task<MergeRequest?> FromHttpRequestAsync(HttpRequestData request)
        {
            var query = System.Web.HttpUtility.ParseQueryString(request.Url.Query);
            var blobUrl = query["blobUrl"];
            var containerName = query["containerName"] ?? "reports";
            var folderPath = query["folderPath"];
            var clientName = query["clientName"];
            var dateValue = query["date"];

            var body = await request.ReadFromJsonAsync<MergeRequestPayload>();
            blobUrl = body?.BlobUrl ?? blobUrl;
            containerName = body?.ContainerName ?? containerName;
            folderPath = body?.FolderPath ?? folderPath;
            clientName = body?.ClientName ?? clientName;
            dateValue = body?.Date ?? dateValue;

            if (!string.IsNullOrWhiteSpace(blobUrl))
            {
                var parsedRequest = ParseBlobUrl(blobUrl, clientName);
                if (parsedRequest is not null)
                {
                    return parsedRequest;
                }
            }

            if (!string.IsNullOrWhiteSpace(containerName) && !string.IsNullOrWhiteSpace(clientName))
            {
                return new MergeRequest(containerName, clientName);
            }

            if (!string.IsNullOrWhiteSpace(containerName) && !string.IsNullOrWhiteSpace(folderPath))
            {
                return new MergeRequest(containerName, folderPath);
            }

            if (!string.IsNullOrWhiteSpace(containerName) && !string.IsNullOrWhiteSpace(dateValue))
            {
                return new MergeRequest(containerName, string.Empty);
            }

            return null;
        }

        private static MergeRequest? ParseBlobUrl(string blobUrl, string? clientName)
        {
            if (!Uri.TryCreate(blobUrl, UriKind.Absolute, out var uri))
            {
                return null;
            }

            var path = uri.AbsolutePath.Trim('/');
            var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
            if (segments.Length < 2 || string.IsNullOrWhiteSpace(clientName))
            {
                return null;
            }

            var containerName = segments[0];
            var baseFolder = segments[1];
            var folderPath = $"{baseFolder}/{clientName}";
            return new MergeRequest(containerName, folderPath);
        }
    }

    private sealed class MergeRequestPayload
    {
        public string? BlobUrl { get; set; }
        public string? ContainerName { get; set; }
        public string? FolderPath { get; set; }
        public string? ClientName { get; set; }
        public string? Date { get; set; }
    }
}
