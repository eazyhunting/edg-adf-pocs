using System.Globalization;
using Azure.Storage.Blobs;
using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.Data;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace CsvMergeFunction.Functions;

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
        [HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequestData request)
    {
        var mergeRequest = await MergeRequest.FromHttpRequestAsync(request);
        if (mergeRequest is null)
        {
            var badResponse = request.CreateResponse(System.Net.HttpStatusCode.BadRequest);
            await badResponse.WriteStringAsync("Provide 'clientName' and 'date' parameters.");
            return badResponse;
        }

        var containerName = mergeRequest.ClientName.Trim().ToLowerInvariant();
        var folderPath = mergeRequest.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        _logger.LogInformation("Merging CSV files for container {Container} and folder {Folder}.", containerName, folderPath);

        var containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
        var blobs = containerClient.GetBlobsAsync(prefix: $"{folderPath}/");

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
            await notFoundResponse.WriteStringAsync("No CSV files found for the provided client and date.");
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

        var response = request.CreateResponse(System.Net.HttpStatusCode.OK);
        response.Headers.Add("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        response.Headers.Add("Content-Disposition", $"attachment; filename=merged-{containerName}-{folderPath}.xlsx");
        await response.WriteBytesAsync(outputStream.ToArray());
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

    private sealed record MergeRequest(string ClientName, DateTime Date)
    {
        public static async Task<MergeRequest?> FromHttpRequestAsync(HttpRequestData request)
        {
            var query = System.Web.HttpUtility.ParseQueryString(request.Url.Query);
            var clientName = query["clientName"];
            var dateValue = query["date"];

            if (string.IsNullOrWhiteSpace(clientName) || string.IsNullOrWhiteSpace(dateValue))
            {
                var body = await request.ReadFromJsonAsync<MergeRequestPayload>();
                clientName = body?.ClientName ?? clientName;
                dateValue = body?.Date ?? dateValue;
            }

            if (string.IsNullOrWhiteSpace(clientName) || string.IsNullOrWhiteSpace(dateValue))
            {
                return null;
            }

            if (!DateTime.TryParse(dateValue, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var date))
            {
                return null;
            }

            return new MergeRequest(clientName, date.Date);
        }
    }

    private sealed class MergeRequestPayload
    {
        public string? ClientName { get; set; }
        public string? Date { get; set; }
    }
}
