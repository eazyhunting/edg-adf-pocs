using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
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
    private const int UploadChunkSize = 10 * 1024 * 1024;
    private static readonly HttpClient HttpClient = new();

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

        if (request?.CsvFiles == null || request.CsvFiles.Count == 0)
        {
            return new BadRequestObjectResult("Payload must include a non-empty 'csv_files' array.");
        }

        foreach (var entry in request.CsvFiles)
        {
            if (string.IsNullOrWhiteSpace(entry.Name) || string.IsNullOrWhiteSpace(entry.Url))
            {
                return new BadRequestObjectResult("Each csv_files entry must contain 'name' and 'url'.");
            }
        }

        var outputFilename = string.IsNullOrWhiteSpace(request.OutputFilename)
            ? "combined.xlsx"
            : request.OutputFilename;

        var tempFilePath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.xlsx");

        try
        {
            await BuildWorkbookAsync(request.CsvFiles, tempFilePath, cancellationToken);
            var sharepointUrl = await UploadToSharePointAsync(tempFilePath, outputFilename, cancellationToken);

            return new OkObjectResult(new
            {
                status = "uploaded",
                output_filename = outputFilename,
                sharepoint_url = sharepointUrl,
            });
        }
        catch (HttpRequestException ex)
        {
            log.LogError(ex, "Failed to download CSVs or upload to SharePoint.");
            return new ObjectResult(ex.Message) { StatusCode = (int)HttpStatusCode.BadGateway };
        }
        catch (Exception ex)
        {
            log.LogError(ex, "Unhandled error while creating Excel file.");
            return new ObjectResult(ex.Message) { StatusCode = (int)HttpStatusCode.InternalServerError };
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
        IReadOnlyList<CsvFile> csvFiles,
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

            await WriteCsvToWorksheetAsync(csvFile.Url, worksheetPart, cancellationToken);

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
        string csvUrl,
        WorksheetPart worksheetPart,
        CancellationToken cancellationToken)
    {
        await using var csvStream = await HttpClient.GetStreamAsync(csvUrl, cancellationToken);
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

    private static async Task<string> UploadToSharePointAsync(
        string filePath,
        string outputFilename,
        CancellationToken cancellationToken)
    {
        var accessToken = await GetGraphAccessTokenAsync(cancellationToken);
        var siteId = GetRequiredEnv("SHAREPOINT_SITE_ID");
        var driveId = GetRequiredEnv("SHAREPOINT_DRIVE_ID");
        var targetFolder = (Environment.GetEnvironmentVariable("SHAREPOINT_TARGET_FOLDER") ?? string.Empty).Trim('/');
        var filePathOnDrive = string.IsNullOrWhiteSpace(targetFolder)
            ? outputFilename
            : $"{targetFolder}/{outputFilename}";

        var uploadSessionUrl =
            $"https://graph.microsoft.com/v1.0/sites/{siteId}/drives/{driveId}/root:/{filePathOnDrive}:/createUploadSession";

        using var createRequest = new HttpRequestMessage(HttpMethod.Post, uploadSessionUrl)
        {
            Content = new StringContent(
                JsonSerializer.Serialize(new
                {
                    item = new Dictionary<string, string>
                    {
                        ["@microsoft.graph.conflictBehavior"] = "replace",
                        ["name"] = outputFilename,
                    },
                }),
                Encoding.UTF8,
                "application/json"),
        };
        createRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);

        using var createResponse = await HttpClient.SendAsync(createRequest, cancellationToken);
        createResponse.EnsureSuccessStatusCode();
        var createPayload = await createResponse.Content.ReadAsStringAsync(cancellationToken);
        var uploadSession = JsonSerializer.Deserialize<UploadSession>(createPayload, JsonOptions());
        if (uploadSession?.UploadUrl == null)
        {
            throw new InvalidOperationException("Upload session response missing upload URL.");
        }

        await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        var totalLength = fileStream.Length;
        var buffer = new byte[UploadChunkSize];
        long offset = 0;
        string? webUrl = null;

        while (offset < totalLength)
        {
            var chunkSize = (int)Math.Min(UploadChunkSize, totalLength - offset);
            var bytesRead = await fileStream.ReadAsync(buffer.AsMemory(0, chunkSize), cancellationToken);
            if (bytesRead == 0)
            {
                break;
            }

            using var chunkRequest = new HttpRequestMessage(HttpMethod.Put, uploadSession.UploadUrl)
            {
                Content = new ByteArrayContent(buffer, 0, bytesRead),
            };
            chunkRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            chunkRequest.Content.Headers.ContentRange = new ContentRangeHeaderValue(offset, offset + bytesRead - 1, totalLength);
            chunkRequest.Content.Headers.ContentLength = bytesRead;

            using var chunkResponse = await HttpClient.SendAsync(chunkRequest, cancellationToken);
            if (chunkResponse.StatusCode == HttpStatusCode.Created || chunkResponse.StatusCode == HttpStatusCode.OK)
            {
                var responsePayload = await chunkResponse.Content.ReadAsStringAsync(cancellationToken);
                var uploadResult = JsonSerializer.Deserialize<UploadResult>(responsePayload, JsonOptions());
                webUrl = uploadResult?.WebUrl;
                break;
            }

            chunkResponse.EnsureSuccessStatusCode();
            offset += bytesRead;
        }

        return webUrl ?? string.Empty;
    }

    private static async Task<string> GetGraphAccessTokenAsync(CancellationToken cancellationToken)
    {
        var tenantId = GetRequiredEnv("GRAPH_TENANT_ID");
        var clientId = GetRequiredEnv("GRAPH_CLIENT_ID");
        var clientSecret = GetRequiredEnv("GRAPH_CLIENT_SECRET");

        var tokenUrl = $"https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token";
        using var tokenRequest = new HttpRequestMessage(HttpMethod.Post, tokenUrl)
        {
            Content = new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["grant_type"] = "client_credentials",
                ["client_id"] = clientId,
                ["client_secret"] = clientSecret,
                ["scope"] = "https://graph.microsoft.com/.default",
            }),
        };

        using var response = await HttpClient.SendAsync(tokenRequest, cancellationToken);
        response.EnsureSuccessStatusCode();
        var payload = await response.Content.ReadAsStringAsync(cancellationToken);
        var tokenResponse = JsonSerializer.Deserialize<TokenResponse>(payload, JsonOptions());
        if (string.IsNullOrWhiteSpace(tokenResponse?.AccessToken))
        {
            throw new InvalidOperationException("Token response missing access token.");
        }

        return tokenResponse.AccessToken;
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

    private static JsonSerializerOptions JsonOptions() => new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private sealed record CombineRequest(
        [property: JsonPropertyName("csv_files")] List<CsvFile> CsvFiles,
        [property: JsonPropertyName("output_filename")] string? OutputFilename);

    private sealed record CsvFile(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("url")] string Url);

    private sealed record TokenResponse([property: JsonPropertyName("access_token")] string AccessToken);

    private sealed record UploadSession([property: JsonPropertyName("uploadUrl")] string UploadUrl);

    private sealed record UploadResult([property: JsonPropertyName("webUrl")] string? WebUrl);
}
