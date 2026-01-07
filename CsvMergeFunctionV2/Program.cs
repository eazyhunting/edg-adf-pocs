using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

var builder = FunctionsApplication.CreateBuilder(args);

builder.ConfigureFunctionsWebApplication();

builder.Services.AddOptions<BlobStorageOptions>()
    .Bind(builder.Configuration.GetSection("BlobStorage"))
    .Validate(options => !string.IsNullOrWhiteSpace(options.ConnectionString), "BlobStorage:ConnectionString is required.")
    .ValidateOnStart();

builder.Services.AddSingleton(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    var options = sp.GetRequiredService<IOptions<BlobStorageOptions>>().Value;
    var blobConnectionString = options.ConnectionString
        ?? configuration.GetConnectionString("BlobStorage")
        ?? configuration["AzureWebJobsStorage"]
        ?? configuration["BlobStorageConnectionString"];

    if (string.IsNullOrWhiteSpace(blobConnectionString))
    {
        throw new InvalidOperationException(
            "Blob storage connection string not configured. Set BlobStorage:ConnectionString, AzureWebJobsStorage, BlobStorageConnectionString, or ConnectionStrings:BlobStorage.");
    }

    return new BlobServiceClient(blobConnectionString);
});

// Application Insights isn't enabled by default. See https://aka.ms/AAt8mw4.
// builder.Services
//     .AddApplicationInsightsTelemetryWorkerService()
//     .ConfigureFunctionsApplicationInsights();

builder.Build().Run();

internal sealed class BlobStorageOptions
{
    public string? ConnectionString { get; set; }
}
