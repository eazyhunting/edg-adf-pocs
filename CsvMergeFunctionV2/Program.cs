using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = FunctionsApplication.CreateBuilder(args);

builder.ConfigureFunctionsWebApplication();

var blobConnectionString = builder.Configuration.GetConnectionString("BlobStorage")
    ?? builder.Configuration["AzureWebJobsStorage"]
    ?? builder.Configuration["BlobStorageConnectionString"];

if (string.IsNullOrWhiteSpace(blobConnectionString))
{
    throw new InvalidOperationException(
        "Blob storage connection string not configured. Set AzureWebJobsStorage, BlobStorageConnectionString, or ConnectionStrings:BlobStorage.");
}

builder.Services.AddSingleton(new BlobServiceClient(blobConnectionString));

// Application Insights isn't enabled by default. See https://aka.ms/AAt8mw4.
// builder.Services
//     .AddApplicationInsightsTelemetryWorkerService()
//     .ConfigureFunctionsApplicationInsights();

builder.Build().Run();
