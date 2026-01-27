using ActivityReportsAutomation.Options;
using ActivityReportsAutomation.Services;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices((context, services) =>
    {
        services.AddApplicationInsightsTelemetryWorkerService();
        services.ConfigureFunctionsApplicationInsights();

        services.AddOptions<ActivityReportsOptions>()
            .Bind(context.Configuration.GetSection("ActivityReports"))
            .PostConfigure(options =>
            {
                if (string.IsNullOrWhiteSpace(options.EnvironmentName))
                {
                    options.EnvironmentName = Environment.GetEnvironmentVariable("AZURE_FUNCTIONS_ENVIRONMENT") ?? "dev";
                }
            });

        services.AddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<ActivityReportsOptions>>().Value;
            if (string.IsNullOrWhiteSpace(options.KeyVaultUri))
            {
                throw new InvalidOperationException("ActivityReports:KeyVaultUri is required.");
            }

            return new SecretClient(new Uri(options.KeyVaultUri), new DefaultAzureCredential());
        });

        services.AddSingleton<IReportStorageService, ReportStorageService>();
        services.AddSingleton<ISharePointUploadService, SharePointUploadService>();
        services.AddHttpClient();
    })
    .Build();

host.Run();
