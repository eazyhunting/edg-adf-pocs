namespace ActivityReportsAutomation.Options;

public sealed class ActivityReportsOptions
{
    public string KeyVaultUri { get; set; } = string.Empty;
    public string StorageUrlSecretName { get; set; } = "StorageUrl";
    public string SharePointUploadUrlSecretName { get; set; } = "SharePointUploadUrl";
    public string SharePointAccessTokenSecretName { get; set; } = "SharePointAccessToken";
    public string EnvironmentName { get; set; } = "dev";
}
