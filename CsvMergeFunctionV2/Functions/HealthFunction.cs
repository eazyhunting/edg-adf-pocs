using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;

namespace CsvMergeFunctionV2.Functions;

public class HealthFunction
{
    [Function("Health")]
    public HttpResponseData Run([HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequestData request)
    {
        var response = request.CreateResponse(System.Net.HttpStatusCode.OK);
        return response;
    }
}
