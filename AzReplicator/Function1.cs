
using System.IO;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using AzReplicatorLibrary.DataLayer;
using AzReplicatorLibrary.TableEntities;
using System;
using System.Threading.Tasks;
using AzReplicatorLibrary;

namespace AzReplicator
{
    public static class Function1
    {
        [FunctionName("Function1")]
        public async static Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]HttpRequest req, TraceWriter log)
        {
            
            Common.storageAccountConnectionString = "DefaultEndpointsProtocol=https;AccountName=geppettomaster;AccountKey=cI7++Gu/uNvWHCKoqy29feeb6duugqNnczi9sc/h6iWwn9MbbG3JW6mLfiibJATOnqWicx9IepfBKryCELc0Aw==;EndpointSuffix=core.windows.net";
            log.Info("C# HTTP trigger function processed a request.");

            string requestBody = new StreamReader(req.Body).ReadToEnd();

            try
            {
                TableStorage<AzReplicatorEventGridEvent> tableStorage = new TableStorage<AzReplicatorEventGridEvent>("events");
                AzReplicatorEventGridEvent tableEvent = new AzReplicatorEventGridEvent();
                tableEvent.eventJson = requestBody;

                //dataObject.ToString();
               await tableStorage.InsertAsync(tableEvent);
            }
            catch (Exception ex)
            {
                log.Info($"Exception writing to table: {ex.Message}");
            }
            
            return (ActionResult)new OkObjectResult("Okay");              
        }
    }
}
