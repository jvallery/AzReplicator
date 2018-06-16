// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGridExtensionConfig?functionName={functionname}

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using AzReplicatorLibrary;
using System;
using AzReplicatorLibrary.DataLayer;
using AzReplicatorLibrary.TableEntities;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Threading.Tasks;

namespace AzReplicatorFunc
{

    public static class AzReplicatorEventGrid
    {
        [FunctionName("AzReplicatorEventGrid")]
        public static async void Run([EventGridTrigger]EventGridEvent eventGridEvent, TraceWriter log)
        {
            string storageAccountConnectionString = Environment.GetEnvironmentVariable("MasterStorageAccount");
            log.Info("EventGrid trigger function processed a request.");

            AzReplicatorEngine replicator = new AzReplicatorEngine(storageAccountConnectionString);
            await replicator.insertEventGridEventAsync(JsonConvert.SerializeObject(eventGridEvent));

            if(eventGridEvent.EventType == "Microsoft.Storage.BlobCreated")
            {
                await replicator.handleEventGridBlobCreated(eventGridEvent.Data.ToString());
            }

        }
    }
}
