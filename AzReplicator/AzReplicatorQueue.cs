using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using AzReplicator.AWS.Models;
using Newtonsoft.Json;

namespace AzReplicatorFunc
{
    public static class AzReplicatorQueue
    {
        [FunctionName("AzReplicatorQueue")]
        public static void Run(
            [QueueTrigger("newfiles", Connection = "QueueConnectionString")]string queueMessage, 
            TraceWriter log)
        {
            log.Info($"C# Queue trigger function processed: {queueMessage}");

            // Read the message from the queue and extract the file event
            // details we need to initiate the replication from the 
            // URL to a container.
            var fileEvent = JsonConvert.DeserializeObject<GridEvent<FileEvent>>(queueMessage);

            // Copy the URL to the master storage account
            var url = fileEvent.Data.Url;
            var filename = fileEvent.Data.Name;
            // TODO: Implement
        }
    }
}
