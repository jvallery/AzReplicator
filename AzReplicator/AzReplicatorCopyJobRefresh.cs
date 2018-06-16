using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using AzReplicatorLibrary;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;


namespace AzReplicatorFunc
{
    public static class AzReplicatorCopyJobRefresh
    {
        [FunctionName("AzReplicatorCopyJobRefresh")]
        public static async void Run([TimerTrigger("0/30 * * * * *")]TimerInfo myTimer, TraceWriter log)
        {

            log.Info($"Refresh CopyJob trigger function executed at: {DateTime.Now}");
            string storageAccountConnectionString = Environment.GetEnvironmentVariable("MasterStorageAccount");

            AzReplicatorEngine replicator = new AzReplicatorEngine(storageAccountConnectionString);
            await replicator.refreshCopyJobAsync();

        }
    }
}
