using AzReplicatorLibrary.DataLayer;
using AzReplicatorLibrary.TableEntities;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AzReplicatorLibrary
{
    public class AzReplicatorEngine
    {
        CloudStorageAccount masterStorageAccount;

        public AzReplicatorEngine(string masterStorageConnectionString)
        {
            masterStorageAccount = CloudStorageAccount.Parse(masterStorageConnectionString);            
        }

        public AzReplicatorEngine(CloudStorageAccount account)
        {
            masterStorageAccount = account;
        }


        public async Task handleEventGridBlobCreated(string eventGridBlobCreatedAsJson)
        {
            
            //Handle PutBlob/PutBlockList 
            BlobEventGridData blobEvent = JsonConvert.DeserializeObject<BlobEventGridData>(eventGridBlobCreatedAsJson);
            if (blobEvent.api == "PutBlockList" || blobEvent.api == "PutBlob")
            {
                CloudBlob sourceBlob = new CloudBlob(new Uri(blobEvent.url));
                BlobStorage sourceBlobStorage = new BlobStorage(masterStorageAccount, sourceBlob.Container.Name);
                Uri sourceBlobUri = sourceBlobStorage.GetReadBlobSasUri(sourceBlob.Name, 24);

                TableStorage<ReplicationStorageAccount> accountTableStorage = new TableStorage<ReplicationStorageAccount>(masterStorageAccount, "accounts");
                var accounts = await accountTableStorage.GetAllAsync();

                Parallel.ForEach(accounts, async (targetAccountEntity) =>
                {
                    CloudStorageAccount targetAccount = CloudStorageAccount.Parse(targetAccountEntity.connectionString);
                    CloudBlobClient targetBlobClient = targetAccount.CreateCloudBlobClient();
                    CloudBlobContainer targetContainer = targetBlobClient.GetContainerReference(sourceBlob.Container.Name);
                    await targetContainer.CreateIfNotExistsAsync();
                    
                    CloudBlockBlob targetBlob = targetContainer.GetBlockBlobReference(sourceBlob.Name);
                    await targetBlob.StartCopyAsync(sourceBlobUri);

                    await targetBlob.FetchAttributesAsync();
                    CopyState state = targetBlob.CopyState;

                    TableStorage<CopyJob> copyJobtableStorage = new TableStorage<CopyJob>(masterStorageAccount, "jobs");
                    CopyJob job = new CopyJob(state, targetContainer.Name, targetBlob.Name, targetBlob.Uri.ToString(), targetAccountEntity.RowKey);
                    await copyJobtableStorage.InsertOrReplaceAsync(job);
                    
                    //log.Info($"Copyiing {blobUri} to {targetAccountEntity.accountName} in region {targetAccountEntity.region}");
                });

            }
            //Handle copy complete 
            if (blobEvent.api == "CopyBlob")
            {

            }
        }
        
        public async Task insertEventGridEventAsync(string eventGridEventasJson)            
        {
            try
            {
                EventGridEventObj eventObj = JsonConvert.DeserializeObject<EventGridEventObj>(eventGridEventasJson);
                TableStorage<AzReplicatorEventGridEvent> tableStorage = new TableStorage<AzReplicatorEventGridEvent>(masterStorageAccount, "events");
                AzReplicatorEventGridEvent tableEvent = new AzReplicatorEventGridEvent(eventObj.id);
                tableEvent.id = eventObj.id;
                tableEvent.topic = eventObj.topic;
                tableEvent.subject = eventObj.subject;
                tableEvent.eventTime = eventObj.eventTime;
                tableEvent.eventType = eventObj.eventType;
                tableEvent.dataVersion = eventObj.dataVersion;
                tableEvent.metadataVersion = eventObj.metadataVersion;
                  

                    tableEvent.eventJson = eventGridEventasJson;
                    await tableStorage.InsertAsync(tableEvent);
                }
                catch (StorageException ex)
                {
                   // log.Info($"Exception writing to table: {ex.Message}");
                }
            
            return;
        }

        public async Task refreshCopyJobAsync()
        {
            TableStorage<CopyJob> copyJobTableStorage = new TableStorage<CopyJob>(masterStorageAccount, "jobs");
            TableStorage<CopyJob> copyCompleteJobTableStorage = new TableStorage<CopyJob>(masterStorageAccount, "completejobs");
            TableStorage<ReplicationStorageAccount> accountTableStorage = new TableStorage<ReplicationStorageAccount>(masterStorageAccount, "accounts");
            List<ReplicationStorageAccount> accounts = await accountTableStorage.GetAllAsync();

            List<Task> tasks = new List<Task>();

            Parallel.ForEach<ReplicationStorageAccount>(accounts, async (account) =>
            {
                TableContinuationToken continuationToken = null;
                TableQuery<CopyJob> query = new TableQuery<CopyJob>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, account.RowKey));
                CloudStorageAccount targetStorageAccount = CloudStorageAccount.Parse(account.connectionString);
                CloudBlobClient blobClient = targetStorageAccount.CreateCloudBlobClient();
                do
                {
                    var response = await copyJobTableStorage.table.ExecuteQuerySegmentedAsync(query, continuationToken);
                    continuationToken = response.ContinuationToken;

                    tasks.Add(Task.Run(async () =>
                    {
                        foreach (var job in response.Results)
                        {

                            CloudBlobContainer container = blobClient.GetContainerReference(job.targetContainer);
                            CloudBlockBlob blob = container.GetBlockBlobReference(job.targetBlobName);

                            await blob.FetchAttributesAsync();
                            CopyState state = blob.CopyState;
                            job.Update(state);

                            //log.Info($"Updated {job.copyId} -  status {job.copyStatus}");


                            if (job.copyStatus == "Success")
                            {
                                try
                                {
                                    await copyCompleteJobTableStorage.InsertOrReplaceAsync(job);
                                    await copyJobTableStorage.Delete(job);
                                    //log.Info($"Deleted completed job {job.copyId}");
                                }
                                catch (StorageException ex)
                                {
                                   // log.Info($"Exception deleting CopyJob {job.copyId}: {ex.Message}");
                                }
                            }
                            else
                            {
                                await copyJobTableStorage.InsertOrReplaceAsync(job);
                            }
                        }
                    }));
                }
                while (continuationToken != null);

            });

            Task.WaitAll(tasks.ToArray());
        }


    }
}
