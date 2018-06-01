using AzReplicatorLibrary.DataLayer;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzReplicatorLibrary.TableEntities
{
    public class CopyJob : TableEntity
    {
        public string targetStorageAccount { get; set; }
        public string targetContainer { get; set; }
        public string targetBlobName { get; set; }
        public string targetUri { get; set; }
        public string sourceUri { get; set; }
        public string copyId { get; set; }
        public string copyStatus { get; set; }

        //CopyState
        public string copyStatusDescription { get; set; }  
        public Nullable<long> bytesCopied { get; set; }
        public Nullable<long> totalBytes { get; set; }
        public Nullable<DateTime> completionTime { get; set; }
        public Nullable<DateTime> destinationSnapshotTime { get; set; }

        public CopyJob() { }
        public CopyJob(CopyState state, string lTargetContainer, string lTargetBlobName, string lTargetUri, string targetStorageAccountId)
        {
            
            RowKey = state.CopyId;
            PartitionKey = targetStorageAccountId;
            targetStorageAccount = targetStorageAccountId;
            targetContainer = lTargetContainer;
            targetBlobName = lTargetBlobName;
            targetUri = lTargetUri;
            sourceUri = state.Source.ToString();
            Update(state);
        }

        private void Update(CopyState state)
        {
            copyId = state.CopyId;
            copyStatus = state.Status.ToString();
            copyStatusDescription = state.StatusDescription;
            bytesCopied = state.BytesCopied;
            totalBytes = state.TotalBytes;

            if (state.CompletionTime.HasValue)
                completionTime = (state.CompletionTime.Value).DateTime;

            if (state.DestinationSnapshotTime.HasValue)
                destinationSnapshotTime = (state.DestinationSnapshotTime.Value).DateTime;

        }

        public async void RefreshStatus()
        {
            TableStorage<ReplicationStorageAccount> tableStorage = new TableStorage<ReplicationStorageAccount>("accounts");

            var account = await tableStorage.GetSingleAsync("account", targetStorageAccount);
            if(account != null)
            {               
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(account.connectionString);                
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer container = blobClient.GetContainerReference(targetContainer);
                CloudBlockBlob blob = container.GetBlockBlobReference(targetBlobName);
              
                await blob.FetchAttributesAsync();
                CopyState state = blob.CopyState;
                Update(state);

                TableStorage<CopyJob> tableCopyJobStorage = new TableStorage<CopyJob>("jobs");
                await tableCopyJobStorage.InsertOrReplaceAsync(this);

            }
          
          
        }



    }
}
