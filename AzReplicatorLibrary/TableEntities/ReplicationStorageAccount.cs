using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzReplicatorLibrary.TableEntities
{
    public class ReplicationStorageAccount : TableEntity
    {

        public string accountName { get; set; }
        public string region { get; set; }        
        public string connectionString { get; set; }

        public ReplicationStorageAccount() { }
        public ReplicationStorageAccount(string lConnectionString, string lRegion)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(lConnectionString);
            RowKey = Common.MD5(storageAccount.BlobEndpoint.ToString());
            PartitionKey = "account";          
            connectionString = lConnectionString;
            accountName = storageAccount.BlobEndpoint.Host.ToString();
            region = lRegion;            
        }

    }
}
