using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzReplicatorLibrary.DataLayer
{

    public class TableStorage<T> where T : TableEntity, new()
    {
        public CloudStorageAccount storageAccount; 
        public CloudTableClient tableClient;
        public CloudTable table;

        public static Dictionary<string, bool> tableExists = new Dictionary<string, bool>();

        public TableStorage(CloudStorageAccount account, string tableName)
        {
            // Create the table client.
            storageAccount = account;
            tableClient = storageAccount.CreateCloudTableClient();
            table = tableClient.GetTableReference(tableName);

            // Create the table if it doesn't exist and keep reference in a local static collection so we're not hitting table storage all the time.
            if (!tableExists.ContainsKey(tableName))
            {
                try
                {
                 //   table.CreateIfNotExists();
                   // tableExists.Add(tableName, true);
                } catch (Exception ex)
                {
                    Logger.TrackException(ex, 0, "Error creating table");
                }
            }

        }

        //INSERT operations


        public async void InsertOrReplaceBatch(List<T> entities)
        {
            TableBatchOperation batchCreateOperation = new TableBatchOperation();

            int x = 0;
            foreach (T entity in entities)
            {
                x++;
                batchCreateOperation.Add(TableOperation.InsertOrReplace(entity));

                if (batchCreateOperation.Count() >= 100 || x >= entities.Count)
                {
                    await table.ExecuteBatchAsync(batchCreateOperation);
                    batchCreateOperation.Clear();
                }
            }

        }
        public async Task<T> InsertOrReplaceAsync(T entity)
        {
            TableOperation insertOperation = TableOperation.InsertOrReplace(entity);
            await table.ExecuteAsync(insertOperation);
            return entity;
        }

        public async Task<T> InsertAsync(T entity)
        {
            TableOperation insertOperation = TableOperation.Insert(entity);
            await table.ExecuteAsync(insertOperation);
            return entity;
        }

        public async Task<T> InsertOrMergeASync(T entity)
        {
            TableOperation insertOperation = TableOperation.InsertOrMerge(entity);
            await table.ExecuteAsync(insertOperation);
            return entity;
        }


        //GET OPERATIONS
        public async Task<List<T>> GetAllAsync()
        {

            TableContinuationToken continuationToken = null;
            TableQuery<T> query = new TableQuery<T>();
            List<T> results = new List<T>();
            do
            {
                var response = await table.ExecuteQuerySegmentedAsync(query, continuationToken);
                continuationToken = response.ContinuationToken;
                results.AddRange(response.Results);
            }
            while (continuationToken != null);

            return results;
        }
        public async Task<List<T>> GetAllForPartitionAsync(string partitionKey)
        {            
            List<T> results = new List<T>();
            if (results.Count == 0)
            {
                TableContinuationToken continuationToken = null;
                TableQuery<T> query = new TableQuery<T>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));
         
                do
                {
                    var response = await table.ExecuteQuerySegmentedAsync(query, continuationToken);
                    continuationToken = response.ContinuationToken;
                    results.AddRange(response.Results);
                }
                while (continuationToken != null);

            }
            return results;
        }
        


        public async Task<T> GetSingleAsync(string partitionKey, string rowKey)
        {

            string key = string.Format("{0}:{1}:{2}", typeof(T).Name, partitionKey, rowKey);

            T entity = null;

            TableQuery<T> query = new TableQuery<T>().Where(
                TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey)));

            var tableSet = await table.ExecuteQuerySegmentedAsync(query, null);

            if (tableSet.Count<T>() >= 1)
            {
                entity = tableSet.First();
            }


            return entity;
        }

        //DELETE operations
        public async Task Delete(T entity)
        {
            TableOperation deleteOperation = TableOperation.Delete(entity);
            deleteOperation.Entity.ETag = "*";
            await table.ExecuteAsync(deleteOperation);

        }
    }
}
