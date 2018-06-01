using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace AzReplicatorLibrary.DataLayer
{
    public class QueueStorage<T>
    {
        CloudStorageAccount storageAccount;
        CloudQueueClient queueClient;
        CloudQueue queue;

        public Nullable<int> approximateMessageCount
        {
            get
            {
                return queue.ApproximateMessageCount;
            }
        }

        public QueueStorage(string queuename) : this(queuename, Common.storageAccountConnectionString) { }

        public QueueStorage(string queuename, string storageAccountConnectionString)
        {
            storageAccount = CloudStorageAccount.Parse(storageAccountConnectionString);
            queueClient = storageAccount.CreateCloudQueueClient();
            queue = queueClient.GetQueueReference(queuename);
        }

        /*
        public CloudQueueMessage EnqueueMessage(T message)
        {
            CloudQueueMessage cloudMessage = new CloudQueueMessage(Common.SerializeToJson(message));
            queue.AddMessage(cloudMessage);
            return cloudMessage;
        }
        */

        public async Task<CloudQueueMessage> DequeueMessageAsync(TimeSpan ttl)
        {
            return await queue.GetMessageAsync(ttl, null, null);
        }

        public async void DeleteMessageAsync(CloudQueueMessage message)
        {
           await queue.DeleteMessageAsync(message);
        }

        public async void UpdateMessageTTL(CloudQueueMessage message, TimeSpan ttl)
        {
           await queue.UpdateMessageAsync(message, ttl, MessageUpdateFields.Visibility);
        }



    }
}
