using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzReplicatorLibrary.TableEntities
{
    public class AzReplicatorEventGridEvent : TableEntity
    {
        public string eventJson { get; set; }
        public string subject { get; set; }
        public string eventType { get; set; }
        public DateTime eventTime { get; set; }
        public string id { get; set; }
        public string dataVersion { get; set; }
        public string metadataVersion { get; set; }

        public string topic { get; set; }

        public AzReplicatorEventGridEvent() { }
        public AzReplicatorEventGridEvent(string id)
        {
            PartitionKey = DateTime.Today.ToUniversalTime().Ticks.ToString();
            RowKey = id;
        }
    }
}
