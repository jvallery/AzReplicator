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
        public string eventJson {get; set; }

        public AzReplicatorEventGridEvent() {
            PartitionKey = DateTime.Today.ToUniversalTime().Ticks.ToString();
            RowKey = DateTime.Now.ToUniversalTime().Ticks.ToString();
        }



    }
}
