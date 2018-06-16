using AzReplicatorLibrary;
using System;
using System.Threading.Tasks;

namespace AzReplicatorTestHarness
{
    class Program
    {
        static  void Main(string[] args)
        {
            
            string masterConnectionString = "xxx";
            AzReplicatorEngine replicator = new AzReplicatorEngine(masterConnectionString);
            replicator.refreshCopyJobAsync().Wait();

        }
    }
}
