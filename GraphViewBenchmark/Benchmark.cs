using System;
using System.Configuration;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using StartAzureBatch;

namespace GraphViewBenchmark
{
    public class Benchmark
    {
        public static void Main(string[] args)
        {
            string batchAccountName = ConfigurationManager.AppSettings["BatchAccountName"];
            string batchAccountKey = ConfigurationManager.AppSettings["BatchAccountKey"];
            string batchAccountUrl = ConfigurationManager.AppSettings["BatchAccountUrl"];
            string storageAccountName = ConfigurationManager.AppSettings["StorageAccountName"];
            string storageAccountKey = ConfigurationManager.AppSettings["StorageAccountKey"];
            string poolId = "BenchmarkPool";
            int virtualMachineNumber = 8;
            string virtualMachineSize = "Medium"; // virtual machine size information: https://docs.microsoft.com/en-us/azure/cloud-services/cloud-services-sizes-specs

            AzureBatchJobManager jobManager = new AzureBatchJobManager(batchAccountName, batchAccountKey, batchAccountUrl,
                storageAccountName, storageAccountKey, poolId, virtualMachineNumber, virtualMachineSize);

            //jobManager.DeletePool(); return;

            string docDBEndPoint = ConfigurationManager.AppSettings["DocDBEndPoint"];
            string docDBKey = ConfigurationManager.AppSettings["DocDBKey"];
            string docDBDatabaseId = ConfigurationManager.AppSettings["DocDBDatabaseId"];
            string docDBCollectionId = ConfigurationManager.AppSettings["DocDBCollectionId-TwitterLists"];

            List<int> parallelismList = new List<int>() { 1, 4, 8 };

            foreach (int parallelism in parallelismList)
            {
                List<NodePlan> nodePlans = new List<NodePlan>();
                int id = 1;
                for (int i = 0; i < parallelism; i++)
                {
                    List<string> partitionValues = new List<string>();
                    HashSet<int> inValues = new HashSet<int>();
                    for (int j = 0; j < LoadData.PARTITION_NUM / parallelism; j++)
                    {
                        partitionValues.Add(id.ToString());
                        inValues.Add(id);
                        id++;
                    }
                    while (i == parallelism - 1 && id < LoadData.PARTITION_NUM)
                    {
                        partitionValues.Add(id.ToString());
                        inValues.Add(id);
                        id++;
                    }

                    NodePlan plan = new NodePlan(
                        LoadData.PARTITION_BY_KEY,
                        partitionValues,
                        new PartitionPlan(inValues, null));
                    nodePlans.Add(plan);
                }
                GraphViewAzureBatchJob job = new GraphViewAzureBatchJob(parallelism, nodePlans, 
                    docDBEndPoint, docDBKey, docDBDatabaseId, docDBCollectionId,
                    LoadData.USE_REVERSE_EDGE, LoadData.PARTITION_BY_KEY, LoadData.SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI);

                job.Traversal = job.Command.g().V().Count();
                List<string> results = jobManager.TestQuery(job);
                foreach (var res in results)
                {
                    Console.WriteLine(res);
                }
            }
        }
    }
}
