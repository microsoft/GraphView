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
            string virtualMachineSize = "Large";// "Large", "Medium", "Small"; // virtual machine size information: https://docs.microsoft.com/en-us/azure/cloud-services/cloud-services-sizes-specs

            AzureBatchJobManager jobManager = new AzureBatchJobManager(batchAccountName, batchAccountKey, batchAccountUrl,
                storageAccountName, storageAccountKey, poolId, virtualMachineNumber, virtualMachineSize);

            //return;
            //jobManager.ClearResource(); return;
            //jobManager.DeletePool(); return;

            string docDBEndPoint = ConfigurationManager.AppSettings["DocDBEndPoint"];
            string docDBKey = ConfigurationManager.AppSettings["DocDBKey"];
            string docDBDatabaseId = ConfigurationManager.AppSettings["DocDBDatabaseId"];
            string docDBCollectionId = ConfigurationManager.AppSettings["DocDBCollectionId-TwitterLists"];

            List<int> parallelismList = new List<int>() { 1, 2, 4, 8 };
            List<ParallelLevel> parallelLevelList = new List<ParallelLevel>()
            {
                new ParallelLevel(), // all accessive
                new ParallelLevel(true, true, true, 1000), // middle
                new ParallelLevel(true, true, true, 2500), // middle
                new ParallelLevel(true, true, true, 5000), // middle
                new ParallelLevel(true, true, true, 10000), // middle
                new ParallelLevel(true, true, true, 15000), // middle
                new ParallelLevel(true, true, true, 30000), // middle
                new ParallelLevel(true, true, true), // exclusive
            };

            using (System.IO.StreamWriter file =
                new System.IO.StreamWriter(@"..\..\BenchmarkResult.txt"))
            {
                foreach (ParallelLevel parallelLevel in parallelLevelList)
                {
                    foreach (int parallelism in parallelismList)
                    {
                        List<NodePlan> nodePlans = new List<NodePlan>();
                        int id = 0;
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

                        GraphViewAzureBatchJob job = new GraphViewAzureBatchJob(parallelism, nodePlans, parallelLevel,
                            docDBEndPoint, docDBKey, docDBDatabaseId, docDBCollectionId,
                            LoadData.USE_REVERSE_EDGE, LoadData.PARTITION_BY_KEY, LoadData.SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI);

                        var watch = System.Diagnostics.Stopwatch.StartNew();
                        List<string> results = new List<string>();

                        try
                        {
                            // Query 1
                            watch = System.Diagnostics.Stopwatch.StartNew();
                            job.Traversal = job.Command.g().V().Count();
                            results = jobManager.TestQuery(job);
                            watch.Stop();
                            file.WriteLine($"Query g.V().count(), " +
                                           $"ParallelLevel: {parallelLevelList.IndexOf(parallelLevel)}, " +
                                           $"parallelism: {parallelism}, " +
                                           $"Total use time: {watch.ElapsedMilliseconds}, " +
                                           $"Task running time: {job.UseTime}");
                        }
                        catch (Exception)
                        {
                            file.WriteLine($"Query g.V().count(), " +
                                           $"ParallelLevel: {parallelLevelList.IndexOf(parallelLevel)}, " +
                                           $"parallelism: {parallelism}, Error");
                        }

                        try
                        {
                            // Query 2
                            watch = System.Diagnostics.Stopwatch.StartNew();
                            job.Traversal = job.Command.g().V().Out().Count();
                            results = jobManager.TestQuery(job);
                            watch.Stop();
                            file.WriteLine($"Query g.V().out().count(), " +
                                           $"ParallelLevel: {parallelLevelList.IndexOf(parallelLevel)}, " +
                                           $"parallelism: {parallelism}, " +
                                           $"Total use time: {watch.ElapsedMilliseconds}, " +
                                           $"Task running time: {job.UseTime}");
                        }
                        catch (Exception)
                        {
                            file.WriteLine($"Query g.V().out().count(), " +
                                           $"ParallelLevel: {parallelLevelList.IndexOf(parallelLevel)}, " +
                                           $"parallelism: {parallelism}, Error");
                        }

                        try
                        {
                            // Query 3
                            watch = System.Diagnostics.Stopwatch.StartNew();
                            job.Traversal = job.Command.g().V().Out().Out().Count();
                            results = jobManager.TestQuery(job);
                            watch.Stop();
                            file.WriteLine($"Query g.V().out().out().count(), " +
                                           $"ParallelLevel: {parallelLevelList.IndexOf(parallelLevel)}, " +
                                           $"parallelism: {parallelism}, " +
                                           $"Total use time: {watch.ElapsedMilliseconds}, " +
                                           $"Task running time: {job.UseTime}");
                        }
                        catch (Exception)
                        {
                            file.WriteLine($"Query g.V().out().out().count(), " +
                                           $"ParallelLevel: {parallelLevelList.IndexOf(parallelLevel)}, " +
                                           $"parallelism: {parallelism}, Error");
                        }

                        try
                        {
                            // Query 4
                            watch = System.Diagnostics.Stopwatch.StartNew();
                            job.Traversal = job.Command.g().V().Map(GraphTraversal.__().Out().Count());
                            results = jobManager.TestQuery(job);
                            watch.Stop();
                            file.WriteLine($"Query g.V().map(__.out().count()), " +
                                           $"ParallelLevel: {parallelLevelList.IndexOf(parallelLevel)}, " +
                                           $"parallelism: {parallelism}, " +
                                           $"Total use time: {watch.ElapsedMilliseconds}, " +
                                           $"Task running time: {job.UseTime}");
                        }
                        catch (Exception)
                        {
                            file.WriteLine($"Query g.V().map(__.out().count()), " +
                                           $"ParallelLevel: {parallelLevelList.IndexOf(parallelLevel)}, " +
                                           $"parallelism: {parallelism}, Error");
                        }

                        // Query n...
                    }
                }
            }
        }
    }
}
