using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;
using System.Diagnostics;
using GraphViewUnitTest;
using System;
using GraphView;
using GraphViewUnitTest.Gremlin;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using Microsoft.Azure.Documents.Client;

namespace GraphViewConsoleTest
{
    class Program
    {
        static void Main(string[] args)
        {
            //partitionQueryTestCommon("MarvelTest");
            //Console.WriteLine("#######start PartitionTestCit test");
            //partitionQueryTestCommon("PartitionTestCit");
            //Console.WriteLine("#######start PartitionTestCitRep2 test");
            //partitionQueryTestCommon("PartitionTestCitRep2");
            //insertCitDataBulkInsert();
            //insertCitHashPartitionByBulkInsert();
            //insertCitGreedyPartitionByBulkInsert();
            //getStatistic();
            //insertInSamePartition();
            //insertCitHashPartitionByBulkInsert();
            //partitionQueryTestCommon("CitFakeDiffPartition", "CitFakeDiffPartition");
            //partitionQueryTestCommon("CitFakeDiffPartition", "CitFakeSamePartition");
            //partitionQueryTestCommon("CitHashPartition1000item", "CitHashPartition1000item");
            //partitionQueryTestCommon("CitHashPartition1000item", "CitGreedyPartition1000item");
            //insertDiffKeyPartitionBulkInsert("PartitionTest_100Key", 100);
            //insertDiffKeyPartitionBulkInsert("PartitionTest_50Key",50);
            //insertDiffKeyPartitionBulkInsert("PartitionTest_10Key", 10);
            //insertDiffKeyPartitionBulkInsert("PartitionTest_3Key", 3);
            //insertDiffKeyPartitionBulkInsert("PartitionTest_3Key", 1);
            //partitionQueryTestCommon("PartitionTest_3Key", "PartitionTest_100Key");
            //partitionQueryTestCommon("PartitionTest_3Key", "PartitionTest_50Key");
            //partitionQueryTestCommon("PartitionTest_3Key", "PartitionTest_10Key");
            //partitionQueryTestCommon("PartitionTest_3Key", "PartitionTest_3Key");
            //partitionQueryTestCommon("PartitionTest_3Key", "PartitionTest_1Key");
            //insertControlPartitionkeyBulkInsert("PartitionQueryCheck_100", 100);
            //partitionQueryDiffPartitionTest("PartitionQueryCheck_100");
            //querySpecificIDsList("PartitionQueryCheck_100");
            querySpecificIDsListUseSystemCall_SameVertexCount_DiffPartitionNum("PartitionQueryCheck_100");
            Console.ReadLine();
        }

        public static void querySpecificIDsListUseSystemCall_SamePartitionCount_DiffVertexCount(String collectionName)
        {
            var queryOptions = new FeedOptions
            {
                EnableCrossPartitionQuery = true,
                MaxItemCount = 100000,
                EnableScanInQuery = true,
            };

            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
               "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
               "GroupMatch", collectionName, GraphType.GraphAPIOnly, false,
               1, null);
            GraphViewCommand graph = new GraphViewCommand(connection);
            //graph.OutputFormat = OutputFormat.GraphSON;
            // (0) warm query
            var vertexIds = new StringBuilder();
            var partitionIds = new HashSet<String>();
            var partitionIdStr = new StringBuilder();

            for (int p = 1; p < 2; p--)
            {
                for (int i = 0; i < 1; i++)
                {
                    //vertexIds.Add(p + "-" + i);
                    var id = p + "-" + i;
                    vertexIds.Append("'" + id + "',");
                    if (!partitionIds.Contains(p.ToString()))
                    {
                        partitionIds.Add(p.ToString());
                        partitionIdStr.Append("'" + p + "',");
                    }
                }
            }
            vertexIds.Remove(vertexIds.Length - 1, 1);
            partitionIdStr.Remove(partitionIdStr.Length - 1, 1);

            var start0 = Stopwatch.StartNew();
            //var results = graph.g().V(vertexIds).Next();
            var results0 = connection.ExecuteQuery("SELECT N_3 FROM Node N_3  WHERE IS_DEFINED(N_3._isEdgeDoc) = false AND(N_3.id in (" + vertexIds + ")) AND (N_3._partition in (" + partitionIdStr + "))", queryOptions);

            foreach (var result in results0)
            {
                //Console.WriteLine(result);
                var t = result;
            }

            start0.Stop();
            Console.WriteLine("warm query" + (start0.ElapsedMilliseconds) + "ms");

            // (1) query 1 partition
            //List<Object> vertexIds = new List<object>();
            //var vertexIds = new StringBuilder();
            //var partitionIds = new HashSet<String>();
            //var partitionIdStr = new StringBuilder();
            vertexIds.Clear();
            partitionIdStr.Clear();
            partitionIds.Clear();

            for (int p = 1; p < 2; p++)
            {
                for (int i = 0; i < 30; i++)
                {
                    //vertexIds.Add(p + "-" + i);
                    var id = p + "-" + i;
                    vertexIds.Append("'" + id + "',");
                    if (!partitionIds.Contains(p.ToString()))
                    {
                        partitionIds.Add(p.ToString());
                        partitionIdStr.Append("'" + p + "',");
                    }
                }
            }
            vertexIds.Remove(vertexIds.Length - 1, 1);
            partitionIdStr.Remove(partitionIdStr.Length - 1, 1);

            var start1 = Stopwatch.StartNew();
            //var results = graph.g().V(vertexIds).Next();
            var results = connection.ExecuteQuery("SELECT N_3 FROM Node N_3  WHERE IS_DEFINED(N_3._isEdgeDoc) = false AND(N_3.id in (" + vertexIds + ")) AND (N_3._partition in (" + partitionIdStr + "))", queryOptions);

            foreach (var result in results)
            {
                //Console.WriteLine(result);
                var t = result;
            }

            start1.Stop();
            Console.WriteLine("partition count" + 1 + "  " + collectionName + "(0)" + (start1.ElapsedMilliseconds) + "ms");

            // (2) 2 p
            vertexIds.Clear();
            partitionIdStr.Clear();
            partitionIds.Clear();
            for (int p = 1; p < 2; p++)
            {
                for (int i = 0; i < 50; i++)
                {
                    //vertexIds.Add(p + "-" + i);
                    var id = p + "-" + i;
                    vertexIds.Append("'" + id + "',");
                    if (!partitionIds.Contains(p.ToString()))
                    {
                        partitionIds.Add(p.ToString());
                        partitionIdStr.Append("'" + p + "',");
                    }
                }
            }
            vertexIds.Remove(vertexIds.Length - 1, 1);
            partitionIdStr.Remove(partitionIdStr.Length - 1, 1);

            var start2 = Stopwatch.StartNew();
            //var results = graph.g().V(vertexIds).Next();
            results = connection.ExecuteQuery("SELECT N_3 FROM Node N_3  WHERE IS_DEFINED(N_3._isEdgeDoc) = false AND(N_3.id in (" + vertexIds + ")) AND (N_3._partition in (" + partitionIdStr + "))", queryOptions);

            foreach (var result in results)
            {
                //Console.WriteLine(result);
                var t = result;
            }

            start2.Stop();
            Console.WriteLine("partition count" + 2 + "  " + collectionName + "(1)" + (start2.ElapsedMilliseconds) + "ms");

            // (3) 10 partition
            vertexIds.Clear();
            partitionIdStr.Clear();
            partitionIds.Clear();
            for (int p = 1; p < 2; p++)
            {
                for (int i = 0; i < 100; i++)
                {
                    //vertexIds.Add(p + "-" + i);
                    var id = p + "-" + i;
                    vertexIds.Append("'" + id + "',");
                    if (!partitionIds.Contains(p.ToString()))
                    {
                        partitionIds.Add(p.ToString());
                        partitionIdStr.Append("'" + p + "',");
                    }
                }
            }
            vertexIds.Remove(vertexIds.Length - 1, 1);
            partitionIdStr.Remove(partitionIdStr.Length - 1, 1);

            var start3 = Stopwatch.StartNew();
            //var results = graph.g().V(vertexIds).Next();
            results = connection.ExecuteQuery("SELECT N_3 FROM Node N_3  WHERE IS_DEFINED(N_3._isEdgeDoc) = false AND(N_3.id in (" + vertexIds + ")) AND (N_3._partition in (" + partitionIdStr + "))", queryOptions);

            foreach (var result in results)
            {
                //Console.WriteLine(result);
                var t = result;
            }

            start3.Stop();
            Console.WriteLine("partition count" + 10 + "  " + collectionName + "(2)" + (start3.ElapsedMilliseconds) + "ms");   
        }

        public static void querySpecificIDsListUseSystemCall_SameVertexCount_DiffPartitionNum(String collectionName)
        {
            var queryOptions = new FeedOptions
            {
                EnableCrossPartitionQuery = true,
                MaxItemCount = 100000,
                EnableScanInQuery = true,
            };

            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
               "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
               "GroupMatch", collectionName, GraphType.GraphAPIOnly, false,
               1, null);
            GraphViewCommand graph = new GraphViewCommand(connection);
            //graph.OutputFormat = OutputFormat.GraphSON;
            // (0) warm query
            var vertexIds = new StringBuilder();
            var partitionIds = new HashSet<String>();
            var partitionIdStr = new StringBuilder();

            for (int p = 100; p > 99; p--)
            {
                for (int i = 0; i < 1; i++)
                {
                    //vertexIds.Add(p + "-" + i);
                    var id = p + "-" + i;
                    vertexIds.Append("'" + id + "',");
                    if (!partitionIds.Contains(p.ToString()))
                    {
                        partitionIds.Add(p.ToString());
                        partitionIdStr.Append("'" + p + "',");
                    }
                }
            }
            vertexIds.Remove(vertexIds.Length - 1, 1);
            partitionIdStr.Remove(partitionIdStr.Length - 1, 1);

            var start0 = Stopwatch.StartNew();
            //var results = graph.g().V(vertexIds).Next();
            var results0 = connection.ExecuteQuery("SELECT N_3 FROM Node N_3  WHERE IS_DEFINED(N_3._isEdgeDoc) = false AND(N_3.id in (" + vertexIds + ")) AND (N_3._partition in (" + partitionIdStr + "))", queryOptions);

            foreach (var result in results0)
            {
                //Console.WriteLine(result);
                var t = result;
            }

            start0.Stop();
            Console.WriteLine("warm query" + (start0.ElapsedMilliseconds) + "ms");

            // (1) query 1 partition
            //List<Object> vertexIds = new List<object>();
            //var vertexIds = new StringBuilder();
            //var partitionIds = new HashSet<String>();
            //var partitionIdStr = new StringBuilder();
            vertexIds.Clear();
            partitionIdStr.Clear();
            partitionIds.Clear();

            for (int p = 1; p < 2; p++)
            {
                for (int i = 0; i < 30; i++)
                {
                    //vertexIds.Add(p + "-" + i);
                    var id = p + "-" + i;
                    vertexIds.Append("'" + id + "',");
                    if(!partitionIds.Contains(p.ToString()))
                    {
                        partitionIds.Add(p.ToString());
                        partitionIdStr.Append("'" + p + "',");
                    }
                }
            }
            vertexIds.Remove(vertexIds.Length - 1, 1);
            partitionIdStr.Remove(partitionIdStr.Length - 1, 1);

            var start1 = Stopwatch.StartNew();
            //var results = graph.g().V(vertexIds).Next();
            var results = connection.ExecuteQuery("SELECT N_3 FROM Node N_3  WHERE IS_DEFINED(N_3._isEdgeDoc) = false AND(N_3.id in (" + vertexIds + ")) AND (N_3._partition in (" + partitionIdStr + "))", queryOptions);

            foreach (var result in results)
            {
                //Console.WriteLine(result);
                var t = result;
            }

            start1.Stop();
            Console.WriteLine("partition count" + 1 + "  " + collectionName + "(0)" + (start1.ElapsedMilliseconds) + "ms");

            // (2) 2 p
            vertexIds.Clear();
            partitionIdStr.Clear();
            partitionIds.Clear();
            for (int p = 1; p < 3; p++)
            {
                for (int i = 0; i < 16; i++)
                {
                    //vertexIds.Add(p + "-" + i);
                    var id = p + "-" + i;
                    vertexIds.Append("'" + id + "',");
                    if (!partitionIds.Contains(p.ToString()))
                    {
                        partitionIds.Add(p.ToString());
                        partitionIdStr.Append("'" + p + "',");
                    }
                }
            }
            vertexIds.Remove(vertexIds.Length - 1, 1);
            partitionIdStr.Remove(partitionIdStr.Length - 1, 1);

            var start2 = Stopwatch.StartNew();
            //var results = graph.g().V(vertexIds).Next();
            results = connection.ExecuteQuery("SELECT N_3 FROM Node N_3  WHERE IS_DEFINED(N_3._isEdgeDoc) = false AND(N_3.id in (" + vertexIds + ")) AND (N_3._partition in (" + partitionIdStr + "))", queryOptions);

            foreach (var result in results)
            {
                //Console.WriteLine(result);
                var t = result;
            }

            start2.Stop();
            Console.WriteLine("partition count" + 2 + "  " + collectionName + "(1)" + (start2.ElapsedMilliseconds) + "ms");

            // (3) 10 partition
            vertexIds.Clear();
            partitionIdStr.Clear();
            partitionIds.Clear();
            for (int p = 1; p < 11; p++)
            {
                for (int i = 0; i < 3; i++)
                {
                    //vertexIds.Add(p + "-" + i);
                    var id = p + "-" + i;
                    vertexIds.Append("'" + id + "',");
                    if (!partitionIds.Contains(p.ToString()))
                    {
                        partitionIds.Add(p.ToString());
                        partitionIdStr.Append("'" + p + "',");
                    }
                }
            }
            vertexIds.Remove(vertexIds.Length - 1, 1);
            partitionIdStr.Remove(partitionIdStr.Length - 1, 1);

            var start3 = Stopwatch.StartNew();
            //var results = graph.g().V(vertexIds).Next();
            results = connection.ExecuteQuery("SELECT N_3 FROM Node N_3  WHERE IS_DEFINED(N_3._isEdgeDoc) = false AND(N_3.id in (" + vertexIds + ")) AND (N_3._partition in (" + partitionIdStr + "))", queryOptions);

            foreach (var result in results)
            {
                //Console.WriteLine(result);
                var t = result;
            }

            start3.Stop();
            Console.WriteLine("partition count" + 10 + "  " + collectionName + "(2)" + (start3.ElapsedMilliseconds) + "ms");
            // (4) 30 partition

            vertexIds.Clear();
            partitionIdStr.Clear();
            partitionIds.Clear();
            for (int p = 1; p < 31; p++)
            {
                for (int i = 0; i < 1; i++)
                {
                    //vertexIds.Add(p + "-" + i);
                    var id = p + "-" + i;
                    vertexIds.Append("'" + id + "',");
                    if (!partitionIds.Contains(p.ToString()))
                    {
                        partitionIds.Add(p.ToString());
                        partitionIdStr.Append("'" + p + "',");
                    }
                }
            }
            vertexIds.Remove(vertexIds.Length - 1, 1);
            partitionIdStr.Remove(partitionIdStr.Length - 1, 1);

            var start4 = Stopwatch.StartNew();
            //var results = graph.g().V(vertexIds).Next();
            results = connection.ExecuteQuery("SELECT N_3 FROM Node N_3  WHERE IS_DEFINED(N_3._isEdgeDoc) = false AND(N_3.id in (" + vertexIds + ")) AND (N_3._partition in (" + partitionIdStr + "))", queryOptions);

            foreach (var result in results)
            {
                //Console.WriteLine(result);
                var t = result;
            }

            start4.Stop();
            Console.WriteLine("partition count" + 30 + "  " + collectionName + "(4)" + (start4.ElapsedMilliseconds) + "ms");
        }

        public static void querySpecificIDsList(String collectionName)
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
               "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
               "GroupMatch", collectionName, GraphType.GraphAPIOnly, false,
               1, null);
            GraphViewCommand graph = new GraphViewCommand(connection);
            //graph.OutputFormat = OutputFormat.GraphSON;
            // (1) query 1 partition
            List<Object> vertexIds = new List<object>();
            for (int p = 1; p < 2; p++) { 
                for (int i = 0; i < 30; i++)
                {
                    vertexIds.Add(p + "-" + i);
                }
            }
            var start1 = Stopwatch.StartNew();
            var results = graph.g().V(vertexIds).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
            start1.Stop();
            Console.WriteLine("partition count" + 1 + "  " + collectionName + "(0)" + (start1.ElapsedMilliseconds) + "ms");

            // (2) query 10 partition

            vertexIds.Clear();
            for (int p = 0; p < 10; p++)
            {
                for (int i = 1; i < 4; i++)
                {
                    vertexIds.Add(p + "-" + i);
                }
            }
            var start2 = Stopwatch.StartNew();
            results = graph.g().V(vertexIds).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
            start2.Stop();
            Console.WriteLine("partition count" + 10 + "  " + collectionName + "(1)" + (start2.ElapsedMilliseconds) + "ms");

            // (3) query 30 partition

            vertexIds.Clear();
            for (int p = 0; p < 30; p++)
            {
                for (int i = 1; i < 2; i++)
                {
                    vertexIds.Add(p + "-" + i);
                }
            }
            var start3 = Stopwatch.StartNew();
            results = graph.g().V(vertexIds).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
            start3.Stop();
            Console.WriteLine("partition count" + 30 + "  " + collectionName + "(2)" + (start3.ElapsedMilliseconds) + "ms");

            // (4) query 5 partition
            vertexIds.Clear();
            for (int p = 0; p < 5; p++)
            {
                for (int i = 1; i < 7; i++)
                {
                    vertexIds.Add(p + "-" + i);
                }
            }
            var start5 = Stopwatch.StartNew();
            results = graph.g().V(vertexIds).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
            start5.Stop();
            Console.WriteLine("partition count" + 5 + "  " + collectionName + "(3)" + (start5.ElapsedMilliseconds) + "ms");

            // (5) query 2 partition
            vertexIds.Clear();
            for (int p = 0; p < 2; p++)
            {
                for (int i = 1; i < 16; i++)
                {
                    vertexIds.Add(p + "-" + i);
                }
            }
            var start2p = Stopwatch.StartNew();
            results = graph.g().V(vertexIds).Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
            start2p.Stop();
            Console.WriteLine("partition count" + 2 + "  " + collectionName + "(4)" + (start2p.ElapsedMilliseconds) + "ms");
        }

        public static void partitionQueryTestCommon(String sampleCollection, String collectionName)
        {
            Console.WriteLine(collectionName + "start test");
            GraphViewConnection connection0 = new GraphViewConnection("https://graphview.documents.azure.com:443/",
             "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
             "GroupMatch", sampleCollection, GraphType.GraphAPIOnly, false,
             1, null);

            //GraphViewCommand graph0 = new GraphViewCommand(connection0);
            //var sample = new List<Object>();
            //var resultsS = graph0.g().V().Sample(20).Next();

            //foreach (var result in resultsS)
            //{
            //    sample.Add(result.Replace("v", "").Replace("[", "").Replace("]", ""));
            //}

            // (1) FindNeighbours (FN): finds the neighbours of all nodes.
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
               "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
               "GroupMatch", collectionName, GraphType.GraphAPIOnly, false,
               1, null);

            GraphViewCommand graph = new GraphViewCommand(connection);


            graph.OutputFormat = OutputFormat.GraphSON;
            var start1 = Stopwatch.StartNew();
            var results = graph.g().V().Next();
            //var results = graph.g().V().Sample(10).Out().Next();

            foreach (var result in results)
            {
                //Console.WriteLine(result);
            }
            start1.Stop();
            Console.WriteLine(collectionName + "(0)" + (start1.ElapsedMilliseconds) + "ms");

            //graph.OutputFormat = OutputFormat.GraphSON;
            //var start1 = Stopwatch.StartNew();
            //var results = graph.g().V().Sample(10).Out().Next();
            ////var results = graph.g().V().Sample(10).Out().Next();

            //foreach (var result in results)
            //{
            //    //Console.WriteLine(result);
            //}
            //start1.Stop();
            //Console.WriteLine(collectionName + "(1)" + (start1.ElapsedMilliseconds) + "ms");

            //// (2) FindAdjacentNodes (FA): finds the 3-hop adjacent
            //var start2 = Stopwatch.StartNew();
            //results = graph.g().V().Sample(10).Out().Out().Next();
            ////results = graph.g().V().Sample(10).Out().Out().Next();

            //foreach (var result in results)
            //{
            //    //var a = result;
            //    //Console.WriteLine(result);
            //}
            //start2.Stop();
            //Console.WriteLine(collectionName + "(2)" + (start2.ElapsedMilliseconds) + "ms");
            //// (3) Shortest Path: FindShortestPath (FS): finds the shortest path between the first node and 100 randomly picked nodes.

            //var start3 = Stopwatch.StartNew();
            //String src = sample[0].ToString();
            //sample.RemoveAt(0);
            //foreach (var node in sample)
            //{
            //    String des = node.ToString();
            //    ShortestPathTest.GetShortestPath(src, des, graph);
            //}
            //start3.Stop();
            //Console.WriteLine(collectionName + "(3)" + (start3.ElapsedMilliseconds) + "ms");
            //Console.WriteLine(collectionName + "end test");
        }

        //   public static void insertCitData()
        //   {
        //          GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
        //"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
        //"GroupMatch", "CitAllDataHashPartition", 
        //false, 1, "id");
        //       connection.EdgeSpillThreshold = 1;
        //       GraphViewConnection.useHashPartitionWhenCreateDoc = true;
        //       GraphViewConnection.useBulkInsert = true;
        //       GraphViewCommand cmd = new GraphViewCommand(connection);
        //       HashSet<String> nodeIdSet = new HashSet<String>();

        //       int c = 1;
        //       var linesE = File.ReadLines("D:\\dataset\\thsinghua_dataset\\cit_network\\cit-HepTh.txt\\Cit-HepTh.txt");
        //       foreach (var lineE in linesE)
        //       {
        //           if (c > 4)
        //           {
        //               var split = lineE.Split('\t');
        //               var src = split[0];
        //               var des = split[1];

        //               if (!nodeIdSet.Contains(src))
        //               {
        //                   cmd.CommandText = "g.addV('id', '" + src + "').property('name', '" + src + "').next()";
        //                   cmd.Execute();
        //                   nodeIdSet.Add(src);
        //               }

        //               if (!nodeIdSet.Contains(des))
        //               {
        //                   cmd.CommandText = "g.addV('id', '" + des + "').property('name', '" + des + "').next()";
        //                   cmd.Execute();
        //                   nodeIdSet.Add(des);
        //               }

        //               cmd.CommandText = "g.V('" + src + "').addE('appear').to(g.V('" + des + "')).next()";
        //               cmd.Execute();
        //           }
        //           else
        //           {
        //               c++;
        //           }
        //       }
        //       connection.getMetricsOfGraphPartition();
        //   }

        public static void insertCitHashPartitionByBulkInsert()
        {
            //GraphViewConnection connection =
            //new GraphViewConnection("https://graphview.documents.azure.com:443/",
            //   "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
            //   "GroupMatch", "CitHashPartition1000item", GraphType.GraphAPIOnly, false,
            //   1, null);

            GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
       "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
       "GroupMatch", "CitHashPartition1000item",
       false, 1, "id");

       //GraphViewConnection connection =
       //new GraphViewConnection("https://graphview.documents.azure.com:443/",
       //   "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
       //   "GroupMatch", "MarvelTest", GraphType.GraphAPIOnly, false,
       //   1, null);
            connection.EdgeSpillThreshold = 1;
            GraphViewConnection.useHashPartitionWhenCreateDoc = true;
            GraphViewConnection.useBulkInsert = true;
            GraphViewCommand cmd = new GraphViewCommand(connection);
            HashSet<String> nodeIdSet = new HashSet<String>();

            int c = 1;
            var linesE = File.ReadLines("D:\\dataset\\thsinghua_dataset\\cit_network\\cit-HepTh.txt\\Cit-HepTh.txt");
            BulkInsertUtils blk = new BulkInsertUtils(GraphViewConnection.partitionNum);
            blk.threadNum = 10;
            blk.initBulkInsertUtilsForParseData(GraphViewConnection.partitionNum, linesE.Count(), connection);
            int i = 0;
            foreach (var lineE in linesE)
            {
                if (i > 1000)
                {
                    break;
                }
                else
                {
                    i++;
                }
                if (c > 4)
                {
                    blk.stringBufferList.Add(lineE);
                    Console.WriteLine(c);
                    c++;
                }
                else
                {
                    c++;
                }
            }
            blk.startParseThread();
            blk.parseDataCountDownLatch.Await();
            blk.initAndStartInsertNodeStringCMD();
            blk.insertNodeCountDownLatch.Await();
            blk.initAndStartInsertEdgeStringCMD();
            GraphViewConnection.bulkInsertUtil.startParseThread();
            connection.getMetricsOfGraphPartition();
        }

        public static void insertCitGreedyPartitionByBulkInsert()
        {
            GraphViewConnection connection =
            new GraphViewConnection("https://graphview.documents.azure.com:443/",
               "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
               "GroupMatch", "CitGreedyPartition1000item", GraphType.GraphAPIOnly, false,
               1, null);

            //GraphViewConnection connection =
            //new GraphViewConnection("https://graphview.documents.azure.com:443/",
            //   "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
            //   "GroupMatch", "MarvelTest", GraphType.GraphAPIOnly, false,
            //   1, null);
            connection.EdgeSpillThreshold = 1;
            GraphViewConnection.useHashPartitionWhenCreateDoc = false;
            GraphViewConnection.useGreedyPartitionWhenCreateDoc = true;
            GraphViewConnection.useBulkInsert = true;
            GraphViewCommand cmd = new GraphViewCommand(connection);
            HashSet<String> nodeIdSet = new HashSet<String>();

            int c = 1;
            var linesE = File.ReadLines("D:\\dataset\\thsinghua_dataset\\cit_network\\cit-HepTh.txt\\Cit-HepTh.txt");
            BulkInsertUtils blk = new BulkInsertUtils(GraphViewConnection.partitionNum);
            blk.threadNum = 10;
            blk.initBulkInsertUtilsForParseData(GraphViewConnection.partitionNum, linesE.Count(), connection);
            int i = 0;
            foreach (var lineE in linesE)
            {
                if (i > 1000)
                {
                    break;
                }
                else
                {
                    i++;
                }
                if (c > 4)
                {
                    blk.stringBufferList.Add(lineE);
                    Console.WriteLine(c);
                    c++;
                }
                else
                {
                    c++;
                }
            }
            blk.startParseThread();
            blk.parseDataCountDownLatch.Await();
            blk.initAndStartInsertNodeStringCMD();
            blk.insertNodeCountDownLatch.Await();
            blk.initAndStartInsertEdgeStringCMD();
            GraphViewConnection.bulkInsertUtil.startParseThread();
            connection.getMetricsOfGraphPartition();
        }
        //public static void insertCitDataBulkInsert()
        //{
        //    //          GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
        //    //"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
        //    //"GroupMatch", "CitAllDataHashPartition",
        //    //false, 1, "id");
        //    GraphViewConnection connection =
        //    new GraphViewConnection("https://graphview.documents.azure.com:443/",
        //       "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
        //       "GroupMatch", "CitAllDataHashPartition", GraphType.GraphAPIOnly, false,
        //       1, null);

        //    //GraphViewConnection connection =
        //    //new GraphViewConnection("https://graphview.documents.azure.com:443/",
        //    //   "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
        //    //   "GroupMatch", "MarvelTest", GraphType.GraphAPIOnly, false,
        //    //   1, null);
        //    connection.EdgeSpillThreshold = 1;
        //    GraphViewConnection.useHashPartitionWhenCreateDoc = true;
        //    GraphViewConnection.useBulkInsert = true;
        //    GraphViewCommand cmd = new GraphViewCommand(connection);
        //    HashSet<String> nodeIdSet = new HashSet<String>();

        //    int c = 1;
        //    var linesE = File.ReadLines("D:\\dataset\\thsinghua_dataset\\cit_network\\cit-HepTh.txt\\Cit-HepTh.txt");
        //    BulkInsertUtils blk = new BulkInsertUtils();
        //    blk.threadNum = 10;
        //    blk.initBulkInsertUtilsForParseData(GraphViewConnection.partitionNum, linesE.Count(), connection);
        //    int i = 0;
        //    foreach (var lineE in linesE)
        //    {
        //        //if(i > 100)
        //        //{
        //        //    break;
        //        //} else
        //        //{
        //        //    i++;
        //        //}
        //        if (c > 4)
        //        {
        //            blk.stringBufferList.Add(lineE);
        //            Console.WriteLine(c);
        //            c++;
        //        }
        //        else
        //        {
        //            c++;
        //        }
        //    }
        //    blk.startParseThread();
        //    blk.parseDataCountDownLatch.Await();
        //    blk.initAndStartInsertNodeStringCMD();
        //    blk.insertNodeCountDownLatch.Await();
        //    blk.initAndStartInsertEdgeStringCMD();
        //    GraphViewConnection.bulkInsertUtil.startParseThread();
        //    connection.getMetricsOfGraphPartition();
        //}

        public static void getStatistic()
        {
            Console.WriteLine("start statistic 1");
            GraphViewConnection connection1 =
        new GraphViewConnection("https://graphview.documents.azure.com:443/",
           "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
           "GroupMatch", "CitHashPartition1000item", GraphType.GraphAPIOnly, false,
           1, null);
            //connection1.getMetricsOfGraphPartition();
            connection1.getMetricsOfGraphPartitionInCache(10);

            Console.WriteLine("end statistic 1");
            Console.WriteLine("start statistic 2");
            GraphViewConnection connection2 =
        new GraphViewConnection("https://graphview.documents.azure.com:443/",
           "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
           "GroupMatch", "CitGreedyPartition1000item", GraphType.GraphAPIOnly, false,
           1, null);
            //connection2.getMetricsOfGraphPartition();
            connection2.getMetricsOfGraphPartitionInCache(10);
            Console.WriteLine("end statistic 2");

            Console.WriteLine("start statistic 3");
            GraphViewConnection connection3 =
        new GraphViewConnection("https://graphview.documents.azure.com:443/",
           "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
           "GroupMatch", "CitFakeDiffPartition", GraphType.GraphAPIOnly, false,
           1, null);
            //connection1.getMetricsOfGraphPartition();
            connection3.getMetricsOfGraphPartitionInCache(3);

            Console.WriteLine("end statistic 3");
            Console.WriteLine("start statistic 4");
            GraphViewConnection connection4 =
        new GraphViewConnection("https://graphview.documents.azure.com:443/",
           "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
           "GroupMatch", "CitFakeSamePartition", GraphType.GraphAPIOnly, false,
           1, null);
            //connection2.getMetricsOfGraphPartition();
            connection4.getMetricsOfGraphPartitionInCache(1);
            Console.WriteLine("end statistic 2");
        }

        public static void insertInSamePartition()
        {
            var edgeList = new List<String>();
            // partitionData in 3 partitions
            // partitionData in 3 partitions
            for (int i = 0; i < 5; i++)
            {
                // p1
                for (int j = 0; j < 5; j++)
                {
                    edgeList.Add(0 + "-" + i + "\t" + 1 + "-" + j);
                    // p2
                    for (int k = 0; k < 5; k++)
                    {
                        // p3
                        edgeList.Add(0 + "-" + i + "\t" + 1 + "-" + k);
                        edgeList.Add(1 + "-" + j + "\t" + 2 + "-" + k);
                        edgeList.Add(2 + "-" + k + "\t" + 0 + "-" + i);
                    }
                }
            }

            GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
  "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
  "GroupMatch", "CitFakeSamePartition", false, 1, "name");
            connection.EdgeSpillThreshold = 1;
            GraphViewConnection.partitionNum = 3;
            GraphViewConnection.useHashPartitionWhenCreateDoc = false;
            GraphViewConnection.useFakePartitionWhenCreateDoc = true;
            GraphViewConnection.useBulkInsert = true;
            GraphViewConnection.useFakePartitionWhenCreateDocIn1Partition = true;
            GraphViewCommand cmd = new GraphViewCommand(connection);
            HashSet<String> nodeIdSet = new HashSet<String>();
          
            BulkInsertUtils blk = new BulkInsertUtils(GraphViewConnection.partitionNum);
            blk.threadNum = 3;
            blk.initBulkInsertUtilsForParseData(GraphViewConnection.partitionNum, edgeList.Count, connection);
            //int j = 0;
            var linesE = edgeList;
            foreach (var lineE in linesE)
            {
                blk.stringBufferList.Add(lineE);
            }
            blk.startParseThread();
            blk.parseDataCountDownLatch.Await();
            blk.initAndStartInsertNodeStringCMD();
            blk.insertNodeCountDownLatch.Await();
            blk.initAndStartInsertEdgeStringCMD();
            GraphViewConnection.bulkInsertUtil.startParseThread();
            //connection.getMetricsOfGraphPartition();
        }

        public void partitionQueryTestCommonForQuery(String collectionName)
        {
            // (1) FindNeighbours (FN): finds the neighbours of all nodes.
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
               "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
               "GroupMatch", collectionName, GraphType.GraphAPIOnly, false, 1, "name");
            GraphViewCommand graph = new GraphViewCommand(connection);
            //graph.OutputFormat = OutputFormat.GraphSON;
            DateTime start1 = DateTime.Now;
            var results = graph.g().V("9304045").Out().Out().Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
            DateTime end1 = DateTime.Now;
            Console.WriteLine("(1)" + (end1.Millisecond - start1.Millisecond) + "ms");

            //// (2) FindAdjacentNodes (FA): finds the 3-hop adjacent
            DateTime start2 = DateTime.Now;
            results = graph.g().V().Out().Out().Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
            DateTime end2 = DateTime.Now;
            Console.WriteLine("(2)" + (end2.Millisecond - start2.Millisecond) + "ms");
            // (3) Shortest Path: FindShortestPath (FS): finds the shortest path between the first node and 100 randomly picked nodes.

            //results = graph.g().V().Next(); // change the the result format or just hack a test suite
            //HashSet<int> index = new HashSet<int> { 1, 15 };
            //List<String> nodes = new List<string>();
            //int i = 0;
            //foreach (var result in results)
            //{
            //    i++;
            //    if (index.Contains(i))
            //    {
            //        nodes.Add(result);
            //    }
            //}

            //DateTime start3 = DateTime.Now;
            //String src = nodes[0];
            //nodes.RemoveAt(0);
            //foreach (var node in nodes)
            //{
            //    String des = node;
            //    ShortestPathTest.GetShortestPath(src, des, graph);
            //}
            //DateTime end3 = DateTime.Now;
            //Console.WriteLine("(3)" + (end3.Millisecond - start3.Millisecond) + "ms");
        }

        public static void insertDiffKeyPartitionBulkInsert(String collectionName, int partitionNum)
        {
            //GraphViewConnection connection =
            //new GraphViewConnection("https://graphview.documents.azure.com:443/",
            //   "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
            //   "GroupMatch", "CitHashPartition1000item", GraphType.GraphAPIOnly, false,
            //   1, null);

            GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
       "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
       "GroupMatch", collectionName,
       false, 1, "id");
            connection.initPartitionConfig(partitionNum);
            //GraphViewConnection connection =
            //new GraphViewConnection("https://graphview.documents.azure.com:443/",
            //   "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
            //   "GroupMatch", "MarvelTest", GraphType.GraphAPIOnly, false,
            //   1, null);
            connection.EdgeSpillThreshold = 1;
            GraphViewConnection.useHashPartitionWhenCreateDoc = true;
            GraphViewConnection.useBulkInsert = true;
            //GraphViewConnection.partitionNum = partitionNum;
            GraphViewCommand cmd = new GraphViewCommand(connection);
            HashSet<String> nodeIdSet = new HashSet<String>();

            int c = 1;
            var linesE = File.ReadLines("D:\\dataset\\thsinghua_dataset\\cit_network\\cit-HepTh.txt\\Cit-HepTh.txt");
            BulkInsertUtils blk = new BulkInsertUtils(GraphViewConnection.partitionNum);
            //blk.threadNum = 10;
            blk.initBulkInsertUtilsForParseData(GraphViewConnection.partitionNum, linesE.Count(), connection);
            int i = 0;
            foreach (var lineE in linesE)
            {
                if (i > 1000)
                {
                    break;
                }
                else
                {
                    i++;
                }
                if (c > 4)
                {
                    blk.stringBufferList.Add(lineE);
                    Console.WriteLine(c);
                    c++;
                }
                else
                {
                    c++;
                }
            }
            blk.startParseThread();
            blk.parseDataCountDownLatch.Await();
            blk.initAndStartInsertNodeStringCMD();
            blk.insertNodeCountDownLatch.Await();
            blk.initAndStartInsertEdgeStringCMD();
            GraphViewConnection.bulkInsertUtil.startParseThread();
            ////connection.getMetricsOfGraphPartition();
        }

        public static void insertControlPartitionkeyBulkInsert(String collectionName, int partitionNum)
        {

            var edgeList = new List<String>();
            // partitionData in 3 partitions
            // partitionData in 3 partitions
            int partitionNodesPerPartition = 100;
            var rnd = new Random();
            HashSet<String> vertex = new HashSet<string>();
            for (int t = 0; t < partitionNum; t++)
            {
                // p1
                for (int j = 0; j < partitionNodesPerPartition / 3; j++)
                {
                    for(int k = 0; k < partitionNodesPerPartition / 3; k ++)
                    {
                        var src = t + "-" + j;
                        var des = t + "-" + k;
                        var r = rnd.Next();
                        var d = r % 10;
                        if(src == des)
                        {
                            continue;
                        }
                        if (vertex.Contains(src) && vertex.Contains(des) && (d > 2))
                        {
                            continue;
                        }
                        else
                        {
                            edgeList.Add(t + "-" + j + "\t" + t + "-" + k);
                            vertex.Add(src);
                            vertex.Add(des);
                        }
                    }
                }
            }

            //     GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
            //"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
            //"GroupMatch", collectionName,
            //false, 1, "id");
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", collectionName, GraphType.GraphAPIOnly, false, 1, "name");
            connection.initPartitionConfig(partitionNum);
            connection.EdgeSpillThreshold = 1;
            GraphViewConnection.useHashPartitionWhenCreateDoc = false;
            GraphViewConnection.useFakePartitionWhenCreateDoc = true;
            GraphViewConnection.useBulkInsert = true;
            GraphViewCommand cmd = new GraphViewCommand(connection);
            HashSet<String> nodeIdSet = new HashSet<String>();

            int c = 1;
            //var linesE = File.ReadLines("D:\\dataset\\thsinghua_dataset\\cit_network\\cit-HepTh.txt\\Cit-HepTh.txt");
            var linesE = edgeList.ToList();
            BulkInsertUtils blk = new BulkInsertUtils(GraphViewConnection.partitionNum);
            blk.initBulkInsertUtilsForParseData(GraphViewConnection.partitionNum, linesE.Count(), connection);
            int i = 0;

            foreach (var lineE in linesE)
            {
                //if (i > 1000)
                //{
                //    break;
                //}
                //else
                //{
                //    i++;
                //}
                //if (c > 4)
                //{
                    blk.stringBufferList.Add(lineE);
                    Console.WriteLine(c);
                //    c++;
                //}
                //else
                //{
                //    c++;
                //}
            }
            blk.startParseThread();
            blk.parseDataCountDownLatch.Await();
            blk.initAndStartInsertNodeStringCMD();
            blk.insertNodeCountDownLatch.Await();
            blk.initAndStartInsertEdgeStringCMD();
            GraphViewConnection.bulkInsertUtil.startParseThread();
            ////connection.getMetricsOfGraphPartition();
        }

        public static void partitionQueryDiffPartitionTest(String collectionName)
        {
            // (1) FindNeighbours (FN): finds the neighbours of all nodes.
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
               "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
               "GroupMatch", collectionName, GraphType.GraphAPIOnly, false, 1, "name");
            GraphViewCommand graph = new GraphViewCommand(connection);
            //graph.OutputFormat = OutputFormat.GraphSON;

            var start0 = Stopwatch.StartNew();
            var results0 = graph.g().V(new List<Object> {"100-57", "100-69", "100-66"}).Next();
            var rc0 = results0.Count();

            foreach (var result in results0)
            {
                Console.WriteLine(result);
            }
            start0.Stop();
            Console.WriteLine("partition" + 2 + " Result Count" + rc0 + "(1)" + (start0.ElapsedMilliseconds) + "ms");



            List<Object> ids1 = new List<object>();
            for (int p = 0; p < 1; p++)
            {
                for(int v = 0; v < 100; v ++)
                {
                    ids1.Add(p + "-" + v);
                }
            }
            var start1 = Stopwatch.StartNew();
            var results = graph.g().V(ids1).Next();
            var rc1 = results.Count;

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
            start1.Stop();
            Console.WriteLine("partition" + ids1.Count() + " Result Count" + rc1 + "(1)" + (start1.ElapsedMilliseconds) + "ms");

            List<Object> ids2 = new List<object>();
            for (int p = 0; p < 10; p++)
            {
                for (int v = 0; v < 10; v++)
                {
                    ids2.Add(p + "-" + v);
                }
            }
            var start2 = Stopwatch.StartNew();
            results = graph.g().V(ids2).Next();
            var rc2 = results.Count;

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
            start2.Stop();
            Console.WriteLine("partition" + ids2.Count() + " Result Count" + rc2 + "(2)" + (start2.ElapsedMilliseconds) + "ms");


            List<Object> ids3 = new List<object>();
            for (int p = 0; p < 100; p++)
            {
                for (int v = 0; v < 1; v++)
                {
                    ids3.Add(p + "-" + v);
                }
            }
            var start3 = Stopwatch.StartNew();
            results = graph.g().V(ids3).Next();
            var rc3 = results.Count;
            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
            start3.Stop();
            Console.WriteLine("partition:" + ids3.Count() + " Result Count" + rc3 + "(3)" + (start3.ElapsedMilliseconds) + "ms");
        } 
    }
}
