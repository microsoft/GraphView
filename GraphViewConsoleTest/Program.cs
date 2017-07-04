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
            insertCitDataBulkInsert();
            Console.ReadLine();
        }

        public static void partitionQueryTestCommon(String collectionName)
        {
            GraphViewConnection connection0 = new GraphViewConnection("https://graphview.documents.azure.com:443/",
             "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
             "GroupMatch", "PartitionTestCit", GraphType.GraphAPIOnly, false,
             1, null);

            GraphViewCommand graph0 = new GraphViewCommand(connection0);
            var sample = new List<Object>();
            var resultsS = graph0.g().V().Sample(10).Next();

            foreach (var result in resultsS)
            {
                sample.Add(result.Replace("v", "").Replace("[", "").Replace("]", ""));
            }

            // (1) FindNeighbours (FN): finds the neighbours of all nodes.
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
               "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
               "GroupMatch", collectionName, GraphType.GraphAPIOnly, false,
               1, null);

            GraphViewCommand graph = new GraphViewCommand(connection);

            //graph.OutputFormat = OutputFormat.GraphSON;
            var start1 = Stopwatch.StartNew();
            var results = graph.g().V(sample).Out().Next();
            //var results = graph.g().V().Sample(10).Out().Next();

            foreach (var result in results)
            {
                //Console.WriteLine(result);
            }
            start1.Stop();
            Console.WriteLine("(1)" + (start1.Elapsed.Milliseconds) + "ms");

            //// (2) FindAdjacentNodes (FA): finds the 3-hop adjacent
            var start2 = Stopwatch.StartNew();
            results = graph.g().V(sample).Out().Out().Next();
            //results = graph.g().V().Sample(10).Out().Out().Next();

            foreach (var result in results)
            {
                //Console.WriteLine(result);
            }
            start2.Stop();
            Console.WriteLine("(2)" + (start2.Elapsed.Milliseconds) + "ms");
            // (3) Shortest Path: FindShortestPath (FS): finds the shortest path between the first node and 100 randomly picked nodes.

            //results = graph.g().V().Sample(10).Next(); // change the the result format or just hack a test suite
            results = graph.g().V(sample).Next(); // change the the result format or just hack a test suite

            List<String> nodes = new List<string>();
            foreach (var result in results)
            {
               nodes.Add(result);
            }

            var start3 = Stopwatch.StartNew();
            String src = nodes[0];
            nodes.RemoveAt(0);
            foreach (var node in nodes)
            {
                String des = node;
                ShortestPathTest.GetShortestPath(src, des, graph);
            }
            start3.Stop();
            Console.WriteLine("(3)" + (start3.Elapsed.Milliseconds) + "ms");
        }

        public static void insertCitData()
        {
               GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
     "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
     "GroupMatch", "CitAllDataHashPartition", 
     false, 1, "id");
            connection.EdgeSpillThreshold = 1;
            GraphViewConnection.useHashPartitionWhenCreateDoc = true;
            GraphViewConnection.useBulkInsert = true;
            GraphViewCommand cmd = new GraphViewCommand(connection);
            HashSet<String> nodeIdSet = new HashSet<String>();

            int c = 1;
            var linesE = File.ReadLines("D:\\dataset\\thsinghua_dataset\\cit_network\\cit-HepTh.txt\\Cit-HepTh.txt");
            foreach (var lineE in linesE)
            {
                if (c > 4)
                {
                    var split = lineE.Split('\t');
                    var src = split[0];
                    var des = split[1];

                    if (!nodeIdSet.Contains(src))
                    {
                        cmd.CommandText = "g.addV('id', '" + src + "').property('name', '" + src + "').next()";
                        cmd.Execute();
                        nodeIdSet.Add(src);
                    }

                    if (!nodeIdSet.Contains(des))
                    {
                        cmd.CommandText = "g.addV('id', '" + des + "').property('name', '" + des + "').next()";
                        cmd.Execute();
                        nodeIdSet.Add(des);
                    }

                    cmd.CommandText = "g.V('" + src + "').addE('appear').to(g.V('" + des + "')).next()";
                    cmd.Execute();
                }
                else
                {
                    c++;
                }
            }
            connection.getMetricsOfGraphPartition();
        }

        public static void insertCitDataBulkInsert()
        {
            //          GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
            //"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
            //"GroupMatch", "CitAllDataHashPartition",
            //false, 1, "id");
            GraphViewConnection connection =
            new GraphViewConnection("https://graphview.documents.azure.com:443/",
               "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
               "GroupMatch", "CitAllDataHashPartition", GraphType.GraphAPIOnly, false,
               1, null);

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
            BulkInsertUtils blk = new BulkInsertUtils();
            blk.threadNum = 10;
            blk.initBulkInsertUtilsForParseData(GraphViewConnection.partitionNum, linesE.Count(), connection);
            int i = 0;
            foreach (var lineE in linesE)
            {
                //if(i > 100)
                //{
                //    break;
                //} else
                //{
                //    i++;
                //}
                if (c > 4)
                {
                    blk.stringBufferList.Add(lineE);
                    //var split = lineE.Split('\t');
                    //var src = split[0];
                    //var des = split[1];

                    //if (!nodeIdSet.Contains(src))
                    //{
                    //    cmd.CommandText = "g.addV('id', '" + src + "').property('name', '" + src + "').next()";
                    //    cmd.Execute();
                    //    nodeIdSet.Add(src);
                    //}

                    //if (!nodeIdSet.Contains(des))
                    //{
                    //    cmd.CommandText = "g.addV('id', '" + des + "').property('name', '" + des + "').next()";
                    //    cmd.Execute();
                    //    nodeIdSet.Add(des);
                    //}

                    //cmd.CommandText = "g.V('" + src + "').addE('appear').to(g.V('" + des + "')).next()";
                    //cmd.Execute();
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
    }
}
