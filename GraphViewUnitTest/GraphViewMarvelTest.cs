using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using GraphViewUnitTest.Gremlin;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.IO;
using System.Collections;
using System.Collections.Generic;

namespace GraphViewUnitTest
{
    [TestClass]
    public class GraphViewMarvelTest
    {
        [TestMethod]
        public void SelectMarvelQuery1()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
                AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, null);

            GraphViewCommand graph = new GraphViewCommand(connection);
            graph.OutputFormat = OutputFormat.GraphSON;
            var results = graph.g().V().Has("weapon", "shield").As("character").Out("appeared").As("comicbook").Select("character").Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery1b()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
                AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, null);

            GraphViewCommand cmd = new GraphViewCommand(connection);
            cmd.CommandText =
                "g.V().has('weapon','shield').as('character').out('appeared').as('comicbook').select('character').next()";
            cmd.OutputFormat = OutputFormat.GraphSON;
            var results = cmd.Execute();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery1c()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
                AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, null);

            GraphViewCommand cmd = new GraphViewCommand(connection);
            cmd.CommandText =
                "g.V().has('weapon','shield').as('character').outE('appeared').next()";
            cmd.OutputFormat = OutputFormat.GraphSON;
            var results = cmd.Execute();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery2()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
                AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, null);
            GraphViewCommand graph = new GraphViewCommand(connection);

            var results =
                graph.g()
                    .V()
                    .Has("weapon", "lasso")
                    .As("character")
                    .Out("appeared")
                    .As("comicbook")
                    .Select("comicbook")
                    .Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery2b()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
                AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, null);
            GraphViewCommand graph = new GraphViewCommand(connection);
            graph.CommandText = "g.V().has('weapon', 'lasso').as('character').out('appeared').as('comicbook').select('comicbook').next()";
            graph.OutputFormat = OutputFormat.GraphSON;
            var results = graph.Execute();

            foreach (string result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery3()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
                AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, null);
            GraphViewCommand graph = new GraphViewCommand(connection);
            var results = graph.g().V().Has("name", "AVF 4").In("appeared").Values("name").Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery3b()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
                AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, null);
            GraphViewCommand graph = new GraphViewCommand(connection);
            graph.CommandText = "g.V().has('name', 'AVF 4').in('appeared').values('name').next()";
            var results = graph.Execute();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery4()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
                AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, null);
            GraphViewCommand graph = new GraphViewCommand(connection);
            var results = graph.g().V().Has("name", "AVF 4").In("appeared").Has("weapon", "shield").Values("name").Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQuery4b()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
                AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, null);
            GraphViewCommand graph = new GraphViewCommand(connection);
            graph.CommandText = "g.V().has('name', 'AVF 4').in('appeared').has('weapon', 'shield').values('name').next()";
            var results = graph.Execute();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        /// <summary>
        /// Print the characters and the comic-books they appeared in where the characters had a weapon that was a shield or claws.
        /// </summary>
        [TestMethod]
        public void SelectMarvelQueryNativeAPI1()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
                AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, null);
            GraphViewCommand graph = new GraphViewCommand(connection);
            var results =
                graph.g().V()
                    .As("character")
                    .Has("weapon", Predicate.within("shield", "claws"))
                    .Out("appeared")
                    .As("comicbook")
                    .Select("character");

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void SelectMarvelQueryNativeAPI2()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
                AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, null);
            GraphViewCommand graph = new GraphViewCommand(connection);
            var results =
                graph.g().V()
                    .As("CharacterNode")
                    .Values("name")
                    .As("character")
                    .Select("CharacterNode")
                    .Has("weapon", Predicate.without("shield", "claws"))
                    .Out("appeared")
                    .Values("name")
                    .As("comicbook")
                    .Select("comicbook")
                    .Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
        }

        [TestMethod]
        public void GraphViewMarvelInsertDeleteTest()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
                AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, null);
            GraphViewCommand graph = new GraphViewCommand(connection);

            graph.g().AddV("character").Property("name", "VENUS II").Property("weapon", "shield").Next();
            graph.g().AddV("comicbook").Property("name", "AVF 4").Next();
            graph.g().V().Has("name", "VENUS II").AddE("appeared").To(graph.g().V().Has("name", "AVF 4")).Next();
            graph.g().AddV("character").Property("name", "HAWK").Property("weapon", "claws").Next();
            graph.g().V().As("v").Has("name", "HAWK").AddE("appeared").To(graph.g().V().Has("name", "AVF 4")).Next();
            graph.g().AddV("character").Property("name", "WOODGOD").Property("weapon", "lasso").Next();
            graph.g().V().As("v").Has("name", "WOODGOD").AddE("appeared").To(graph.g().V().Has("name", "AVF 4")).Next();
        }

        [TestMethod]
        public void GraphViewMarvelInsertTest()
        {
            //GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
            //    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
            //    "GroupMatch", "PartitionTest", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
            //    AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, null);

            GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
              "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
              "GroupMatch", "PartitionTest", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection.EdgeSpillThreshold = 1;
            GraphViewCommand cmd = new GraphViewCommand(connection);

            cmd.CommandText = "g.addV('character').property('name', 'VENUS II').property('weapon', 'shield').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('comicbook').property('name', 'AVF 4').next()";
            cmd.Execute();
            cmd.CommandText = "g.V().has('name', 'VENUS II').addE('appeared').to(g.V().has('name', 'AVF 4')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('character').property('name', 'HAWK').property('weapon', 'claws').next()";
            cmd.Execute();
            cmd.CommandText = "g.V().as('v').has('name', 'HAWK').addE('appeared').to(g.V().has('name', 'AVF 4')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('character').property('name', 'WOODGOD').property('weapon', 'lasso').next()";
            cmd.Execute();
            cmd.CommandText = "g.V().as('v').has('name', 'WOODGOD').addE('appeared').to(g.V().has('name', 'AVF 4')).next()";
            cmd.Execute();
        }

        [TestMethod]
        public void GraphStaticsticTest()
        {
            Console.WriteLine("Test1");
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "CitHashPartition1000item", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
                AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection.getMetricsOfGraphPartition();
            Console.WriteLine("Test2");

            GraphViewConnection connection1 = new GraphViewConnection("https://graphview.documents.azure.com:443/",
           "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
           "GroupMatch", "CitGreedyPartition1000item", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
           AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection1.getMetricsOfGraphPartition();
            Console.WriteLine("Test3");

  //          GraphViewConnection connection2 = new GraphViewConnection("https://graphview.documents.azure.com:443/",
  //         "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
  //         "GroupMatch", "PartitionTestCitRep2", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
  //         AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
  //          connection2.getMetricsOfGraphPartition();

  //          GraphViewConnection connection3 = new GraphViewConnection("https://graphview.documents.azure.com:443/",
  //"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
  //"GroupMatch", "PartitionTestCitRepInc", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
  //AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
  //          connection3.getMetricsOfGraphPartition();

//            GraphViewConnection connection4 = new GraphViewConnection("https://graphview.documents.azure.com:443/",
//"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
//"GroupMatch", "PartitionTestCitRepIncComp", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
//AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
//            connection4.getMetricsOfGraphPartition();
        }

        [TestMethod]
        public void incRepartitionTest1000Items()
        {
            GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
        "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
        "GroupMatch", "PartitionTestCitRepInc", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection.EdgeSpillThreshold = 1;
            GraphViewConnection.useIncRepartitionDoc = true;
            GraphViewConnection.partitionNum = 100;
            connection.incRepartitionDocBatchSize = 50;
            GraphViewCommand cmd = new GraphViewCommand(connection);
            HashSet<String> nodeIdSet = new HashSet<String>();
            // Add edge
            int edgeCount = 1000;

            int c = 1;
            var linesE = File.ReadLines("D:\\dataset\\thsinghua_dataset\\cit_network\\cit-HepTh.txt\\Cit-HepTh.txt");
            foreach (var lineE in linesE)
            {
                edgeCount--;
                if (edgeCount < 0)
                {
                    break;
                }
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

        [TestMethod]
        public void incRepartitionTest()
        {
            GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
         "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
         "GroupMatch", "PartitionTestCitRepInc", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection.EdgeSpillThreshold = 1;
            GraphViewConnection.useIncRepartitionDoc = true;
            GraphViewConnection.partitionNum = 3;
            connection.incRepartitionDocBatchSize = 2;
            GraphViewCommand cmd = new GraphViewCommand(connection);

            cmd.CommandText = "g.addV('id', '1').property('name', '1').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '2').property('name', '2').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('1').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '3').property('name', '3').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('3').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '4').property('name', '4').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('4').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '5').property('name', '5').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('5').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();


            cmd.CommandText = "g.addV('id', '11').property('name', '11').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '12').property('name', '12').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('11').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '13').property('name', '13').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('13').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '14').property('name', '14').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('14').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '15').property('name', '15').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('15').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();

            cmd.CommandText = "g.addV('id', '21').property('name', '21').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '22').property('name', '22').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('21').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '23').property('name', '23').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('23').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '24').property('name', '24').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('24').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '25').property('name', '25').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('25').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
        }

        [TestMethod]
        public void incRepartitionTestComp()
        {
            GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
         "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
         "GroupMatch", "PartitionTestCitRepIncComp", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection.EdgeSpillThreshold = 1;
            GraphViewConnection.useIncRepartitionDoc = false;
            connection.incRepartitionDocBatchSize = 2;
            GraphViewConnection.partitionNum = 3;
            GraphViewCommand cmd = new GraphViewCommand(connection);

            cmd.CommandText = "g.addV('id', '1').property('name', '1').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '2').property('name', '2').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('1').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '3').property('name', '3').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('3').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '4').property('name', '4').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('4').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '5').property('name', '5').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('5').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();


            cmd.CommandText = "g.addV('id', '11').property('name', '11').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '12').property('name', '12').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('11').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '13').property('name', '13').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('13').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '14').property('name', '14').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('14').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '15').property('name', '15').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('15').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();

            cmd.CommandText = "g.addV('id', '21').property('name', '21').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '22').property('name', '22').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('21').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '23').property('name', '23').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('23').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '24').property('name', '24').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('24').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '25').property('name', '25').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('25').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
        }

        [TestMethod]
        public void randomPeekEdge1()
        {
            GraphViewConnection connection1 = new GraphViewConnection("https://graphview.documents.azure.com:443/",
              "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
              "GroupMatch", "PartitionTest", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
              AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection1.EdgeSpillThreshold = 1;
            GraphViewCommand cmd = new GraphViewCommand(connection1);

            GraphViewConnection connection2 = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
          "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
          "GroupMatch", "PartitionTestCitRep2", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection2.EdgeSpillThreshold = 1;
            connection2.AssignSeenDesNotSeenSrcToBalance = true;
            GraphViewConnection.partitionLoad = new int[GraphViewConnection.partitionNum];
            connection2.repartitionTheCollection(connection1);
            connection2.getMetricsOfGraphPartition();
        }

        [TestMethod]
        public void randomPeekEdge2()
        {
            GraphViewConnection connection1 = new GraphViewConnection("https://graphview.documents.azure.com:443/",
              "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
              "GroupMatch", "PartitionTest", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
              AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection1.EdgeSpillThreshold = 1;
            GraphViewCommand cmd = new GraphViewCommand(connection1);

            GraphViewConnection connection2 = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
          "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
          "GroupMatch", "PartitionTestCitRep2", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection2.EdgeSpillThreshold = 1;
            connection2.AssignSeenDesNotSeenSrcToBalance = true;
            GraphViewConnection.partitionLoad = new int[GraphViewConnection.partitionNum];
            connection2.repartitionTheCollection(connection1);
            connection2.getMetricsOfGraphPartition();
        }

        [TestMethod]
        public void randomCitRePartitionTestLoadBalanceForUnSeenSrc()
        {
            GraphViewConnection connection1 = new GraphViewConnection("https://graphview.documents.azure.com:443/",
              "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
              "GroupMatch", "PartitionTestCit", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
              AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection1.EdgeSpillThreshold = 1;
            GraphViewCommand cmd = new GraphViewCommand(connection1);

            GraphViewConnection connection2 = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
          "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
          "GroupMatch", "PartitionTestCitRep2", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection2.EdgeSpillThreshold = 1;
            connection2.AssignSeenDesNotSeenSrcToBalance = true;
            GraphViewConnection.partitionLoad = new int[GraphViewConnection.partitionNum];
            connection2.repartitionTheCollection(connection1);
            connection2.getMetricsOfGraphPartition();
        }

        [TestMethod]
        public void randomCitRePartitionTestNoLoadBalanceForUnSeenSrc()
        {
            GraphViewConnection connection1 = new GraphViewConnection("https://graphview.documents.azure.com:443/",
            "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
            "GroupMatch", "PartitionTestCit", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
            AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection1.EdgeSpillThreshold = 1;
            GraphViewCommand cmd = new GraphViewCommand(connection1);

            GraphViewConnection connection2 = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
          "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
          "GroupMatch", "PartitionTestCitRep1", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection2.EdgeSpillThreshold = 1;
            GraphViewConnection.partitionLoad = new int[GraphViewConnection.partitionNum];
            connection2.repartitionTheCollection(connection1);
            connection2.getMetricsOfGraphPartition();
        }

        [TestMethod]
        public void randomCitPartitionTest()
        {
            GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
     "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
     "GroupMatch", "PartitionTestCit", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection.EdgeSpillThreshold = 1;
            GraphViewCommand cmd = new GraphViewCommand(connection);
            HashSet<String> nodeIdSet = new HashSet<String>();
            // Add edge
            int edgeCount = 1000;

            int c = 1;
            var linesE = File.ReadLines("D:\\dataset\\thsinghua_dataset\\cit_network\\cit-HepTh.txt\\Cit-HepTh.txt");
            foreach (var lineE in linesE)
            {
                edgeCount--;
                if (edgeCount < 0)
                {
                    break;
                }
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

        [TestMethod]
        public void GraphViewRepartitionTest()
        {
            GraphViewConnection connection1 = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
              "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
              "GroupMatch", "PartitionTest", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection1.EdgeSpillThreshold = 1;
            GraphViewCommand cmd = new GraphViewCommand(connection1);
            
            cmd.CommandText = "g.addV('id', '1').property('name', '1').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '2').property('name', '2').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('1').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '3').property('name', '3').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('3').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '4').property('name', '4').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('4').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '5').property('name', '5').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('5').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();

            cmd.CommandText = "g.addV('id', '11').property('name', '11').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '12').property('name', '12').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('11').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '13').property('name', '13').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('13').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '14').property('name', '14').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('14').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '15').property('name', '15').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('15').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();

            cmd.CommandText = "g.addV('id', '21').property('name', '21').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '22').property('name', '22').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('21').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '23').property('name', '23').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('23').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '24').property('name', '24').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('24').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '25').property('name', '25').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('25').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();

            GraphViewConnection connection2 = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
          "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
          "GroupMatch", "PartitionTest2", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection2.EdgeSpillThreshold = 1;
            GraphViewConnection.partitionLoad = new int[GraphViewConnection.partitionNum];
            connection2.repartitionTheCollection(connection1);
            connection2.getMetricsOfGraphPartition();
        }

        [TestMethod]
        public void GraphViewRepartitionTestAssignBalance()
        {
            GraphViewConnection connection1 = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
              "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
              "GroupMatch", "PartitionTest", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection1.EdgeSpillThreshold = 1;
            GraphViewCommand cmd = new GraphViewCommand(connection1);

            cmd.CommandText = "g.addV('id', '1').property('name', '1').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '2').property('name', '2').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('1').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '3').property('name', '3').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('3').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '4').property('name', '4').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('4').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '5').property('name', '5').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('5').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();

            cmd.CommandText = "g.addV('id', '11').property('name', '11').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '12').property('name', '12').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('11').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '13').property('name', '13').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('13').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '14').property('name', '14').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('14').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '15').property('name', '15').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('15').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();

            cmd.CommandText = "g.addV('id', '21').property('name', '21').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '22').property('name', '22').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('21').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '23').property('name', '23').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('23').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '24').property('name', '24').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('24').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '25').property('name', '25').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('25').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();

            GraphViewConnection connection2 = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
          "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
          "GroupMatch", "PartitionTest2", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection2.EdgeSpillThreshold = 1;
            connection2.AssignSeenDesNotSeenSrcToBalance = true;
            connection2.repartitionTheCollection(connection1);
            connection2.getMetricsOfGraphPartition();
        }
        
        [TestMethod]
        public void GraphViewFakePartitionDataInsertTest()
        {
            GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
              "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
              "GroupMatch", "PartitionTest", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection.EdgeSpillThreshold = 1;

            GraphViewCommand cmd = new GraphViewCommand(connection);

            cmd.CommandText = "g.addV('id', '1').property('name', '1').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '2').property('name', '2').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('1').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '3').property('name', '3').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('3').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '4').property('name', '4').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('4').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '5').property('name', '5').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('5').addE('appeared').to(g.V('2')).next()";
            cmd.Execute();


            cmd.CommandText = "g.addV('id', '11').property('name', '11').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '12').property('name', '12').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('11').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '13').property('name', '13').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('13').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '14').property('name', '14').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('14').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '15').property('name', '15').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('15').addE('appeared').to(g.V('12')).next()";
            cmd.Execute();

            cmd.CommandText = "g.addV('id', '21').property('name', '21').next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '22').property('name', '22').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('21').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '23').property('name', '23').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('23').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '24').property('name', '24').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('24').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
            cmd.CommandText = "g.addV('id', '25').property('name', '25').next()";
            cmd.Execute();
            cmd.CommandText = "g.V('25').addE('appeared').to(g.V('22')).next()";
            cmd.Execute();
        }


        [TestMethod]
        public void GraphViewInsertWikiTalkTempNetworkDataVertex()
        {

            GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
      "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
      "GroupMatch", "PartitionTest", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection.EdgeSpillThreshold = 1;
            GraphViewCommand cmd = new GraphViewCommand(connection);

            // Add vertex g.addV("id", 1)
            var idSet = new HashSet<String>();
            var lines = File.ReadLines("D:\\dataset\\thsinghua_dataset\\wiki\\wiki-talk-temporal-usernames.txt\\wiki-talk-temporal-usernames.txt");
            foreach (var line in lines)
            {
                var split = line.Split(' ');
                var id = split[0];
                var name = split[1];
                cmd.CommandText = "g.addV('id', '" + id + "').property('name', '" + name + "').next()";
                idSet.Add(id);
                cmd.Execute();
                Console.WriteLine(cmd.CommandText);
            }
        }

        [TestMethod]
        public void GraphViewInsertWikiTalkTempNetworkDataEdge()
        {

            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "Wiki_Temp", GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
                AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, null);
            GraphViewCommand cmd = new GraphViewCommand(connection);
            // Add edge
            var linesE = File.ReadLines("D:\\dataset\\thsinghua_dataset\\wiki\\wiki-talk-temporal.txt\\wiki-talk-temporal.txt");
            foreach (var lineE in linesE)
            {
                var split = lineE.Split(' ');
                var src = split[0];
                var des = split[1];
                var date = split[2];
                cmd.CommandText = "g.V('" + src + "').addE('" + date + "').to(g.V('" + des + "')).next()";
                cmd.Execute();
            }
        }

        [TestMethod]
        public void GraphViewInsertCitNetworkDataVertex()
        {
            GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
               "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
               "GroupMatch", "PartitionTest", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection.EdgeSpillThreshold = 1;
            GraphViewCommand cmd = new GraphViewCommand(connection);

            // Add vertex abstract
            var linesA = File.ReadLines("D:\\dataset\\thsinghua_dataset\\cit_network\\cit-HepTh-dates.txt\\Cit-HepTh-dates.txt");
            var verDic = new Dictionary<String, String>();
            int c = 0;
            foreach (var lineA in linesA)
            {
                if (c > 0)
                {
                    var split = lineA.Split('\t');
                    var id = split[0];
                    var date = split[1];
                    if (verDic.ContainsKey(id))
                    {
                        verDic[id] = verDic[id] + "," + date;
                    }
                    else
                    {
                        verDic.Add(id, date);
                    }
                    Console.WriteLine(cmd.CommandText);
                }
                c++;
            }

            var verDicTemp = new Dictionary<String, String>();
            foreach (var v in verDic)
            {
                verDicTemp[v.Key] = ".property('date', '" + verDic[v.Key] + "')";
            }

            verDic = verDicTemp;

            int c1 = 0;
            // Add vertex submit time
            for (int i = 1992; i <= 2003; i++)
            {
                if (c1 > 1)
                {
                    break;
                }
                c1++;
                var Dir = "D:\\dataset\\thsinghua_dataset\\cit_network\\cit-HepTh-abstracts.tar\\cit-HepTh-abstracts\\" + i;
                DirectoryInfo TheFolder = new DirectoryInfo(Dir);
                foreach (FileInfo NextFile in TheFolder.GetFiles())
                {

                    var lineBs = File.ReadAllText(NextFile.DirectoryName + "\\" + NextFile.Name);
                    var splits1 = lineBs.Split('\\');
                    var prop = splits1[2];
                    var properties = prop.Split('\n');
                    var vid = NextFile.Name.Replace(".abs", "").ToString();


                    foreach (var p in properties)
                    {
                        if (p != "")
                        {
                            var subSplit = p.Split(':');

                            if (subSplit.Length == 2)
                            {
                                // clean the data
                                var value = subSplit[1].Replace("$", "").Replace("\"", "").Replace("'", "");
                                if (verDic.ContainsKey(vid))
                                {
                                    verDic[vid] = verDic[vid] + ".property('" + subSplit[0] + "', '" + value + "')";
                                }
                                else
                                {
                                    verDic.Add(vid, ".property('" + subSplit[0] + "', '" + value + "')");
                                }
                            }
                        }
                    }

                    // var content = splits1[4];
                    // verDic[vid] = verDic[vid] + ".property('" + "content" + "', '" + content + "')";
                }
            }

            foreach (var v in verDic)
            {
                cmd.CommandText = "g.addV('id', '" + v.Key + "')" + v.Value + ".next()";
                cmd.Execute();
                Console.WriteLine("Finish");
            }
            Console.WriteLine("Finish");
        }

        [TestMethod]
        public void GraphViewInsertCitNetworkDataEdge()
        {
            GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
      "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
      "GroupMatch", "PartitionTest", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection.EdgeSpillThreshold = 1;
            GraphViewCommand cmd = new GraphViewCommand(connection);
            // Add edge
            int c = 1;
            var linesE = File.ReadLines("D:\\dataset\\thsinghua_dataset\\cit_network\\cit-HepTh.txt\\Cit-HepTh.txt");
            foreach (var lineE in linesE)
            {
                if (c > 4)
                {
                    var split = lineE.Split('\t');
                    var src = split[0];
                    var des = split[1];
                    var traversal = cmd.g().V(src);
                    var results1 = traversal.Next();
                    var traversal2 = cmd.g().V(des);
                    var results2 = traversal2.Next();

                    bool flag = false;
                    if (results1.Count > 0 && results2.Count > 0)
                    {
                        flag = true;
                    }

                    if (flag)
                    {
                        cmd.CommandText = "g.V('" + src + "').addE('" + DateTime.Now.Millisecond.ToString() + "').to(g.V('" + des + "')).next()";
                        cmd.Execute();
                    }
                    else
                    {
                        Console.WriteLine("Contains null vertex, Don't add Edge");
                    }
                    Console.WriteLine(cmd.CommandText);
                }
                else
                {
                    c++;
                }
            }
        }

        public void partitionQueryTestCommon(String collectionName)
        {
            // (1) FindNeighbours (FN): finds the neighbours of all nodes.
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
               "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
               "GroupMatch", collectionName, GraphType.GraphAPIOnly, AbstractGremlinTest.TEST_USE_REVERSE_EDGE,
               AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, null);
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
            results = graph.g().V("9304045").Out().Out().Next();

            foreach (var result in results)
            {
                Console.WriteLine(result);
            }
            DateTime end2 = DateTime.Now;
            Console.WriteLine("(2)" + (end2.Millisecond - start2.Millisecond) + "ms");
            // (3) Shortest Path: FindShortestPath (FS): finds the shortest path between the first node and 100 randomly picked nodes.

            results = graph.g().V().Next(); // change the the result format or just hack a test suite
            HashSet<int> index = new HashSet<int> { 1, 15 };
            List<String> nodes = new List<string>();
            int i = 0;
            foreach (var result in results)
            {
                i++;
                if (index.Contains(i))
                {
                    nodes.Add(result);
                }
            }

            DateTime start3 = DateTime.Now;
            String src = nodes[0];
            nodes.RemoveAt(0);
            foreach (var node in nodes)
            {
                String des = node;
                ShortestPathTest.GetShortestPath(src, des, graph);
            }
            DateTime end3 = DateTime.Now;
            Console.WriteLine("(3)" + (end3.Millisecond - start3.Millisecond) + "ms");
        }

        [TestMethod]
        public void partitionPerformanceTestForHashPartition()
        {
            partitionQueryTestCommon("CitHashPartition1000item");
        }

        [TestMethod]
        public void partitionPerformanceTestForGreedyPartition()
        {
            partitionQueryTestCommon("CitGreedyPartition1000item");
        }

        [TestMethod]
        public void partitionPerformanceMarvel()
        {
            partitionQueryTestCommon("MarvelTest");
        }



        [TestMethod]
        public void citDataHashPartitionTestAllData()
        {
            GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
     "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
     "GroupMatch", "CitAllDataHashPartition", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection.EdgeSpillThreshold = 1;
            GraphViewConnection.useHashPartitionWhenCreateDoc = true;
            GraphViewConnection.useBulkInsert = true;
            GraphViewCommand cmd = new GraphViewCommand(connection);
            HashSet<String> nodeIdSet = new HashSet<String>();
            // Add edge
            //int edgeCount = 1000;

            int c = 1;
            var linesE = File.ReadLines("D:\\dataset\\thsinghua_dataset\\cit_network\\cit-HepTh.txt\\Cit-HepTh.txt");
            foreach (var lineE in linesE)
            {
                //edgeCount--;
                //if (edgeCount < 0)
                //{
                //    break;
                //}
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

        [TestMethod]
        public void citDataGreedyPartitionTestAllData()
        {
            GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
     "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
     "GroupMatch", "CitAllDataGreedyPartition", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection.EdgeSpillThreshold = 1;
            GraphViewConnection.useGreedyPartitionWhenCreateDoc = true;
            GraphViewConnection.useBulkInsert = true;
            GraphViewCommand cmd = new GraphViewCommand(connection);
            HashSet<String> nodeIdSet = new HashSet<String>();
            // Add edge
            //int edgeCount = 1000;

            int c = 1;
            var linesE = File.ReadLines("D:\\dataset\\thsinghua_dataset\\cit_network\\cit-HepTh.txt\\Cit-HepTh.txt");
            foreach (var lineE in linesE)
            {
                //edgeCount--;
                //if (edgeCount < 0)
                //{
                //    break;
                //}
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
        [TestMethod]
        public void insertFakeDiffPartitionWorkload()
        {
            //       var edgeList = new List<String>();
            //       // partitionData in 3 partitions
            //       for(int i = 0; i < 5; i++)
            //       {
            //           // p1
            //           for(int j = 0; j < 5; j++)
            //           {
            //               edgeList.Add(0 + "-" + i + "\t" + 1 + "-" + j);
            //               // p2
            //               for(int k = 0; k < 5; k++)
            //               {
            //                   // p3
            //                   edgeList.Add(0 + "-" + i + "\t" + 1 + "-" + k);
            //                   edgeList.Add(1 + "-" + j + "\t" + 2 + "-" + k);
            //                   edgeList.Add(2 + "-" + k + "\t" + 0 + "-" + i);
            //               }
            //           }
            //       }

            //       GraphViewConnection connection = GraphViewConnection.ResetGraphAPICollection("https://graphview.documents.azure.com:443/",
            //"MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
            //"GroupMatch", "CitFakeDiffPartition", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            //       connection.EdgeSpillThreshold = 1;
            //       GraphViewConnection.useHashPartitionWhenCreateDoc = false;
            //       GraphViewConnection.useFakePartitionWhenCreateDoc = true;
            //       GraphViewConnection.useBulkInsert = true;
            //       GraphViewCommand cmd = new GraphViewCommand(connection);
            //       HashSet<String> nodeIdSet = new HashSet<String>();
            //       // Add edge

            //       int c = 1;
            //       var linesE = edgeList;
            //       foreach (var lineE in linesE)
            //       {
            //           if (c > 0)
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

            // partition data in 3 partitions

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
  "GroupMatch", "CitFakeSamePartition", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection.EdgeSpillThreshold = 1;
            GraphViewConnection.partitionNum = 3;
            GraphViewConnection.useHashPartitionWhenCreateDoc = false;
            GraphViewConnection.useFakePartitionWhenCreateDoc = true;
            GraphViewConnection.useBulkInsert = true;
            GraphViewConnection.useFakePartitionWhenCreateDocIn1Partition = false;
            GraphViewCommand cmd = new GraphViewCommand(connection);
            HashSet<String> nodeIdSet = new HashSet<String>();
            // Add edge
            //int c = 1;
            //var linesE = edgeList;
            //foreach (var lineE in linesE)
            //{

            //    if (c > 0)
            //    {
            //        var split = lineE.Split('\t');
            //        var src = split[0];
            //        var des = split[1];

            //        if (!nodeIdSet.Contains(src))
            //        {
            //            cmd.CommandText = "g.addV('id', '" + src + "').property('name', '" + src + "').next()";
            //            cmd.Execute();
            //            nodeIdSet.Add(src);
            //        }

            //        if (!nodeIdSet.Contains(des))
            //        {
            //            cmd.CommandText = "g.addV('id', '" + des + "').property('name', '" + des + "').next()";
            //            cmd.Execute();
            //            nodeIdSet.Add(des);
            //        }

            //        cmd.CommandText = "g.V('" + src + "').addE('appear').to(g.V('" + des + "')).next()";
            //        cmd.Execute();
            //    }
            //    else
            //    {
            //        c++;
            //    }
            //}
            //connection.getMetricsOfGraphPartition();
            BulkInsertUtils blk = new BulkInsertUtils(GraphViewConnection.partitionNum);
            blk.threadNum = 3;
            blk.initBulkInsertUtilsForParseData(GraphViewConnection.partitionNum, edgeList.Count, connection);
            //int j = 0;
            var linesE = edgeList;
            foreach (var lineE in linesE)
            {
                var split = lineE.Split('\t');
                var src = split[0];
                var des = split[1];
            }
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
            connection.getMetricsOfGraphPartition();
        }
        [TestMethod]
        public void insertFakeSamePartitionWorkload()
        {
            // partition data in 3 partitions

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
  "GroupMatch", "CitFakeSamePartition", AbstractGremlinTest.TEST_USE_REVERSE_EDGE, AbstractGremlinTest.TEST_SPILLED_EDGE_THRESHOLD_VIAGRAPHAPI, AbstractGremlinTest.TEST_PARTITION_BY_KEY);
            connection.EdgeSpillThreshold = 1;
            GraphViewConnection.partitionNum = 3;
            GraphViewConnection.useHashPartitionWhenCreateDoc = false;
            GraphViewConnection.useFakePartitionWhenCreateDoc = true;
            GraphViewConnection.useBulkInsert = true;
            GraphViewConnection.useFakePartitionWhenCreateDocIn1Partition = true;
            GraphViewCommand cmd = new GraphViewCommand(connection);
            HashSet<String> nodeIdSet = new HashSet<String>();
            // Add edge
            //int c = 1;
            //var linesE = edgeList;
            //foreach (var lineE in linesE)
            //{

            //    if (c > 0)
            //    {
            //        var split = lineE.Split('\t');
            //        var src = split[0];
            //        var des = split[1];

            //        if (!nodeIdSet.Contains(src))
            //        {
            //            cmd.CommandText = "g.addV('id', '" + src + "').property('name', '" + src + "').next()";
            //            cmd.Execute();
            //            nodeIdSet.Add(src);
            //        }

            //        if (!nodeIdSet.Contains(des))
            //        {
            //            cmd.CommandText = "g.addV('id', '" + des + "').property('name', '" + des + "').next()";
            //            cmd.Execute();
            //            nodeIdSet.Add(des);
            //        }

            //        cmd.CommandText = "g.V('" + src + "').addE('appear').to(g.V('" + des + "')).next()";
            //        cmd.Execute();
            //    }
            //    else
            //    {
            //        c++;
            //    }
            //}
            //connection.getMetricsOfGraphPartition();
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
            connection.getMetricsOfGraphPartition();
        }
    }
}
