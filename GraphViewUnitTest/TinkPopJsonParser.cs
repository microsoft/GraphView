using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using GraphView;
using Newtonsoft.Json.Linq;
using System.IO;
using System.Threading;
using System.Diagnostics;

namespace GraphViewUnitTest
{
    [TestClass]
    public class TinkPopJsonParser
    {
        [TestMethod]
        public void ResetCollection(String collectionName)
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                    "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                    "GroupMatch", collectionName);
            connection.SetupClient();

            connection.DocDB_finish = false;
            connection.BuildUp();
            while (!connection.DocDB_finish)
                System.Threading.Thread.Sleep(10);

            connection.ResetCollection();

            connection.DocDB_finish = false;
            connection.BuildUp();
            while (!connection.DocDB_finish)
                System.Threading.Thread.Sleep(10);
        }
        [TestMethod]
        public void SpecialDataProcessingTest1()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
              "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
              "GroupMatch", "MarvelTest");
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            ResetCollection("MarvelTest");
            // Insert node from collections
            String value = "Jim O'Meara (Gaelic footballer)".Replace("'", "\\'");
            String tempSQL = "g.addV('id','30153','properties.id','30152','properties.value','" + value + "','label','Person')";
            parser.Parse(tempSQL.ToString()).Generate(connection).Next();
            Console.WriteLine(tempSQL);
        }
        [TestMethod]
        public void insertJsonMultiTheadByCountDownlatch()
        {
            int i = 0;
            var lines = File.ReadLines(@"D:\dataset\AzureIOT\graphson-dataset.json");
            int index = 0;
            var nodePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();
            var outEdgePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();
            var inEdgePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();

            foreach (var line in lines)
            {
                JObject root = JObject.Parse(line);
                var nodeIdJ = root["id"];
                var nodeLabelJ = root["label"];
                var nodePropertiesJ = root["properties"];
                var nodeOutEJ = root["outE"];
                var nodeInEJ = root["inE"];

                // parse nodeId
                var nodeIdV = nodeIdJ.First.Next.ToString();
                // parse label
                var nodeLabelV = nodeLabelJ.ToString();
                // parse node properties
                foreach (var property in nodePropertiesJ.Children())
                {
                    if (property.HasValues && property.First.HasValues && property.First.First.Next != null)
                    {
                        var tempPChild = property.First.First.Next.Children();
                        foreach (var child1Properties in tempPChild)
                        {
                            // As no API to get the properties name, make it not general
                            var id = nodeIdJ.Last.ToString();
                            if (id != null)
                            {
                                if (id != null)
                                {
                                    var propertyId = child1Properties["id"];
                                    var node = new Dictionary<String, String>();
                                    nodePropertiesHashMap[id.ToString()] = node;
                                    nodePropertiesHashMap[id.ToString()]["id"] = propertyId.Last.ToString();
                                }
                                var value = child1Properties["value"];
                                if (value != null)
                                {
                                    nodePropertiesHashMap[id.ToString()]["value"] = value.ToString().Replace("'", "\\'").Replace("\"", "\\\"");
                                }
                                var label = nodeLabelJ.ToString();
                                if (label != null)
                                {
                                    nodePropertiesHashMap[id.ToString()]["label"] = label.ToString().Replace("'", "\\'").Replace("\"", "\\\"");
                                }
                            }
                        }
                    }
                }
                // parse outE
                var nString = nodeOutEJ.ToString();
                if (nodeOutEJ.HasValues && nodeOutEJ.ToString().Contains("extends"))
                {
                    var tempE = nodeOutEJ.First.Root;
                    foreach (var outEdge in nodeOutEJ.First.First.Last.Children())
                    {
                        var id = outEdge["id"].First.Next;
                        var inV = outEdge["inV"].First.Next;
                        var edgeString = inV + "_" + nodeIdJ.Last();
                        var dic = new Dictionary<string, string>();
                        outEdgePropertiesHashMap[edgeString] = dic;
                        outEdgePropertiesHashMap[edgeString].Add("id", id.ToString());
                    }
                }
                // parse inE
                var inString = nodeInEJ.ToString();

                if (nodeInEJ.HasValues && nodeInEJ.ToString().Contains("shown_as"))
                {
                    var tempE = nodeInEJ.First.Root;
                    foreach (var inEdge in nodeInEJ.First.First.Last.Children())
                    {
                        var id = inEdge["id"].First.Next;
                        var outV = inEdge["outV"].First.Next;
                        var edgeString = outV + "_" + nodeIdJ.Last();
                        var dic = new Dictionary<string, string>();
                        inEdgePropertiesHashMap[edgeString] = dic;
                        inEdgePropertiesHashMap[edgeString].Add("id", id.ToString());
                    }
                }
            }

            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            ResetCollection("MarvelTest");
            // Insert node from collections
            int taskNum = nodePropertiesHashMap.Count;
            CountdownEvent cde = new CountdownEvent(taskNum);

            foreach (var node in nodePropertiesHashMap)
            {
                InsertNodeObjectDocDB insertObj = new InsertNodeObjectDocDB();
                WaitCallback callBack = new WaitCallback(InsertDoc);
                insertObj.parser = parser;
                insertObj.connection = connection;
                insertObj.cde = cde;
                insertObj.node = node;
                ThreadPool.QueueUserWorkItem(callBack, insertObj);
            }
            
            cde.Dispose();

            // Insert out edge from collections
            foreach (var edge in outEdgePropertiesHashMap)
            {
                String[] nodeIds = edge.Key.Split('_');
                String srcId = nodeIds[0];
                String desId = nodeIds[1];
                StringBuilder edgePropertyList = new StringBuilder(",");
                edgePropertyList.Append("'id',");
                edgePropertyList.Append("'" + edge.Value["id"].ToString() + "'");
                String tempInsertSQL = "g.V.as('v').has('id','" + srcId + "').as('a').select('v').has('id','" + desId + "').as('b').select('a','b').addOutE('a','extends','b'" + edgePropertyList.ToString() + ")";
                parser.Parse(tempInsertSQL).Generate(connection).Next();
                Console.WriteLine(tempInsertSQL);
            }
            // Insert in edge from collections
            foreach (var edge in inEdgePropertiesHashMap)
            {
                String[] nodeIds = edge.Key.Split('_');
                String srcId = nodeIds[0];
                String desId = nodeIds[1];
                // Inset Edge
                StringBuilder edgePropertyList = new StringBuilder(",");
                edgePropertyList.Append("'id',");
                edgePropertyList.Append("'" + edge.Value["id"].ToString() + "'");
                String tempInsertSQL = "g.V.as('v').has('id','" + srcId + "').as('a').select('v').has('id','" + desId + "').as('b').select('a','b').addInE('a','shown_as','b'" + edgePropertyList.ToString() + ")";
                parser.Parse(tempInsertSQL).Generate(connection).Next();
                Console.WriteLine(tempInsertSQL);
            }
        }

        static void InsertDoc(Object state)
        {
            var stateObj = (InsertNodeObjectDocDB)state;
            var node = stateObj.node;
            StringBuilder tempSQL = new StringBuilder("g.addV(");
            tempSQL.Append("\'id\',");
            tempSQL.Append("\'" + node.Key + "\',");
            tempSQL.Append("\'" + "properties.id" + "\',");
            tempSQL.Append("\'" + node.Value["id"] + "\',");
            tempSQL.Append("\'" + "properties.value" + "\',");
            tempSQL.Append("\'" + node.Value["value"] + "\',");
            tempSQL.Append("\'" + "label" + "\',");
            tempSQL.Append("\'" + node.Value["label"] + "\'");
            tempSQL.Append(")");
            Console.WriteLine(tempSQL);
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
      "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
      "GroupMatch", "MarvelTest");
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            parser.Parse(tempSQL.ToString()).Generate(connection).Next();
            stateObj.cde.Signal();
        }
        [TestMethod]
        public void parseJsonSingleThread()
        {
            int i = 0;
            var lines = File.ReadLines(@"D:\dataset\AzureIOT\graphson-addition1.json");
            int index = 0;
            var nodePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();
            var outEdgePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();
            var inEdgePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();

            foreach (var line in lines)
            {
                JObject root = JObject.Parse(line);
                var nodeIdJ = root["id"];
                var nodeLabelJ = root["label"];
                var nodePropertiesJ = root["properties"];
                var nodeOutEJ = root["outE"];
                var nodeInEJ = root["inE"];

                // parse nodeId
                var nodeIdV = nodeIdJ.First.Next.ToString();
                // parse label
                var nodeLabelV = nodeLabelJ.ToString();
                // parse node properties
                foreach (var property in nodePropertiesJ.Children())
                {
                    if (property.HasValues && property.First.HasValues && property.First.First.Next != null)
                    {
                        var tempPChild = property.First.First.Next.Children();
                        string prefixName = null;
                        if (property.ToString().Contains("name"))
                        {
                            prefixName = "name";
                        }

                        if (property.ToString().Contains("manufacturer"))
                        {
                            prefixName = "manufacturer";
                        }

                        if (property.ToString().Contains("modelNumber"))
                        {
                            prefixName = "modelNumber";
                        }

                        var id = nodeIdJ.Last.ToString();
                        var node = new Dictionary<String, String>();
                        nodePropertiesHashMap[id.ToString()] = node;
                        var label = nodeLabelJ.ToString();
                        if (label != null)
                        {
                            nodePropertiesHashMap[id.ToString()]["label"] = label.ToString().Replace("'", "\\'").Replace("\"", "\\\"");
                        }

                        foreach (var child1Properties in tempPChild)
                        {
                            // As no API to get the properties name, make it not general
                            if (id != null)
                            {
                                if (id != null)
                                {
                                    var propertyId = child1Properties["id"];
                                    nodePropertiesHashMap[id.ToString()]["properties."+prefixName+".id"] = propertyId.Last.ToString();
                                }
                                var value = child1Properties["value"];
                                if (value != null)
                                {
                                    nodePropertiesHashMap[id.ToString()]["properties." + prefixName + ".value"] = value.ToString().Replace("'", "\\'").Replace("\"", "\\\"");
                                }
          
                            }
                        }
                    }
                }
                // parse outE
                var nString = nodeOutEJ.ToString();

                if (nodeOutEJ.HasValues && nodeOutEJ.ToString().Contains("shown_as"))
                {
                    var tempE = nodeOutEJ.First.Root;
                    foreach (var outEdge in nodeOutEJ.First.First.Last.Children())
                    {
                        var id = outEdge["id"].First.Next;
                        var inV = outEdge["inV"].First.Next;
                        var edgeString = inV + "_" + nodeIdJ.Last();
                        var dic = new Dictionary<string, string>();
                        outEdgePropertiesHashMap[edgeString] = dic;
                        outEdgePropertiesHashMap[edgeString].Add("id", id.ToString());
                        outEdgePropertiesHashMap[edgeString].Add("edge_type", "shown_as");
                    }
                }
                // parse inE
                var inString = nodeInEJ.ToString();
                var iter = nodeInEJ.First;
                while (iter.Next != null)
                {

                    nodeInEJ = iter;
                    if (nodeInEJ.HasValues && nodeInEJ.ToString().Contains("extends"))
                    {
                        var tempE = nodeInEJ.First.Root;
                        foreach (var inEdge in nodeInEJ.First.Last.Children())
                        {
                            var id = inEdge["id"].First.Next;
                            var outV = inEdge["outV"].First.Next;
                            var edgeString = outV + "_" + nodeIdJ.Last();
                            var dic = new Dictionary<string, string>();
                            inEdgePropertiesHashMap[edgeString] = dic;
                            inEdgePropertiesHashMap[edgeString].Add("id", id.ToString());
                            inEdgePropertiesHashMap[edgeString].Add("edge_type", "extends");
                        }
                    }

                    if (nodeInEJ.HasValues && nodeInEJ.ToString().Contains("type_of"))
                    {
                        var tempE = nodeInEJ.First.Root;
                        foreach (var inEdge in nodeInEJ.First.Last.Children())
                        {
                            var id = inEdge["id"].First.Next;
                            var outV = inEdge["outV"].First.Next;
                            var edgeString = outV + "_" + nodeIdJ.Last();
                            var dic = new Dictionary<string, string>();
                            inEdgePropertiesHashMap[edgeString] = dic;
                            inEdgePropertiesHashMap[edgeString].Add("id", id.ToString());
                            inEdgePropertiesHashMap[edgeString].Add("edge_type", "type_of");
                        }
                    }

                    iter = iter.Next;
                }
            }

            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            ResetCollection("MarvelTest");
            var result = new List<Double>();
            var sumTime = 0.0;
            var nodeInsertNumbers = 100;
            var edgeInsertNumbers = 100;

            // Insert node from collections
            var nodeCount = 0;
            foreach (var node in nodePropertiesHashMap)
            {
                StringBuilder tempSQL = new StringBuilder("g.addV(");
                tempSQL.Append("\'id\',");
                tempSQL.Append("\'" + node.Key + "\',");

                foreach (var keyV in node.Value)
                {
                    tempSQL.Append("\'" + keyV.Key + "\',");
                    tempSQL.Append("\'" + keyV.Value + "\',");
                }

                tempSQL.Remove(tempSQL.Length - 1, 1);
                tempSQL.Append(")");
                Stopwatch sw = new Stopwatch();
                sw.Start();
                parser.Parse(tempSQL.ToString()).Generate(connection).Next();
                sw.Stop();
                sumTime += sw.Elapsed.TotalMilliseconds;
                result.Add(sw.Elapsed.TotalMilliseconds);
                //Console.WriteLine("query{0} time is:{1}", i, sw.Elapsed.TotalMilliseconds);
                Console.WriteLine("{0} time is:{1}", i, sw.Elapsed.TotalMilliseconds);
                nodeCount++;
                if(nodeCount > nodeInsertNumbers)
                {
                    break;
                }
            }

            if (result.Count > 0)
            {
                Console.WriteLine("max insert node time is: {0}", result.Max());
                Console.WriteLine("min insert node time is: {0}", result.Min());
                Console.WriteLine("avg insert node time is: {0}", result.Average());
                Console.WriteLine("stdDev insert node time is: {0}", DocDBUtils.stdDev(result));
                Console.WriteLine("avg,max,min,stdDev");
                Console.WriteLine("{0}, {1}, {2}, {3}", result.Average(), result.Max(), result.Min(), DocDBUtils.stdDev(result));
            }
            // wait for node insert finish

            var outEdgeNum = 0;
            result = new List<Double>();

            // Insert out edge from collections
            foreach (var edge in outEdgePropertiesHashMap)
            {
                String[] nodeIds = edge.Key.Split('_');
                String srcId = nodeIds[0];
                String desId = nodeIds[1];
                // Inset Edge
                StringBuilder edgePropertyList = new StringBuilder(",");
                edgePropertyList.Append("'id',");
                edgePropertyList.Append("'" + edge.Value["id"].ToString() + "'");
                var edgeType = edge.Value["edge_type"];
                String tempInsertSQL = "g.V.as('v').has('id','" + srcId + "').as('a').select('v').has('id','" + desId + "').as('b').select('a','b').addOutE('a','" + edgeType + "','b'" + edgePropertyList.ToString() + ")";
                Console.WriteLine(tempInsertSQL);
                Stopwatch sw = new Stopwatch();
                sw.Start();
                parser.Parse(tempInsertSQL).Generate(connection).Next();
                sw.Stop();
                result.Add(sw.Elapsed.TotalMilliseconds);
                outEdgeNum++ ;
                if(outEdgeNum > edgeInsertNumbers)
                {
                    break;
                }
            }

            if (result.Count > 0)
            {
                Console.WriteLine("max insert outEdge time is: {0}", result.Max());
                Console.WriteLine("min insert outEdge time is: {0}", result.Min());
                Console.WriteLine("avg insert outEdge time is: {0}", result.Average());
                Console.WriteLine("stdDev insert outEdge time is: {0}", DocDBUtils.stdDev(result));
                Console.WriteLine("avg,max,min,stdDev");
                Console.WriteLine("{0}, {1}, {2}, {3}", result.Average(), result.Max(), result.Min(), DocDBUtils.stdDev(result));
            }

            var inEdgeNum = 0;
            result = new List<Double>();

            // Insert in edge from collections
            foreach (var edge in inEdgePropertiesHashMap)
            {
                String[] nodeIds = edge.Key.Split('_');
                String srcId = nodeIds[0];
                String desId = nodeIds[1];
                // Inset Edge
                StringBuilder edgePropertyList = new StringBuilder(",");
                edgePropertyList.Append("'id',");
                edgePropertyList.Append("'" + edge.Value["id"].ToString() + "'");
                var edgeType = edge.Value["edge_type"];
                String tempInsertSQL = "g.V.as('v').has('id','" + srcId + "').as('a').select('v').has('id','" + desId + "').as('b').select('a','b').addInE('a','" + edgeType + "','b'" + edgePropertyList.ToString() + ")";
                Stopwatch sw = new Stopwatch();
                Console.WriteLine(tempInsertSQL);
                sw.Start();
                parser.Parse(tempInsertSQL).Generate(connection).Next();
                sw.Stop();
                result.Add(sw.Elapsed.TotalMilliseconds);
                inEdgeNum++;
                if (inEdgeNum > edgeInsertNumbers)
                {
                    break;
                }
            }

            if (result.Count > 0)
            {
                Console.WriteLine("max insert in edge time is: {0}", result.Max());
                Console.WriteLine("min insert in edge time is: {0}", result.Min());
                Console.WriteLine("avg insert in edge time is: {0}", result.Average());
                Console.WriteLine("stdDev insert in edge time is: {0}", DocDBUtils.stdDev(result));
                Console.WriteLine("avg,max,min,stdDev");
                Console.WriteLine("{0}, {1}, {2}, {3}", result.Average(), result.Max(), result.Min(), DocDBUtils.stdDev(result));
            }
        }
        [TestMethod]
        public void InsertJsonMultiThreadByBoundedBufferByCommand()
        {
            string path = @"D:\dataset\AzureIOT\graphson-exception2.json";
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest");
            string collectionName = "MarvelTest";
            int threadNumber = 1;
            GraphLoaderFactory.loadAzureIOT(path, connection, collectionName, true, threadNumber);
        }

        [TestMethod]
        public void InsertJsonMultiThreadByBoundedBufferBySQLString()
        {
            // parse data
            int i = 0;
            var lines = File.ReadLines(@"D://graphson-subset.json");
            int index = 0;
            var nodePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();
            var outEdgePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();
            var inEdgePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();

            foreach (var line in lines)
            {
                JObject root = JObject.Parse(line);
                var nodeIdJ = root["id"];
                var nodeLabelJ = root["label"];
                var nodePropertiesJ = root["properties"];
                var nodeOutEJ = root["outE"];
                var nodeInEJ = root["inE"];

                // parse nodeId
                var nodeIdV = nodeIdJ.First.Next.ToString();
                // parse label
                var nodeLabelV = nodeLabelJ.ToString();
                // parse node properties
                foreach (var property in nodePropertiesJ.Children())
                {
                    // test
                    //var path = property.Path;
                    //var _id = property["id"];
                    //var _value = property["value"];
                    // test
                    if (property.HasValues && property.First.HasValues && property.First.First.Next != null)
                    {
                        var tempPChild = property.First.First.Next.Children();
                        string prefixName = null;
                        if (property.Path.ToString().Contains("name"))
                        {
                            prefixName = "name";
                        }

                        if (property.Path.ToString().Contains("manufacturer"))
                        {
                            prefixName = "manufacturer";
                        }

                        if (property.Path.ToString().Contains("modelNumber"))
                        {
                            prefixName = "modelNumber";
                        }

                        if (property.Path.ToString().Contains("deviceId"))
                        {
                            prefixName = "deviceId";
                        }

                        if (property.Path.ToString().Contains("location"))
                        {
                            prefixName = "location";
                        }

                        var id = nodeIdJ.Last.ToString();
                        if (!nodePropertiesHashMap.ContainsKey(id.ToString()))
                        {
                            var node = new Dictionary<String, String>();
                            nodePropertiesHashMap[id.ToString()] = node;
                        }

                        var label = nodeLabelJ.ToString();
                        if (label != null)
                        {
                            nodePropertiesHashMap[id.ToString()]["label"] = label.ToString().Replace("'", "\\'").Replace("\"", "\\\"");
                        }

                        foreach (var child1Properties in tempPChild)
                        {
                            // As no API to get the properties name, make it not general
                            if (id != null)
                            {
                                if (id != null)
                                {
                                    var propertyId = child1Properties["id"];
                                    nodePropertiesHashMap[id.ToString()]["properties." + prefixName + ".id"] = propertyId.Last.ToString();
                                }
                                var value = child1Properties["value"];
                                if (value != null)
                                {
                                    nodePropertiesHashMap[id.ToString()]["properties." + prefixName + ".value"] = value.ToString().Replace("'", "\\'").Replace("\"", "\\\"");
                                }

                            }
                        }
                    }
                }
                // parse outE
                var iterOut = nodeOutEJ.First;
                while (iterOut.Next != null)
                {
                    nodeOutEJ = iterOut;
                    if (nodeOutEJ.HasValues && nodeOutEJ.ToString().Contains("extends"))
                    {
                        var tempE = nodeOutEJ.First.Root;
                        foreach (var outEdge in nodeOutEJ.First.Last.Children())
                        {
                            var id = outEdge["id"].First.Next;
                            var inV = outEdge["inV"].First.Next;
                            var edgeString = inV + "_" + nodeIdJ.Last();
                            var dic = new Dictionary<string, string>();
                            outEdgePropertiesHashMap[edgeString] = dic;
                            outEdgePropertiesHashMap[edgeString].Add("id", id.ToString());
                            outEdgePropertiesHashMap[edgeString].Add("edge_type", "extends");
                        }
                    }

                    if (nodeOutEJ.HasValues && nodeOutEJ.ToString().Contains("shown_as"))
                    {
                        var tempE = nodeOutEJ.First.Root;
                        foreach (var outEdge in nodeOutEJ.First.Last.Children())
                        {
                            var id = outEdge["id"].First.Next;
                            var inV = outEdge["inV"].First.Next;
                            var edgeString = inV + "_" + nodeIdJ.Last();
                            var dic = new Dictionary<string, string>();
                            outEdgePropertiesHashMap[edgeString] = dic;
                            outEdgePropertiesHashMap[edgeString].Add("id", id.ToString());
                            outEdgePropertiesHashMap[edgeString].Add("edge_type", "shown_as");
                        }
                    }

                    if (nodeOutEJ.HasValues && nodeOutEJ.ToString().Contains("type_of"))
                    {
                        var tempE = nodeOutEJ.First.Root;
                        foreach (var outEdge in nodeOutEJ.First.Last.Children())
                        {
                            var id = outEdge["id"].First.Next;
                            var inV = outEdge["inV"].First.Next;
                            var edgeString = inV + "_" + nodeIdJ.Last();
                            var dic = new Dictionary<string, string>();
                            outEdgePropertiesHashMap[edgeString] = dic;
                            outEdgePropertiesHashMap[edgeString].Add("id", id.ToString());
                            outEdgePropertiesHashMap[edgeString].Add("edge_type", "type_of");
                        }
                    }

                    iterOut = iterOut.Next;
                }
                
                // parse inE
                //var inString = nodeInEJ.ToString();
                var iter = nodeInEJ.First;
                while (iter.Next != null)
                {

                    nodeInEJ = iter;
                    if (nodeInEJ.HasValues && nodeInEJ.ToString().Contains("extends"))
                    {
                        var tempE = nodeInEJ.First.Root;
                        foreach (var inEdge in nodeInEJ.First.Last.Children())
                        {
                            var id = inEdge["id"].First.Next;
                            var outV = inEdge["outV"].First.Next;
                            var edgeString = outV + "_" + nodeIdJ.Last();
                            var dic = new Dictionary<string, string>();
                            inEdgePropertiesHashMap[edgeString] = dic;
                            inEdgePropertiesHashMap[edgeString].Add("id", id.ToString());
                            inEdgePropertiesHashMap[edgeString].Add("edge_type", "extends");
                        }
                    }

                    if (nodeInEJ.HasValues && nodeInEJ.ToString().Contains("type_of"))
                    {
                        var tempE = nodeInEJ.First.Root;
                        foreach (var inEdge in nodeInEJ.First.Last.Children())
                        {
                            var id = inEdge["id"].First.Next;
                            var outV = inEdge["outV"].First.Next;
                            var edgeString = outV + "_" + nodeIdJ.Last();
                            var dic = new Dictionary<string, string>();
                            inEdgePropertiesHashMap[edgeString] = dic;
                            inEdgePropertiesHashMap[edgeString].Add("id", id.ToString());
                            inEdgePropertiesHashMap[edgeString].Add("edge_type", "type_of");
                        }
                    }

                    if (nodeInEJ.HasValues && nodeInEJ.ToString().Contains("shown_as"))
                    {
                        var tempE = nodeInEJ.First.Root;
                        foreach (var inEdge in nodeInEJ.First.Last.Children())
                        {
                            var id = inEdge["id"].First.Next;
                            var outV = inEdge["outV"].First.Next;
                            var edgeString = outV + "_" + nodeIdJ.Last();
                            var dic = new Dictionary<string, string>();
                            inEdgePropertiesHashMap[edgeString] = dic;
                            inEdgePropertiesHashMap[edgeString].Add("id", id.ToString());
                            inEdgePropertiesHashMap[edgeString].Add("edge_type", "shown_as");
                        }
                    }

                    iter = iter.Next;
                }
            }

            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "IOTTest1");
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            ResetCollection("IOTTest1");
            // Insert node from collections
            BoundedBuffer<string> inputBuffer = new BoundedBuffer<string>(10000);
            int threadNum = 100;
            List<Thread> insertThreadList = new List<Thread>();

            for (int j = 0; j < threadNum; j++)
            {
                DocDBInsertWorker worker1 = new DocDBInsertWorker(connection, inputBuffer);
                worker1.threadId = j;
                Thread t1 = new Thread(worker1.BulkInsert);
                insertThreadList.Add(t1);
            }

            for (int j = 0; j < threadNum; j++)
            {
                insertThreadList[j].Start();
                Console.WriteLine("Start the thread" + j);
            }
            
            //var nodeIter = nodePropertiesHashMap.GetEnumerator
            Console.WriteLine("finish the parse");
            inputBuffer.Close();

            for (int j = 0; j < threadNum; j++)
            {
                insertThreadList[j].Join();
            }

            for (int j = 0; j < threadNum; j++)
            {
                insertThreadList[j].Abort();
            }

            Console.WriteLine("Finish init the dataset");
        }

    }
    public class InsertNodeObjectDocDB
    {
        public string tempSQL;
        public GraphViewGremlinParser parser;
        public GraphViewConnection connection;
        public CountdownEvent cde = null;
        public KeyValuePair<string, Dictionary<string, string>> node;
    }
}
