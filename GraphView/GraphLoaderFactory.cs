using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using System.IO;
using System.Threading;
using System.Diagnostics;

namespace GraphView
{
    public static class GraphLoaderFactory
    {
        public static void loadAzureIOTNewDataSet(string pathSet, Boolean isNode, GraphViewConnection connection, string collectionName, Boolean resetCollection, int threadNum)
        {
            // parse data
                var paths = pathSet.Split(';');
                foreach (var path in paths)
                {
                    var lines = File.ReadLines(path);
                    var recordCount = lines.Count();
                    BoundedBuffer<String> inputBuffer = new BoundedBuffer<String>(recordCount);
                    foreach (var line in lines)
                    {
                        inputBuffer.Add(line);
                    }
                    executor(path, isNode, connection, collectionName, resetCollection, threadNum, inputBuffer);
                }
        }
        public static void executor(string path, Boolean isNode, GraphViewConnection connection, string collectionName, Boolean resetCollection, int threadNum
           , BoundedBuffer<String> inputBuffer)
        {
            if (isNode)
            {
                List<Thread> insertNodeThreadList = new List<Thread>();

                for (int j = 0; j < threadNum; j++)
                {
                   
                    DocDBInsertNodeWorkerCommon worker1 = new DocDBInsertNodeWorkerCommon(connection, inputBuffer);
                    worker1.threadId = j;
                    //worker1.result = result;
                    Thread t1 = new Thread(worker1.BulkInsert);
                    insertNodeThreadList.Add(t1);
                }

                for (int j = 0; j < threadNum; j++)
                {
                    insertNodeThreadList[j].Start();
                    Console.WriteLine("Start the thread" + j);
                }

                Console.WriteLine("finish the parse");
                //inputNodeBuffer.Close();

                for (int j = 0; j < threadNum; j++)
                {
                    insertNodeThreadList[j].Join();
                }

                for (int j = 0; j < threadNum; j++)
                {
                    insertNodeThreadList[j].Abort();
                }
            } else
            {
                var edgeResult = new List<Double>();

                List<Thread> insertEdgeThreadList = new List<Thread>();

                for (int j = 0; j < threadNum; j++)
                {
                    DocDBInsertEdgeWorkerCommon worker1 = new DocDBInsertEdgeWorkerCommon(connection, inputBuffer);
                    worker1.threadId = j;
                    worker1.result = edgeResult;
                    Thread t1 = new Thread(worker1.BulkInsert);
                    insertEdgeThreadList.Add(t1);
                }

                for (int j = 0; j < threadNum; j++)
                {
                    insertEdgeThreadList[j].Start();
                    Console.WriteLine("Start the thread" + j);
                }

                Console.WriteLine("finish the parse");
                //inputInEdgeBuffer.Close();
                //inputOutEdgeBuffer.Close();

                for (int j = 0; j < threadNum; j++)
                {
                    insertEdgeThreadList[j].Join();
                }

                for (int j = 0; j < threadNum; j++)
                {
                    insertEdgeThreadList[j].Abort();
                }
            }
            // Insert Edge
            Console.WriteLine("try to print result");
            //if (isNode && result.Count > 0)
            //{
            //    Console.WriteLine("max insert node time is: {0}", result.Max());
            //    Console.WriteLine("min insert node time is: {0}", result.Min());
            //    Console.WriteLine("avg insert node time is: {0}", result.Average());
            //    Console.WriteLine("sum insert node time is: {0}", result.Sum());
            //    Console.WriteLine("item count" + result.Count);
            //    //Console.WriteLine("stdDev insert node time is: {0}", DocDBUtils.stdDev(result));
            //    Console.WriteLine("avg,max,min,stdDev");
            //    //Console.WriteLine("{0}, {1}, {2}, {3}", result.Average(), result.Max(), result.Min(), DocDBUtils.stdDev(result));
            //}

            //if (!isNode && edgeResult.Count > 0)
            //{
            //    Console.WriteLine("max insert edge time is: {0}", edgeResult.Max());
            //    Console.WriteLine("min insert edge time is: {0}", edgeResult.Min());
            //    Console.WriteLine("avg insert edge time is: {0}", edgeResult.Average());
            //    Console.WriteLine("sum insert node time is: {0}", edgeResult.Sum());
            //    Console.WriteLine("item count" + edgeResult.Count);
            //    //Console.WriteLine("stdDev insert node time is: {0}", DocDBUtils.stdDev(result));
            //    Console.WriteLine("avg,max,min,stdDev");
            //    //Console.WriteLine("{0}, {1}, {2}, {3}", result.Average(), result.Max(), result.Min(), DocDBUtils.stdDev(result));
            //}

            Console.WriteLine("Finish init the dataset");
        }

        public static void parseData(string path, GraphViewConnection connection, string collectionName, Boolean resetCollection, int threadNum,
            Dictionary<string, Dictionary<string, string>> nodePropertiesHashMap, Dictionary<string, Dictionary<string, string>> outEdgePropertiesHashMap, Dictionary<string, Dictionary<string, string>> inEdgePropertiesHashMap)
        {
            // parse data
            int i = 0;
            var lines = File.ReadLines(path);
            int index = 0;
         
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
                    var id = nodeIdJ.Last.ToString();
                    if (!nodePropertiesHashMap.ContainsKey(id.ToString()))
                    {
                        var node = new Dictionary<String, String>();
                        nodePropertiesHashMap[id.ToString()] = node;
                    }

                    if (property.HasValues && property.First.HasValues && property.First.First.Next != null)
                    {
                        var tempPChild = property.First.First.Next.Children();

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
                                    nodePropertiesHashMap[id.ToString()][property.Path.ToString().Replace('.', '_') + "_id"] = propertyId.Last.ToString();
                                }
                                var value = child1Properties["value"];
                                if (value != null)
                                {
                                    nodePropertiesHashMap[id.ToString()][property.Path.ToString().Replace('.', '_') + "_value"] = value.ToString().Replace("'", " ").Replace("\"", "\\\"").Replace(')', ' ').Replace('(', ' ');
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
                    var tempE = nodeOutEJ.First.Root;

                    foreach (var outEdge in nodeOutEJ.First.Last.Children())
                    {
                        var id = outEdge["id"].First.Next;
                        var inV = outEdge["inV"].First.Next;
                        var edgeString = inV + "_" + nodeIdJ.Last();
                        var dic = new Dictionary<string, string>();
                        outEdgePropertiesHashMap[edgeString] = dic;
                        outEdgePropertiesHashMap[edgeString].Add("id", id.ToString());
                        var edgeTypeArray = nodeOutEJ.Path.Split('.');
                        outEdgePropertiesHashMap[edgeString].Add("type", edgeTypeArray[edgeTypeArray.Length - 1]);
                    }

                    iterOut = iterOut.Next;
                }

                // parse inE
                var iter = nodeInEJ.First;

                while (iter.Next != null)
                {

                    nodeInEJ = iter;
                    var tempE = nodeInEJ.First.Root;
                    foreach (var inEdge in nodeInEJ.First.Last.Children())
                    {
                        var id = inEdge["id"].First.Next;
                        var outV = inEdge["outV"].First.Next;
                        var edgeString = outV + "_" + nodeIdJ.Last();
                        var dic = new Dictionary<string, string>();
                        inEdgePropertiesHashMap[edgeString] = dic;
                        inEdgePropertiesHashMap[edgeString].Add("id", id.ToString());
                        var edgeTypeArray = nodeInEJ.Path.Split('.');
                        inEdgePropertiesHashMap[edgeString].Add("type", edgeTypeArray[edgeTypeArray.Length - 1]);
                    }

                    iter = iter.Next;
                }
            }
        }

        public static void loadAzureIOT(string path, GraphViewConnection connection, string collectionName,  Boolean resetCollection, int threadNum)
        {
            // parse data
            var nodePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();
            var outEdgePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();
            var inEdgePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();

            parseData(path, connection, collectionName, resetCollection, threadNum, nodePropertiesHashMap, outEdgePropertiesHashMap, inEdgePropertiesHashMap);
            connection.ResetCollection();
            var result = new List<Double>();
            var sumTime = 0.0;
            var nodeInsertNumbers = 100;
            var edgeInsertNumbers = 100;

            // Insert node from collections
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputNodeBuffer = new BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>>(nodePropertiesHashMap.Count + 1);
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputInEdgeBuffer = new BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>>(inEdgePropertiesHashMap.Count + 1);
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputOutEdgeBuffer = new BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>>(outEdgePropertiesHashMap.Count + 1);
            var nodeIter = nodePropertiesHashMap.GetEnumerator();

            while (nodeIter.MoveNext())
            {
                inputNodeBuffer.Add(nodeIter.Current);
            }

            var inEdgeIter = inEdgePropertiesHashMap.GetEnumerator();

            while (inEdgeIter.MoveNext())
            {
                inputInEdgeBuffer.Add(inEdgeIter.Current);
            }

            var outEdgeIter = outEdgePropertiesHashMap.GetEnumerator();

            while (outEdgeIter.MoveNext())
            {
                inputOutEdgeBuffer.Add(outEdgeIter.Current);
            }

            List<Thread> insertNodeThreadList = new List<Thread>();

            for (int j = 0; j < threadNum; j++)
            {
                DocDBInsertNodeWorkerByNewAPI worker1 = new DocDBInsertNodeWorkerByNewAPI(connection, inputNodeBuffer, inputInEdgeBuffer, inputOutEdgeBuffer);
                worker1.threadId = j;
                worker1.result = result;
                Thread t1 = new Thread(worker1.BulkInsert);
                insertNodeThreadList.Add(t1);
            }

            for (int j = 0; j < threadNum; j++)
            {
                insertNodeThreadList[j].Start();
                Console.WriteLine("Start the thread" + j);
            }

            Console.WriteLine("finish the parse");
            inputNodeBuffer.Close();

            for (int j = 0; j < threadNum; j++)
            {
                insertNodeThreadList[j].Join();
            }

            for (int j = 0; j < threadNum; j++)
            {
                insertNodeThreadList[j].Abort();
            }
            
            // Insert Edge
            var edgeResult = new List<Double>();

            List<Thread> insertEdgeThreadList = new List<Thread>();

            for (int j = 0; j < threadNum; j++)
            {
                DocDBInsertEdgeWorkerByNewAPI worker1 = new DocDBInsertEdgeWorkerByNewAPI(connection, inputNodeBuffer, inputInEdgeBuffer, inputOutEdgeBuffer);
                worker1.threadId = j;
                worker1.result = edgeResult;
                Thread t1 = new Thread(worker1.BulkInsert);
                insertEdgeThreadList.Add(t1);
            }
            
            for (int j = 0; j < threadNum; j++)
            {
                insertEdgeThreadList[j].Start();
                Console.WriteLine("Start the thread" + j);
            }

            Console.WriteLine("finish the parse");
            inputInEdgeBuffer.Close();
            inputOutEdgeBuffer.Close();

            for (int j = 0; j < threadNum; j++)
            {
                insertEdgeThreadList[j].Join();
            }

            for (int j = 0; j < threadNum; j++)
            {
                insertEdgeThreadList[j].Abort();
            }

            Console.WriteLine("try to print result");
            if (result.Count > 0)
            {
                Console.WriteLine("max insert node time is: {0}", result.Max());
                Console.WriteLine("min insert node time is: {0}", result.Min());
                Console.WriteLine("avg insert node time is: {0}", result.Average());
                Console.WriteLine("sum insert node time is: {0}", result.Sum());
                Console.WriteLine("item count" + result.Count);
                //Console.WriteLine("stdDev insert node time is: {0}", DocDBUtils.stdDev(result));
                Console.WriteLine("avg,max,min,stdDev");
                //Console.WriteLine("{0}, {1}, {2}, {3}", result.Average(), result.Max(), result.Min(), DocDBUtils.stdDev(result));
            }

            if (edgeResult.Count > 0)
            {
                Console.WriteLine("max insert edge time is: {0}", edgeResult.Max());
                Console.WriteLine("min insert edge time is: {0}", edgeResult.Min());
                Console.WriteLine("avg insert edge time is: {0}", edgeResult.Average());
                Console.WriteLine("sum insert node time is: {0}", edgeResult.Sum());
                Console.WriteLine("item count" + edgeResult.Count);
                //Console.WriteLine("stdDev insert node time is: {0}", DocDBUtils.stdDev(result));
                Console.WriteLine("avg,max,min,stdDev");
                //Console.WriteLine("{0}, {1}, {2}, {3}", result.Average(), result.Max(), result.Min(), DocDBUtils.stdDev(result));
            }

            Console.WriteLine("Finish init the dataset");
        }

        public static void parseAndDumpIOTData(string path, GraphViewConnection connection, string collectionName, Boolean resetCollection, int threadNum, string nodeFilePath, string edgeFilePath)
        {
            // parse data
            var nodePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();
            var outEdgePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();
            var inEdgePropertiesHashMap = new Dictionary<string, Dictionary<string, string>>();
            StreamWriter nodeFile = new StreamWriter(nodeFilePath);
            StreamWriter edgeFile = new StreamWriter(edgeFilePath);
            parseData(path, connection, collectionName, resetCollection, threadNum, nodePropertiesHashMap, outEdgePropertiesHashMap, inEdgePropertiesHashMap);
            connection.ResetCollection();
            var result = new List<Double>();
            var sumTime = 0.0;
            var nodeInsertNumbers = 100;
            var edgeInsertNumbers = 100;

            // Insert node from collections
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputNodeBuffer = new BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>>(nodePropertiesHashMap.Count + 1);
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputInEdgeBuffer = new BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>>(inEdgePropertiesHashMap.Count + 1);
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputOutEdgeBuffer = new BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>>(outEdgePropertiesHashMap.Count + 1);
            var nodeIter = nodePropertiesHashMap.GetEnumerator();

            while (nodeIter.MoveNext())
            {
                inputNodeBuffer.Add(nodeIter.Current);
            }

            var inEdgeIter = inEdgePropertiesHashMap.GetEnumerator();

            while (inEdgeIter.MoveNext())
            {
                inputInEdgeBuffer.Add(inEdgeIter.Current);
            }

            var outEdgeIter = outEdgePropertiesHashMap.GetEnumerator();

            while (outEdgeIter.MoveNext())
            {
                inputOutEdgeBuffer.Add(outEdgeIter.Current);
            }

            List<Thread> insertNodeThreadList = new List<Thread>();

            for (int j = 0; j < threadNum; j++)
            {
                DocDBInsertNodeToFileWorkerByNewAPI worker1 = new DocDBInsertNodeToFileWorkerByNewAPI(connection, inputNodeBuffer, inputInEdgeBuffer, inputOutEdgeBuffer);
                worker1.threadId = j;
                worker1.result = result;
                worker1.nodeFile = nodeFile;
                Thread t1 = new Thread(worker1.BulkInsert);
                insertNodeThreadList.Add(t1);
            }

            for (int j = 0; j < threadNum; j++)
            {
                insertNodeThreadList[j].Start();
                Console.WriteLine("Start the thread" + j);
            }

            Console.WriteLine("finish the parse");
            inputNodeBuffer.Close();

            for (int j = 0; j < threadNum; j++)
            {
                insertNodeThreadList[j].Join();
            }

            for (int j = 0; j < threadNum; j++)
            {
                insertNodeThreadList[j].Abort();
            }

            // Insert Edge
            var edgeResult = new List<Double>();

            List<Thread> insertEdgeThreadList = new List<Thread>();

            for (int j = 0; j < threadNum; j++)
            {
                DocDBInsertEdgeToFileWorkerByNewAPI worker1 = new DocDBInsertEdgeToFileWorkerByNewAPI(connection, inputNodeBuffer, inputInEdgeBuffer, inputOutEdgeBuffer);
                worker1.threadId = j;
                worker1.result = edgeResult;
                worker1.edgeFile = edgeFile;
                Thread t1 = new Thread(worker1.BulkInsert);
                insertEdgeThreadList.Add(t1);
            }

            for (int j = 0; j < threadNum; j++)
            {
                insertEdgeThreadList[j].Start();
                Console.WriteLine("Start the thread" + j);
            }

            Console.WriteLine("finish the parse");
            inputInEdgeBuffer.Close();
            inputOutEdgeBuffer.Close();

            for (int j = 0; j < threadNum; j++)
            {
                insertEdgeThreadList[j].Join();
            }

            for (int j = 0; j < threadNum; j++)
            {
                insertEdgeThreadList[j].Abort();
            }

            Console.WriteLine("try to print result");
            if (result.Count > 0)
            {
                Console.WriteLine("max insert node time is: {0}", result.Max());
                Console.WriteLine("min insert node time is: {0}", result.Min());
                Console.WriteLine("avg insert node time is: {0}", result.Average());
                Console.WriteLine("sum insert node time is: {0}", result.Sum());
                Console.WriteLine("item count" + result.Count);
                //Console.WriteLine("stdDev insert node time is: {0}", DocDBUtils.stdDev(result));
                Console.WriteLine("avg,max,min,stdDev");
                //Console.WriteLine("{0}, {1}, {2}, {3}", result.Average(), result.Max(), result.Min(), DocDBUtils.stdDev(result));
            }

            if (edgeResult.Count > 0)
            {
                Console.WriteLine("max insert edge time is: {0}", edgeResult.Max());
                Console.WriteLine("min insert edge time is: {0}", edgeResult.Min());
                Console.WriteLine("avg insert edge time is: {0}", edgeResult.Average());
                Console.WriteLine("sum insert node time is: {0}", edgeResult.Sum());
                Console.WriteLine("item count" + edgeResult.Count);
                //Console.WriteLine("stdDev insert node time is: {0}", DocDBUtils.stdDev(result));
                Console.WriteLine("avg,max,min,stdDev");
                //Console.WriteLine("{0}, {1}, {2}, {3}", result.Average(), result.Max(), result.Min(), DocDBUtils.stdDev(result));
            }
            
            Console.WriteLine("Finish init the dataset");
        }
    }

    public class DocDBInsertNodeToFileWorkerByNewAPI
    {
        public int threadId;
        GraphViewConnection connection = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputNodeBuffer = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputInEdgeBuffer = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputOutEdgeBuffer = null;
        public List<Double> result = null;
        public StreamWriter nodeFile = null;

        public DocDBInsertNodeToFileWorkerByNewAPI(GraphViewConnection _connection,
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputNodeBuffer,
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputInEdgeBuffer,
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputOutEdgeBuffer)
        {
            this.connection = _connection;
            this.inputNodeBuffer = _inputNodeBuffer;
            this.inputInEdgeBuffer = _inputInEdgeBuffer;
            this.inputOutEdgeBuffer = _inputOutEdgeBuffer;
        }

        public void BulkInsert()
        {
            // Insert node from collections
            // Gremlin API
            GraphTraversal g = new GraphTraversal(connection);
            // SQL API
            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;
            var node = inputNodeBuffer.Retrieve();

            while (node.Key != null)
            {
                // new API
                List<string> PropList = new List<string>();
                PropList.Add("id");
                PropList.Add(node.Key);

                // SQL API
                var key = new StringBuilder();
                var value = new StringBuilder();
                key.Append("id");
                key.Append("#");
                key.Append(node.Key);
                key.Append("#");
                string tempValue = "";

                foreach (var x in node.Value)
                {
                    PropList.Add(x.Key);
                    PropList.Add(x.Value);
                    if(x.Key == "properties_location_value")
                    {
                        tempValue = @"{class:com.thinkaurelius.titan.core.attribute.Geoshape coordinates : [[D [-73.4597583576709 39.820514835425385]]}";
                    } else
                    {
                        tempValue = x.Value;
                    }
                    
                    key.Append(x.Key);
                    key.Append("#");
                    key.Append(tempValue);
                    key.Append("#");
                }

                key.Remove(key.Length - 1, 1);
                Console.WriteLine(key);
                nodeFile.WriteLine(key);
                node = inputNodeBuffer.Retrieve();
            }
            Thread.Sleep(1000); // wait for the last record write to the file
            Console.WriteLine("Thread Insert Finish");
        }

        public void Dispose()
        {
        }
    }

    public class DocDBInsertEdgeToFileWorkerByNewAPI
    {
        public int threadId;
        //GraphViewGremlinParser parser = new GraphViewGremlinParser();
        GraphViewConnection connection = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputNodeBuffer = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputInEdgeBuffer = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputOutEdgeBuffer = null;
        public List<Double> result = null;
        public StreamWriter edgeFile = null;

        public DocDBInsertEdgeToFileWorkerByNewAPI(GraphViewConnection _connection,
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputNodeBuffer,
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputInEdgeBuffer,
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputOutEdgeBuffer)
        {
            this.connection = _connection;
            this.inputNodeBuffer = _inputNodeBuffer;
            this.inputInEdgeBuffer = _inputInEdgeBuffer;
            this.inputOutEdgeBuffer = _inputOutEdgeBuffer;
        }

        public void BulkInsert()
        {
            // Insert outEdge from collections
            GraphTraversal g = new GraphTraversal(connection);
            KeyValuePair<string, Dictionary<string, string>> outEdge = inputOutEdgeBuffer.Retrieve();
            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            while (outEdge.Key != null)
            {
                String[] nodeIds = outEdge.Key.Split('_');
                String srcId = nodeIds[0];
                String desId = nodeIds[1];
                // Inset Edge
                List<string> PropList = new List<string>();
                // SQL API
                var key = new StringBuilder();
                var value = new StringBuilder();
                key.Append("srcId#");
                key.Append(srcId);
                key.Append("#");
                key.Append("desId");
                key.Append("#");
                key.Append(desId);
                key.Append("#");

                foreach (var x in outEdge.Value)
                {
                    PropList.Add(x.Key);
                    PropList.Add(x.Value);
                    key.Append(x.Key);
                    key.Append("#");
                    key.Append(x.Value);
                    key.Append("#");
                }

                key.Remove(key.Length - 1, 1);
                Console.WriteLine(key);
                edgeFile.WriteLine(key);
                outEdge = inputOutEdgeBuffer.Retrieve();
            }
            // Insert in edge from collections
            var edge = inputInEdgeBuffer.Retrieve();
            while (edge.Key != null)
            {
                String[] nodeIds = edge.Key.Split('_');
                String srcId = nodeIds[0];
                String desId = nodeIds[1];
                // Inset Edge
                // SQL API
                var key = new StringBuilder();
                var value = new StringBuilder();
                List<string> PropList = new List<string>();
                key.Append("srcId#");
                key.Append(srcId);
                key.Append("#");
                key.Append("desId");
                key.Append("#");
                key.Append(desId);
                key.Append("#");

                foreach (var x in edge.Value)
                {
                    PropList.Add(x.Key);
                    PropList.Add(x.Value);
                    key.Append(x.Key);
                    key.Append("#");
                    key.Append(x.Value);
                    key.Append("#");
                }

                key.Remove(key.Length - 1, 1);
                Console.WriteLine(key);
                edgeFile.WriteLine(key);
                edge = inputInEdgeBuffer.Retrieve();
            }
            Thread.Sleep(1000); // wait for the last record write to the file
            Console.WriteLine("Thread Insert Finish");
        }

        public void Dispose()
        {
        }
    }
    public class DocDBInsertNodeWorkerCommon
    {
        public int threadId;
        //GraphViewGremlinParser parser = new GraphViewGremlinParser();
        GraphViewConnection connection = null;
        BoundedBuffer<String> inputNodeBuffer = null;
        //BoundedBuffer<String> inputInEdgeBuffer = null;
        //BoundedBuffer<String> inputOutEdgeBuffer = null;
        public List<Double> result = null;
        public String dataSetName = "";

        public DocDBInsertNodeWorkerCommon(GraphViewConnection _connection,
            BoundedBuffer<String> _inputNodeBuffer)
            //BoundedBuffer<String> _inputInEdgeBuffer,
            //BoundedBuffer<String> _inputOutEdgeBuffer)
        {
            this.connection = _connection;
            this.inputNodeBuffer = _inputNodeBuffer;
            //this.inputInEdgeBuffer = _inputInEdgeBuffer;
            //this.inputOutEdgeBuffer = _inputOutEdgeBuffer;
        }

        public void BulkInsert()
        {
            // Insert node from collections
            // Gremlin API
            GraphTraversal g = new GraphTraversal(connection);
            // SQL API
            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            var node = inputNodeBuffer.Retrieve();
            while (node != null)
            {
                switch (dataSetName)  
                {
                    case (String)("cloud"):
                        NewIOTDataParseAndProcessUtils.insertCloudNode(gcmd, node, result);
                        break;
                    case (String)("clusters"):
                        NewIOTDataParseAndProcessUtils.insertClustersNode(gcmd, node, result);
                        break;
                    case (String)("containers"):
                        NewIOTDataParseAndProcessUtils.insertContianersNode(gcmd, node, result);
                        break;
                    case (String)("dcs"):
                        NewIOTDataParseAndProcessUtils.insertDcsNode(gcmd, node, result);
                        break;
                    case (String)("deployments"):
                        NewIOTDataParseAndProcessUtils.insertDeploymentsNode(gcmd, node, result);
                        break;
                    case (String)("networkdevicehwskus"):
                        NewIOTDataParseAndProcessUtils.insertNetworkdevicehwskusNode(gcmd, node, result);
                        break;
                    case (String)("networkdeviceroutertypes"):
                        NewIOTDataParseAndProcessUtils.insertNetworkdeviceroutertypesNode(gcmd, node, result);
                        break;
                    case (String)("networkdevices"):
                        NewIOTDataParseAndProcessUtils.insertNetworkDevicesNode(gcmd, node, result);
                        break;
                    case (String)("nodes"):
                        NewIOTDataParseAndProcessUtils.insertNodesNode(gcmd, node, result);
                        break;
                    case (String)("rsRegions"):
                        NewIOTDataParseAndProcessUtils.insertRsRegionsNode(gcmd, node, result);
                        break;
                    case (String)("subscriptions"):
                        NewIOTDataParseAndProcessUtils.insertSubscriptionsNode(gcmd, node, result);
                        break;
                    default:
                        Console.WriteLine("The dataSetName configuration contains error");
                        break;

                }
                node = inputNodeBuffer.Retrieve();
            }

            Console.WriteLine("Thread Insert Finish");
        }

        public void Dispose()
        {
        }
    }

    public class DocDBInsertEdgeWorkerCommon
    {
        public int threadId;
        GraphViewConnection connection = null;
        //BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputNodeBuffer = null;
        BoundedBuffer<String> inputBuffer = null;
        //BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputOutEdgeBuffer = null;
        public List<Double> result = null;
        public String fileName = "";
        public DocDBInsertEdgeWorkerCommon(GraphViewConnection _connection, BoundedBuffer<String> _inputInEdgeBuffer)
        {
            this.connection = _connection;
            this.inputBuffer = _inputInEdgeBuffer;
        }

        public void BulkInsert()
        {
            // Insert outEdge from collections
            GraphTraversal g = new GraphTraversal(connection);
            String outEdge = inputBuffer.Retrieve();
            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            while (outEdge != null)
            {
                switch (fileName)
                {
                    case (String)("cluster2node"):
                        NewIOTDataParseAndProcessUtils.insertCluster2NodeEdge(gcmd, outEdge, result);
                        break;
                    case (String)("deployment2container"):
                        NewIOTDataParseAndProcessUtils.insertDeployment2ContainerEdge(gcmd, outEdge, result);
                        break;
                    case (String)("networkdevice2networkdevice"):
                        NewIOTDataParseAndProcessUtils.insertNetworkDevice2NetworkDeviceEdge(gcmd, outEdge, result);
                        break;
                    case (String)("networkdevice2routertype"):
                        NewIOTDataParseAndProcessUtils.insertNetworkDevice2RouterTypeEdge(gcmd, outEdge, result);
                        break;
                    case (String)("node2container"):
                        NewIOTDataParseAndProcessUtils.insertNode2ContainerEdge(gcmd, outEdge, result);
                        break;
                    case (String)("node2networkdevice"):
                        NewIOTDataParseAndProcessUtils.insertNode2NetworkDeviceEdge(gcmd, outEdge, result);
                        break;
                    case (String)("subscription2deployment"):
                        NewIOTDataParseAndProcessUtils.insertSubscription2DeploymentEdge(gcmd, outEdge, result);
                        break;
                    default:
                        Console.WriteLine("The dataSetName configuration contains error");
                        break;
                    outEdge = inputBuffer.Retrieve();
                }
            }
            Console.WriteLine("Thread Insert Finish");
        }

        public void Dispose()
        {
        }
    }

    public class DocDBInsertNodeWorkerByNewAPI
    {
        public int threadId;
        //GraphViewGremlinParser parser = new GraphViewGremlinParser();
        GraphViewConnection connection = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputNodeBuffer = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputInEdgeBuffer = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputOutEdgeBuffer = null;
        public List<Double> result = null;

        public DocDBInsertNodeWorkerByNewAPI(GraphViewConnection _connection,
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputNodeBuffer,
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputInEdgeBuffer,
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputOutEdgeBuffer)
        {
            this.connection = _connection;
            this.inputNodeBuffer = _inputNodeBuffer;
            this.inputInEdgeBuffer = _inputInEdgeBuffer;
            this.inputOutEdgeBuffer = _inputOutEdgeBuffer;
        }

        public void BulkInsert()
        {
            // Insert node from collections
            // Gremlin API
            GraphTraversal g = new GraphTraversal(connection);
            // SQL API
            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            var node = inputNodeBuffer.Retrieve();
            while (node.Key != null)
            {
                // new API
                List<string> PropList = new List<string>();
                PropList.Add("id");
                PropList.Add(node.Key);

                // SQL API
                var key = new StringBuilder();
                var value = new StringBuilder();
                key.Append("id");
                key.Append(",");
                value.Append("'" + node.Key + "'");
                value.Append(",");

                foreach (var x in node.Value)
                {
                    PropList.Add(x.Key);
                    PropList.Add(x.Value);
                    key.Append(x.Key);
                    key.Append(",");
                    value.Append("'");
                    value.Append(x.Value);
                    value.Append("'");
                    value.Append(",");
                }

                key.Remove(key.Length - 1, 1);
                value.Remove(value.Length - 1, 1);
                Stopwatch sw = new Stopwatch();
                var tempSQL = @"
                INSERT INTO Node (" + key + ") VALUES (" + value + ");";
                Console.WriteLine(tempSQL);
                sw.Start();
                // Gremlin API
                gcmd.CommandText = tempSQL;
                gcmd.ExecuteNonQuery();
                sw.Stop();
                result.Add(sw.Elapsed.TotalMilliseconds);
                Console.WriteLine("insert v " + node.Key + " time cost " + sw.Elapsed.TotalMilliseconds);
                node = inputNodeBuffer.Retrieve();
            }

            Console.WriteLine("Thread Insert Finish");
        }

        public void Dispose()
        {
        }
    }
    public class DocDBInsertEdgeWorkerByNewAPI
    {
        public int threadId;
        GraphViewConnection connection = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputNodeBuffer = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputInEdgeBuffer = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputOutEdgeBuffer = null;
        public List<Double> result = null;
        public DocDBInsertEdgeWorkerByNewAPI(GraphViewConnection _connection,
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputNodeBuffer,
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputInEdgeBuffer,
            BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputOutEdgeBuffer)
        {
            this.connection = _connection;
            this.inputNodeBuffer = _inputNodeBuffer;
            this.inputInEdgeBuffer = _inputInEdgeBuffer;
            this.inputOutEdgeBuffer = _inputOutEdgeBuffer;
        }

        public void BulkInsert()
        {
            // Insert outEdge from collections
            GraphTraversal g = new GraphTraversal(connection);
            KeyValuePair<string, Dictionary<string, string>> outEdge = inputOutEdgeBuffer.Retrieve();
            GraphViewCommand gcmd = new GraphViewCommand();
            gcmd.GraphViewConnection = connection;

            while (outEdge.Key != null)
            {
                String[] nodeIds = outEdge.Key.Split('_');
                String srcId = nodeIds[0];
                String desId = nodeIds[1];
                // Inset Edge
                List<string> PropList = new List<string>();
                // SQL API
                var key = new StringBuilder();
                var value = new StringBuilder();

                foreach (var x in outEdge.Value)
                {
                    PropList.Add(x.Key);
                    PropList.Add(x.Value);

                    key.Append(x.Key);
                    key.Append(",");
                    value.Append("'" + x.Value + "'" + ",");
                }
                key.Remove(key.Length - 1, 1);
                value.Remove(value.Length - 1, 1);

                var tempSQL = @"
                INSERT INTO Edge (" + key + @")
                SELECT A, B, " + value + @"
                FROM   Node A, Node B
                WHERE  A.id = '" + srcId + "' AND B.id = '" + desId + "'";
                Console.WriteLine(tempSQL);
                Stopwatch sw = new Stopwatch();
                sw.Start();
                // Gremlin API
                //g.V().has("id", srcId).addE(PropList).to(g.V().has("id", desId));
                gcmd.CommandText = tempSQL;
                gcmd.ExecuteNonQuery();
                sw.Stop();
                result.Add(sw.Elapsed.TotalMilliseconds);
                Console.WriteLine("insert outE " + outEdge.Key + " \n time cost " + sw.Elapsed.TotalMilliseconds);
                outEdge = inputOutEdgeBuffer.Retrieve();
            }
            // Insert in edge from collections
            var edge = inputInEdgeBuffer.Retrieve();
            while (edge.Key != null)
            {
                String[] nodeIds = edge.Key.Split('_');
                String srcId = nodeIds[0];
                String desId = nodeIds[1];
                // Inset Edge
                // SQL API
                var key = new StringBuilder();
                var value = new StringBuilder();

                List<string> PropList = new List<string>();
                foreach (var x in edge.Value)
                {
                    PropList.Add(x.Key);
                    PropList.Add(x.Value);

                    key.Append(x.Key);
                    key.Append(",");
                    value.Append("'" + x.Value + "'" + ",");
                }
                key.Remove(key.Length - 1, 1);
                value.Remove(value.Length - 1, 1);

                var tempSQL = @"
                INSERT INTO Edge (" + key + @")
                SELECT A, B, " + value + @"
                FROM   Node A, Node B
                WHERE  A.id = '" + srcId + "' AND B.id = '" + desId + "'";
                Console.WriteLine(tempSQL);
                // Gremlin parser
                //g.V().has("id", srcId).addE(PropList).to(g.V().has("id", desId));
                Stopwatch sw = new Stopwatch();
                sw.Start();
                // Gremlin API
                //g.V().has("id", srcId).addE(PropList).to(g.V().has("id", desId));
                gcmd.CommandText = tempSQL;
                gcmd.ExecuteNonQuery();
                sw.Stop();
                result.Add(sw.Elapsed.TotalMilliseconds);

                Console.WriteLine("insert inE " + edge.Key + " time cost \n " + sw.Elapsed.TotalMilliseconds);
                edge = inputInEdgeBuffer.Retrieve();
            }
            Console.WriteLine("Thread Insert Finish");
        }

        public void Dispose()
        {
        }
    }

    public class DocDBInsertWorker
    {
        BoundedBuffer<string> inputStream;
        public int threadId;
        GraphViewGremlinParser parser = new GraphViewGremlinParser();
        GraphViewConnection connection = null;

        public DocDBInsertWorker(GraphViewConnection _connection,
            BoundedBuffer<string> _inputStream)
        {
            this.connection = _connection;
            this.inputStream = _inputStream;
        }

        public void BulkInsert()
        {
            string doc = inputStream.Retrieve();
            List<string> docList = new List<string>();
            int docNum = 1;

            while (doc != null)
            {
                parser.Parse(doc.ToString()).Generate(connection).Next();
                Console.WriteLine("Thread" + threadId + " docCount" + docNum);
                docNum += 1;
                doc = inputStream.Retrieve();
            }

            Console.WriteLine("Thread Insert Finish");
        }

        public void Dispose()
        {
        }
    }

    public class BoundedBuffer<T>
    {
        public int bufferSize;
        public Queue<T> boundedBuffer;
        // Whether the queue expects more elements to come
        bool more;

        public bool More
        {
            get { return more; }
        }

        Object _monitor;
        public BoundedBuffer(int bufferSize)
        {
            boundedBuffer = new Queue<T>(bufferSize);
            this.bufferSize = bufferSize;
            more = true;
            _monitor = new object();
        }

        public void Add(T element)
        {
            lock (_monitor)
            {
                while (boundedBuffer.Count == bufferSize)
                {
                    Monitor.Wait(_monitor);
                }

                boundedBuffer.Enqueue(element);
                Monitor.Pulse(_monitor);
            }
        }

        public T Retrieve()
        {
            T element = default(T);

            lock (_monitor)
            {
                while (boundedBuffer.Count == 0 && more)
                {
                    Monitor.Wait(_monitor);
                }

                if (boundedBuffer.Count > 0)
                {
                    element = boundedBuffer.Dequeue();
                    Monitor.Pulse(_monitor);
                }
            }

            return element;
        }

        public void Close()
        {
            lock (_monitor)
            {
                more = false;
                Monitor.PulseAll(_monitor);
            }
        }
    }

    public class NewIOTDataParseAndProcessUtils
    {
        public static void insertCloudNode(GraphViewCommand gcmd, String node, List<Double> result)
        {
            Stopwatch sw = new Stopwatch();
            //var tempSQL = @"
            //    INSERT INTO Node (" + key + ") VALUES (" + value + ");";
            //Console.WriteLine(tempSQL);
            //sw.Start();
            //// Gremlin API
            //gcmd.CommandText = tempSQL;
            //gcmd.ExecuteNonQuery();
            //sw.Stop();
            //result.Add(sw.Elapsed.TotalMilliseconds);
            //Console.WriteLine("insert v " + node.Key + " time cost " + sw.Elapsed.TotalMilliseconds);
        }

        public static void insertClustersNode(GraphViewCommand gcmd, String node, List<Double> result)
        {

        }

        public static void insertContianersNode(GraphViewCommand gcmd, String node, List<Double> result)
        {

        }

        public static void insertDcsNode(GraphViewCommand gcmd, String node, List<Double> result)
        {

        }

        public static void insertDeploymentsNode(GraphViewCommand gcmd, String node, List<Double> result)
        {

        }

        public static void insertNetworkdevicehwskusNode(GraphViewCommand gcmd, String node, List<Double> result)
        {

        }

        public static void insertNetworkdeviceroutertypesNode(GraphViewCommand gcmd, String node, List<Double> result)
        {

        }
        public static void insertNetworkDevicesNode(GraphViewCommand gcmd, String node, List<Double> result)
        {

        }
        public static void insertNodesNode(GraphViewCommand gcmd, String node, List<Double> result)
        {

        }

        public static void insertRsRegionsNode(GraphViewCommand gcmd, String node, List<Double> result)
        {

        }
        public static void insertSubscriptionsNode(GraphViewCommand gcmd, String node, List<Double> result)
        {

        }
        public static void insertCluster2NodeEdge(GraphViewCommand gcmd, String node, List<Double> result)
        {

        }
        public static void insertDeployment2ContainerEdge(GraphViewCommand gcmd, String node, List<Double> result)
        {

        }
        public static void insertNetworkDevice2NetworkDeviceEdge(GraphViewCommand gcmd, String node, List<Double> result)
        {

        }
        public static void insertNetworkDevice2RouterTypeEdge(GraphViewCommand gcmd, String node, List<Double> result)
        {

        }
        public static void insertNode2ContainerEdge(GraphViewCommand gcmd, String node, List<Double> result)
        {

        }
        public static void insertNode2NetworkDeviceEdge(GraphViewCommand gcmd, String node, List<Double> result)
        {

        }
        public static void insertSubscription2DeploymentEdge(GraphViewCommand gcmd, String node, List<Double> result)
        {

        }
    }
}
