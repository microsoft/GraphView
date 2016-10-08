using System;
using System.Collections.Generic;
using System.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using GraphView;
using Newtonsoft.Json.Linq;
using System.IO;
using System.Threading;
namespace GraphView
{
    public static class GraphLoaderFactory
    {
        public static void loadAzureIOT(string path, GraphViewConnection connection, string collectionName,  Boolean resetCollection)
        {
            // parse data
            int i = 0;
            var lines = File.ReadLines(path);
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
                                    nodePropertiesHashMap[id.ToString()][property.Path.ToString().Replace('.', '_') + "_value"] = value.ToString().Replace("'", "\\'").Replace("\"", "\\\"");
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

            ResetCollection(collectionName, connection);
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

            int threadNum = 100;
            List<Thread> insertNodeThreadList = new List<Thread>();

            for (int j = 0; j < threadNum; j++)
            {
                DocDBInsertNodeWorkerByNewAPI worker1 = new DocDBInsertNodeWorkerByNewAPI(connection, inputNodeBuffer, inputInEdgeBuffer, inputOutEdgeBuffer);
                worker1.threadId = j;
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
            List<Thread> insertEdgeThreadList = new List<Thread>();

            for (int j = 0; j < threadNum; j++)
            {
                DocDBInsertEdgeWorkerByNewAPI worker1 = new DocDBInsertEdgeWorkerByNewAPI(connection, inputNodeBuffer, inputInEdgeBuffer, inputOutEdgeBuffer);
                worker1.threadId = j;
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

            Console.WriteLine("Finish init the dataset");
        }

        public static void ResetCollection(String collectionName, GraphViewConnection connection)
        {
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
    }

    public class DocDBInsertNodeWorkerByNewAPI
    {
        public int threadId;
        //GraphViewGremlinParser parser = new GraphViewGremlinParser();
        GraphViewConnection connection = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputNodeBuffer = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputInEdgeBuffer = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputOutEdgeBuffer = null;

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
            GraphTraversal g = new GraphTraversal(connection);
            var node = inputNodeBuffer.Retrieve();

            while (node.Key != null)
            {
                // new API
                List<string> PropList = new List<string>();
                PropList.Add("id");
                PropList.Add(node.Key);
                foreach (var x in node.Value)
                {
                    PropList.Add(x.Key);
                    PropList.Add(x.Value);
                }
                var D = g.V().addV(PropList);
                Console.WriteLine("insert v " + node.Key);
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
        //GraphViewGremlinParser parser = new GraphViewGremlinParser();
        GraphViewConnection connection = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputNodeBuffer = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputInEdgeBuffer = null;
        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputOutEdgeBuffer = null;

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
            // Insert node from collections
            GraphTraversal g = new GraphTraversal(connection);
            KeyValuePair<string, Dictionary<string, string>> outEdge = inputOutEdgeBuffer.Retrieve();

            while (outEdge.Key != null)
            {
                String[] nodeIds = outEdge.Key.Split('_');
                String srcId = nodeIds[0];
                String desId = nodeIds[1];
                // Inset Edge

                List<string> PropList = new List<string>();
                foreach (var x in outEdge.Value)
                {
                    PropList.Add(x.Key);
                    PropList.Add(x.Value);
                }
                g.V().has("id", srcId).addE(PropList).to(g.V().has("id", desId));
                Console.WriteLine("insert outE " + outEdge.Key);
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

                List<string> PropList = new List<string>();
                foreach (var x in edge.Value)
                {
                    PropList.Add(x.Key);
                    PropList.Add(x.Value);
                }
                g.V().has("id", srcId).addE(PropList).to(g.V().has("id", desId));
                Console.WriteLine("insert inE " + edge.Key);
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
}
