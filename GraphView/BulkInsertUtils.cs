using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using System.IO;
using System.Threading;
using System.Diagnostics;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using static GraphView.GraphViewKeywords;
using System.Collections.Concurrent;
namespace GraphView
{

    public class BulkInsertUtils
    {
        public List<Thread> insertNodeThreadList = new List<Thread>();
        public List<BoundedBuffer<BufferObject>> bufferList;
        public List<InsertDocFromStringWorker> stringWorkerList;
        public BoundedBuffer<String> stringBufferList;
        public BoundedBuffer<String> insertNodeBuffer;
        public BoundedBuffer<String> insertEdgeBuffer;
        public CountDownLatch parseDataCountDownLatch;
        public CountDownLatch insertNodeCountDownLatch;
        public CountDownLatch insertEdgeCountDownLatch;
        public ConcurrentDictionary<String, String> vertexIdsSet;
        List<GraphViewConnection> connList;
        public List<DocumentClient> clientList = new List<DocumentClient>();
        public int threadNum;

        // new
        public BoundedBuffer<String> vertexRawStringBuffer;
        public BoundedBuffer<String> edgeRawStringBuffer;
        String[] vertexSchema;
        String vertexSeperator;
        String[] edgeSchema;
        String edgeSeperator;

        public BulkInsertUtils(int _threadNum)
        {
            threadNum = _threadNum;
        }

        public void initBulkInsertUtilsForFormatDataFile(int threadNum, int bufferSize, GraphViewConnection conn, String[] _vertexSchema, String[] _edgeSchema,
            String _vertexSeperator, String _edgeSeperator)
        {

            vertexIdsSet = new ConcurrentDictionary<String, String>();
            stringBufferList = new BoundedBuffer<String>(bufferSize);
            connList = new List<GraphViewConnection>();
            parseDataCountDownLatch = new CountDownLatch(threadNum);
            insertNodeCountDownLatch = new CountDownLatch(threadNum);
            insertEdgeCountDownLatch = new CountDownLatch(threadNum);
            insertNodeBuffer = new BoundedBuffer<String>(bufferSize * 2);
            insertEdgeBuffer = new BoundedBuffer<String>(bufferSize);
            stringWorkerList = new List<InsertDocFromStringWorker>();

            // new
            vertexRawStringBuffer = new BoundedBuffer<string>(bufferSize * 2);
            edgeRawStringBuffer = new BoundedBuffer<string>(bufferSize);
            vertexSchema = _vertexSchema;
            vertexSeperator = _vertexSeperator;
            edgeSchema = _edgeSchema;
            edgeSeperator = _vertexSeperator;

            Console.WriteLine("init thread and buffer started for parse data");
            //(1) init the thread list
            for (int j = 0; j < threadNum; j++)
            {
                InsertDocFromStringWorker worker1 = new InsertDocFromStringWorker();
                worker1.workerIndex = j;
                worker1.blk = this;
                worker1.buffer = stringBufferList;
                worker1.DocDBClient = conn.getDocDBClient();
                worker1.insertNodeBuffer = insertNodeBuffer;
                worker1.insertEdgeBuffer = insertEdgeBuffer;

                // raw string parser
                worker1.vertexRawStringBuffer = vertexRawStringBuffer;
                worker1.edgeRawStringBuffer = edgeRawStringBuffer;
                worker1.vertexSchema = vertexSchema;
                worker1.vertexSeperator = vertexSeperator;
                worker1.edgeSchema = edgeSchema;
                worker1.edgeSeperator = edgeSeperator;

                stringWorkerList.Add(worker1);
                Thread t1 = new Thread(worker1.parseFormatdoc);
                insertNodeThreadList.Add(t1);
                Console.WriteLine("init parse thread" + j);

                GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                conn.DocDBDatabaseId, conn.DocDBCollectionId, GraphType.GraphAPIOnly, false,
                1, "name");
                worker1.connList = connList;
                connList.Add(connection);
            }

            Console.WriteLine("init thread and buffer finished for parse data");
        }

        public void initBulkInsertUtilsForCreateDoc(int _threadNum, int bufferSize, GraphViewConnection conn)
        {
            threadNum = _threadNum;
            //threadNum = 1;
            bufferList = new List<BoundedBuffer<BufferObject>>();
            Console.WriteLine("init thread and buffer started");
            //(1) init the thread list
            for (int j = 0; j < threadNum; j++)
            {
                InsertDocWorker worker1 = new InsertDocWorker();
                worker1.workerIndex = threadNum;

                BoundedBuffer<BufferObject> buffer = new BoundedBuffer<BufferObject>(bufferSize);
                bufferList.Add(buffer);

                worker1.buffer = buffer;
                worker1.DocDBClient = conn.getDocDBClient();
                clientList.Add(new DocumentClient(new Uri(conn.DocDBUrl),
                                    conn.DocDBPrimaryKey,
                                    new ConnectionPolicy
                                    {
                                        ConnectionMode = ConnectionMode.Direct,
                                        ConnectionProtocol = Protocol.Tcp,
                                    }));
                worker1.client = clientList[j];
                Thread t1 = new Thread(worker1.insertDoc);
                insertNodeThreadList.Add(t1);
                Console.WriteLine("init thread" + j);
            }
        }

        public void startParseThread()
        {
            for (int j = 0; j < threadNum; j++)
            {
                insertNodeThreadList[j].Start();
                Console.WriteLine("Start thread" + j);
            }
            Console.WriteLine("startParseThread init thread and buffer finished");
        }

        public void processDoc(Uri _docDBCollectionUri, JObject doc, GraphViewConnection connection)
        {
            try {
                var par = doc["_partition"];
                var partitionNum = int.Parse(par.ToString());
                BufferObject bf = new BufferObject(_docDBCollectionUri, doc, connection);
                if (bufferList.Count() == 1)
                {
                    bufferList[0].Add(bf);
                }
                else
                {
                    bufferList[partitionNum].Add(bf);
                }
            } catch (Exception e)
            {
                throw e;
            }
        }

        public void initBulkInsertUtilsForParseData(int threadNum, int bufferSize, GraphViewConnection conn)
        {
            vertexIdsSet = new ConcurrentDictionary<String, String>();
            stringBufferList = new BoundedBuffer<String>(bufferSize);
            connList = new List<GraphViewConnection>();
            parseDataCountDownLatch = new CountDownLatch(threadNum);
            insertNodeCountDownLatch = new CountDownLatch(threadNum);
            insertEdgeCountDownLatch = new CountDownLatch(threadNum);
            insertNodeBuffer = new BoundedBuffer<String>(bufferSize * 2);
            insertEdgeBuffer = new BoundedBuffer<String>(bufferSize);
            stringWorkerList = new List<InsertDocFromStringWorker>();
            Console.WriteLine("init thread and buffer started for parse data");
            //(1) init the thread list
            for (int j = 0; j < threadNum; j++)
            {
                InsertDocFromStringWorker worker1 = new InsertDocFromStringWorker();
                worker1.workerIndex = j;
                worker1.blk = this;
                worker1.buffer = stringBufferList;
                worker1.DocDBClient = conn.getDocDBClient();
                worker1.insertNodeBuffer = insertNodeBuffer;
                worker1.insertEdgeBuffer = insertEdgeBuffer;
                stringWorkerList.Add(worker1);
                Thread t1 = new Thread(worker1.parseDoc);
                insertNodeThreadList.Add(t1);
                Console.WriteLine("init parse thread" + j);

                GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                conn.DocDBDatabaseId, conn.DocDBCollectionId, GraphType.GraphAPIOnly, false,
                1, "name");
                worker1.connList = connList;
                connList.Add(connection);
            }

            //for (int j = 0; j < threadNum; j++)
            //{
            //    insertNodeThreadList[j].Start();
            //    Console.WriteLine("Start parse thread for parse data" + j);
            //}

            Console.WriteLine("init thread and buffer finished for parse data");
        }

        public void initAndStartInsertNodeStringCMD()
        {
            Console.WriteLine(" initAndStartInsertNodeStringCMD");
            insertNodeThreadList.Clear();
            //(1) init the thread list
            for (int j = 0; j < threadNum; j++)
            {
                Thread t1 = new Thread(stringWorkerList[j].insertNode);
                insertNodeThreadList.Add(t1);
                Console.WriteLine("init parse thread" + j);
            }

            for (int j = 0; j < threadNum; j++)
            {
                insertNodeThreadList[j].Start();
                Console.WriteLine("initAndStartInsertNodeStringCMD" + j);
            }
        }

        public void initAndStartInsertEdgeStringCMD()
        {
            Console.WriteLine("initAndStartInsertEdgeStringCMD");
            insertNodeThreadList.Clear();
            //(1) init the thread list
            for (int j = 0; j < threadNum; j++)
            {
                Thread t1 = new Thread(stringWorkerList[j].insertEdge);
                insertNodeThreadList.Add(t1);
                Console.WriteLine("init parse thread" + j);
            }

            for (int j = 0; j < threadNum; j++)
            {
                insertNodeThreadList[j].Start();
                Console.WriteLine("initAndStartInsertEdgeStringCMD" + j);
            }
        }
    }

    public class InsertDocFromStringWorker{

        public DocumentClient DocDBClient;
        public int workerIndex;
        public BoundedBuffer<String> buffer;
        public BoundedBuffer<String> insertNodeBuffer;
        public BoundedBuffer<String> insertEdgeBuffer;
        public DocumentClient client;
        public List<GraphViewConnection> connList;
        public BulkInsertUtils blk;

        // raw string parser
        public BoundedBuffer<String> vertexRawStringBuffer;
        public BoundedBuffer<String> edgeRawStringBuffer;
        public String[] vertexSchema;
        public String vertexSeperator;
        public String[] edgeSchema;
        public String edgeSeperator;

        /*
         * Ref: Neo4j loader and format 
         * https://neo4j.com/blog/bulk-data-import-neo4j-3-0/
         * http://neo4j.com/docs/developer-manual/current/cypher/clauses/load-csv/#csv-file-format
         * "Id","Name","Year"
         * "1","ABBA","1992"
         * **/
        public String vertexParser(String line, String[] schema, String wordSeperator)
        {
            StringBuilder vertexInsertCMD = new StringBuilder();
            var splitLine = line.Split(',');
            //var insertV1 = "g.addV('id', '" + src + "').property('name', '" + src + "').next()";
            var i = 0;
            String prefix = null;

            foreach (var s in schema)
            {
                if (s.ToLower() == "id")
                {
                    prefix = "g.addV('" + s + "', '" + splitLine[i] + "')";

                    if (!blk.vertexIdsSet.ContainsKey(splitLine[i]))
                    {
                        blk.vertexIdsSet.TryAdd(splitLine[i], null);
                    }
                }
                else
                {
                    vertexInsertCMD.Append(".property('" + s + "', '" + splitLine[i] + "')");
                }
                i++;
            }

            return prefix + vertexInsertCMD.ToString() + ".next()";
        }

        public String edgeParser(String line, String schema, String wordSeperator)
        {
            StringBuilder edgeInsertCMD = new StringBuilder();
            var splitLine = line.Split(',');
            // var insertE = "g.V('" + src + "').addE('appear').to(g.V('" + des + "')).next()";
            var i = 0;
            String prefix = null;
            prefix = "g.addE('" + splitLine[0] + "', '" + splitLine[1] + "')";

            foreach (var s in schema)
            {
                if (i > 1)
                {
                    edgeInsertCMD.Append(".property('" + s + "', '" + splitLine[i] + "')");
                }
                i++;
            }

            return prefix + edgeInsertCMD.ToString() + ".next()";
        }

        public void parseFormatdoc()
        {
            parseVertexDocWithProp();
            parseEdgeDocWithProp();
        }
        public void parseVertexDocWithProp()
        {
            var doc = vertexRawStringBuffer.Retrieve();
            var cmd = new GraphViewCommand(connList[workerIndex]);
            HashSet<String> nodeIdSet = new HashSet<String>();

            while (doc != null)
            {
                var lineE = doc;
                var insertV1 = vertexParser(lineE, vertexSchema, vertexSeperator);
                insertNodeBuffer.Add(insertV1);
                doc = vertexRawStringBuffer.Retrieve();
                Console.WriteLine("threadNum:" + workerIndex + " buffer size" + buffer.boundedBuffer.Count + " cmd:" + lineE);
                if (buffer.boundedBuffer.Count() == 0)
                {
                    buffer.more = false;
                }
            }
            blk.parseDataCountDownLatch.CountDown();
        }

        public void parseEdgeDocWithProp()
        {
            var doc = edgeRawStringBuffer.Retrieve();
            var cmd = new GraphViewCommand(connList[workerIndex]);
            HashSet<String> nodeIdSet = new HashSet<String>();

            while (doc != null)
            {
                var lineE = doc;
                var insertV1 = vertexParser(lineE, edgeSchema, edgeSeperator);
                insertEdgeBuffer.Add(insertV1);
                doc = edgeRawStringBuffer.Retrieve();
                Console.WriteLine("threadNum:" + workerIndex + " buffer size" + buffer.boundedBuffer.Count + " cmd:" + lineE);
                if (buffer.boundedBuffer.Count() == 0)
                {
                    buffer.more = false;
                }
            }
            blk.parseDataCountDownLatch.CountDown();
        }

        public void parseDoc()
        {
            try
            {
                var doc = buffer.Retrieve();
                var cmd = new GraphViewCommand(connList[workerIndex]);
                HashSet<String> nodeIdSet = new HashSet<String>();

                while (doc != null)
                {
                    var lineE = doc;
                    var split = lineE.Split('\t');
                    var src = split[0];
                    var des = split[1];
                    var insertV1 = "g.addV('id', '" + src + "').property('name', '" + src + "').next()";
                    var insertV2 = "g.addV('id', '" + des + "').property('name', '" + des + "').next()";
                    var insertE = "g.V('" + src + "').addE('appear').to(g.V('" + des + "')).next()";
                    if (!blk.vertexIdsSet.ContainsKey(src))
                    {
                        insertNodeBuffer.Add(insertV1);
                        blk.vertexIdsSet.TryAdd(src, null);
                    }

                    if (!blk.vertexIdsSet.ContainsKey(des))
                    {
                        insertNodeBuffer.Add(insertV2);
                        blk.vertexIdsSet.TryAdd(des, null);
                    }
                    insertEdgeBuffer.Add(insertE);
                    doc = buffer.Retrieve();
                    Console.WriteLine("threadNum:" + workerIndex + " buffer size" + buffer.boundedBuffer.Count + " cmd:" + lineE);
                    if (buffer.boundedBuffer.Count() == 0)
                    {
                        buffer.more = false;
                       
                    }
                }

                blk.parseDataCountDownLatch.CountDown();
            }   catch(Exception e)
            {
                throw e;
            }
        }

        public void insertNode()
        {
            try
            {
                var doc = insertNodeBuffer.Retrieve();
                var cmd = new GraphViewCommand(connList[workerIndex]);
                HashSet<String> nodeIdSet = new HashSet<String>();

                while (doc != null)
                {
                    var lineE = doc;
                    cmd.CommandText = lineE;
                    cmd.Execute();
                    doc = insertNodeBuffer.Retrieve();
                    if (insertNodeBuffer.boundedBuffer.Count() == 0)
                    {
                        insertNodeBuffer.more = false;
                    }
                }

                blk.insertNodeCountDownLatch.CountDown();
            } catch(Exception e)
            {
                throw e;
            }
        }

        public void insertEdge()
        {
            try
            {
                var doc = insertEdgeBuffer.Retrieve();
                var cmd = new GraphViewCommand(connList[workerIndex]);
                HashSet<String> nodeIdSet = new HashSet<String>();

                while (doc != null)
                {
                    var lineE = doc;
                    cmd.CommandText = lineE;
                    cmd.Execute();
                    doc = insertEdgeBuffer.Retrieve();
                    if (insertEdgeBuffer.boundedBuffer.Count() == 0)
                    {
                        insertEdgeBuffer.more = false;
                    }
                }
            } catch(Exception e)
            {
                Console.WriteLine(e.StackTrace);
                //throw e;
            }
        }
    }
    public class BufferObject
    {
        public Uri url;
        public JObject doc;
        public GraphViewConnection connection;
        public BufferObject(Uri _url, JObject _doc, GraphViewConnection _connection)
        {
            url = _url;
            doc = _doc;
            connection = _connection;
        }
    }

    public class InsertDocWorker
    {
        public DocumentClient DocDBClient;
        public int workerIndex;
        public BoundedBuffer<BufferObject> buffer;
        public DocumentClient client;

        public void insertDoc()
        {
            var doc = buffer.Retrieve();
            while (doc != null)
            {
                JObject docObject = doc.doc;
                Uri _docDBCollectionUri = doc.url;
                GraphViewConnection conn = doc.connection;
                // need test the async to sync
                Console.WriteLine(_docDBCollectionUri);
                var docTemp = conn.ExecuteQuery("SELECT * FROM Node where Node.id=\"" + docObject["id"] + "\"").ToList();
                if(docTemp.Count == 0)
                {
                    Document createdDocument = client.CreateDocumentAsync(_docDBCollectionUri, docObject).Result;
                    Debug.Assert((string)docObject[KW_DOC_ID] == createdDocument.Id);
                    docObject[KW_DOC_ETAG] = createdDocument.ETag;
                    // make this async, not block at this
                    conn.updateVertexCacheEtag(createdDocument);
                }
                doc = buffer.Retrieve();
            }
        }
    }

    public class CountDownLatch
    {
        private object lockobj;
        private int counts;

        public CountDownLatch(int counts)
        {
            this.counts = counts;
            lockobj = new object();
        }

        public void Await()
        {
            lock (lockobj)
            {
                while (counts > 0)
                {
                    Monitor.Wait(lockobj);
                }
            }
        }

        public void CountDown()
        {
            lock (lockobj)
            {
                counts--;
                Monitor.PulseAll(lockobj);
            }
            Console.WriteLine("countdown" + counts);
        }
    }

    public class BoundedBuffer<T>
    {
        public int bufferSize;
        public Queue<T> boundedBuffer;
        // Whether the queue expects more elements to come
        public bool more;

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
                //while (boundedBuffer.Count == 0 && more)
                //{
                //    Monitor.Wait(_monitor);
                //}

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
