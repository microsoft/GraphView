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
    //public static class GraphLoaderFactory
    //{
    //    public static void parseData(string path, GraphViewConnection connection, string collectionName, Boolean resetCollection, int threadNum,
    //        Dictionary<string, Dictionary<string, string>> nodePropertiesHashMap, Dictionary<string, Dictionary<string, string>> outEdgePropertiesHashMap, Dictionary<string, Dictionary<string, string>> inEdgePropertiesHashMap)
    //    {
    //    }

    //    public static void loadAzureIOT(string path, GraphViewConnection connection, string collectionName,  Boolean resetCollection, int threadNum)
    //    {
         
    //    }

    //    public static void parseAndDumpIOTData(string path, GraphViewConnection connection, string collectionName, Boolean resetCollection, int threadNum, string nodeFilePath, string edgeFilePath)
    //    {
           
    //    }
    //}

    //public class DocDBInsertNodeToFileWorkerByNewAPI
    //{
    //    public int threadId;
    //    GraphViewConnection connection = null;
    //    BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputNodeBuffer = null;
    //    BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputInEdgeBuffer = null;
    //    BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputOutEdgeBuffer = null;
    //    public List<Double> result = null;
    //    public StreamWriter nodeFile = null;

    //    public DocDBInsertNodeToFileWorkerByNewAPI(GraphViewConnection _connection,
    //        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputNodeBuffer,
    //        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputInEdgeBuffer,
    //        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputOutEdgeBuffer)
    //    {
    //        this.connection = _connection;
    //        this.inputNodeBuffer = _inputNodeBuffer;
    //        this.inputInEdgeBuffer = _inputInEdgeBuffer;
    //        this.inputOutEdgeBuffer = _inputOutEdgeBuffer;
    //    }

    //    public void BulkInsert()
    //    {
          
    //    }

    //    public void Dispose()
    //    {
    //    }
    //}

    //public class DocDBInsertEdgeToFileWorkerByNewAPI
    //{
    //    public int threadId;
    //    //GraphViewGremlinParser parser = new GraphViewGremlinParser();
    //    GraphViewConnection connection = null;
    //    BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputNodeBuffer = null;
    //    BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputInEdgeBuffer = null;
    //    BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputOutEdgeBuffer = null;
    //    public List<Double> result = null;
    //    public StreamWriter edgeFile = null;

    //    public DocDBInsertEdgeToFileWorkerByNewAPI(GraphViewConnection _connection,
    //        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputNodeBuffer,
    //        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputInEdgeBuffer,
    //        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputOutEdgeBuffer)
    //    {
    //        this.connection = _connection;
    //        this.inputNodeBuffer = _inputNodeBuffer;
    //        this.inputInEdgeBuffer = _inputInEdgeBuffer;
    //        this.inputOutEdgeBuffer = _inputOutEdgeBuffer;
    //    }

    //    public void BulkInsert()
    //    {
        
    //    }

    //    public void Dispose()
    //    {
    //    }
    //}

    //public class DocDBInsertNodeWorkerByNewAPI
    //{
    //    public int threadId;
    //    //GraphViewGremlinParser parser = new GraphViewGremlinParser();
    //    GraphViewConnection connection = null;
    //    BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputNodeBuffer = null;
    //    BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputInEdgeBuffer = null;
    //    BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputOutEdgeBuffer = null;
    //    public List<Double> result = null;

    //    public DocDBInsertNodeWorkerByNewAPI(GraphViewConnection _connection,
    //        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputNodeBuffer,
    //        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputInEdgeBuffer,
    //        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputOutEdgeBuffer)
    //    {
    //        this.connection = _connection;
    //        this.inputNodeBuffer = _inputNodeBuffer;
    //        this.inputInEdgeBuffer = _inputInEdgeBuffer;
    //        this.inputOutEdgeBuffer = _inputOutEdgeBuffer;
    //    }

    //    public void BulkInsert()
    //    {
            
    //    }

    //    public void Dispose()
    //    {
    //    }
    //}
    //public class DocDBInsertEdgeWorkerByNewAPI
    //{
    //    public int threadId;
    //    GraphViewConnection connection = null;
    //    BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputNodeBuffer = null;
    //    BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputInEdgeBuffer = null;
    //    BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> inputOutEdgeBuffer = null;
    //    public List<Double> result = null;
    //    public DocDBInsertEdgeWorkerByNewAPI(GraphViewConnection _connection,
    //        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputNodeBuffer,
    //        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputInEdgeBuffer,
    //        BoundedBuffer<KeyValuePair<string, Dictionary<string, string>>> _inputOutEdgeBuffer)
    //    {
    //        this.connection = _connection;
    //        this.inputNodeBuffer = _inputNodeBuffer;
    //        this.inputInEdgeBuffer = _inputInEdgeBuffer;
    //        this.inputOutEdgeBuffer = _inputOutEdgeBuffer;
    //    }

    //    public void BulkInsert()
    //    {
           
    //    }

    //    public void Dispose()
    //    {
    //    }
    //}

    //public class DocDBInsertWorker
    //{
    //    BoundedBuffer<string> inputStream;
    //    public int threadId;
    //    GraphViewGremlinParser parser = new GraphViewGremlinParser();
    //    GraphViewConnection connection = null;

    //    public DocDBInsertWorker(GraphViewConnection _connection,
    //        BoundedBuffer<string> _inputStream)
    //    {
    //        this.connection = _connection;
    //        this.inputStream = _inputStream;
    //    }

    //    public void BulkInsert()
    //    {
          
    //    }

    //    public void Dispose()
    //    {
    //    }
    //}

    //public class BoundedBuffer<T>
    //{
    //    public int bufferSize;
    //    public Queue<T> boundedBuffer;
    //    // Whether the queue expects more elements to come
    //    bool more;

    //    public bool More
    //    {
    //        get { return more; }
    //    }

    //    Object _monitor;
    //    public BoundedBuffer(int bufferSize)
    //    {
    //        boundedBuffer = new Queue<T>(bufferSize);
    //        this.bufferSize = bufferSize;
    //        more = true;
    //        _monitor = new object();
    //    }

    //    public void Add(T element)
    //    {
    //        lock (_monitor)
    //        {
    //            while (boundedBuffer.Count == bufferSize)
    //            {
    //                Monitor.Wait(_monitor);
    //            }

    //            boundedBuffer.Enqueue(element);
    //            Monitor.Pulse(_monitor);
    //        }
    //    }

    //    public T Retrieve()
    //    {
    //        T element = default(T);

    //        lock (_monitor)
    //        {
    //            while (boundedBuffer.Count == 0 && more)
    //            {
    //                Monitor.Wait(_monitor);
    //            }

    //            if (boundedBuffer.Count > 0)
    //            {
    //                element = boundedBuffer.Dequeue();
    //                Monitor.Pulse(_monitor);
    //            }
    //        }

    //        return element;
    //    }

    //    public void Close()
    //    {
    //        lock (_monitor)
    //        {
    //            more = false;
    //            Monitor.PulseAll(_monitor);
    //        }
    //    }
    //}
}
