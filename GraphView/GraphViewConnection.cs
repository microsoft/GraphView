// GraphView
// 
// Copyright (c) 2015 Microsoft Corporation
// 
// All rights reserved. 
// 
// MIT License
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
// 

#pragma warning disable CS3003 // Type is not CLS-compliant

#define EASY_DEBUG

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System.Threading;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using static GraphView.GraphViewKeywords;

// For debugging

namespace GraphView
{
    /// <summary>
    /// Connector to a graph database. The class inherits most functions of SqlConnection,
    /// and provides a number of GraphView-specific functions.
    /// </summary>
    public partial class GraphViewConnection : IDisposable
    {
        internal static int InClauseLimit { get; } = 1000;


        public string DocDBUrl { get; }
        public string DocDBPrimaryKey { get; }
        public string DocDBDatabaseId { get; }
        public string DocDBCollectionId { get; }

        public GraphType GraphType { get; internal set; }


        /// <summary>
        /// Whether to generate "id" for edgeObject
        /// </summary>
        public bool GenerateEdgeId { get; } = true;

        /// <summary>
        /// Spill if how many edges are in a edge-document?
        /// </summary>
        public int EdgeSpillThreshold { get; set; } = 1;


        internal VertexObjectCache VertexCache { get; }

        internal string Identifier { get; }


        private readonly Uri _docDBDatabaseUri, _docDBCollectionUri;
        private bool _disposed;

        private DocumentClient DocDBClient { get; }  // Don't expose DocDBClient to outside!

        internal CollectionType CollectionType { get; private set; }

        /// <summary>
        /// Warning: This is actually a collection meta property.
        /// Once this flag is set to false and data modifications are applied on a collection, 
        /// then it should never be set to true again.
        /// </summary>
        public bool UseReverseEdges { get; }


        public string RealPartitionKey { get; }  // Like "location", "id"... but not "_partition"

        public string PartitionPath { get; }  // Like "/location/nested/_value"

        public string PartitionPathTopLevel { get; }  // Like "location"



        public static GraphViewConnection ResetGraphAPICollection(
            string endpoint,
            string authKey,
            string databaseId,
            string collectionId,
            bool useReverseEdge,
            int spilledEdgeThreshold,
            string partitionByKey = null)
        {
            using (DocumentClient client = new DocumentClient(
                new Uri(endpoint), authKey, new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp,
                }
            ))
            {
                if (string.IsNullOrEmpty(partitionByKey))
                {
                    ResetCollection(client, databaseId, collectionId, null);
                }
                else
                {
                    ResetCollection(client, databaseId, collectionId, $"/{KW_DOC_PARTITION}");
                }
            }

            return new GraphViewConnection(endpoint, authKey, databaseId, collectionId, GraphType.GraphAPIOnly,
                useReverseEdge, spilledEdgeThreshold, partitionByKey);
        }

        public static GraphViewConnection ResetFlatCollection(
            string endpoint,
            string authKey,
            string databaseId,
            string collectionId,
            string partitionByKey = null)
        {
            using (DocumentClient client = new DocumentClient(
                new Uri(endpoint), authKey, new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp,
                }
            ))
            {
                if (string.IsNullOrEmpty(partitionByKey))
                {
                    ResetCollection(client, databaseId, collectionId, null);
                }
                else
                {
                    ResetCollection(client, databaseId, collectionId, $"/{partitionByKey}");
                }
            }

            return new GraphViewConnection(endpoint, authKey, databaseId, collectionId, GraphType.CompatibleOnly, false, 1, null);
        }


        /// <summary>
        /// Initializes a new connection to DocDB.
        /// Contains four string: Url, Key, Database's ID, Collection's ID
        /// </summary>
        /// <param name="docDBEndpointUrl">The Url</param>
        /// <param name="docDBAuthorizationKey">The Key</param>
        /// <param name="docDBDatabaseID">Database's ID</param>
        /// <param name="docDBCollectionID">Collection's ID</param>
        /// <param name="graphType">The type of graph, compatible only, graph api only, or hybrid</param>
        /// <param name="useReverseEdges">Whether use reverse edges</param>
        /// <param name="edgeSpillThreshold">For compatible and hybrid graph, it must be 1.</param>
        /// <param name="partitionByKeyIfViaGraphAPI">This parameter takes effect only when creating a partitioned collection using graph api.</param>
        /// <param name="preferredLocation"></param>
        public GraphViewConnection(
            string docDBEndpointUrl,
            string docDBAuthorizationKey,
            string docDBDatabaseID,
            string docDBCollectionID,
            GraphType graphType,
            bool useReverseEdges,
            int? edgeSpillThreshold,
            string partitionByKeyIfViaGraphAPI,
            string preferredLocation = null)
        {
            // TODO: Parameter checking!

            // Initialze the two URI for future use
            // They are immutable during the life of this connection
            this._docDBDatabaseUri = UriFactory.CreateDatabaseUri(docDBDatabaseID);
            this._docDBCollectionUri = UriFactory.CreateDocumentCollectionUri(docDBDatabaseID, docDBCollectionID);

            this.DocDBUrl = docDBEndpointUrl;
            this.DocDBPrimaryKey = docDBAuthorizationKey;
            this.DocDBDatabaseId = docDBDatabaseID;
            this.DocDBCollectionId = docDBCollectionID;
            this.UseReverseEdges = useReverseEdges;

            this.GraphType = graphType;

            ConnectionPolicy connectionPolicy = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
            };
            if (!string.IsNullOrEmpty(preferredLocation))
            {
                connectionPolicy.PreferredLocations.Add(preferredLocation);
            }

            this.DocDBClient = new DocumentClient(new Uri(this.DocDBUrl),
                                                  this.DocDBPrimaryKey,
                                                  connectionPolicy);
            this.DocDBClient.OpenAsync().Wait();


            //
            // Check whether it is a partitioned collection (if exists)
            //
            DocumentCollection docDBCollection;
            try
            {
                docDBCollection = this.DocDBClient.CreateDocumentCollectionQuery(this._docDBDatabaseUri)
                    .Where(c => c.Id == this.DocDBCollectionId)
                    .AsEnumerable()
                    .FirstOrDefault();
            }
            catch (AggregateException aggex)
            when ((aggex.InnerException as DocumentClientException)?.Error.Code == "NotFound")
            {
                // Now the database does not exist!
                // NOTE: If the database exists, but the collection does not exist, it won't be an exception
                docDBCollection = null;
            }

            if (docDBCollection == null)
            {
                throw new GraphViewException("This collection does not exist.");
            }

            //
            // If the collection has existed, try to retrive its partition key path
            // If not partitioned, set `PartitionPath` to null
            //
            Collection<string> partitionKey = docDBCollection.PartitionKey.Paths;
            if (partitionKey.Count == 1)
            {
                this.CollectionType = CollectionType.PARTITIONED;

                this.PartitionPath = partitionKey[0];
                this.PartitionPathTopLevel = this.PartitionPath.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries)[0];

                if (this.PartitionPath == $"/{KW_DOC_PARTITION}")
                {  // Partitioned, created via GraphAPI
                    Debug.Assert(partitionByKeyIfViaGraphAPI != null);
                    Debug.Assert(graphType == GraphType.GraphAPIOnly);
                    this.RealPartitionKey = partitionByKeyIfViaGraphAPI;
                }
                else
                {
                    Debug.Assert(partitionByKeyIfViaGraphAPI == null);
                    this.RealPartitionKey = this.PartitionPathTopLevel;
                }
            }
            else
            {
                this.CollectionType = CollectionType.STANDARD;

                Debug.Assert(partitionKey.Count == 0);
                this.PartitionPath = null;
                this.PartitionPathTopLevel = null;
                this.RealPartitionKey = null;

                Debug.Assert(partitionByKeyIfViaGraphAPI == null);
            }

            if (graphType != GraphType.GraphAPIOnly)
            {
                Debug.Assert(edgeSpillThreshold == 1);
            }

            this.Identifier = $"{docDBEndpointUrl}\0{docDBDatabaseID}\0{docDBCollectionID}";
            this.VertexCache = new VertexObjectCache(this);

            this.EdgeSpillThreshold = edgeSpillThreshold ?? 0;
    }


    internal DbPortal CreateDatabasePortal()
        {
            return new DocumentDbPortal(this);
        }


        /// <summary>
        /// Releases all resources used by GraphViewConnection.
        /// This function is NOT thread-safe!
        /// </summary>
        public void Dispose()
        {
            if (this._disposed) return;

            this.DocDBClient.Dispose();
            this._disposed = true;
        }


        public static void EnsureDatabaseExist(DocumentClient client, string databaseId)
        {
            Database docDBDatabase = client.CreateDatabaseQuery()
                                           .Where(db => db.Id == databaseId)
                                           .AsEnumerable()
                                           .FirstOrDefault();

            // If the database does not exist, create one
            if (docDBDatabase == null)
            {
                client.CreateDatabaseAsync(new Database { Id = databaseId }).Wait();
            }
        }


        /// <summary>
        /// If the collection has existed, reset the collection.
        ///   - collectionType = STANDARD: the collection is reset to STANDARD
        ///   - collectionType = PARTITIONED: the collection is reset to PARTITIONED
        ///   - collectionType = UNDEFINED: the collection's partition property remains the same as original one
        /// If the collection does not exist, create the collection
        ///   - collectionType = STANDARD: the newly created collection is STANDARD
        ///   - collectionType = PARTITIONED: the newly created collection is PARTITIONED
        ///   - collectionType = UNDEFINED: an exception is thrown!
        /// </summary>
        public static void ResetCollection(
            DocumentClient client, string databaseId, string collectionId,
            string partitionPath = null)
        {
            EnsureDatabaseExist(client, databaseId);

            DocumentCollection docDBCollection = client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseId))
                                                     .Where(c => c.Id == collectionId)
                                                     .AsEnumerable()
                                                     .FirstOrDefault();

            // Delete the collection if it exists
            if (docDBCollection != null)
            {
                DeleteCollection(client, databaseId, collectionId);
            }

            CreateCollection(client, databaseId, collectionId, partitionPath);

            Trace.WriteLine($"[ResetCollection] Database/Collection {databaseId}/{collectionId} has been reset.");
        }



        private static void CreateCollection(DocumentClient client, string databaseId, string collectionId, string partitionPath = null)
        {
            DocumentCollection collection = new DocumentCollection
            {
                Id = collectionId,
            };
            if (!string.IsNullOrEmpty(partitionPath))
            {
                collection.PartitionKey.Paths.Add(partitionPath);
            }

            client.CreateDocumentCollectionAsync(
                UriFactory.CreateDatabaseUri(databaseId),
                collection,
                new RequestOptions
                {
                    OfferThroughput = 10000
                }
            ).Wait();
        }

        private static void DeleteCollection(DocumentClient client, string databaseId, string collectionId)
        {
            client.DeleteDocumentCollectionAsync(
                UriFactory.CreateDocumentCollectionUri(databaseId, collectionId)
            ).Wait();
        }


        public static int partitionNum { get; set; } = 100;
        public static int[] partitionLoad = new int[partitionNum];
        public bool usePartitionWhenCreateDoc { get; set; } = true;
        public int repartitionBatchRandomIterSize { get; set; } = 996;
        /// <summary>
        /// partition the document data
        /// The <paramref name="docObject"/> will be updated (Add the "id" field)
        /// </summary>
        /// <param name="docObject"></param>
        public JObject partitionDocumentByGreedyVertexCut(JObject docObject)
        {
            try
            {
                var isEdgeDoc = docObject["_isEdgeDoc"];
                // (1) For edge doc 
                if (isEdgeDoc != null && Convert.ToBoolean(isEdgeDoc.ToString()))
                {
                    var edgePartition = "0";
                    var srcId = docObject["_vertex_id"];
                    var edge = docObject["_edge"][0];
                    var desId = edge["_sinkV"];
                    List<dynamic> srcPartitionList = ExecuteQuery("SELECT Node._partition FROM Node where Node.id =\"" + srcId + "\"").ToList();
                    List<dynamic> desPartitionList = ExecuteQuery("SELECT Node._partition FROM Node where Node.id =\"" + desId + "\"").ToList();
                    var srcPartition = ((JObject)srcPartitionList[0])["_partition"].ToString();
                    var desPartition = ((JObject)desPartitionList[0])["_partition"].ToString();
                    int a = 0;
                    // (1) src and des are in the same partition
                    if (srcPartition == desPartition)
                    {
                        edgePartition = srcPartition;
                    }
                    // (2) src and des are not in the same partition
                    if (srcPartition != desPartition)
                    {
                        //if(partitionLoad[Convert.ToInt32(srcPartition)] > partitionLoad[Convert.ToInt32(desPartition)])
                        //{
                        //    edgePartition = desPartition;
                        //} else
                        //{
                        //    edgePartition = srcPartition;
                        //}
                        edgePartition = srcPartition; // For future design: keep the safety of the transaction
                    }
                    docObject["_partition"] = edgePartition;
                    partitionLoad[Convert.ToInt32(edgePartition)]++;
                    return docObject;
                }
                else
                {
                    // For vertex doc, random load balance assign
                    // (1) Rule1: if the vertex is first time to be insert
                    var minValue = partitionLoad.Min();
                    var minIndex = Array.IndexOf(partitionLoad, minValue);
                    if (docObject["_partition"] == null)
                    {
                        docObject.Add("_partition", minIndex.ToString());
                    }
                    else
                    {
                        docObject["_partition"] = minIndex.ToString();
                    }
                    partitionLoad[minIndex]++;
                    return docObject;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.InnerException);
                throw e;
            }
            // new yj
        }

        /// <summary>
        /// Metrics
        /// </summary>
        public void getMetricsOfGraphPartition()
        {
            // (1) Get balance metrics, What we can do is just statistic the partition key balance. 
            // We can't get the physical partition information
            int[] partitionDocCount = new int[partitionNum];
            int docCount = 0;
            //int edgeCount = 0;
            int vertexCount = 0;
            for (int i = 0; i < partitionNum; i++)
            {
                List<dynamic> result = ExecuteQuery("SELECT * FROM Node where Node._partition = \"" + i + "\"").ToList();
                partitionDocCount[i] = Convert.ToInt32(result.Count);
                docCount += partitionDocCount[i];
                Console.WriteLine("Partititon: " + i + " Count:" + partitionDocCount[i]);
            }

            double average = partitionDocCount.Average();
            double sumOfSquaresOfDifferences = partitionDocCount.Select(val => (val - average) * (val - average)).Sum();
            double sd = Math.Sqrt(sumOfSquaresOfDifferences / partitionDocCount.Length);
            Console.WriteLine("Partition Doc Count SDV is " + sd);

            // (2) Get Graph Clustering metrics: VertexCut ratio
            List<dynamic> edgeList = ExecuteQuery("SELECT * FROM Node where Node._isEdgeDoc=true").ToList();
            int vertexCut = 0;
            int edgeCount = 0;
            foreach (var e in edgeList)
            {
                var edge = (JObject)e;
                var srcId = edge["_vertex_id"];
                var desId = edge["_edge"][0]["_sinkV"];
                var srcDocFromSrcCol = (JObject)ExecuteQuery("SELECT * FROM Node where Node.id=\"" + srcId + "\"").ToList()[0];
                var desDocFromSrcCol = (JObject)ExecuteQuery("SELECT * FROM Node where Node.id=\"" + desId + "\"").ToList()[0];
                var srcPartition = srcDocFromSrcCol["_partition"].ToString();
                var desPartition = desDocFromSrcCol["_partition"].ToString();
                var edgePartition = edge["_partition"].ToString();
                edgeCount++;

                if (edgePartition != srcPartition)
                {
                    vertexCut ++;
                }
                 
                if(edgePartition != desPartition)
                {
                    vertexCut ++;
                }
            }

            vertexCount = docCount - edgeCount;
            Console.WriteLine("Vertex cut ratio" + ((double)vertexCut / (2 * (double)edgeCount)));
            Console.WriteLine("Doc Count: " + docCount);
            Console.WriteLine("Edge Count: " + edgeCount);
            Console.WriteLine("Vertex Count: " + vertexCount);
        }

        /// <summary>
        /// partition the collection
        /// </summary>
        public Boolean AssignSeenDesNotSeenSrcToBalance { set; get; } = false;
        public int getMinLoadPartitionIndex()
        {
            var minValue = partitionLoad.Min();
            var minIndex = Array.IndexOf(partitionLoad, minValue);
            return minIndex;
        }

        public void repartitionTheCollection(GraphViewConnection srcConnection)
        {
            Random rnd = new Random();
            usePartitionWhenCreateDoc = false;
            List<dynamic> edgeList = srcConnection.ExecuteQuery("SELECT * FROM Node where Node._isEdgeDoc=true").ToList();
            List<JObject> edgeBatchList = new List<JObject>();
            int tempColCount = 0;
            foreach (var e1 in edgeList)
            {
                tempColCount++;
                edgeBatchList.Add((JObject)e1);
                if (edgeBatchList.Count < repartitionBatchRandomIterSize && tempColCount < edgeList.Count)
                {
                    continue;
                }

                var shuffleList = edgeBatchList.OrderBy(x => (x.ToString().GetHashCode())).ToList();
                foreach(var e in shuffleList)
                {
                    var edge = (JObject)e;
                    var srcId = edge["_vertex_id"];
                    var desId = edge["_edge"][0]["_sinkV"];
                    var srcDocFromSrcCol = (JObject)srcConnection.ExecuteQuery("SELECT * FROM Node where Node.id=\"" + srcId + "\"").ToList()[0];
                    var desDocFromSrcCol = (JObject)srcConnection.ExecuteQuery("SELECT * FROM Node where Node.id=\"" + desId + "\"").ToList()[0];
                    var srcDocFromDesCol = ExecuteQuery("SELECT * FROM Node where Node.id=\"" + srcId + "\"").ToList();
                    var desDocFromDesCol = ExecuteQuery("SELECT * FROM Node where Node.id=\"" + desId + "\"").ToList();
                    var edgePartition = "";
                    // (1) neither vertex is insert
                    if (srcDocFromDesCol.Count == 0 && desDocFromDesCol.Count == 0)
                    {
                        edgePartition = getMinLoadPartitionIndex().ToString();
                        edge["_partition"] = edgePartition;
                        srcDocFromSrcCol["_partition"] = edgePartition;
                        desDocFromSrcCol["_partition"] = edgePartition;

                        var t = desDocFromSrcCol["_partition"];
                        // insert src doc
                        CreateDocumentAsync(srcDocFromSrcCol);
                        // insert des doc
                        CreateDocumentAsync(desDocFromSrcCol);
                        // insert edge
                        CreateDocumentAsync(edge);
                        partitionLoad[Convert.ToInt32(edge["_partition"])] += 3;
                        continue;
                    }
                    // (2) only one the vertex insert
                    if (srcDocFromDesCol.Count != 0 && desDocFromDesCol.Count == 0)
                    {
                        var srcDocPartition = ((JObject)(srcDocFromDesCol[0]))["_partition"];
                        desDocFromSrcCol["_partition"] = srcDocPartition;
                        edge["_partition"] = srcDocPartition;
                        // insert des doc
                        CreateDocumentAsync(desDocFromSrcCol);
                        // insert edge
                        CreateDocumentAsync(edge);
                        partitionLoad[Convert.ToInt32(edge["_partition"])] += 2;
                        continue;
                    }

                    if (desDocFromDesCol.Count != 0 && srcDocFromDesCol.Count == 0)
                    {
                        var index = "";
                        if (AssignSeenDesNotSeenSrcToBalance)
                        {
                            index = getMinLoadPartitionIndex().ToString();
                        }
                        else
                        {
                            index = ((JObject)(desDocFromDesCol[0]))["_partition"].ToString();
                        }

                        srcDocFromSrcCol["_partition"] = index;
                        edge["_partition"] = index;
                        // insert src doc
                        CreateDocumentAsync(srcDocFromSrcCol);
                        // insert edge
                        CreateDocumentAsync(edge);
                        partitionLoad[Convert.ToInt32(edge["_partition"])] += 2;
                        continue;
                    }

                    if (srcDocFromDesCol.Count != 0 && desDocFromDesCol.Count != 0)
                    {
                        var srcDocFromDes = (JObject)srcDocFromDesCol[0];
                        var desDocFromDes = (JObject)srcDocFromDesCol[0];
                        var srcPartition = srcDocFromDes["_partition"];
                        var desPartition = desDocFromDes["_partition"];
                        if (srcPartition == desPartition)
                        {
                            // (3) src and des in the same partition
                            var srcDocPartition = srcDocFromDesCol[0]["_partition"];
                            edge["_partition"] = srcDocPartition;
                            CreateDocumentAsync(edge);
                            partitionLoad[Convert.ToInt32(edge["_partition"])] += 1;
                            continue;
                        }
                        else
                        {
                            // (4) src and des not int the same partition
                            //if (partitionLoad[Convert.ToInt32(srcPartition)] > partitionLoad[Convert.ToInt32(desPartition)])
                            //{
                            //    edge["_partition"] = desPartition;
                            //}
                            //else
                            //{
                            //    edge["_partition"] = srcPartition;
                            //}
                            edge["_partition"] = srcPartition; // For design of the transaction
                            CreateDocumentAsync(edge);
                            partitionLoad[Convert.ToInt32(edge["_partition"])] += 1;
                            continue;
                        }
                    }
                    int a = 0;
                }
                edgeBatchList.Clear();
            }
        }

        /// <summary>
        /// Create a new document (return the new docId)
        /// The <paramref name="docObject"/> will be updated (Add the "id" field)
        /// </summary>
        /// <param name="docObject"></param>
        internal async Task<string> CreateDocumentAsync(JObject docObject)
        {
            Debug.Assert(docObject != null, "The newly created document should not be null");
            Debug.Assert(docObject[KW_DOC_ID] != null, $"The newly created document should specify '{KW_DOC_ID}' field");
            //Debug.Assert(docObject[KW_DOC_PARTITION] != null, $"The newly created document should specify '{KW_DOC_PARTITION}' field");

            // new yj
            if(usePartitionWhenCreateDoc)
            {
                docObject = partitionDocumentByGreedyVertexCut(docObject);
            }
            // new yj
            Document createdDocument = await this.DocDBClient.CreateDocumentAsync(this._docDBCollectionUri, docObject);
            Debug.Assert((string)docObject[KW_DOC_ID] == createdDocument.Id);

            //
            // Save the created document's etag
            //
            docObject[KW_DOC_ETAG] = createdDocument.ETag;
            this.VertexCache.UpdateCurrentEtag(createdDocument);

            return createdDocument.Id;
        }

        /// <summary>
        /// Use HashSet but not IEnumerable here to ensure that there are no two
        /// docObject sharing the same reference, which causes adding "id" field to
        /// the docObject twice.
        /// </summary>
        /// <param name="docObjects"></param>
        /// <returns></returns>
        internal async Task CreateDocumentsAsync(HashSet<JObject> docObjects)
        {
            foreach (JObject docObject in docObjects)
            {
                Debug.Assert(docObject != null, "The newly created document should not be null");
                Debug.Assert(docObject[KW_DOC_ID] != null, $"The newly created document should contain '{KW_DOC_ID}' field");
                //Debug.Assert(docObject[KW_DOC_PARTITION] != null, $"The newly created document should contain '{KW_DOC_PARTITION}' field");

                Document createdDoc = await this.DocDBClient.CreateDocumentAsync(this._docDBCollectionUri, docObject);
                Debug.Assert((string)docObject[KW_DOC_ID] == createdDoc.Id);
            }
        }


        internal async Task ReplaceOrDeleteDocumentAsync(string docId, JObject docObject, string partition)
        {
            if (docObject != null)
            {
                Debug.Assert((string)docObject[KW_DOC_ID] == docId);
            }

            RequestOptions option = new RequestOptions();
            if (this.CollectionType == CollectionType.PARTITIONED)
            {
                option.PartitionKey = new PartitionKey(partition);
            }

            option.AccessCondition = new AccessCondition
            {
                Type = AccessConditionType.IfMatch,
                Condition = this.VertexCache.GetCurrentEtag(docId),
            };

            Uri documentUri = UriFactory.CreateDocumentUri(this.DocDBDatabaseId, this.DocDBCollectionId, docId);
            if (docObject == null)
            {
                this.DocDBClient.DeleteDocumentAsync(documentUri, option).Wait();

                // Remove the document's etag from saved
                this.VertexCache.RemoveEtag(docId);
            }
            else
            {
                Debug.Assert(docObject[KW_DOC_ID] is JValue);
                Debug.Assert((string)docObject[KW_DOC_ID] == docId, "The replaced document should match ID in the parameter");
                //Debug.Assert(partition != null && partition == (string)docObject[KW_DOC_PARTITION]);

                Document document = await this.DocDBClient.ReplaceDocumentAsync(documentUri, docObject, option);

                // Update the document's etag
                docObject[KW_DOC_ETAG] = document.ETag;
                this.VertexCache.UpdateCurrentEtag(document);
            }
        }

        internal async Task ReplaceOrDeleteDocumentsAsync(Dictionary<string, Tuple<JObject, string>> documentsMap)
        {
#if DEBUG
            // Make sure that there aren't two docObject (not null) sharing the same reference
            List<Tuple<JObject, string>> docObjectList = documentsMap.Values.Where(tuple => tuple.Item1 != null).ToList();
            HashSet<Tuple<JObject, string>> docObjectSet = new HashSet<Tuple<JObject, string>>(docObjectList);
            Debug.Assert(docObjectList.Count == docObjectSet.Count, "Replacing documents with two docObject sharing the same reference");
#endif
            foreach (KeyValuePair<string, Tuple<JObject, string>> pair in documentsMap)
            {
                string docId = pair.Key;
                JObject docObject = pair.Value.Item1;  // Can be null (null means deletion)
                string partition = pair.Value.Item2;  // Partition
                if (docObject != null)
                {
                    Debug.Assert(partition == this.GetDocumentPartition(docObject));
                }
                await this.ReplaceOrDeleteDocumentAsync(docId, docObject, partition);
            }
        }




        internal JObject RetrieveDocumentById(string docId, string partition)
        {
            Debug.Assert(!string.IsNullOrEmpty(docId), "'docId' should not be null or empty");

            if (partition != null)
            {
                Debug.Assert(this.PartitionPath != null);
            }

            string script = $"SELECT * FROM Doc WHERE Doc.{KW_DOC_ID} = '{docId}'" +
                            (partition != null ? $" AND Doc{this.GetPartitionPathIndexer()} = '{partition}'" : "");
            JObject result = this.ExecuteQueryUnique(script);

            //
            // Save etag of the fetched document
            // No override!
            //
            if (result != null)
            {
                this.VertexCache.SaveCurrentEtagNoOverride(result);
            }

            return result;
        }


        public string GetDocumentPartition(JObject document)
        {
            if (this.PartitionPath == null)
            {
                return null;
            }

            JToken token = document;
            string[] paths = this.PartitionPath.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (string part in paths)
            {
                if (part.StartsWith("["))
                {  // Like /[0]
                    Debug.Assert(part.EndsWith("]"));
                    Debug.Assert((token is JArray));
                    token = ((JArray)token)[int.Parse(part.Substring(1, part.Length - 2))];
                }
                else if (part.StartsWith("\""))
                {  // Like /"property with space"
                    Debug.Assert(part.EndsWith("\""));
                    token = token[part.Substring(1, part.Length - 2)];
                }
                else
                {   // Like /normal_property
                    token = token[part];
                }
            }

            Debug.Assert(token is JValue);
            Debug.Assert(((JValue)token).Type == JTokenType.String);
            return (string)token;
        }

        public string GetPartitionPathIndexer()
        {
            if (this.PartitionPath == null)
            {
                return "";
            }

            StringBuilder partitionIndexerBuilder = new StringBuilder();
            string[] paths = this.PartitionPath.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (string part in paths)
            {
                if (part.StartsWith("["))
                {  // Like /[0]
                    Debug.Assert(part.EndsWith("]"));
                    partitionIndexerBuilder.Append(part);
                }
                else if (part.StartsWith("\""))
                {  // Like /"property with space"
                    Debug.Assert(part.EndsWith("\""));
                    partitionIndexerBuilder.AppendFormat("[{0}]", part);
                }
                else
                {   // Like /normal_property
                    partitionIndexerBuilder.AppendFormat("[\"{0}\"]", part);
                }
            }

            return partitionIndexerBuilder.ToString();
        }


        internal IEnumerable<dynamic> ExecuteQuery(string queryScript, FeedOptions queryOptions = null)
        {
            if (queryOptions == null)
            {
                queryOptions = new FeedOptions
                {
                    MaxItemCount = -1,
                    EnableScanInQuery = true,
                };
                if (this.CollectionType == CollectionType.PARTITIONED)
                {
                    queryOptions.EnableCrossPartitionQuery = true;
                }
            }

            return this.DocDBClient.CreateDocumentQuery(
                this._docDBCollectionUri,
                queryScript,
                queryOptions);
        }

        internal JObject ExecuteQueryUnique(string queryScript, FeedOptions queryOptions = null)
        {
            try
            {
                //
                // It seems that DocDB won't return an empty result, but throw an exception instead!
                // 
                List<dynamic> result = ExecuteQuery(queryScript, queryOptions).ToList();

                Debug.Assert(result.Count <= 1, "A unique query should have at most 1 result");
                return (result.Count == 0)
                           ? null
                           : (JObject)result[0];

            }
            catch (AggregateException aggex) when ((aggex.InnerException as DocumentClientException)?.Error.Code == "NotFound")
            {
                return null;
            }
        }


#if EASY_DEBUG
        private static long __currId = 0;
#endif       
        internal static string GenerateDocumentId()
        {
#if EASY_DEBUG
            return "ID_" + (++__currId).ToString();
#else
            // TODO: Implement a stronger Id generation
            Guid guid = Guid.NewGuid();
            return guid.ToString("D");
#endif
        }

    }

}
