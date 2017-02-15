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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System.Threading;
using Newtonsoft.Json.Linq;

// For debugging

namespace GraphView
{
    /// <summary>
    /// Connector to a graph database. The class inherits most functions of SqlConnection,
    /// and provides a number of GraphView-specific functions.
    /// </summary>
    public partial class GraphViewConnection : IDisposable
    {
        public string DocDBUrl { get; }
        public string DocDBPrimaryKey { get; }
        public string DocDBDatabaseId { get; }
        public string DocDBCollectionId { get; }

        internal VertexObjectCache VertexCache { get; }

        private readonly Uri _docDBDatabaseUri, _docDBCollectionUri;
        private bool _disposed;        

        private DocumentClient DocDBClient { get; }  // Don't expose DocDBClient to outside!


        /// <summary>
        /// Initializes a new connection to DocDB.
        /// Contains four string: Url, Key, Database's ID, Collection's ID
        /// </summary>
        /// <param name="docDBEndpointUrl">The Url</param>
        /// <param name="docDBAuthorizationKey">The Key</param>
        /// <param name="docDBDatabaseID">Database's ID</param>
        /// <param name="docDBCollectionID">Collection's ID</param>
        public GraphViewConnection(
            string docDBEndpointUrl,
            string docDBAuthorizationKey,
            string docDBDatabaseID,
            string docDBCollectionID)
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
            this.DocDBClient = new DocumentClient(new Uri(this.DocDBUrl),
                                                  this.DocDBPrimaryKey,
                                                  new ConnectionPolicy {
                                                      ConnectionMode = ConnectionMode.Direct,
                                                      ConnectionProtocol = Protocol.Tcp,
                                                  });
            this.DocDBClient.OpenAsync().Wait();

            this.VertexCache = new VertexObjectCache(this);
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


        public void EnsureDatabaseExist()
        {
            Database docDBDatabase = this.DocDBClient.CreateDatabaseQuery()
                                         .Where(db => db.Id == this.DocDBDatabaseId)
                                         .AsEnumerable()
                                         .FirstOrDefault();

            // If the database does not exist, create one
            if (docDBDatabase == null) {
                this.DocDBClient.CreateDatabaseAsync(new Database {Id = this.DocDBDatabaseId}).Wait();
            }
        }


        public void ResetCollection()
        {
            EnsureDatabaseExist();

            DocumentCollection docDBCollection = this.DocDBClient.CreateDocumentCollectionQuery(
                                                         UriFactory.CreateDatabaseUri(this.DocDBDatabaseId)
                                                     ).Where(c => c.Id == this.DocDBCollectionId)
                                                     .AsEnumerable()
                                                     .FirstOrDefault();

            // Delete the collection if it exists
            if (docDBCollection != null) {
                DeleteCollection();
            }
            CreateCollection();

            Trace.WriteLine($"Database/Collection {this.DocDBDatabaseId}/{this.DocDBCollectionId} has been reset.");
        }

        private void CreateCollection()
        {
            // TODO: Make the OfferType configurable?

            this.DocDBClient.CreateDocumentCollectionAsync(
                this._docDBDatabaseUri,
                new DocumentCollection {Id = this.DocDBCollectionId},
                new RequestOptions {OfferType = "S3"}
            ).Wait();
        }

        private void DeleteCollection()
        {
            this.DocDBClient.DeleteDocumentCollectionAsync(this._docDBCollectionUri).Wait();
        }


        /// <summary>
        /// Create a new document (return the new docId)
        /// The <paramref name="docObject"/> will be updated (Add the "id" field)
        /// </summary>
        /// <param name="docObject"></param>
        internal async Task<string> CreateDocumentAsync(JObject docObject)
        {
            Debug.Assert(docObject != null, "The newly created document should not be null");
            Debug.Assert(docObject["id"] == null, "The newly created document should not contains 'id' field");

            Document createdDocument = await this.DocDBClient.CreateDocumentAsync(this._docDBCollectionUri, docObject);
            docObject["id"] = createdDocument.Id;
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
            foreach (JObject docObject in docObjects) {
                Debug.Assert(docObject != null, "The newly created document should not be null");
                Debug.Assert(docObject["id"] == null, "The newly created document should not contains 'id' field");

                Document createdDoc = await this.DocDBClient.CreateDocumentAsync(this._docDBCollectionUri, docObject);
                docObject["id"] = createdDoc.Id;
            }
        }


        internal async Task ReplaceOrDeleteDocumentAsync(string docId, JObject docObject)
        {
            Uri documentUri = UriFactory.CreateDocumentUri(this.DocDBDatabaseId, this.DocDBCollectionId, docId);
            if (docObject == null) {
                this.DocDBClient.DeleteDocumentAsync(documentUri).Wait();
            }
            else {
#if DEBUG
                if (docObject["id"] != null) {
                    Debug.Assert(docObject["id"] is JValue);
                    Debug.Assert((string)docObject["id"] == docId, "The replaced document should match ID in the parameter");
                }
#endif
                await this.DocDBClient.ReplaceDocumentAsync(documentUri, docObject);
                docObject["id"] = docId;
            }
        }

        internal async Task ReplaceOrDeleteDocumentsAsync(Dictionary<string, JObject> documentsMap)
        {
#if DEBUG
            // Make sure that there aren't two docObject (not null) sharing the same reference
            List<JObject> docObjectList = documentsMap.Values.Where(docObject => docObject != null).ToList();
            HashSet<JObject> docObjectSet = new HashSet<JObject>(docObjectList);
            Debug.Assert(docObjectList.Count == docObjectSet.Count, "Replacing documents with two docObject sharing the same reference");
#endif
            foreach (KeyValuePair<string, JObject> pair in documentsMap) {
                string docId = pair.Key;
                JObject docObject = pair.Value;  // Can be null (null means deletion)
                await ReplaceOrDeleteDocumentAsync(docId, docObject);
            }
        }

        


        internal JObject RetrieveDocumentById(string docId)
        {
            Debug.Assert(!string.IsNullOrEmpty(docId), "'docId' should not be null or empty");

            string script = $"SELECT * FROM Doc WHERE Doc.id = '{docId}'";
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };  // dynamic paging
            List<dynamic> result = this.DocDBClient.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(this.DocDBDatabaseId, this.DocDBCollectionId),
                script,
                queryOptions
            ).ToList();

            Debug.Assert(result.Count <= 1, $"BUG: Found multiple documents sharing the same docId: {docId}");
            return (result.Count == 0) ? null : (JObject)result[0];
        }


        internal List<dynamic> ExecuteQuery(string queryScript, FeedOptions queryOptions = null)
        {
            if (queryOptions == null) {
                queryOptions = new FeedOptions { MaxItemCount = -1 };
            }

            return this.DocDBClient.CreateDocumentQuery(
                this._docDBCollectionUri,
                queryScript,
                queryOptions).ToList();
        }

        internal JObject ExecuteQueryUnique(string queryScript, FeedOptions queryOptions = null)
        {
            List<dynamic> result = ExecuteQuery(queryScript, queryOptions);

            Debug.Assert(result.Count <= 1, "A unique query should have at most 1 result");
            return (result.Count == 0)
                       ? null
                       : result[0];

        }



        //public void BulkInsertNodes(List<string> nodes)
        //{
        //    if (!nodes.Any()) return;

        //    string collectionLink = "dbs/" + DocDBDatabaseId + "/colls/" + DocDBCollectionId;

        //    // Each batch size is determined by maxJsonSize.
        //    // maxJsonSize should be so that:
        //    // -- it fits into one request (MAX request size is ???).
        //    // -- it doesn't cause the script to time out, so the batch number can be minimzed.
        //    const int maxJsonSize = 50000;

        //    // Prepare the BulkInsert stored procedure
        //    string jsBody = File.ReadAllText(@"..\..\BulkInsert.js");
        //    StoredProcedure sproc = new StoredProcedure
        //    {
        //        Id = "BulkInsert",
        //        Body = jsBody,
        //    };

        //    var bulkInsertCommand = new GraphViewCommand(this);
        //    //Create the BulkInsert stored procedure if it doesn't exist
        //    Task<StoredProcedure> spTask = bulkInsertCommand.TryCreatedStoredProcedureAsync(collectionLink, sproc);
        //    spTask.Wait();
        //    sproc = spTask.Result;
        //    var sprocLink = sproc.SelfLink;

        //    // If you are sure that the proc already exist on the server side, 
        //    // you can comment out the TryCreatedStoredProcude code above and use the URI directly instead
        //    //var sprocLink = "dbs/" + DocDBDatabaseId + "/colls/" + DocDBCollectionId + "/sprocs/" + sproc.Id;

        //    int currentCount = 0;
        //    while (currentCount < nodes.Count)
        //    {
        //        // Get the batch json string whose size won't exceed the maxJsonSize
        //        string json_arr = GraphViewCommand.GenerateNodesJsonString(nodes, currentCount, maxJsonSize);
        //        var objs = new dynamic[] { JsonConvert.DeserializeObject<dynamic>(json_arr) };

        //        // Execute the batch
        //        Task<int> insertTask = bulkInsertCommand.BulkInsertAsync(sprocLink, objs);
        //        insertTask.Wait();

        //        // Prepare for next batch
        //        currentCount += insertTask.Result;
        //        Console.WriteLine(insertTask.Result + " nodes has already been inserted.");
        //    }
        //}
    }


    internal sealed class VertexObjectCache
    {
        public GraphViewConnection Connection { get; }

        /// <summary>
        /// NOTE: VertexCache is per-connection! (cross-connection may lead to unpredictable errors)
        /// </summary>
        /// <param name="dbConnection"></param>
        public VertexObjectCache(GraphViewConnection dbConnection)
        {
            this.Connection = dbConnection;
        }

        //
        // NOTE: _cachedVertex is ALWAYS up-to-date with DocDB!
        // Every query operation could be directly done with the cache
        // Every vertex/edge modification MUST be synchonized with the cache
        //
        private readonly ConcurrentDictionary<string, VertexField> _cachedVertexField = new ConcurrentDictionary<string, VertexField>();


        /// <summary>
        /// Everytime when retrieving vertexes from DocDB, we attempt to add the vertexes into cache,
        /// although they might already be in the cache. (for debugging perpose, we check to make sure
        /// the newly retrieved vertex is just identical to the cached one)
        /// </summary>
        /// <param name="vertexObject"></param>
        /// <returns></returns>
        public VertexField TryAddVertexField(JObject vertexObject)
        {
            string vertexId = (string)vertexObject["id"];
            Debug.Assert(!string.IsNullOrEmpty(vertexId), "Vertex Id should exist (not null nor empty)");

            // Make sure this is a vertex-document
            // TODO: Fix this in DocDB query (Don't obtain non-vertex-documents)
            if (vertexObject["_edge"] == null || vertexObject["_reverse_edge"] == null) {
                Debug.WriteLine($"[AddVertexField] Try to add a non-vertex object! id = {vertexId}");
                return null;
            }

            // Try to get or add the VertexField
            bool[] isAdd = {false};
            VertexField result = this._cachedVertexField.GetOrAdd(vertexId, (dummy) => {
                isAdd[0] = true;
                return FieldObject.ConstructVertexField(this.Connection, vertexObject);
            });
#if DEBUG
            if (!isAdd[0]) {
                //TODO: Check the retrieved vertexfield is identical to the added one
            }
#endif

            return result;
        }

        public VertexField GetVertexField(string vertexId, JObject vertexObject = null)
        {
            return this._cachedVertexField.GetOrAdd(vertexId, dummy => {
                vertexObject = vertexObject ?? this.Connection.RetrieveDocumentById(vertexId);
                return FieldObject.ConstructVertexField(this.Connection, vertexObject);
            });
        }

        public bool TryRemoveVertexField(string vertexId)
        {
            VertexField vertexField;
            bool found = this._cachedVertexField.TryRemove(vertexId, out vertexField);
            if (found) {
                Debug.Assert(vertexField.AdjacencyList.Edges.Count == 0, "The deleted edge's should contain no outgoing edges");
                Debug.Assert(vertexField.RevAdjacencyList.Edges.Count == 0, "The deleted edge's should contain no incoming edges");
            }
            return found;
        }
    }


    //internal sealed class VertexObjectCache
    //{
    //    public GraphViewConnection Connection { get; }

    //    /// <summary>
    //    /// NOTE: VertexCache is per-connection! (cross-connection may lead to unpredictable errors)
    //    /// </summary>
    //    /// <param name="dbConnection"></param>
    //    public VertexObjectCache(GraphViewConnection dbConnection)
    //    {
    //        this.Connection = dbConnection;
    //    }

    //    //
    //    // Can we use ConcurrentDictionary<string, VertexField> here?
    //    // Yes, I reckon...
    //    //
    //    private readonly Dictionary<string, VertexField> _cachedVertexCollection = new Dictionary<string, VertexField>();
    //    private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

    //    public VertexField GetVertexField(string vertexId, string vertexJson)
    //    {
    //        try {
    //            this._lock.EnterUpgradeableReadLock();

    //            // Try to retrieve vertexObject from the cache
    //            VertexField vertexField;
    //            if (this._cachedVertexCollection.TryGetValue(vertexId, out vertexField)) {
    //                return vertexField;
    //            }

    //            // Cache miss: parse vertexJson, and add the result to cache
    //            try {
    //                this._lock.EnterWriteLock();

    //                JObject vertexObject = JObject.Parse(vertexJson);
    //                vertexField = FieldObject.ConstructVertexField(this.Connection, vertexObject);
    //                this._cachedVertexCollection.Add(vertexId, vertexField);
    //            }
    //            finally {
    //                if (this._lock.IsWriteLockHeld) {
    //                    this._lock.ExitWriteLock();
    //                }
    //            }
    //            return vertexField;
    //        }
    //        finally {
    //            if (this._lock.IsUpgradeableReadLockHeld) {
    //                this._lock.ExitUpgradeableReadLock();
    //            }
    //        }
    //    }
    //}
}