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
        public int EdgeSpillThreshold { get; private set; } = 0;


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


        public string PartitionByKeyIfViaGraphAPI { get; }  // Like "location"

        public string PartitionPath { get; private set; }  // Like "/location/nested/_value"

        public string PartitionPathTopLevel { get; private set; }  // Like "/location"



        public static GraphViewConnection ResetGraphAPICollection(
            string endpoint,
            string authKey,
            string databaseId,
            string collectionId,
            bool useReverseEdge,
            string partitionByKey = null)
        {
            using (DocumentClient client = new DocumentClient(
                new Uri(endpoint), authKey, new ConnectionPolicy {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp,
                }
            )) {
                if (string.IsNullOrEmpty(partitionByKey)) {
                    ResetCollection(client, databaseId, collectionId, null);
                }
                else {
                    ResetCollection(client, databaseId, collectionId, $"/{KW_DOC_PARTITION}");
                }
            }

            return new GraphViewConnection(endpoint, authKey, databaseId, collectionId, GraphType.GraphAPIOnly, useReverseEdge, 1, partitionByKey);
        }

        public static GraphViewConnection ResetFlatCollection(
            string endpoint,
            string authKey,
            string databaseId,
            string collectionId,
            string partitionByKey = null)
        {
            using (DocumentClient client = new DocumentClient(
                new Uri(endpoint), authKey, new ConnectionPolicy {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp,
                }
            )) {
                if (string.IsNullOrEmpty(partitionByKey)) {
                    ResetCollection(client, databaseId, collectionId, null);
                }
                else {
                    ResetCollection(client, databaseId, collectionId, $"/{partitionByKey}");
                }
            }

            return new GraphViewConnection(endpoint, authKey, databaseId, collectionId, GraphType.CompatibleOnly, false, 1);
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
            bool useReverseEdges = false,
            int? edgeSpillThreshold = null,
            string partitionByKeyIfViaGraphAPI = null,
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

            ConnectionPolicy connectionPolicy = new ConnectionPolicy {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
            };
            if (!string.IsNullOrEmpty(preferredLocation)) {
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
            try {
                docDBCollection = this.DocDBClient.CreateDocumentCollectionQuery(this._docDBDatabaseUri)
                    .Where(c => c.Id == this.DocDBCollectionId)
                    .AsEnumerable()
                    .FirstOrDefault();
            }
            catch (AggregateException aggex) 
            when ((aggex.InnerException as DocumentClientException)?.Error.Code == "NotFound") {
                // Now the database does not exist!
                // NOTE: If the database exists, but the collection does not exist, it won't be an exception
                docDBCollection = null;
            }

            if (docDBCollection == null) {
                throw new GraphViewException("This collection does not exist.");
            }

            //
            // If the collection has existed, try to retrive its partition key path
            // If not partitioned, set `PartitionPath` to null
            //
            Collection<string> partitionKey = docDBCollection.PartitionKey.Paths;
            if (partitionKey.Count == 1) {
                this.CollectionType = CollectionType.PARTITIONED;

                this.PartitionPath = partitionKey[0];
                this.PartitionPathTopLevel = this.PartitionPath.Split(new[] {'/'}, StringSplitOptions.RemoveEmptyEntries)[0];

                if (this.PartitionPath == $"/{KW_DOC_PARTITION}") {  // Partitioned, created via GraphAPI
                    this.PartitionByKeyIfViaGraphAPI = partitionByKeyIfViaGraphAPI;
                    Debug.Assert(partitionByKeyIfViaGraphAPI != null);
                    Debug.Assert(graphType == GraphType.GraphAPIOnly);
                }
                else {
                    Debug.Assert(partitionByKeyIfViaGraphAPI == null);
                }
            }
            else {
                this.CollectionType = CollectionType.STANDARD;

                Debug.Assert(partitionKey.Count == 0);
                this.PartitionPath = null;
                this.PartitionPathTopLevel = null;

                Debug.Assert(partitionByKeyIfViaGraphAPI == null);
            }

            if (graphType != GraphType.GraphAPIOnly) {
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
            if (docDBDatabase == null) {
                client.CreateDatabaseAsync(new Database {Id = databaseId}).Wait();
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
        private static void ResetCollection(
            DocumentClient client, string databaseId, string collectionId,
            string partitionPath = null)
        {
            EnsureDatabaseExist(client, databaseId);

            DocumentCollection docDBCollection = client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseId))
                                                     .Where(c => c.Id == collectionId)
                                                     .AsEnumerable()
                                                     .FirstOrDefault();

            // Delete the collection if it exists
            if (docDBCollection != null) {
                DeleteCollection(client, databaseId, collectionId);
            }

            CreateCollection(client, databaseId, collectionId, partitionPath);
            
            Trace.WriteLine($"[ResetCollection] Database/Collection {databaseId}/{collectionId} has been reset.");
        }



        private static void CreateCollection(DocumentClient client, string databaseId, string collectionId, string partitionPath = null)
        {
            DocumentCollection collection = new DocumentCollection {
                Id = collectionId,
            };
            if (!string.IsNullOrEmpty(partitionPath)) {
                collection.PartitionKey.Paths.Add(partitionPath);
            }

            client.CreateDocumentCollectionAsync(
                UriFactory.CreateDatabaseUri(databaseId),
                collection,
                new RequestOptions {
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
            foreach (JObject docObject in docObjects) {
                Debug.Assert(docObject != null, "The newly created document should not be null");
                Debug.Assert(docObject[KW_DOC_ID] != null, $"The newly created document should contain '{KW_DOC_ID}' field");
                //Debug.Assert(docObject[KW_DOC_PARTITION] != null, $"The newly created document should contain '{KW_DOC_PARTITION}' field");

                Document createdDoc = await this.DocDBClient.CreateDocumentAsync(this._docDBCollectionUri, docObject);
                Debug.Assert((string)docObject[KW_DOC_ID] == createdDoc.Id);
            }
        }


        internal async Task ReplaceOrDeleteDocumentAsync(string docId, JObject docObject, string partition)
        {
            if (docObject != null) {
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
            if (docObject == null) {
                this.DocDBClient.DeleteDocumentAsync(documentUri, option).Wait();

                // Remove the document's etag from saved
                this.VertexCache.RemoveEtag(docId);
            }
            else {
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
            foreach (KeyValuePair<string, Tuple<JObject, string>> pair in documentsMap) {
                string docId = pair.Key;
                JObject docObject = pair.Value.Item1;  // Can be null (null means deletion)
                string partition = pair.Value.Item2;  // Partition
                if (docObject != null) {
                    Debug.Assert(partition == this.GetDocumentPartition(docObject));
                }
                await this.ReplaceOrDeleteDocumentAsync(docId, docObject, partition);
            }
        }




        internal JObject RetrieveDocumentById(string docId)
        {
            Debug.Assert(!string.IsNullOrEmpty(docId), "'docId' should not be null or empty");

            string script = $"SELECT * FROM Doc WHERE Doc.{KW_DOC_ID} = '{docId}'";
            JObject result = ExecuteQueryUnique(script);

            //
            // Save etag of the fetched document
            // No override!
            //
            if (result != null) {
                this.VertexCache.SaveCurrentEtagNoOverride(result);
            }

            return result;
        }


        public string GetDocumentPartition(JObject document)
        {
            if (this.PartitionPath == null) {
                return null;
            }

            JToken token = document;
            string[] paths = this.PartitionPath.Split(new []{'/'}, StringSplitOptions.RemoveEmptyEntries);
            foreach (string part in paths) {
                if (part.StartsWith("[")) {  // Like /[0]
                    Debug.Assert(part.EndsWith("]"));
                    Debug.Assert((token is JArray));
                    token = ((JArray)token)[int.Parse(part.Substring(1, part.Length - 2))];
                }
                else if (part.StartsWith("\"")) {  // Like /"property with space"
                    Debug.Assert(part.EndsWith("\""));
                    token = token[part.Substring(1, part.Length - 2)];
                }
                else {   // Like /normal_property
                    token = token[part];
                }
            }

            Debug.Assert(token is JValue);
            Debug.Assert(((JValue)token).Type == JTokenType.String);
            return (string)token;
        }


        internal IQueryable<dynamic> ExecuteQuery(string queryScript, FeedOptions queryOptions = null)
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
        private static long __currId = 1000;
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
