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

        public bool UseReverseEdges { get; set; }

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
            string docDBCollectionID,
            CollectionType collectionType = CollectionType.UNDEFINED,
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
            // Check whether it is a partition collection (if exists)
            //
            DocumentCollection docDBCollection;
            try {
                docDBCollection = this.DocDBClient.CreateDocumentCollectionQuery(
                                                             UriFactory.CreateDatabaseUri(this.DocDBDatabaseId))
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

            bool? isPartitionedNow = (docDBCollection?.PartitionKey.Paths.Count > 0);
            if (isPartitionedNow.HasValue && isPartitionedNow.Value) {
                if (collectionType == CollectionType.STANDARD) {
                    throw new Exception("Can't specify CollectionType.STANDARD on an existing partitioned collection");
                }
            }
            else if (isPartitionedNow.HasValue && !isPartitionedNow.Value) {
                if (collectionType == CollectionType.PARTITIONED) {
                    throw new Exception("Can't specify CollectionType.PARTITIONED on an existing standard collection");
                }
            }
            this.CollectionType = collectionType;

            if (collectionType == CollectionType.UNDEFINED) {
                if (docDBCollection == null) {
                    // Do nothing
                }
                else if (docDBCollection.PartitionKey != null && docDBCollection.PartitionKey.Paths.Count < 1) {
                    this.CollectionType = CollectionType.STANDARD;
                }
                else if (docDBCollection.PartitionKey != null
                         && docDBCollection.PartitionKey.Paths.Count > 0
                         && docDBCollection.PartitionKey.Paths[0].Equals($"/{KW_DOC_PARTITION}", StringComparison.OrdinalIgnoreCase)) {
                    this.CollectionType = CollectionType.PARTITIONED;
                }
                else {
                    throw new Exception(string.Format("Collection not properly configured. If you wish to configure a partitioned collection, please chose /{0} as partitionKey", KW_DOC_PARTITION));
                }
            }

            this.Identifier = $"{docDBEndpointUrl}\0{docDBDatabaseID}\0{docDBCollectionID}";
            this.VertexCache = VertexObjectCache.FromConnection(this);

            this.UseReverseEdges = true;
            

            // Retrieve metadata from DocDB
            JObject metaObject = RetrieveDocumentById("metadata");
            if (metaObject != null) {
                Debug.Assert((int?)metaObject["_edgeSpillThreshold"] != null);
                this.EdgeSpillThreshold = (int)metaObject["_edgeSpillThreshold"];
            }
            Debug.Assert(this.EdgeSpillThreshold >= 0, "The edge-spill threshold should >= 0");
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
        public void ResetCollection(CollectionType collectionType = CollectionType.STANDARD, int? edgeSpillThreshold = null)
        {
            EnsureDatabaseExist();

            DocumentCollection docDBCollection = this.DocDBClient.CreateDocumentCollectionQuery(this._docDBDatabaseUri)
                                                     .Where(c => c.Id == this.DocDBCollectionId)
                                                     .AsEnumerable()
                                                     .FirstOrDefault();

            // Delete the collection if it exists
            if (docDBCollection != null) {
                DeleteCollection();
            }

            switch (collectionType) {
            case CollectionType.STANDARD:
            case CollectionType.PARTITIONED:
                break;
            case CollectionType.UNDEFINED:
                if (this.CollectionType == CollectionType.UNDEFINED) {
                    throw new Exception("Can't specify CollectionType.UNDEFINED to reset a non-existing collection");
                }
                collectionType = this.CollectionType;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(collectionType), collectionType, null);
            }

            Debug.Assert(collectionType != CollectionType.UNDEFINED);
            CreateCollection(collectionType == CollectionType.PARTITIONED);
            this.CollectionType = collectionType;

            //
            // Create a meta-data document!
            // Here we just store the "edgeSpillThreshold" in it
            //
            JValue jEdgeSpillThreshold;
            if (edgeSpillThreshold == null || edgeSpillThreshold <= 0) {
                jEdgeSpillThreshold = (JValue)0;
                this.EdgeSpillThreshold = 0;
            }
            else {  // edgeSpillThreshold > 0
                jEdgeSpillThreshold = (JValue)edgeSpillThreshold;
                this.EdgeSpillThreshold = (int)edgeSpillThreshold;
            }
            JObject metaObject = new JObject {
                [KW_DOC_ID] = "metadata",
                [KW_DOC_PARTITION] = "metapartition",
                ["_edgeSpillThreshold"] = jEdgeSpillThreshold
            };
            CreateDocumentAsync(metaObject).Wait();

            Trace.WriteLine($"[ResetCollection] Database/Collection {this.DocDBDatabaseId}/{this.DocDBCollectionId} has been reset.");
        }



        private void CreateCollection(bool isPartitionCollection)
        {
            // TODO: Make the OfferType configurable?

            DocumentCollection collection = new DocumentCollection {
                Id = this.DocDBCollectionId,
            };
            if (isPartitionCollection) {
                collection.PartitionKey.Paths.Add($"/{KW_DOC_PARTITION}");
            }

            this.DocDBClient.CreateDocumentCollectionAsync(
                this._docDBDatabaseUri,
                collection,
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
            Debug.Assert(docObject[KW_DOC_ID] != null, $"The newly created document should specify '{KW_DOC_ID}' field");
            Debug.Assert(docObject[KW_DOC_PARTITION] != null, $"The newly created document should specify '{KW_DOC_PARTITION}' field");

            Document createdDocument = await this.DocDBClient.CreateDocumentAsync(this._docDBCollectionUri, docObject);
            Debug.Assert((string)docObject[KW_DOC_ID] == createdDocument.Id);
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
                Debug.Assert(docObject[KW_DOC_PARTITION] != null, $"The newly created document should contain '{KW_DOC_PARTITION}' field");

                Document createdDoc = await this.DocDBClient.CreateDocumentAsync(this._docDBCollectionUri, docObject);
                Debug.Assert((string)docObject[KW_DOC_ID] == createdDoc.Id);
            }
        }


        internal async Task ReplaceOrDeleteDocumentAsync(string docId, JObject docObject, string partition)
        {
            RequestOptions option = null;
            if (this.CollectionType == CollectionType.PARTITIONED) {
                option = new RequestOptions {
                    PartitionKey = new PartitionKey(partition)
                };
            }

            Uri documentUri = UriFactory.CreateDocumentUri(this.DocDBDatabaseId, this.DocDBCollectionId, docId);
            if (docObject == null) {
                this.DocDBClient.DeleteDocumentAsync(documentUri, option).Wait();
            }
            else {
                Debug.Assert(docObject[KW_DOC_ID] is JValue);
                Debug.Assert((string)docObject[KW_DOC_ID] == docId, "The replaced document should match ID in the parameter");
                Debug.Assert(partition != null && partition == (string)docObject[KW_DOC_PARTITION]);

                await this.DocDBClient.ReplaceDocumentAsync(documentUri, docObject, option);
                docObject[KW_DOC_ID] = docId;
            }
        }

        internal async Task ReplaceOrDeleteDocumentsAsync(Dictionary<string, Tuple<JObject, string>> documentsMap)
        {
#if DEBUG
            // Make sure that there aren't two docObject (not null) sharing the same reference
            List<Tuple<JObject, string>> docObjectList = documentsMap.Values.Where(docObject => docObject != null).ToList();
            HashSet<Tuple<JObject, string>> docObjectSet = new HashSet<Tuple<JObject, string>>(docObjectList);
            Debug.Assert(docObjectList.Count == docObjectSet.Count, "Replacing documents with two docObject sharing the same reference");
#endif
            foreach (KeyValuePair<string, Tuple<JObject, string>> pair in documentsMap) {
                string docId = pair.Key;
                JObject docObject = pair.Value.Item1;  // Can be null (null means deletion)
                string partition = pair.Value.Item2;  // Partition
                if (docObject != null) {
                    Debug.Assert(partition == (string)docObject[KW_DOC_PARTITION]);
                }
                await ReplaceOrDeleteDocumentAsync(docId, docObject, partition);
            }
        }




        internal JObject RetrieveDocumentById(string docId)
        {
            Debug.Assert(!string.IsNullOrEmpty(docId), "'docId' should not be null or empty");

            try {
                //
                // It seems that DocDB won't return an empty result, but throw an exception instead!
                // 
                string script = $"SELECT * FROM Doc WHERE Doc.{KW_DOC_ID} = '{docId}'";
                FeedOptions queryOptions = new FeedOptions {
                    MaxItemCount = -1,  // dynamic paging
                };
                if (this.CollectionType == CollectionType.PARTITIONED) {
                    queryOptions.EnableCrossPartitionQuery = true;
                }

                List<dynamic> result = this.DocDBClient.CreateDocumentQuery(
                    UriFactory.CreateDocumentCollectionUri(this.DocDBDatabaseId, this.DocDBCollectionId),
                    script,
                    queryOptions
                ).ToList();

                Debug.Assert(result.Count <= 1, $"BUG: Found multiple documents sharing the same docId: {docId}");
                return (result.Count == 0) ? null : (JObject)result[0];
            }
            catch (AggregateException aggex)
                when ((aggex.InnerException as DocumentClientException)?.Error.Code == "NotFound") {  // HACK
                return null;
            }
        }


        internal IQueryable<dynamic> ExecuteQuery(string queryScript, FeedOptions queryOptions = null)
        {
            if (queryOptions == null) {
                queryOptions = new FeedOptions {
                    MaxItemCount = -1,
                    EnableScanInQuery = true,
                };
                if (this.CollectionType == CollectionType.PARTITIONED) {
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
            List<dynamic> result = ExecuteQuery(queryScript, queryOptions).ToList();

            Debug.Assert(result.Count <= 1, "A unique query should have at most 1 result");
            return (result.Count == 0)
                       ? null
                       : result[0];

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
