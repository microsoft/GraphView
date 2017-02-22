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

        internal string Identifier { get; }


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

            this.Identifier = $"{docDBEndpointUrl}\0{docDBDatabaseID}\0{docDBCollectionID}";
            this.VertexCache = VertexObjectCache.FromConnection(this);
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

            DocumentCollection docDBCollection = this.DocDBClient.CreateDocumentCollectionQuery(this._docDBDatabaseUri)
                                                     .Where(c => c.Id == this.DocDBCollectionId)
                                                     .AsEnumerable()
                                                     .FirstOrDefault();

            // Delete the collection if it exists
            if (docDBCollection != null) {
                DeleteCollection();
            }
            CreateCollection();

            // Upload the stored procedures
            //UpsertStoredProcedure(STORED_PROCEDURE_ADDE, GraphView.Properties.Resources.sproc_AddE);
            //UpsertStoredProcedure(STORED_PROCEDURE_ADDV, GraphView.Properties.Resources.sproc_AddV);

            Trace.WriteLine($"[ResetCollection] Database/Collection {this.DocDBDatabaseId}/{this.DocDBCollectionId} has been reset.");
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
            Debug.Assert(docObject["id"] != null, "The newly created document should specify 'id' field");

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


        internal IQueryable<dynamic> ExecuteQuery(string queryScript, FeedOptions queryOptions = null)
        {
            if (queryOptions == null) {
                queryOptions = new FeedOptions { MaxItemCount = -1 };
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

        
        internal static string GenerateDocumentId()
        {
            // TODO: Implement a stronger Id generation
            Guid guid = Guid.NewGuid();
            return guid.ToString("D");
        }
    }

    public partial class GraphViewConnection
    {
        private const string STORED_PROCEDURE_ADDE = "sproc_AddE";
        private const string STORED_PROCEDURE_ADDV = "sproc_AddV";
        
        private void UpsertStoredProcedure(string name, string body)
        {
            StoredProcedure sp = new StoredProcedure {
                Id = name,
                Body = body,
            };
            ResourceResponse<StoredProcedure> resp = this.DocDBClient.CreateStoredProcedureAsync(this._docDBCollectionUri, sp).Result;
            if (resp.StatusCode != HttpStatusCode.Created) {
                Trace.WriteLine($"[UpsertStoredProcedure({name})] Error: " +
                                $"Response status code = {resp.StatusCode}({(int)resp.StatusCode})");
            }
            else {
                Trace.WriteLine($"[UpsertStoredProcedure] Upserted: {name}");
            }
        }

        private DocDBStoredProcedureResult ExecuteStoredProcedure(string name, params dynamic[] parameters)
        {
            try {
                StoredProcedureResponse<string> resp = this.DocDBClient.ExecuteStoredProcedureAsync<string>(
                    UriFactory.CreateStoredProcedureUri(this.DocDBDatabaseId, this.DocDBCollectionId, name),
                    parameters).Result;
                return JsonConvert.DeserializeObject<DocDBStoredProcedureResult>(resp.Response);
            }
            catch (AggregateException aggex) when (aggex.InnerExceptions.Count == 1) {
                DocumentClientException ex = aggex.InnerException as DocumentClientException;
                string exMsg = ex.Message;

                //
                // exMsg looks like:
                //   Message: {"Errors":["Encountered exception while executing function. Exception = Error: {\"Status\":-1,\"Message\":\"Unknown error\",\"DocDBError\":{\"message\":\"{\\\"Errors\\\":[\\\"Resource with specified id or name already exists\\\"]}\",\"number\":409},\"Content\":null}\r\nStack trace: Error: {\"Status\":-1,\"Message\":\"Unknown error\",\"DocDBError\":{\"message\":\"{\\\"Errors\\\":[\\\"Resource with specified id or name already exists\\\"]}\",\"number\":409},\"Content\":null}\n   at ERROR (sproc_AddV.js:77:9)\n   at Anonymous function (sproc_AddV.js:145:21)\n   at Anonymous"]}
                //   ActivityId: ff47cb4c-90e7-4387-bc1a-551ed29d0747, Request URI: rntbd://10.172.142.46:10253/apps/DocDbApp/services/DocDbServer5/partitions/a4cb4951-38c8-11e6-8106-8cdcd42c33be/replicas/1p/
                // TODO: A HACK!
                //
                string line = exMsg.Split('\n')[0].Trim();
                if (line.StartsWith("Message:")) {
                    line = "{\"Message\":" + line.Substring("Message:".Length) + "}";
                }
                JObject root = JObject.Parse(line);
                string errMsg = (string)((JArray)root["Message"]["Errors"])[0];
                string errMsgLine = errMsg.Split('\n')[0].Trim();
                string resultJson = errMsgLine.Substring(
                    errMsgLine.IndexOf("Exception = Error:", StringComparison.OrdinalIgnoreCase) + "Exception = Error:".Length);
                return JsonConvert.DeserializeObject<DocDBStoredProcedureResult>(resultJson);
            }
        }

        internal DocDBStoredProcedureResult SP_AddV(JObject vertexObject)
        {
            Debug.Assert((string)vertexObject["id"] != null, "The vertexObject should have 'id' field");
            Debug.Assert((string)vertexObject["_partition"] != null, "The vertexObject should have '_partition' field");
            Debug.Assert((string)vertexObject["id"] == (string)vertexObject["_partition"], "The vertexObject's id != _partition");

            Debug.Assert((long)vertexObject["_nextEdgeOffset"] == 0, "The vertexObject's _nextEdgeOffset should be 0");

            DocDBStoredProcedureResult result = ExecuteStoredProcedure(STORED_PROCEDURE_ADDV, vertexObject);
            Debug.WriteLine($"[SP_AddV({vertexObject["id"]})] Response: {JsonConvert.SerializeObject(result)}");
            return result;
        }


        /// <summary>
        /// TODO: This is not done!
        /// TODO: MUST NOT use this function to insert edges
        /// </summary>
        /// <param name="srcVertexId"></param>
        /// <param name="sinkVertexId"></param>
        /// <param name="edgeObject"></param>
        /// <param name="isReverse"></param>
        /// <returns></returns>
        internal DocDBStoredProcedureResult SP_AddE(string srcVertexId, string sinkVertexId, JObject edgeObject, bool isReverse)
        {
            Debug.Assert(edgeObject["_offset"] == null, "The edgeObject should NOT specify '_offset' field");
            if (isReverse) {
                Debug.Assert((string)edgeObject["_srcV"] == srcVertexId);
            }
            else {
                Debug.Assert((string)edgeObject["_sinkV"] == sinkVertexId);
            }

            DocDBStoredProcedureResult result = ExecuteStoredProcedure(STORED_PROCEDURE_ADDE, srcVertexId, sinkVertexId, edgeObject, isReverse);
            Debug.WriteLine($"[SP_AddE({srcVertexId}->{sinkVertexId})] Response: {JsonConvert.SerializeObject(result)}");
            return result;
        }

    }


    internal enum DocDBStoredProcedureStatus
    {
        Success = 0,

        DBError = -1,
        NotAccepted = -2,
        AssertionFailed = -3,
        InternalError = -4,
    }

    internal class DocDBStoredProcedureResult
    {
        internal class DocDBErrorObject
        {
            [JsonProperty("number", Required = Required.Always)]
            [JsonConverter(typeof(StringEnumConverter))]
            public HttpStatusCode ErrorCode { get; private set; }

            //[JsonProperty("body", Required = Required.Always)]
            [JsonProperty("message", Required = Required.Always)]
            public string MessageBody { get; private set; }
        }

        [JsonProperty("Status", Required = Required.Always)]
        [JsonConverter(typeof(StringEnumConverter))]
        public DocDBStoredProcedureStatus Status { get; private set; }

        [JsonProperty("Message", Required = Required.Always)]
        public string Message { get; private set; }

        [JsonProperty("DocDBError", Required = Required.AllowNull)]
        public DocDBErrorObject DocDBError { get; private set; }

        [JsonProperty("Content", Required = Required.AllowNull)]
        public JObject Content { get; private set; }
    }
}