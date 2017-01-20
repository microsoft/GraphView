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

using System;
using System.Collections.Generic;
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
    ///     Connector to a graph database. The class inherits most functions of SqlConnection,
    ///     and provides a number of GraphView-specific functions.
    /// </summary>
    public partial class GraphViewConnection : IDisposable
    {
        private bool _disposed;
        public DocumentCollection DocDB_Collection;
        public string DocDB_CollectionId;
        public Database DocDB_Database;
        public string DocDB_DatabaseId;
        public bool DocDB_finish;
        public string DocDB_PrimaryKey;

        public string DocDB_Url;
        public DocumentClient DocDBclient;

        internal VertexObjectCache VertexCache { get; private set; }

        /// <summary>
        ///     Initializes a new connection to DocDB.
        ///     Contains four string,
        ///     Url , Key , Database's name , Collection's name
        /// </summary>
        /// <param name="docdb_EndpointUrl">The Url</param>
        /// <param name="docdb_AuthorizationKey">The Key</param>
        /// <param name="docdb_DatabaseID">Database's name</param>
        /// <param name="docdb_CollectionID">Collection's name</param>
        public GraphViewConnection(string docdb_EndpointUrl, string docdb_AuthorizationKey, string docdb_DatabaseID,
            string docdb_CollectionID)
        {
            DocDB_Url = docdb_EndpointUrl;
            DocDB_PrimaryKey = docdb_AuthorizationKey;
            DocDB_DatabaseId = docdb_DatabaseID;
            DocDB_CollectionId = docdb_CollectionID;
            DocDBclient = new DocumentClient(new Uri(DocDB_Url), DocDB_PrimaryKey, 
                new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp,
                });

            DocDBclient.OpenAsync();

            VertexCache = VertexObjectCache.Instance;
        }

        public GraphViewConnection()
        { }

        internal DbPortal CreateDatabasePortal()
        {
            return new DocumentDbPortal(this);
        }

        /// <summary>
        ///     Connection to a SQL database
        /// </summary>
        public bool Overwrite { get; set; }
        
        /// <summary>
        ///     Releases all resources used by GraphViewConnection.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public void ResetCollection()
        {
            DocDB_Database =
                DocDBclient.CreateDatabaseQuery().Where(db => db.Id == DocDB_DatabaseId).AsEnumerable().FirstOrDefault();
            
            // If the database does not exist, create one
            if (DocDB_Database == null)
                CreateDatabaseAsync().Wait();

            DocDB_Collection =
                DocDBclient.CreateDocumentCollectionQuery("dbs/" + DocDB_Database.Id)
                    .Where(c => c.Id == DocDB_CollectionId)
                    .AsEnumerable()
                    .FirstOrDefault();

            // Delete the collection if it exists
            if (DocDB_Collection != null)
                DeleteCollectionAsync().Wait();

            CreateCollectionAsync().Wait();

            Console.Write("Collection " + DocDB_CollectionId + " has been reset.");
        }

        private async Task CreateDatabaseAsync()
        {
            DocDB_Database = await DocDBclient.CreateDatabaseAsync(new Database { Id = DocDB_DatabaseId })
                                    .ConfigureAwait(continueOnCapturedContext: false);
        }

        private async Task CreateCollectionAsync()
        {
            DocDB_Collection = await DocDBclient.CreateDocumentCollectionAsync("dbs/" + DocDB_Database.Id,
                                        new DocumentCollection {Id = DocDB_CollectionId},
                                        new RequestOptions {OfferType = "S3"})
                                            .ConfigureAwait(continueOnCapturedContext: false);
        }

        public async Task DeleteCollectionAsync()
        {
            await
                DocDBclient.DeleteDocumentCollectionAsync(DocDB_Collection.SelfLink)
                    .ConfigureAwait(continueOnCapturedContext: false);
        }

        public void BulkInsertNodes(List<string> nodes)
        {
            if (!nodes.Any()) return;

            string collectionLink = "dbs/" + DocDB_DatabaseId + "/colls/" + DocDB_CollectionId;

            // Each batch size is determined by maxJsonSize.
            // maxJsonSize should be so that:
            // -- it fits into one request (MAX request size is ???).
            // -- it doesn't cause the script to time out, so the batch number can be minimzed.
            const int maxJsonSize = 50000;

            // Prepare the BulkInsert stored procedure
            string jsBody = File.ReadAllText(@"..\..\BulkInsert.js");
            StoredProcedure sproc = new StoredProcedure
            {
                Id = "BulkInsert",
                Body = jsBody,
            };

            var bulkInsertCommand = new GraphViewCommand(this);
            //Create the BulkInsert stored procedure if it doesn't exist
            Task<StoredProcedure> spTask = bulkInsertCommand.TryCreatedStoredProcedureAsync(collectionLink, sproc);
            spTask.Wait();
            sproc = spTask.Result;
            var sprocLink = sproc.SelfLink;

            // If you are sure that the proc already exist on the server side, 
            // you can comment out the TryCreatedStoredProcude code above and use the URI directly instead
            //var sprocLink = "dbs/" + DocDB_DatabaseId + "/colls/" + DocDB_CollectionId + "/sprocs/" + sproc.Id;

            int currentCount = 0;
            while (currentCount < nodes.Count)
            {
                // Get the batch json string whose size won't exceed the maxJsonSize
                string json_arr = GraphViewCommand.GenerateNodesJsonString(nodes, currentCount, maxJsonSize);
                var objs = new dynamic[] { JsonConvert.DeserializeObject<dynamic>(json_arr) };

                // Execute the batch
                Task<int> insertTask = bulkInsertCommand.BulkInsertAsync(sprocLink, objs);
                insertTask.Wait();

                // Prepare for next batch
                currentCount += insertTask.Result;
                Console.WriteLine(insertTask.Result + " nodes has already been inserted.");
            }
        }

        
        /// <summary>
        ///     Releases the unmanaged resources used by GraphViewConnection and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing">
        ///     true to release both managed and unmanaged resources; false to release only unmanaged
        ///     resources.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {

                }
            }
            _disposed = true;
        }
        
    }

    internal sealed class VertexObjectCache
    {
        private static volatile VertexObjectCache instance;
        private static Dictionary<string, VertexField> cachedVertexCollection;
        private static ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

        private VertexObjectCache()
        {
            cachedVertexCollection = new Dictionary<string, VertexField>();
        }

        public static VertexObjectCache Instance
        {
            get
            {
                if (instance == null)
                {
                    lock (_lock)
                    {
                        if (instance == null)
                        {
                            instance = new VertexObjectCache();
                        }
                    }
                }

                return instance;
            }
        }

        public VertexField GetVertexField(string vertexId, string vertexJson)
        {
            _lock.EnterUpgradeableReadLock();
            try
            {
                VertexField vertexObject = null;
                if (cachedVertexCollection.TryGetValue(vertexId, out vertexObject))
                {
                    return vertexObject;
                }
                else
                {
                    _lock.EnterWriteLock();
                    try
                    {
                        JObject jsonObject = JObject.Parse(vertexJson);
                        vertexObject = FieldObject.GetVertexField(jsonObject);
                    }
                    finally
                    {
                        _lock.ExitWriteLock();
                    }
                    return vertexObject;
                }
            }
            finally
            {
                _lock.ExitUpgradeableReadLock();
            }
        }
    }
}