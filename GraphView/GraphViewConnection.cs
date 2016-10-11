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
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using IsolationLevel = System.Data.IsolationLevel;

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
        }
        public GraphViewConnection()
        { }

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

        public void SetupClient()
        {
            DocDBclient = new DocumentClient(new Uri(DocDB_Url), DocDB_PrimaryKey,
                new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp,
                });
        }

        public async Task BuildUp()
        {
            DocDB_Database =
                DocDBclient.CreateDatabaseQuery().Where(db => db.Id == DocDB_DatabaseId).AsEnumerable().FirstOrDefault();

            // If the database does not exist, create a new database
            if (DocDB_Database == null)
            {
                DocDB_Database = await DocDBclient.CreateDatabaseAsync(
                    new Database
                    {
                        Id = DocDB_DatabaseId
                    });
            }

            // Check to verify whether a document collection with the id exists
            DocDB_Collection =
                DocDBclient.CreateDocumentCollectionQuery("dbs/" + DocDB_Database.Id)
                    .Where(c => c.Id == DocDB_CollectionId)
                    .AsEnumerable()
                    .FirstOrDefault();

            // If the document collection does not exist, create a new collection
            if (DocDB_Collection == null)
            {
                DocDB_Collection = await DocDBclient.CreateDocumentCollectionAsync("dbs/" + DocDB_Database.Id,
                    new DocumentCollection{Id = DocDB_CollectionId},
                    new RequestOptions { OfferType = "S3" });
            }

            /*
             using (DocumentClient client = new DocumentClient(new Uri("service endpoint"), "auth key"))
{
    //Create a new collection with an Offer set to S3
    //Not passing in RequestOptions.OfferType will result in a collection with the default Offer set. 
    DocumentCollection coll = await client.CreateDocumentCollectionAsync(databaseLink,
        new DocumentCollection { Id = "My Collection" }, 
        new RequestOptions { OfferType = "S3"} );
}
             */

            DocDB_finish = true;
        }

        public async Task DeleteCollection()
        {
            await DocDBclient.DeleteDocumentCollectionAsync(DocDB_Collection.SelfLink);
            Console.WriteLine("The collection has been deleted");

            DocDB_finish = true;
        }

        public void ResetCollection()
        {
            DocDB_finish = false;
            DeleteCollection();
            while (!DocDB_finish)
                System.Threading.Thread.Sleep(10);
        }

        public void BulkInsertNodes(List<string> nodes)
        {
            if (!nodes.Any()) return;

            string collectionLink = "dbs/" + DocDB_DatabaseId + "/colls/" + DocDB_CollectionId;

            // Each batch size is determined by maxJsonSize.
            // maxJsonSize should be so that:
            // -- it fits into one request (MAX request size is ???).
            // -- it doesn't cause the script to time out.
            const int maxJsonSize = 50000;

            // Prepare the BulkInsert stored procedure
            string jsBody = File.ReadAllText(@"..\..\BulkInsert.js");
            StoredProcedure sproc = new StoredProcedure
            {
                Id = "BulkInsert",
                Body = jsBody,
            };

            var bulkInsertCommand = new BulkInsertCommand(DocDBclient);
            //Create the BulkInsert stored procedure if it doesn't exist
            Task<StoredProcedure> spTask = bulkInsertCommand.TryCreatedStoredProcedure(collectionLink, sproc);
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
                string json_arr = BulkInsertCommand.GenerateNodesJsonString(nodes, currentCount, maxJsonSize);
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
}