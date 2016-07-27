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
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.SqlServer.TransactSql.ScriptDom;
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
        internal const string GraphViewUdfAssemblyName = "GraphViewUDF";

        /// <summary>
        ///     0: _NodeTableCollection,
        ///     1: _NodeTableColumnCollection,
        ///     2: _EdgeAttributeCollection,
        ///     3: _EdgeAverageDegreeCollection,
        ///     4: _StoredProcedureCollection,
        ///     5: _NodeViewColumnCollection,
        ///     6: _EdgeViewAttributeCollection,
        ///     7: _NodeViewCollection
        /// </summary>
        internal static readonly List<string> MetadataTables =
            new List<string>
            {
                "_NodeTableCollection",
                "_NodeTableColumnCollection",
                "_EdgeAttributeCollection",
                "_EdgeAverageDegreeCollection",
                "_StoredProcedureCollection",
                "_NodeViewColumnCollection",
                "_EdgeViewAttributeCollection",
                "_NodeViewCollection"
            };

        internal static readonly List<Tuple<string, string>> Version110MetaUdf =
            new List<Tuple<string, string>>
            {
                Tuple.Create("AGGREGATE", "GraphViewUDFGlobalNodeIdEncoder"),
                Tuple.Create("AGGREGATE", "GraphViewUDFEdgeIdEncoder"),
                Tuple.Create("FUNCTION", "SingletonTable"),
                Tuple.Create("FUNCTION", "DownSizeFunction"),
                Tuple.Create("FUNCTION", "UpSizeFunction"),
                Tuple.Create("ASSEMBLY", "GraphViewUDFAssembly")
            };

        internal static readonly List<Tuple<string, string>> Version111MetaUdf =
            new List<Tuple<string, string>>
            {
                Tuple.Create("AGGREGATE", "GraphViewUDFGlobalNodeIdEncoder"),
                Tuple.Create("AGGREGATE", "GraphViewUDFEdgeIdEncoder"),
                Tuple.Create("FUNCTION", "SingletonTable"),
                Tuple.Create("FUNCTION", "DownSizeFunction"),
                Tuple.Create("FUNCTION", "UpSizeFunction"),
                Tuple.Create("FUNCTION", "ConvertNumberIntoBinaryForPath"),
                Tuple.Create("ASSEMBLY", "GraphViewUDFAssembly")
            };

        internal static readonly List<Tuple<string, string>> Version200MetaUdf =
            new List<Tuple<string, string>>
            {
                Tuple.Create("AGGREGATE", "GraphViewUDFGlobalNodeIdEncoder"),
                Tuple.Create("AGGREGATE", "GraphViewUDFEdgeIdEncoder"),
                Tuple.Create("FUNCTION", "SingletonTable"),
                Tuple.Create("FUNCTION", "DownSizeFunction"),
                Tuple.Create("FUNCTION", "UpSizeFunction"),
                Tuple.Create("FUNCTION", "ConvertNumberIntoBinaryForPath"),
                Tuple.Create("FUNCTION", "ConvertInt64IntoVarbinary"),
                Tuple.Create("ASSEMBLY", "GraphViewUDFAssembly")
            };

        internal static readonly List<Tuple<string, string>> Version111TableUdf =
            new List<Tuple<string, string>>
            {
                Tuple.Create("FUNCTION", "Decoder"),
                Tuple.Create("FUNCTION", "Recycle"),
                Tuple.Create("AGGREGATE", "Encoder"),
                Tuple.Create("FUNCTION", "ExclusiveEdgeGenerator"),
                Tuple.Create("FUNCTION", "bfsPath"),
                Tuple.Create("FUNCTION", "bfsPathWithMessage"),
                Tuple.Create("FUNCTION", "PathMessageEncoder"),
                Tuple.Create("FUNCTION", "PathMessageDecoder")
            };

        internal static readonly List<Tuple<string, string>> Version200TableUdf =
            new List<Tuple<string, string>>
            {
                Tuple.Create("FUNCTION", "Decoder"),
                Tuple.Create("FUNCTION", "Recycle"),
                Tuple.Create("AGGREGATE", "Encoder"),
                Tuple.Create("FUNCTION", "ExclusiveEdgeGenerator"),
                Tuple.Create("FUNCTION", "bfsPath"),
                Tuple.Create("FUNCTION", "bfsPath_DifferNodes"),
                Tuple.Create("FUNCTION", "bfsPathWithMessage"),
                Tuple.Create("FUNCTION", "PathMessageEncoder"),
                Tuple.Create("FUNCTION", "PathMessageDecoder"),
                Tuple.Create("FUNCTION", "ExclusiveNodeGenerator")
            };

        private static readonly string VersionTable = "VERSION";
        private static readonly string version = "2.00";
        
        private readonly List<Tuple<string, string>> _currentTableUdf = Version200TableUdf;

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
        ///     Initializes a new instance of the GraphViewConnection class.
        /// </summary>
        public GraphViewConnection()
        {
            Overwrite = false;
            _disposed = false;
            Conn = new SqlConnection();
            TranslationConnection = new SqlConnection();
        }

        /// <summary>
        ///     connectionString
        ///     Initializes a new connection to a graph database.
        ///     The database could be a SQL Server instance or Azure SQL Database, as specified by the connection string.
        /// </summary>
        /// <param name="connectionString">The connection string of the SQL database.</param>
        public GraphViewConnection(string connectionString)
        {
            _disposed = false;
            Conn = new SqlConnection(connectionString);
            TranslationConnection = new SqlConnection(connectionString);
            GraphDbAverageDegreeSamplingRate = 200;
            GraphDbEdgeColumnSamplingRate = 200;
        }

        /// <summary>
        ///     Initializes a new connection to a graph database.
        ///     The database could be a SQL Server instance or Azure SQL Database,
        ///     as specified by the connection string and the SQL credential.
        /// </summary>
        /// <param name="connectionString">The connection string of the SQL database</param>
        /// <param name="sqlCredential">A SqlCredential object</param>
        public GraphViewConnection(string connectionString, SqlCredential sqlCredential)
        {
            _disposed = false;
            Conn = new SqlConnection(connectionString, sqlCredential);
            TranslationConnection = new SqlConnection(connectionString, sqlCredential);
        }

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
        }

        /// <summary>
        ///     Sampling rate for checking average degree. Set to 100 by default.
        /// </summary>
        public double GraphDbAverageDegreeSamplingRate { get; set; }

        /// <summary>
        ///     Sampling rate for edge columns. Set to 100 by default.
        /// </summary>
        public double GraphDbEdgeColumnSamplingRate { get; set; }

        /// <summary>
        ///     Connection to a SQL database
        /// </summary>
        public SqlConnection Conn { get; }

        /// <summary>
        ///     Connection to guarantee consistency in Graph View
        /// </summary>
        internal SqlConnection TranslationConnection { get; }
        
        /// <summary>
        ///     When set to true, database will check validity if DbInit is set to false.
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
            DocDBclient = new DocumentClient(new Uri(DocDB_Url), DocDB_PrimaryKey);
            // Check to verify a database with the id=GroupMatch does not exist
            //DocDB_finish = false;
            //BuildUp();
            //while (!DocDB_finish)
            //    System.Threading.Thread.Sleep(10);
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

            // Check to verify a document collection with the id=GraphOne does not exist
            DocDB_Collection =
                DocDBclient.CreateDocumentCollectionQuery("dbs/" + DocDB_Database.Id)
                    .Where(c => c.Id == DocDB_CollectionId)
                    .AsEnumerable()
                    .FirstOrDefault();

            // If the document collection does not exist, create a new collection
            if (DocDB_Collection == null)
            {
                DocDB_Collection = await DocDBclient.CreateDocumentCollectionAsync("dbs/" + DocDB_Database.Id,
                    new DocumentCollection
                    {
                        Id = DocDB_CollectionId
                    });
            }
            DocDB_finish = true;
        }

        public async Task DeleteCollection()
        {
            await DocDBclient.DeleteDocumentCollectionAsync(DocDB_Collection.SelfLink);
            Console.WriteLine("deleted collection");

            DocDB_finish = true;
        }

        public void ResetCollection()
        {
            DocDB_finish = false;
            DeleteCollection();
            while (!DocDB_finish)
                System.Threading.Thread.Sleep(10);
        }
        
        /// <summary>
        ///     Starts a database transaction.
        /// </summary>
        /// <returns></returns>
        public SqlTransaction BeginTransaction()
        {
            return Conn.BeginTransaction();
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
                    Conn.Dispose();
                    TranslationConnection.Dispose();
                }
            }
            _disposed = true;
        }
        
    }
}