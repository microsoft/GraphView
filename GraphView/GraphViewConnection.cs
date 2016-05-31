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
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Azure.Documents.Client;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using IsolationLevel = System.Data.IsolationLevel;
// For debugging


namespace GraphView
{
    /// <summary>
    /// Connector to a graph database. The class inherits most functions of SqlConnection, 
    /// and provides a number of GraphView-specific functions. 
    /// </summary>
    public partial class GraphViewConnection : IDisposable
    {
        /// <summary>
        /// Sampling rate for checking average degree. Set to 100 by default.
        /// </summary>
        public double GraphDbAverageDegreeSamplingRate { get; set; }

        /// <summary>
        /// Sampling rate for edge columns. Set to 100 by default.
        /// </summary>
        public double GraphDbEdgeColumnSamplingRate { get; set; }

        /// <summary>
        /// Connection to a SQL database
        /// </summary>
        public SqlConnection Conn { get; private set; }

        /// <summary>
        /// Connection to guarantee consistency in Graph View
        /// </summary>
        internal SqlConnection TranslationConnection { get; private set; }

        public string ConnectionString
        {
            get { return Conn.ConnectionString; }
            set
            {
                Conn.ConnectionString = value;
                TranslationConnection.ConnectionString = value;
            }
        }

        /// <summary>
        /// When set to true, database will check validity if DbInit is set to false.
        /// </summary>
        public bool Overwrite { get; set; }

        private bool _disposed;

        /// <summary>
        /// 0: _NodeTableCollection,
        /// 1: _NodeTableColumnCollection,
        /// 2: _EdgeAttributeCollection,
        /// 3: _EdgeAverageDegreeCollection,
        /// 4: _StoredProcedureCollection,
        /// 5: _NodeViewColumnCollection,
        /// 6: _EdgeViewAttributeCollection,
        /// 7: _NodeViewCollection
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

        internal static readonly List<Tuple<string, string>> Version110MetaUdf= 
            new List<Tuple<string, string>>
            {
                Tuple.Create("AGGREGATE", "GraphViewUDFGlobalNodeIdEncoder"),
                Tuple.Create("AGGREGATE","GraphViewUDFEdgeIdEncoder"),
                Tuple.Create("FUNCTION","SingletonTable"),
                Tuple.Create("FUNCTION","DownSizeFunction"),
                Tuple.Create("FUNCTION","UpSizeFunction"),
                Tuple.Create("ASSEMBLY","GraphViewUDFAssembly")
            };

        internal static readonly List<Tuple<string, string>> Version111MetaUdf= 
            new List<Tuple<string, string>>
            {
                Tuple.Create("AGGREGATE", "GraphViewUDFGlobalNodeIdEncoder"),
                Tuple.Create("AGGREGATE","GraphViewUDFEdgeIdEncoder"),
                Tuple.Create("FUNCTION","SingletonTable"),
                Tuple.Create("FUNCTION","DownSizeFunction"),
                Tuple.Create("FUNCTION","UpSizeFunction"),
                Tuple.Create("FUNCTION","ConvertNumberIntoBinaryForPath"),
                Tuple.Create("ASSEMBLY","GraphViewUDFAssembly"),
            };

        internal static readonly List<Tuple<string, string>> Version200MetaUdf= 
            new List<Tuple<string, string>>
            {
                Tuple.Create("AGGREGATE", "GraphViewUDFGlobalNodeIdEncoder"),
                Tuple.Create("AGGREGATE","GraphViewUDFEdgeIdEncoder"),
                Tuple.Create("FUNCTION","SingletonTable"),
                Tuple.Create("FUNCTION","DownSizeFunction"),
                Tuple.Create("FUNCTION","UpSizeFunction"),
                Tuple.Create("FUNCTION","ConvertNumberIntoBinaryForPath"),
                Tuple.Create("FUNCTION","ConvertInt64IntoVarbinary"),
                Tuple.Create("ASSEMBLY","GraphViewUDFAssembly"),
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
                Tuple.Create("FUNCTION", "PathMessageDecoder"),
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
                Tuple.Create("FUNCTION", "ExclusiveNodeGenerator"),
            };

        private static readonly string VersionTable = "VERSION";

        private readonly List<Tuple<string, string>> _currentMetaUdf = Version200MetaUdf;
        private readonly List<Tuple<string, string>> _currentTableUdf = Version200TableUdf;
        private static readonly string version = "2.00";
        private string currentVersion = "";
        public string CurrentVersion { get { return currentVersion; } }

        internal const string GraphViewUdfAssemblyName = "GraphViewUDF";

        public string DocDB_Url;
        public string DocDB_Key;
        public string DocDB_DatabaseId;
        public string DocDB_CollectionId;
        public bool DocDB_finish;
        public DocumentClient client;
        /// <summary>
        /// Initializes a new instance of the GraphViewConnection class.
        /// </summary>
        public GraphViewConnection()
        {
            Overwrite = false;
            _disposed = false;
            Conn = new SqlConnection();
            TranslationConnection = new SqlConnection();
        }

        /// <summary>connectionString
        /// Initializes a new connection to a graph database.
        /// The database could be a SQL Server instance or Azure SQL Database, as specified by the connection string.
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
        /// Initializes a new connection to a graph database.
        /// The database could be a SQL Server instance or Azure SQL Database, 
        /// as specified by the connection string and the SQL credential. 
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
        /// Initializes a new connection to DocDB.
        /// Contains four string,
        /// Url , Key , Database's name , Collection's name
        /// </summary>
        /// <param name="docdb_EndpointUrl">The Url</param>
        /// <param name="docdb_AuthorizationKey">The Key</param>
        /// <param name="docdb_DatabaseID">Database's name</param>
        /// <param name="docdb_CollectionID">Collection's name</param>
        public GraphViewConnection(string docdb_EndpointUrl, string docdb_AuthorizationKey, string docdb_DatabaseID, string docdb_CollectionID)
        {
            DocDB_Url = docdb_EndpointUrl;
            DocDB_Key = docdb_AuthorizationKey;
            DocDB_DatabaseId = docdb_DatabaseID;
            DocDB_CollectionId = docdb_CollectionID;
        }

        public void createclient()
        {
            client = new DocumentClient(new Uri(DocDB_Url), DocDB_Key);
        }

        /// <summary>
        /// Starts a database transaction.
        /// </summary>
        /// <returns></returns>
        public SqlTransaction BeginTransaction()
        {
            return Conn.BeginTransaction();
        }

        /// <summary>
        /// Starts a database transaction with the specified isolation level and transaction name.
        /// </summary>
        /// <param name="level"></param>
        /// <param name="tranName"></param>
        /// <returns></returns>
        public SqlTransaction BeginTransaction(IsolationLevel level, string tranName)
        {
            return Conn.BeginTransaction(level, tranName);
        }

        /// <summary>
        /// Starts a database transaction with the specified isolation level.
        /// </summary>
        /// <param name="level"></param>
        /// <returns></returns>
        public SqlTransaction BeginTransaction(IsolationLevel level)
        {
            return Conn.BeginTransaction(level);
        }

        /// <summary>
        /// Starts a database transaction with the specified transaction name.
        /// </summary>
        /// <param name="tranName"></param>
        /// <returns></returns>
        public SqlTransaction BeginTransaction(string tranName)
        {
            return Conn.BeginTransaction(tranName);
        }

        public GraphViewCommand CreateCommand()
        {
            return new GraphViewCommand(null, this);
        }

        /// <summary>
        /// Initializes a graph database and creates meta-data, 
        /// including table ID, graph column and edge attribute information.
        /// </summary>
        internal void CreateMetadata(SqlTransaction transaction)
        {
            var tx = transaction;
            try
            {
                using (var command = new SqlCommand(null, Conn))
                {
                    command.Transaction = tx;
                    command.CommandText = string.Format(@"
                        CREATE TABLE [{0}] (
                            [ColumnId] [bigint] NOT NULL IDENTITY(0, 1),
                            [TableId] [bigint] NOT NULL,
                            [TableSchema] [nvarchar](128) NOT NULL,
                            [TableName] [nvarchar](128) NOT NULL,
                            [ColumnName] [nvarchar](128) NOT NULL,
                            [ColumnRole] [int] NOT NULL,
                            [Reference] [nvarchar](128) NULL,
                            [RefTableSchema] [nvarchar](128) NULL,
                            [HasReversedEdge] [bit] NOT NULL DEFAULT 0,
                            [IsReversedEdge] [bit] NOT NULL DEFAULT 0,
                            [EdgeUdfPrefix] [nvarchar](512) NULL,
                            PRIMARY KEY CLUSTERED ([ColumnId] ASC)
                        )", MetadataTables[1]);


                    command.ExecuteNonQuery();

                    command.CommandText = string.Format(@"
                        CREATE TABLE [{0}] (
                            [TableId] [bigint] NOT NULL IDENTITY(0, 1),
                            [TableSchema] [nvarchar](128) NOT NULL,
                            [TableName] [nvarchar](128) NOT NULL,
                            [TableRole] [int] NOT NULL DEFAULT 0,
                            PRIMARY KEY CLUSTERED ([TableId] ASC)
                        )", MetadataTables[0]);
                    command.ExecuteNonQuery();

                    command.CommandText = string.Format(@"
                        CREATE TABLE [{0}] (
                            [AttributeId] [bigint] NOT NULL IDENTITY(0, 1),
                            [ColumnId] [bigint] NOT NULL,
                            [TableSchema] [nvarchar](128) NOT NULL,
                            [TableName] [nvarchar](128) NOT NULL,
                            [ColumnName] [nvarchar](128) NOT NULL,
                            [AttributeName] [nvarchar](128) NOT NULL,
                            [AttributeType] [nvarchar](128) NOT NULL,
                            [AttributeEdgeId] [int],
                            PRIMARY KEY CLUSTERED ([AttributeId] ASC)
                        )", MetadataTables[2]);
                    command.ExecuteNonQuery();

                    command.CommandText = string.Format(@"
                        CREATE TABLE [{0}] (
                            [TableSchema] [nvarchar](128) NOT NULL,
                            [TableName] [nvarchar](128) NOT NULL,
                            [ColumnName] [nvarchar](128) NOT NULL,
                            [ColumnId] [bigint] NOT NULL,
                            [AverageDegree] [float] DEFAULT(5),
                            [SampleRowCount] [int] DEFAULT(1000)
                            PRIMARY KEY CLUSTERED ([TableName] ASC, [TableSchema] ASC, [ColumnName] ASC)
                        )", MetadataTables[3]);
                    command.ExecuteNonQuery();

                    command.CommandText = string.Format(@"
                        CREATE TABLE [{0}](
                            [ProcId] [bigint] NOT NULL IDENTITY(0, 1),
                            [ProcSchema] [nvarchar](128) NOT NULL,
                            [ProcName] [nvarchar](128) NOT NULL,
                            PRIMARY KEY CLUSTERED ([ProcId] ASC)
                        )", MetadataTables[4]);
                    command.ExecuteNonQuery();

                    command.CommandText = @"
                        CREATE TABLE " + MetadataTables[5] + @"(
                            [NodeViewColumnId] [bigint] NOT NULL,
                            [ColumnId] [bigint] NOT NULL
                            PRIMARY KEY CLUSTERED ([NodeViewColumnId] ASC, [ColumnId] ASC)
                        )";
                    command.ExecuteNonQuery();

                    //EdgeViewAttributeReference
                    command.CommandText = @"
                        CREATE TABLE " + MetadataTables[6] + @"(
                            [EdgeViewAttributeId] [bigint] NOT NULL,
                            [EdgeAttributeId] [bigint] NOT NULL
                            PRIMARY KEY CLUSTERED ([EdgeViewAttributeId] ASC, [EdgeAttributeId] ASC)
                        )";
                    command.ExecuteNonQuery();

                    //NodeViewCollection
                    command.CommandText = @"
                        CREATE TABLE " + MetadataTables[7] + @"(
                            [NodeViewTableId] [bigint] NOT NULL,
                            [TableId] [bigint] NOT NULL
                            PRIMARY KEY CLUSTERED ([NodeViewTableId] ASC, [TableId] ASC)
                        )";
                    command.ExecuteNonQuery();

                    //NodeViewColumnCollection
                    //command.CommandText = @"
                    //    CREATE TABLE " + MetadataTables[8] + @"(
                    //        [NodeViewColumnId] [bigint] NOT NULL,
                    //        [ColumnId] [bigint] NOT NULL
                    //        PRIMARY KEY CLUSTERED ([NodeViewColumnId] ASC, [ColumnId] ASC)
                    //    )";
                    //command.ExecuteNonQuery();

                    const string createVersionTable = @"
                        CREATE TABLE [{0}] (
                            [VERSION] [varchar](8) NOT NULL
                        )
                        INSERT INTO [{0}] (VERSION) VALUES({1})";
                    command.CommandText = string.Format(createVersionTable, VersionTable, version);
                    command.ExecuteNonQuery();
                    currentVersion = version;
                }
                const string assemblyName = GraphViewUdfAssemblyName;

                GraphViewDefinedFunctionRegister register =  new MetaFunctionRegister(assemblyName);
                register.Register(Conn, tx);
            }
            catch (SqlException e)
            {
                throw new SqlExecutionException("Failed to create necessary meta-data or system-reserved functions.", e);
            }
        }

        /// <summary>
        /// Clears all node tables in the graph database.
        /// Can be used for initialzing an empty graph.
        /// </summary>
        /// <param name="externalTransaction">An existing SqlTransaction instance under which clear data will occur.</param>
        public void ClearData(SqlTransaction externalTransaction = null)
        {
            SqlTransaction transaction = externalTransaction ?? Conn.BeginTransaction();
            var command = Conn.CreateCommand();
            command.Transaction = transaction;
            try
            {
                //Drops EdgeView
                //const string dropEdgeView = @"
                //SELECT TableSchema, TableName, ColumnName
                //FROM [dbo].[{0}]
                //WHERE ColumnRole = 3";
                //command.CommandText = string.Format(dropEdgeView, MetadataTables[1]);

                //var edgeViewList = new List<Tuple<string, string, string>>();
                ////The list of procedure with schema, table and Column name
                //using (var reader = command.ExecuteReader())
                //{
                //    while (reader.Read())
                //    {
                //        var tableSchema = reader["TableSchema"].ToString();
                //        var tableName = reader["TableName"].ToString();
                //        var columnName = reader["ColumnName"].ToString();
                //        edgeViewList.Add(Tuple.Create(tableSchema, tableName, columnName));
                //    }
                //}

                //foreach (var it in edgeViewList)
                //{
                //    DropEdgeView(it.Item1, it.Item2, it.Item3, transaction);
                //}

                //Drops node view
                const string dropNodeTable = @"
                SELECT [TableSchema],[TableName]
                FROM [dbo].[{0}]
                Where [TableRole] = @tablerole";

                command.Parameters.AddWithValue("tablerole", 1);

                //The list of procedure with schema and table name 
                var nodeTableList = new List<Tuple<string, string>>();

                command.CommandText = string.Format(dropNodeTable, MetadataTables[0]);
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableSchema = reader["TableSchema"].ToString();
                        var tableName = reader["TableName"].ToString();
                        nodeTableList.Add(Tuple.Create(tableSchema, tableName));
                    }
                }
                foreach (var x in nodeTableList)
                {
                    DropNodeView(x.Item1, x.Item2, transaction);
                }

                //Drops procedure
                command.Parameters.Clear();
                const string dropProcedure = @"
                SELECT [ProcSchema],[ProcName]
                FROM [dbo].[{0}]";
                command.CommandText = string.Format(dropProcedure, MetadataTables[4]);

                var procedureList = new List<Tuple<string, string>>();
                    //The list of procedure with schema and procedure name
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var ProcSchema = reader["ProcSchema"].ToString();
                        var ProcName = reader["ProcName"].ToString();
                        procedureList.Add(Tuple.Create(ProcSchema, ProcName));
                    }
                }
                if (procedureList.Count != 0)
                {
                    DropProcedure(
                        string.Format("drop procedure {0}",
                            string.Join(", ", procedureList.Select(x => "[" + x.Item1 + "].[" + x.Item2 + "]"))),
                        transaction);

                }

                //Drops Node table
                command.Parameters.AddWithValue("tablerole", 0);
                nodeTableList = new List<Tuple<string, string>>();
                //The list of procedure with schema and table name 
                command.CommandText = string.Format(dropNodeTable, MetadataTables[0]);
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableSchema = reader["TableSchema"].ToString();
                        var tableName = reader["TableName"].ToString();
                        nodeTableList.Add(Tuple.Create(tableSchema, tableName));
                    }
                }
                foreach (var x in nodeTableList)
                {
                    DropNodeTable("drop table [" + x.Item1 + "].[" + x.Item2 + "]", transaction);
                }

                if (externalTransaction == null)
                {
                    transaction.Commit();
                }
            }
            catch (SqlException e)
            {
                if (externalTransaction == null)
                {
                    transaction.Rollback();
                }
                throw new SqlExecutionException("An error occurred when clearing data \n", e); 
            }
            
        }

        /// <summary>
        /// Clears the GraphView database, including all node table data, meta-data, and system-generated functions.
        /// </summary>
        public void ClearGraphDatabase()
        {
            SqlTransaction transaction = Conn.BeginTransaction();
            var command = Conn.CreateCommand();
            command.Transaction = transaction;

            try
            {
                ClearData(transaction);

                //Drops Version table
                const string dropVersionTable = @" DROP TABLE {0}";
                command.CommandText = string.Format(dropVersionTable, VersionTable);
                command.ExecuteNonQuery();

                //Drops metaTable
                command.Parameters.Clear();
                const string dropTable = @"
                drop table [{0}]";
                foreach (var VARIABLE in MetadataTables)
                {
                    command.CommandText = string.Format(dropTable, VARIABLE);
                    command.ExecuteNonQuery();
                }

                //Drop assembly and UDF
                const string dropAssembly = @"drop {0} {1}";
                foreach (var it in _currentMetaUdf)
                {
                    command.CommandText = string.Format(dropAssembly, it.Item1, it.Item2);
                    command.ExecuteNonQuery();
                }
                transaction.Commit();
            }
            catch (SqlException e)
            {
                transaction.Rollback();
                throw new SqlExecutionException("An error occurred when clearing the database \n", e);
            }
        }

        /// <summary>
        /// Opens the graph database connection. Creates meta-data and system-reserved functions.
        /// </summary>
        public void Open()
        {
            Conn.Open();
            TranslationConnection.Open();
            
            var transaction = Conn.BeginTransaction(); 
            try
            {
                if (!CheckDatabase(transaction))
                {
                    CreateMetadata(transaction);
                }

                if (currentVersion == "1.00")
                {
                    UpgradeFromV100ToV110(transaction);
                }

                if (currentVersion == "1.10")
                {
                    UpgradeFromV110ToV111(transaction); 
                }

                if (currentVersion == "1.11")
                {
                    UpgradeFromV111ToV200(transaction);
                }

                if (currentVersion == "2.00")
                {
                }

                if (currentVersion != version)
                {
                    throw new GraphViewException("Version number in version table is not right.");
                }
                transaction.Commit();
            }
            catch (SqlException e)
            {
                transaction.Rollback();
                throw new SqlExecutionException("An error occurred when opening a database connection", e);
            }
        }

        /// <summary>
        /// Closes the database connection.
        /// </summary>
        public void Close()
        {
            try
            {
                Conn.Close();
                TranslationConnection.Close();
            }
            catch (SqlException e)
            {
                throw new SqlExecutionException("An error occurred when closing a database connection", e);
            }
        }

        private void UpgradeFromV100ToV110(SqlTransaction transaction)
        {
            //var tx = conn.BeginTransaction("UpgradeFromV100ToV101");
            var tables = GetNodeTables(transaction);

            //Upgrade meta tables
            UpgradeMetaTableV100(transaction);

            //Upgrade functions
            foreach (var table in tables)
            {
                DropNodeTableFunctionV100(table.Item1, table.Item2, transaction);
            }
            //UpgradeGraphViewFunctionV100(transaction);
            UpgradeNodeTableFunction(transaction);

            //Upgrade global view
            foreach (var schema in tables.ToLookup(x => x.Item1.ToLower()))
            {
                updateGlobalNodeView(schema.Key, transaction);
            }

            //Upgrade table statistics
            foreach (var table in tables)
            {
                UpdateTableStatistics(table.Item1, table.Item2, transaction);
            }

            //Update version number
            UpdateVersionNumber("1.10", transaction);
        }

        //Add one udf ConvertNumberIntoBinaryForPath
        private void UpgradeFromV110ToV111(SqlTransaction transaction)
        {
            var tables = GetNodeTables(transaction);
            //DropAssemblyAndMetaUDFV110(transaction);
            DropAssemblyAndMetaUdf(Version110MetaUdf, transaction);
            const string assemblyName = GraphViewUdfAssemblyName;

            GraphViewDefinedFunctionRegister register = new MetaFunctionRegister(assemblyName);
            register.Register(Conn, transaction);

            //Update version number
            UpdateVersionNumber("1.11", transaction);
            //Upgrade global view
            foreach (var schema in tables.ToLookup(x => x.Item1.ToLower()))
            {
                updateGlobalNodeView(schema.Key, transaction);
            }
        }

        // 
        private void UpgradeFromV111ToV200(SqlTransaction transaction)
        {
            //Upgrade meta tables
            UpgradeMetaTableV111(transaction);

            var tables = GetNodeTables(transaction);

            //drop table functions, assemblies and global udf
            foreach (var table in tables)
            {
                DropNodeTableFunctionAndAssembly(table.Item1, table.Item2, Version111TableUdf, transaction);
            }
            DropAssemblyAndMetaUdf(Version111MetaUdf, transaction);

            //Upgrade global udf and assembly
            const string assemblyName = GraphViewUdfAssemblyName;
            GraphViewDefinedFunctionRegister register = new MetaFunctionRegister(assemblyName);
            register.Register(Conn, transaction);

            //Upgrade table functions and assemblies
            UpgradeNodeTableFunction(transaction);

            //Update version number
            UpdateVersionNumber("2.00", transaction);

        }

        /// <summary>
        /// Validates the graph database by checking if metadata tables exist.
        /// </summary>
        /// <returns>True, if graph database is valid; otherwise, false.</returns>
        private bool CheckDatabase(SqlTransaction transaction)
        {
            const string checkVersionTable = @"
                select TABLE_NAME
                from INFORMATION_SCHEMA.TABLES
                Where TABLE_CATALOG = @catalog and TABLE_SCHEMA = @schema and TABLE_NAME = @name and TABLE_TYPE = @type";

            const string checkVersion = @"
                Select * 
                From {0}";

            using (var command = Conn.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = checkVersionTable;
                command.Parameters.AddWithValue("@type", "BASE TABLE");
                command.Parameters.AddWithValue("@catalog", Conn.Database);
                command.Parameters.AddWithValue("@schema", "dbo");
                command.Parameters.AddWithValue("@name", VersionTable);
                using (var reader = command.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        return false;
                    }
                }
            }

            using (var command = Conn.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = string.Format(checkVersion, VersionTable);
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        currentVersion = reader["VERSION"].ToString();
                    }
                }
            }

            return true;
            //var tableString = String.Join(", ", MetadataTables.Select(x => "'" + x + "'"));
            //using (var command = Conn.CreateCommand())
            //{

            //    command.CommandText = String.Format(CultureInfo.CurrentCulture, @"
            //        SELECT COUNT([name]) cnt
            //        FROM sysobjects
            //        WHERE [type] = @type AND [category] = @category AND
            //        [name] IN ({0})
            //    ", tableString);

            //    command.Parameters.Add("@type", SqlDbType.NVarChar, 2);
            //    command.Parameters.Add("@category", SqlDbType.Int);
            //    command.Parameters["@type"].Value = "U";
            //    command.Parameters["@category"].Value = 0;

            //    using (var reader = command.ExecuteReader())
            //    {
            //        if (!reader.Read())
            //            return false;
            //        return Convert.ToInt32(reader["cnt"], CultureInfo.CurrentCulture) == MetadataTables.Count;
            //    }
            //}
        }

        /// <summary>
        /// Creates a node table in the graph database.
        /// </summary>
        /// <param name="sqlStr">A CREATE TABLE statement with annotations.</param>
        /// <param name="externalTransaction">An existing SqlTransaction instance under which the create node table will occur.</param>
        /// <returns>True, if the statement is successfully executed.</returns>
        public bool CreateNodeTable(string sqlStr, SqlTransaction externalTransaction = null)
        {
            // get syntax tree of CREATE TABLE command
            var parser = new GraphViewParser();
            List<WNodeTableColumn> columns;
            IList<ParseError> errors;
            var script = parser.ParseCreateNodeTableStatement(sqlStr, out columns, out errors) as WSqlScript;
            if (errors.Count > 0)
                throw new SyntaxErrorException(errors);

            if (script == null || script.Batches.Count == 0)
            {
                throw new SyntaxErrorException("Invalid CREATE TABLE statement.");
            }

            var statement = script.Batches[0].Statements[0] as WCreateTableStatement;

            var tableSchema = statement.SchemaObjectName.SchemaIdentifier != null
                ? statement.SchemaObjectName.SchemaIdentifier.Value
                : "dbo";
            var tableName = statement.SchemaObjectName.BaseIdentifier.Value;

            SqlTransaction tx;
            if (externalTransaction == null)
            {
                tx = Conn.BeginTransaction();
            }
            else
            {
                tx = externalTransaction;
            }
            // Persists the node table's meta-data
            try
            {
                Int64 tableId;
                using (var command = new SqlCommand(null, Conn))
                {
                    command.Transaction = tx;
                    command.CommandText = string.Format(@"
                    INSERT INTO [{0}] ([TableSchema], [TableName])
                    OUTPUT [Inserted].[TableId]
                    VALUES (@tableSchema, @tableName)", MetadataTables[0]);
                    command.Parameters.AddWithValue("@tableSchema", tableSchema);
                    command.Parameters.AddWithValue("@tableName", tableName);

                    // get generated TableId
                    WValueExpression tableIdentitySeed;
                    using (var reader = command.ExecuteReader())
                    {

                        if (!reader.Read())
                        {
                            return false;
                        }
                        tableId = Convert.ToInt64(reader["TableId"], CultureInfo.CurrentCulture);
                        var tableIdSeek = tableId << 48;
                        tableIdentitySeed = new WValueExpression(tableIdSeek.ToString(CultureInfo.InvariantCulture), false);
                    }

                    // create graph table
                    var wColumnDefinition = statement.Definition.ColumnDefinitions
                        .FirstOrDefault(x => x.ColumnIdentifier.Value == "GlobalNodeId");
                    if (wColumnDefinition != null)
                        wColumnDefinition.IdentityOptions.IdentitySeed = tableIdentitySeed;
                    command.CommandText = statement.ToString();
                    command.ExecuteNonQuery();
                }

                var edgeColumnNameToColumnId = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
                var hasReversedEdge = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
                using (var command = new SqlCommand(null, Conn))
                {
                    command.Transaction = tx;
                    // insert graph column
                    command.CommandText = string.Format(@"
                    INSERT INTO [{0}]
                    ([TableSchema], [TableName], [TableId], [ColumnName], [ColumnRole], [Reference], [HasReversedEdge], [EdgeUdfPrefix])
                    OUTPUT [Inserted].[ColumnId]
                    VALUES (@tableSchema, @tableName, @tableid, @columnName, @columnRole, @ref, @hasRevEdge, @udfPrefix)", MetadataTables[1]);

                    command.Parameters.AddWithValue("@tableSchema", tableSchema);
                    command.Parameters.AddWithValue("@tableName", tableName);
                    command.Parameters.AddWithValue("@tableid", tableId);

                    command.Parameters.Add("@columnName", SqlDbType.NVarChar, 128);
                    command.Parameters.Add("@columnRole", SqlDbType.Int);
                    command.Parameters.Add("@ref", SqlDbType.NVarChar, 128);
                    command.Parameters.Add("@hasRevEdge", SqlDbType.Bit);
                    command.Parameters.Add("@udfPrefix", SqlDbType.NVarChar, 512);

                    foreach (var column in columns)
                    {
                        command.Parameters["@columnName"].Value = column.ColumnName.Value;
                        command.Parameters["@columnRole"].Value = (int) column.ColumnRole;
                        command.Parameters["@hasRevEdge"].Value = column.ColumnRole == WNodeTableColumnRole.Edge ? 1 : 0;

                        var edgeColumn = column as WGraphTableEdgeColumn;
                        if (edgeColumn != null)
                        {
                            command.Parameters["@ref"].Value =
                                (edgeColumn.TableReference as WNamedTableReference).ExposedName.Value;
                            command.Parameters["@udfPrefix"].Value = 
                                tableSchema + "_" + tableName + "_" + column.ColumnName.Value;
                        }
                        else
                        {
                            command.Parameters["@ref"].Value = command.Parameters["@udfPrefix"].Value = SqlChars.Null;
                        }

                        using (var reader = command.ExecuteReader())
                        {
                            if (!reader.Read())
                            {
                                return false;
                            }
                            if (column.ColumnRole == WNodeTableColumnRole.Edge)
                            {
                                edgeColumnNameToColumnId[column.ColumnName.Value] = Convert.ToInt32(reader["ColumnId"].ToString(), CultureInfo.CurrentCulture);
                                hasReversedEdge[column.ColumnName.Value] = true;
                            }
                        }
                    }

                    command.CommandText = string.Format(@"
                    INSERT INTO [{0}]
                    ([TableSchema], [TableName], [ColumnName], [ColumnId], [AverageDegree])
                    VALUES (@tableSchema, @tableName, @columnName, @columnid, @AverageDegree)", MetadataTables[3]);
                    command.Parameters.Add("@AverageDegree", SqlDbType.Int);
                    command.Parameters["@AverageDegree"].Value = 5;
                    command.Parameters.Add("@columnid", SqlDbType.Int);

                    foreach (var column in columns.OfType<WGraphTableEdgeColumn>())
                    {
                        command.Parameters["@columnid"].Value = edgeColumnNameToColumnId[column.ColumnName.Value];
                        command.Parameters["@columnName"].Value = column.ColumnName.Value;
                        command.ExecuteNonQuery();
                    }
                }

                // insert graph edge attributes
                using (var command = new SqlCommand(null, Conn))
                {
                    command.Transaction = tx;
                    command.CommandText = string.Format(@"
                    INSERT INTO [{0}]
                    ([TableSchema], [TableName], [ColumnName], [ColumnId], [AttributeName], [AttributeType], [AttributeEdgeId])
                    VALUES (@tableSchema, @tableName, @columnName, @columnid, @attrName, @attrType, @attrId)", MetadataTables[2]);
                    command.Parameters.AddWithValue("@tableSchema", tableSchema);
                    command.Parameters.AddWithValue("@tableName", tableName);

                    command.Parameters.Add("@columnName", SqlDbType.NVarChar, 128);
                    command.Parameters.Add("@attrName", SqlDbType.NVarChar, 128);
                    command.Parameters.Add("@attrType", SqlDbType.NVarChar, 128);
                    command.Parameters.Add("@attrId", SqlDbType.Int);
                    command.Parameters.Add("@columnid", SqlDbType.Int);

                    var createOrder = 1;
                    foreach (var column in columns.OfType<WGraphTableEdgeColumn>())
                    {
                        command.Parameters["@columnName"].Value = column.ColumnName.Value;
                        foreach (var attr in column.Attributes)
                        {
                            command.Parameters["@attrName"].Value = attr.Item1.Value;
                            command.Parameters["@attrType"].Value = attr.Item2.ToString();
                            command.Parameters["@attrId"].Value = (createOrder++).ToString();
                            command.Parameters["@columnid"].Value = edgeColumnNameToColumnId[column.ColumnName.Value];
                            command.ExecuteNonQuery();
                        }
                    }
                }

                // create column edge sampling table
//            using (var command = new SqlCommand(null, Conn))
//            {
//                    command.Transaction = tx;
//                foreach (var column in columns.OfType<WGraphTableEdgeColumn>())
//                {
//                    command.CommandText = String.Format(CultureInfo.CurrentCulture, @"
//                        CREATE TABLE [{0}_{1}_{2}_Sampling] (
//                            [src] [bigint],
//                            [dst] [bigint]
//                        )", tableSchema, tableName, column.ColumnName.Value);
//                    command.ExecuteNonQuery();
//                }
//            }
                // process edge's Decoder function
                //var edgeDict = columns.OfType<WGraphTableEdgeColumn>()
                //    .ToDictionary(col => col.ColumnName.Value,
                //        col =>
                //            col.Attributes.Select(
                //                x =>
                //                    new Tuple<string, string>(x.Item1.Value,
                //                        x.Item2.ToString().ToLower(CultureInfo.CurrentCulture)))
                //                .ToList());
                var edgeDict =
                    columns.OfType<WGraphTableEdgeColumn>()
                        .Select(
                            col =>
                                new Tuple<string,int, List<Tuple<string, string>>>(col.ColumnName.Value,
                                    (int)edgeColumnNameToColumnId[col.ColumnName.Value],
                                    col.Attributes.Select(
                                        x =>
                                            new Tuple<string, string>(x.Item1.Value,
                                                x.Item2.ToString().ToLower(CultureInfo.CurrentCulture)))
                                        .ToList())).ToList();
                var userIdColumn = columns.Where(e => e.ColumnRole == WNodeTableColumnRole.NodeId).ToList();
                string userId = (userIdColumn.Count == 0) ? "" : userIdColumn[0].ColumnName.Value;
                if (edgeDict.Count > 0)
                {
                    var assemblyName = tableSchema + '_' + tableName;
                    GraphViewDefinedFunctionRegister register = new NodeTableRegister(assemblyName, tableName, edgeDict, userId);
                    register.Register(Conn, tx);
                }
                using (var command = new SqlCommand(null, Conn))
                {
                    command.Transaction = tx;
                    foreach (var column in columns.OfType<WGraphTableEdgeColumn>())
                    {
                        command.CommandText = String.Format(CultureInfo.CurrentCulture, @"
                            SELECT * INTO [{0}_{1}_{2}_Sampling] FROM (
                            SELECT ([GlobalNodeID]+0) as [Src], [Edge].*
                            FROM [{0}].[{1}] WITH (NOLOCK)
                            CROSS APPLY {0}_{1}_{2}_Decoder([{2}],[{2}DeleteCol]) AS Edge
                            WHERE 1=0) as EdgeSample",
                            tableSchema, tableName, column.ColumnName.Value);
                        command.ExecuteNonQuery();
                    }
                }
                updateGlobalNodeView(tableSchema, tx);

                // check whether adding reversed edges into the current node table column collection are required
                var reversedEdgeCommandText = String.Empty;
                var reversedEdgeSamplingCommandText = String.Empty;
                var reversedMetaCommandTextList = new List<string>();
                using (var command = new SqlCommand(null, Conn))
                {
                    command.Transaction = tx;
                    command.CommandText = String.Format(@"
                        SELECT [ColumnId], [TableSchema], [TableName], [ColumnName], [ColumnRole], [Reference], [HasReversedEdge], [EdgeUdfPrefix]
                        FROM [{0}]
                        WHERE [ColumnRole] = 1 AND [Reference] = '{1}'", 
                        MetadataTables[1], tableName);

                    var originalColumnIdList = new List<long>();
                    using (var reader = command.ExecuteReader())
                    {
                        var index = 0;
                        while (reader.Read())
                        {
                            if (!bool.Parse(reader["HasReversedEdge"].ToString())) continue;
                            var sourceTableSchema = reader["TableSchema"].ToString();
                            var sourceTableName = reader["TableName"].ToString();
                            var sourceEdgeName = reader["ColumnName"].ToString();
                            var reversedEdgeName = sourceTableName + "_" + sourceEdgeName + "Reversed";
                            var parameters = new string[] { 
                                                            "@tableSchema" + index, 
                                                            "@tableName" + index, 
                                                            "@tableid" + index, 
                                                            "@columnName" + index, 
                                                            "@columnRole" + index, 
                                                            "@ref" + index,
                                                            "@refTableSchema" + index,
                                                            "@isRevEdge" + index,
                                                            "@udfPrefix" + index};
                            
                            // add reversed edge varbinary columns
                            reversedEdgeCommandText += String.Format(@"
                                ALTER TABLE [{0}].[{1}]
                                ADD [{2}] VARBINARY(MAX) NOT NULL DEFAULT 0x,
                                    [{3}] VARBINARY(MAX) NOT NULL DEFAULT 0x,
                                    [{4}] INT NOT NULL DEFAULT 0;
                                ",
                                tableSchema, tableName, 
                                reversedEdgeName,
                                reversedEdgeName + "DeleteCol",
                                reversedEdgeName + "OutDegree");

                            // create reversed edge sampling table
                            reversedEdgeSamplingCommandText += String.Format(@"
                                SELECT * INTO [{0}_{1}_{2}_Sampling] FROM (
                                SELECT ([GlobalNodeID]+0) as [Src], [Edge].*
                                FROM [{0}].[{1}] WITH (NOLOCK)
                                CROSS APPLY {3}_{4}_{5}_Decoder([{2}],[{2}DeleteCol]) AS Edge
                                WHERE 1=0) as EdgeSample
                                ",
                                tableSchema, tableName, reversedEdgeName,
                                sourceTableSchema, sourceTableName, sourceEdgeName);

                            // update meta info about reversed edges
                            reversedMetaCommandTextList.Add(string.Format(@"
                                INSERT INTO [{0}]
                                ([TableSchema], [TableName], [TableId], [ColumnName], [ColumnRole], [Reference], [RefTableSchema], [IsReversedEdge], [EdgeUdfPrefix])
                                OUTPUT [Inserted].[ColumnId]
                                VALUES ({1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})
                                ",
                                MetadataTables[1],
                                parameters[0], parameters[1], parameters[2], 
                                parameters[3], parameters[4], parameters[5], 
                                parameters[6], parameters[7], parameters[8]));

                            command.Parameters.AddWithValue(parameters[0], tableSchema);
                            command.Parameters.AddWithValue(parameters[1], tableName);
                            command.Parameters.AddWithValue(parameters[2], tableId);

                            command.Parameters.AddWithValue(parameters[3], reversedEdgeName);
                            command.Parameters.AddWithValue(parameters[4], Convert.ToInt32(reader["ColumnRole"].ToString(), CultureInfo.CurrentCulture));
                            command.Parameters.AddWithValue(parameters[5], sourceTableName);
                            command.Parameters.AddWithValue(parameters[6], sourceTableSchema);
                            command.Parameters.AddWithValue(parameters[7], 1);
                            command.Parameters.AddWithValue(parameters[8], reader["EdgeUdfPrefix"].ToString());

                            originalColumnIdList.Add(Convert.ToInt32(reader["ColumnId"].ToString(), CultureInfo.CurrentCulture));
                            ++index;
                        }
                    }

                    if (String.IsNullOrEmpty(reversedEdgeCommandText) == false)
                    {
                        command.CommandText = reversedEdgeCommandText;
                        command.ExecuteNonQuery();
                        command.CommandText = reversedEdgeSamplingCommandText;
                        command.ExecuteNonQuery();

                        command.CommandText = String.Format(@"
                            SELECT MAX([AttributeEdgeId]) AS maxAttrEdgeId
                            FROM [{0}]
                            WHERE [TableSchema] = '{1}' AND [TableName] = '{2}'",
                            MetadataTables[2], tableSchema, tableName);

                        var createOrder = 1;
                        using (var reader = command.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                var startOrder = reader["maxAttrEdgeId"].ToString();
                                createOrder = String.IsNullOrEmpty(startOrder) ? 1 : Convert.ToInt32(startOrder, CultureInfo.CurrentCulture) + 1;
                            }
                        }

                        // NodeTableColumnCollection update
                        for (var i = 0; i < reversedMetaCommandTextList.Count; ++i)
                        {
                            command.CommandText = reversedMetaCommandTextList[i];
                            
                            if (command.Parameters.Contains("@attrName"))
                            {
                                command.Parameters.RemoveAt("@tableSchema");
                                command.Parameters.RemoveAt("@tableName");
                                command.Parameters.RemoveAt("@columnName");
                                command.Parameters.RemoveAt("@attrName");
                                command.Parameters.RemoveAt("@attrType");
                                command.Parameters.RemoveAt("@attrId");
                                command.Parameters.RemoveAt("@columnid");
                            }

                            var reversedColumnId = 0;
                            using (var reader = command.ExecuteReader())
                            {
                                if (!reader.Read())
                                {
                                    return false;
                                }
                                if (Convert.ToInt32(command.Parameters["@columnRole"+i].Value, CultureInfo.CurrentCulture) == 1)
                                {
                                    reversedColumnId = Convert.ToInt32(reader["ColumnId"].ToString(), CultureInfo.CurrentCulture);
                                }
                            }
                            
                            // get all original attributes
                            var attributes = new List<Tuple<string, string>>();
                            command.CommandText = String.Format(@"
                                SELECT [AttributeName], [AttributeType]
                                FROM [{0}]
                                WHERE [ColumnId] = {1}",
                                MetadataTables[2], originalColumnIdList[i]);

                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    attributes.Add(new Tuple<string, string>(
                                        reader["AttributeName"].ToString(), reader["AttributeType"].ToString()));
                                }
                            }

                            if (attributes.Count == 0) continue;

                            // EdgeAttributeCollection update
                            command.Parameters.AddWithValue("@tableSchema", tableSchema);
                            command.Parameters.AddWithValue("@tableName", tableName);
                            command.Parameters.AddWithValue("@columnName", command.Parameters["@columnName"+i].Value.ToString());
                            
                            command.Parameters.Add("@attrName", SqlDbType.NVarChar, 128);
                            command.Parameters.Add("@attrType", SqlDbType.NVarChar, 128);
                            command.Parameters.Add("@attrId", SqlDbType.Int);
                            command.Parameters.Add("@columnid", SqlDbType.Int);

                            foreach (var attr in attributes)
                            {
                                command.Parameters["@attrName"].Value = attr.Item1;
                                command.Parameters["@attrType"].Value = attr.Item2;
                                command.Parameters["@attrId"].Value = (createOrder++).ToString();
                                command.Parameters["@columnid"].Value = reversedColumnId;

                                command.CommandText = String.Format(@"
                                    INSERT INTO [{0}]
                                    ([TableSchema], [TableName], [ColumnName], [ColumnId], [AttributeName], [AttributeType], [AttributeEdgeId])
                                    VALUES (@tableSchema, @tableName, @columnName, @columnid, @attrName, @attrType, @attrId)", MetadataTables[2]);
                                command.ExecuteNonQuery();
                            }
                            
                        }
                    }
                }

                // check whether adding reverse edges to pointed node tables are required
                using (var command = new SqlCommand(null, Conn))
                {
                    command.Transaction = tx;
                    foreach (var column in columns.Where(x=>hasReversedEdge.ContainsKey(x.ColumnName.Value)))
                    {
                        command.Parameters.Clear();
                        reversedEdgeCommandText = reversedEdgeSamplingCommandText = String.Empty;

                        var edgeColumn = column as WGraphTableEdgeColumn;
                        if (edgeColumn == null) continue;

                        var refTableName =
                            (edgeColumn.TableReference as WNamedTableReference).ExposedName.Value;
                        string refTableSchema = String.Empty, reversedEdgeName = String.Empty;
                        Int64 refTableId = 0;

                        // skip the edge which connects to the node table itself
                        if (refTableName.Equals(tableName)) continue;

                        command.CommandText = String.Format(@"
                            SELECT [TableId], [TableSchema], [TableName]
                            FROM [{0}]
                            WHERE [TableRole] = 0 AND [TableName] = '{1}'",
                            MetadataTables[0], refTableName);

                        using (var reader = command.ExecuteReader())
                        {
                            // if the target node table exists
                            if (reader.Read())
                            {
                                refTableSchema = reader["TableSchema"].ToString();
                                reversedEdgeName = tableName + "_" + column.ColumnName.Value + "Reversed";

                                // add reversed edge varbinary column
                                reversedEdgeCommandText += String.Format(@"
                                    ALTER TABLE [{0}].[{1}]
                                    ADD [{2}] VARBINARY(MAX) NOT NULL DEFAULT 0x,
                                        [{3}] VARBINARY(MAX) NOT NULL DEFAULT 0x,
                                        [{4}] INT NOT NULL DEFAULT 0
                                    ", 
                                    refTableSchema, refTableName,
                                    reversedEdgeName,
                                    reversedEdgeName + "DeleteCol",
                                    reversedEdgeName + "OutDegree");

                                // create reversed edge sampling table
                                reversedEdgeSamplingCommandText += String.Format(@"
                                    SELECT * INTO [{0}_{1}_{2}_Sampling] FROM (
                                    SELECT ([GlobalNodeID]+0) as [Src], [Edge].*
                                    FROM [{0}].[{1}] WITH (NOLOCK)
                                    CROSS APPLY {3}_{4}_{5}_Decoder([{2}],[{2}DeleteCol]) AS Edge
                                    WHERE 1=0) as EdgeSample
                                    ",
                                    refTableSchema, refTableName, reversedEdgeName,
                                    tableSchema, tableName, column.ColumnName.Value);

                                refTableId = Convert.ToInt64(reader["TableId"], CultureInfo.CurrentCulture);
                            }
                        }

                        // update meta info about reversed edge
                        if (String.IsNullOrEmpty(reversedEdgeCommandText) == false)
                        {
                            command.CommandText = reversedEdgeCommandText;
                            command.ExecuteNonQuery();
                            command.CommandText = reversedEdgeSamplingCommandText;
                            command.ExecuteNonQuery();

                            // NodeTableColumnCollection update
                            command.CommandText = string.Format(@"
                                INSERT INTO [{0}]
                                ([TableSchema], [TableName], [TableId], [ColumnName], [ColumnRole], [Reference], [RefTableSchema], [IsReversedEdge], [EdgeUdfPrefix])
                                OUTPUT [Inserted].[ColumnId]
                                VALUES (@tableSchema, @tableName, @tableid, @columnName, @columnRole, @ref, @refTableSchema, @isRevEdge, @udfPrefix)", MetadataTables[1]);

                            command.Parameters.AddWithValue("@tableSchema", refTableSchema);
                            command.Parameters.AddWithValue("@tableName", refTableName);
                            command.Parameters.AddWithValue("@tableid", refTableId);

                            command.Parameters.AddWithValue("@columnName", reversedEdgeName);
                            command.Parameters.AddWithValue("@columnRole", (int)column.ColumnRole);
                            command.Parameters.AddWithValue("@ref", tableName);
                            command.Parameters.AddWithValue("@refTableSchema", tableSchema);
                            command.Parameters.AddWithValue("@isRevEdge", 1);
                            command.Parameters.AddWithValue("@udfPrefix", tableSchema + "_" + tableName + "_" + column.ColumnName.Value);

                            var reversedColumnId = 0;
                            using (var reader = command.ExecuteReader())
                            {
                                if (!reader.Read())
                                {
                                    return false;
                                }
                                if (column.ColumnRole == WNodeTableColumnRole.Edge)
                                {
                                    reversedColumnId = Convert.ToInt32(reader["ColumnId"].ToString(), CultureInfo.CurrentCulture);
                                }
                            }

                            command.CommandText = String.Format(@"
                                SELECT MAX([AttributeEdgeId]) AS maxAttrEdgeId
                                FROM [{0}]
                                WHERE [TableSchema] = '{1}' AND [TableName] = '{2}'",
                                MetadataTables[2], refTableSchema, refTableName);

                            var createOrder = 1;
                            using (var reader = command.ExecuteReader())
                            {
                                if (reader.Read())
                                {
                                    var startOrder = reader["maxAttrEdgeId"].ToString();
                                    createOrder = String.IsNullOrEmpty(startOrder) ? 1 : Convert.ToInt32(startOrder, CultureInfo.CurrentCulture) + 1;
                                }
                            }

                            // EdgeAttributeCollection update
                            command.Parameters.Add("@attrName", SqlDbType.NVarChar, 128);
                            command.Parameters.Add("@attrType", SqlDbType.NVarChar, 128);
                            command.Parameters.Add("@attrId", SqlDbType.Int);
                            command.Parameters.Add("@columnid", SqlDbType.Int);

                            foreach (var attr in edgeColumn.Attributes)
                            {
                                command.Parameters["@attrName"].Value = attr.Item1.Value;
                                command.Parameters["@attrType"].Value = attr.Item2.ToString();
                                command.Parameters["@attrId"].Value = (createOrder++).ToString();
                                command.Parameters["@columnid"].Value = reversedColumnId;

                                command.CommandText = String.Format(@"
                                    INSERT INTO [{0}]
                                    ([TableSchema], [TableName], [ColumnName], [ColumnId], [AttributeName], [AttributeType], [AttributeEdgeId])
                                    VALUES (@tableSchema, @tableName, @columnName, @columnid, @attrName, @attrType, @attrId)", MetadataTables[2]);
                                command.ExecuteNonQuery();
                            }
                        }
                    }
                }

                if (externalTransaction == null)
                {
                    tx.Commit();
                }
                return true;
            }
            catch (SqlException e)
            {
                if (externalTransaction == null)
                {
                    tx.Rollback();
                }
                throw new SqlExecutionException("An error occurred when creating the node table.\n" + e.Message, e);
            }
        }

        /// <summary>
        /// Drops node table user-defined functions and assembly
        /// </summary>
        /// <param name="tableSchema"> Schema of table to be dropped.</param>
        /// <param name="tableName">Name of table to be dropped.</param>
        /// <param name="tableUdfList">List of UDF to be dropped.</param>
        /// <param name="externalTransaction">An existing SqlTransaction instance under which the drop node table will occur.</param>
        /// <returns>Returns true if functions and assembly are successfully dropped.</returns>
        public bool DropNodeTableFunctionAndAssembly(string tableSchema, string tableName, List<Tuple<string, string>> tableUdfList, 
            SqlTransaction externalTransaction = null)
        {
            SqlTransaction tran = externalTransaction ?? Conn.BeginTransaction();
            //if (externalTransaction == null)
            //    tran = Conn.BeginTransaction();
            try
            {
                // delete metadata
                using (var command = new SqlCommand(null, Conn, tran))
                {
                    var edgeColumns = GetGraphEdgeColumns(tableSchema, tableName, tran);
                    if (edgeColumns.Count > 0)
                    {
                        var assemblyName = tableSchema + '_' + tableName;
                        foreach (var edgeColumn in edgeColumns)
                        {
                            if (edgeColumn.Item3) 
                                DropEdgeView(tableSchema, tableName, edgeColumn.Item1, tran);
                            else
                            {
                                // skip reversed edges since they have no UDF
                                if (!edgeColumn.Item6)
                                {
                                    foreach (var it in tableUdfList)
                                    {
                                        command.CommandText = string.Format(
                                            @"DROP {2} [{0}_{1}_{3}];",
                                            assemblyName,
                                            edgeColumn.Item1, it.Item1, it.Item2);
                                        command.ExecuteNonQuery();
                                    }
                                }
                            }
                        }
                        // skip tables which have only reversed edge columns
                        if (edgeColumns.Count(x => !x.Item6) > 0)
                        {
                            command.CommandText = @"DROP ASSEMBLY [" + assemblyName + "_Assembly]";
                            command.ExecuteNonQuery();
                        }
                    }
                    
                    if (externalTransaction == null)
                    {
                        tran.Commit();
                    }
                    return true;
                }
            }
            catch (SqlException e)
            {
                if (externalTransaction == null)
                {
                    tran.Rollback();
                }
                throw new SqlExecutionException("An error occurred when dropping the node table function.", e);
            }
        }

        /// <summary>
        /// Drops node table and related metadata.
        /// </summary>
        /// <param name="sqlStr"> Name of table to be dropped.</param>
        /// <param name="externalTransaction">An existing SqlTransaction instance under which the drop node table will occur.</param>
        /// <returns>Returns true if the statement is successfully executed.</returns>
        public bool DropNodeTable(string sqlStr, SqlTransaction externalTransaction = null)
        {
            // get syntax tree of DROP TABLE command
            var parser = new GraphViewParser();
            var sr = new StringReader(sqlStr);
            IList<ParseError> errors;
            var script = parser.Parse(sr, out errors) as WSqlScript;
            if (errors.Count > 0)
                throw new SyntaxErrorException(errors);
            if (script == null)
                return false;
            var statement = script.Batches[0].Statements[0] as WDropTableStatement;
            if (statement == null)
                return false;
            SqlTransaction tran;
            if (externalTransaction == null)
            {
                tran = Conn.BeginTransaction();
            }
            else
            {
                tran = externalTransaction;
            }
            try
            {
                // delete metadata
                using (var command = new SqlCommand(null, Conn, tran))
                {
                    var schemaSet = new HashSet<string>();

                    foreach (var obj in statement.Objects)
                    {
                        var tableName = obj.BaseIdentifier.Value;
                        var tableSchema = obj.SchemaIdentifier != null
                            ? obj.SchemaIdentifier.Value
                            : "dbo";
                        if (!schemaSet.Contains(tableSchema.ToLower()))
                        {
                            schemaSet.Add(tableSchema.ToLower());
                        }
                        command.Parameters.AddWithValue("@tableName", tableName);
                        command.Parameters.AddWithValue("@tableSchema", tableSchema);

                        var edgeColumns = GetGraphEdgeColumns(tableSchema, tableName, tran);
                        if (edgeColumns.Count > 0)
                        {
                            var assemblyName = tableSchema + '_' + tableName;
                            foreach (var edgeColumn in edgeColumns)
                            {
                                if (edgeColumn.Item3)
                                    DropEdgeView(tableSchema, tableName, edgeColumn.Item1, tran);
                                else
                                {
                                    command.CommandText = String.Format(CultureInfo.CurrentCulture, @"
                                        DROP TABLE [{0}_{1}_{2}_Sampling]",
                                        tableSchema, tableName, edgeColumn.Item1);
                                    command.ExecuteNonQuery();

                                    // skip reversed edges since they have no UDF
                                    if (!edgeColumn.Item6)
                                    {
                                        foreach (var it in _currentTableUdf)
                                        {
                                            command.CommandText = string.Format(
                                                @"DROP {2} [{0}_{1}_{3}];",
                                                assemblyName,
                                                edgeColumn.Item1, it.Item1, it.Item2);
                                            command.ExecuteNonQuery();
                                        }
                                    }
                                }
                            }
                            // skip tables which have only reversed edge columns
                            if (edgeColumns.Count(x => !x.Item6) > 0)
                            {
                                command.CommandText = @"DROP ASSEMBLY [" + assemblyName + "_Assembly]";
                                command.ExecuteNonQuery();
                            }
                        }
                        foreach (var table in MetadataTables)
                        {
                            if (table == MetadataTables[4] || table == MetadataTables[5] || table == MetadataTables[6] ||
                                table == MetadataTables[7])
                                continue;
                            command.CommandText = String.Format(CultureInfo.CurrentCulture, @"
                            DELETE FROM [{0}]
                            WHERE [TableName] = @tableName AND [TableSchema] = @tableSchema", table);
                            command.ExecuteNonQuery();
                        }
                    }

                    // drop node table
                    command.CommandText = sqlStr;
                    command.ExecuteNonQuery();
                    foreach (var it in schemaSet)
                    {
                        updateGlobalNodeView(it, tran);
                    }
                    if (externalTransaction == null)
                    {
                        tran.Commit();
                    }
                    return true;
                }
            }
            catch (SqlException e)
            {
                if (externalTransaction == null)
                {
                    tran.Rollback();
                }
                throw new SqlExecutionException("An error occurred when dropping the node table.", e);
            }
        }

        /// <summary>
        /// Creates a stored procedure.
        /// </summary>
        /// <param name="sqlStr"> A create procedure script</param>
        /// <param name="externalTransaction">A SqlTransaction instance under which the create procedure will occur.</param>
        /// <returns>True, if the statement is successfully executed.</returns>
        public bool CreateProcedure(string sqlStr, SqlTransaction externalTransaction = null)
        {
            // get syntax tree of CREATE Procedure command
            var parser = new GraphViewParser();
            var sr = new StringReader(sqlStr);
            IList<ParseError> errors;
            var script = parser.Parse(sr, out errors) as WSqlScript;
            if (errors.Count > 0)
                throw new SyntaxErrorException(errors);

            

            SqlTransaction tran = externalTransaction == null ? Conn.BeginTransaction() : externalTransaction;
            // Translation
            var modVisitor = new TranslateDataModificationVisitor(tran);
            modVisitor.Invoke(script);
            var matchVisitor = new TranslateMatchClauseVisitor(tran);
            matchVisitor.Invoke(script);
            if (script == null)
                return false;
            var statement = script.Batches[0].Statements[0] as WCreateProcedureStatement;
            if (statement == null)
                return false;
            var procName = statement.ProcedureReference.Name;
            if (procName.SchemaIdentifier == null)
                procName.Identifiers.Insert(0, new Identifier { Value = "dbo" });
            bool exists = false;

            try
            {
                using (var cmd = Conn.CreateCommand())
                {
                    cmd.Transaction = tran;

                    cmd.CommandText = script.ToString();
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = string.Format(
                        @"SELECT ProcID FROM {0} WHERE ProcName = @procName AND ProcSchema = @procSchema",
                        MetadataTables[4]);
                    cmd.Parameters.AddWithValue("@procName", procName.BaseIdentifier.Value);
                    cmd.Parameters.AddWithValue("@procSchema", procName.SchemaIdentifier.Value);
                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                            exists = true;
                    }
                    if (!exists)
                    {
                        cmd.CommandText = string.Format(@"
                    INSERT INTO [{0}]([ProcSchema], [ProcName]) VALUES (@schema, @name)", MetadataTables[4]);

                        cmd.Parameters.AddWithValue("@schema", procName.SchemaIdentifier.Value);
                        cmd.Parameters.AddWithValue("@name", procName.BaseIdentifier.Value);
                        cmd.ExecuteNonQuery();
                    }
                    if (externalTransaction == null)
                    {
                        tran.Commit();
                    }
                }
            }
            catch (SqlException e)
            {
                if (externalTransaction == null)
                {
                    tran.Rollback();
                }
                throw new SqlExecutionException("An error occurred when creating the procedure.", e);
            }

            return true;
        }

        /// <summary>
        /// Drops a stored procedure
        /// </summary>
        /// <param name="sqlStr">The script that drops the stored procedure</param>
        /// <param name="externalTransaction">A SqlTransaction instance under which the drop procedure will occur.</param>
        /// <returns>True, if the statement is successfully executed.</returns>
        public bool DropProcedure(string sqlStr, SqlTransaction externalTransaction = null)
        {
            // get syntax tree of DROP TABLE command
            var parser = new GraphViewParser();
            var sr = new StringReader(sqlStr);
            IList<ParseError> errors;
            var script = parser.Parse(sr, out errors) as WSqlScript;
            if (errors.Count > 0)
                throw new SyntaxErrorException(errors);
            if (script == null)
                return false;
            var statement = script.Batches[0].Statements[0] as WDropProcedureStatement;
            if (statement == null)
                return false;

            SqlTransaction tran;
            if (externalTransaction == null)
            {
                tran = Conn.BeginTransaction();
            }
            else
            {
                tran = externalTransaction;
            }

            try
            {
                using (var command = new SqlCommand(null, Conn, tran))
                {
                    // delete metadata
                    foreach (var delObject in statement.Objects)
                    {
                        var proSchema = delObject.SchemaIdentifier == null ? "dbo" : delObject.SchemaIdentifier.Value;
                        var procName = delObject.BaseIdentifier.Value;
                        command.Parameters.Clear();
                        command.CommandText = string.Format(
                            @"DELETE FROM {0} WHERE ProcSchema = @procSchema AND ProcName = @procName",
                            MetadataTables[4]);
                        command.Parameters.AddWithValue("@procSchema", proSchema);
                        command.Parameters.AddWithValue("@procName", procName);
                        command.ExecuteNonQuery();
                    }

                    // drop procedure
                    command.CommandText = sqlStr;
                    command.ExecuteNonQuery();
                    if (externalTransaction == null)
                    {
                        tran.Commit();
                    }
                    return true;
                }
            }
            catch (SqlException e)
            {
                if (externalTransaction == null)
                {
                    tran.Rollback();
                }
                throw new SqlExecutionException("An error occurred when dropping the procedure.", e);
            }
        }

        /// <summary>
        /// Gets a list a table's edge columns
        /// </summary>
        /// <param name="tableSchema">The schema of the target table</param>
        /// <param name="tableName">The table name</param>
        /// <returns>A list of string-boolean-boolean-string-boolean-boolean hextuple, with the first field being the edge column name, 
        /// the second field indicating whether or not the edge points to the nodes in the same node table, 
        /// the third field indicating whether or not this edge is an edge view,
        /// the fourth filed being the edge's udf prefix,
        /// the fifth field indicating whether or not this edge has an corresponding reversed edge
        /// and the sixth field indicating whether or not this edge is a reversed edge.</returns>
        internal IList<Tuple<string, bool, bool, string, bool, bool>> GetGraphEdgeColumns(string tableSchema, string tableName,
            SqlTransaction tx = null)
        {
            var edgeColumns = new List<Tuple<string, bool, bool, string, bool, bool>>();

            using (var command = Conn.CreateCommand())
            {
                command.Transaction = tx;
                command.CommandText = string.Format(
                    @"SELECT ColumnName, TableName, Reference, RefTableSchema, ColumnRole, HasReversedEdge, IsReversedEdge, EdgeUdfPrefix
                  FROM [{0}]
                  WHERE TableSchema = @tableSchema AND TableName = @tableName
                  AND (ColumnRole = @columnRole or ColumnRole=@columnRole2)", MetadataTables[1]);
                command.Parameters.AddWithValue("@columnRole", (int) WNodeTableColumnRole.Edge);
                command.Parameters.AddWithValue("@columnRole2", (int)WNodeTableColumnRole.EdgeView);
                command.Parameters.AddWithValue("@tableSchema", tableSchema);
                command.Parameters.AddWithValue("@tableName", tableName);
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        //string refTableName = null, originalEdgeName = null;
                        //var isRevEdge = bool.Parse(reader["IsReversedEdge"].ToString());
                        //var columnName = reader["ColumnName"].ToString();

                        //if (isRevEdge)
                        //{
                        //    var index = columnName.IndexOf('_');
                        //    refTableName = columnName.Substring(0, index);
                        //    originalEdgeName = columnName.Substring(index + 1,
                        //        columnName.Length - "Reversed".Length - index - 1);
                        //}
                        edgeColumns.Add(Tuple.Create(reader["columnName"].ToString(),
                            reader["Reference"].ToString().ToLower() == reader["TableName"].ToString().ToLower(),
                            (WNodeTableColumnRole) reader["ColumnRole"] == WNodeTableColumnRole.EdgeView,
                            reader["EdgeUdfPrefix"].ToString(),
                            bool.Parse(reader["HasReversedEdge"].ToString()),
                            bool.Parse(reader["IsReversedEdge"].ToString())));
                    }
                }
                return edgeColumns;
            }
        }

//        public void DropAssemblyAndMetaUDFV110(SqlTransaction externalTransaction = null)
//        {
//            SqlTransaction tx;
//            tx = externalTransaction ?? Conn.BeginTransaction();
//            try
//            {
//                using (var command = Conn.CreateCommand())
//                {
//                    command.Transaction = tx;
//                    //Drop assembly and UDF
//                    const string dropAssembly = @"
//                    DROP {0} {1}";
//                    command.CommandText = string.Join("\n", Version110MetaUdf.Select(x => string.Format(dropAssembly, x.Item1, x.Item2)));
//                    command.ExecuteNonQuery();
//                }

//            }
//            catch (Exception e)
//            {
//                if (externalTransaction == null)
//                    tx.Rollback();
//                throw new Exception(e.Message);
//            }
//        }

        /// <summary>
        /// Drop the global UDFs and assembly 
        /// </summary>
        public void DropAssemblyAndMetaUdf(List<Tuple<string, string>> metaUdfList, SqlTransaction externalTransaction = null)
        {
            SqlTransaction tx = externalTransaction ?? Conn.BeginTransaction();
            try
            {
                using (var command = Conn.CreateCommand())
                {
                    command.Transaction = tx;
                    //Drop assembly and UDF
                    const string dropAssembly = @"
                    DROP {0} {1}";
                    command.CommandText = string.Join("\n", metaUdfList.Select(x => string.Format(dropAssembly, x.Item1, x.Item2)));
                    command.ExecuteNonQuery();
                }

            }
            catch (Exception e)
            {
                if (externalTransaction == null)
                    tx.Rollback();
                throw new Exception(e.Message);
            }
        }

        /// <summary>
        /// Gets all node tables in the graph database.
        /// </summary>
        /// <returns>A list of tuples of table schema and table name.</returns>
        public IList<Tuple<string, string>> GetNodeTables(SqlTransaction externalTransaction = null)
        {
            SqlTransaction tx;
            tx = externalTransaction ?? Conn.BeginTransaction();
            var tables = new List<Tuple<string, string>>();
            try
            {
                using (var command = Conn.CreateCommand())
                {
                    command.Transaction = tx;
                    command.CommandText = string.Format(@"SELECT TableSchema, TableName FROM {0}", MetadataTables[0]);
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            tables.Add(new Tuple<string, string>(
                                reader["TableSchema"].ToString(),
                                reader["TableName"].ToString()));
                        }
                    }
                    if (externalTransaction == null)
                        tx.Commit();
                    return tables;
                }
            }
            catch (Exception e)
            {
                if (externalTransaction == null)
                    tx.Rollback();
                throw new Exception(e.Message);
            }

        }

        /// <summary>
        /// Updates a node table's statistics
        /// </summary>
        /// <param name="tableSchema">The schema of the table to be updated.</param>
        /// <param name="tableName">The name of the table to be updated.</param>
        public void UpdateTableStatistics(string tableSchema, string tableName, SqlTransaction externalTransaction = null)
        {
            SqlTransaction tx;
            tx = externalTransaction ?? Conn.BeginTransaction();
            try
            {
                var edgeColumns = GetGraphEdgeColumns(tableSchema, tableName, tx);
                foreach (var edgeColumn in edgeColumns)
                {
                    if (!edgeColumn.Item3)
                        UpdateEdgeSampling(tableSchema, tableName, Tuple.Create(edgeColumn.Item1, edgeColumn.Item4), tx);
                    UpdateEdgeAverageDegree(tableSchema, tableName, edgeColumn.Item1, tx);
                }
                if (externalTransaction == null)
                    tx.Commit();
            }
            catch (Exception e)
            {
                if (externalTransaction == null)
                    tx.Rollback();
                throw new Exception(e.Message);
            }

        }

        /// <summary>
        /// Updates the average degree of an edge column in a node table
        /// </summary>
        /// <param name="tableSchema">The schema of the table</param>
        /// <param name="tableName">The table name</param>
        /// <param name="edgeColumn">The edge name</param>
        /// <param name="tx"></param>
        public void UpdateEdgeAverageDegree(string tableSchema, string tableName, string edgeColumn,
            SqlTransaction tx = null)
        {
            try
            {
                using (var command = Conn.CreateCommand())
                {
                    command.Transaction = tx;
                    command.CommandText = String.Format(CultureInfo.CurrentCulture, @"
                        UPDATE [{3}]
                        SET AverageDegree = AveCnt, SampleRowCount = RowCnt
                        FROM
                        (
                            SELECT ISNULL(AVG(CAST(Cnt AS FLOAT)), 0) as AveCnt,
                            ISNULL(SUM(Cnt), 0) as RowCnt
                            FROM (
            	                SELECT COUNT(src) Cnt
            	                FROM [{0}].[{0}_{1}_{2}_Sampling]
                                GROUP BY src
              	            ) DEGREE
                        ) Edge
                        WHERE TableSchema = @tableSchema AND TableName = @tableName AND ColumnName = @edgeColumn",
                        tableSchema,
                        tableName,
                        edgeColumn,
                        MetadataTables[3]
                        );

                    command.Parameters.Add("@tableSchema", SqlDbType.NVarChar, 128);
                    command.Parameters.Add("@tableName", SqlDbType.NVarChar, 128);
                    command.Parameters.Add("@edgeColumn", SqlDbType.NVarChar, 128);
                    command.Parameters.Add("@GraphDbAverageDegreeSamplingRate", SqlDbType.Float);

                    command.Parameters["@tableSchema"].Value = tableSchema;
                    command.Parameters["@tableName"].Value = tableName;
                    command.Parameters["@edgeColumn"].Value = edgeColumn;
                    command.Parameters["@GraphDbAverageDegreeSamplingRate"].Value = GraphDbAverageDegreeSamplingRate;
                    command.ExecuteNonQuery();
                }
            }
            catch (SqlException e)
            {
                throw new SqlExecutionException("An error occurred when updating edges' average degrees.", e);
            }

        }

        /// <summary>
        /// Updates an edge sample
        /// </summary>
        /// <param name="tableSchema">The schema of the table</param>
        /// <param name="tableName">The table name</param>
        /// <param name="edgeColumn">The edge name and udf prefix in the table</param>
        /// <param name="tx"></param>
        public void UpdateEdgeSampling(string tableSchema, string tableName, Tuple<string, string> edgeColumn, 
            SqlTransaction tx = null)
        {
            try
            {
                using (var command = Conn.CreateCommand())
                {
                    //var isRevEdge = edgeInfo.Item1;
                    //var decoderSchemaName = isRevEdge ? edgeInfo.Item2 : tableSchema;
                    //var decoderTableName = isRevEdge ? edgeInfo.Item3 : tableName;
                    //var decoderEdgeName = isRevEdge ? edgeInfo.Item4 : edgeColumn;
                    //var decoderStr = decoderSchemaName + "_" + decoderTableName + "_" + decoderEdgeName + "_Decoder";

                    command.Transaction = tx;
                    command.CommandText = String.Format(CultureInfo.CurrentCulture, @"
                /*TRUNCATE TABLE [{0}_{1}_{2}_Sampling];
                INSERT INTO [{0}_{1}_{2}_Sampling] --([src], [dst])
            		SELECT [GlobalNodeID] [src], [Edge].*
            		FROM [{0}].[{1}] WITH (NOLOCK)
            		CROSS APPLY {0}_{1}_{2}_Decoder([{2}]) AS Edge
                    WHERE (ABS(CAST(
                            (BINARY_CHECKSUM
                                (GlobalNodeId, NEWID())) as int))
                                % 100) < {3};*/
                /*TRUNCATE TABLE [{0}_{1}_{2}_Sampling];
                INSERT INTO [{0}_{1}_{2}_Sampling]
            		SELECT [GlobalNodeID] [src], Edge.*
            		FROM [{0}].[{1}]
            		TABLESAMPLE ({3} rows) WITH (NOLOCK)
            		CROSS APPLY {0}_{1}_{2}_Decoder([{2}],[{2}DeleteCol]) As Edge*/

                TRUNCATE TABLE [{0}_{1}_{2}_Sampling];
                INSERT INTO [{0}_{1}_{2}_Sampling]
            		SELECT [GlobalNodeID] [src], Edge.*
            		FROM [{0}].[{1}]
            		TABLESAMPLE ({3} rows) WITH (NOLOCK)
            		CROSS APPLY {4}_Decoder([{2}],[{2}DeleteCol]) As Edge
                ",
                        tableSchema,
                        tableName,
                        edgeColumn.Item1,
                        GraphDbEdgeColumnSamplingRate,
                        edgeColumn.Item2);
                    command.ExecuteNonQuery();
                }
            }
            catch (SqlException e)
            {
                throw new SqlExecutionException("An error occurred when updating edge sampling statistics", e);
            }

        }

        /// <summary>
        /// Merges the "delta" of an edge column to the edge's original column and frees the space. 
        /// When an adjacency list is modified, the modification is not applied to the list directly,
        /// but is logged in the list's "delta" column. This method is to merge the delta to the original
        /// edge column and free the space. 
        /// </summary>
        /// <param name="tableSchema">The schema of table to be updated.</param>
        /// <param name="tableName">The table name.</param>
        /// <param name="edgeColumns">The edge columns to be merged</param>
        public void MergeDeleteColumn(string tableSchema, string tableName, 
            IList<Tuple<string, string>> edgeColumns,
            SqlTransaction tx = null)
        {
            //var edgeColumns = GetGraphEdgeColumns(tableSchema, tableName);
            int len = 0;
            if (edgeColumns != null)
                len = edgeColumns.Count;
            //    len = edgeColumns.Length;
            if (len == 0)
                throw new GraphViewException(
                    "Merged edge columns should be specified");
            var setSb = new StringBuilder(1024);
            var whereSb = new StringBuilder(1024);
            int i = 0;
            if (len > 0)
            {
                //bool isReversed = edgeColumns[0].Item5.Item1;
                //var recycleSchemaName = isReversed ? edgeColumns[0].Item5.Item2 : tableSchema;
                //var recycleTableName = isReversed ? edgeColumns[0].Item5.Item3 : tableName;
                //var recycleEdgeName = isReversed ? edgeColumns[0].Item5.Item4 : edgeColumns[0].Item1;
                //var recycleName = recycleSchemaName + "_" + recycleTableName + "_" + recycleEdgeName;
                setSb.AppendFormat(
                    //"{2} = ISNULL(dbo.{0}_{1}_{2}_Recycle({2},{2}DeleteCol),0x), {2}DeleteCol = 0x ",
                    // tableSchema,
                    // tableName, edgeColumns[0].Item1);
                    "{1} = ISNULL(dbo.{0}_Recycle({1},{1}DeleteCol),0x), {1}DeleteCol = 0x ",
                    edgeColumns[0].Item2, edgeColumns[0].Item1);

                whereSb.AppendFormat("LEN({0}DeleteCol) != 0 ", edgeColumns[0].Item1);
                i = 1;
            }
            for (; i < len; i++)
            {
                var edgeColumn = edgeColumns[i];
                //bool isReversed = edgeColumn.Item5.Item1;
                //var schemaName = isReversed ? edgeColumn.Item5.Item2 : tableSchema;
                //var tabName = isReversed ? edgeColumn.Item5.Item3 : tableName;
                //var edgeName = isReversed ? edgeColumn.Item5.Item4 : edgeColumn.Item1;
                //var recycleName = schemaName + "_" + tabName + "_" + edgeName;
                setSb.AppendFormat(
                    //",{2} = ISNULL(dbo.{0}_{1}_{2}_Recycle({2},{2}DeleteCol),0x), {2}DeleteCol = 0x",
                    //tableSchema,
                    //tableName, edgeColumn.Item1);
                    ",{1} = ISNULL(dbo.{0}_Recycle({1},{1}DeleteCol),0x), {1}DeleteCol = 0x",
                    edgeColumn.Item2, edgeColumn.Item1);

                whereSb.AppendFormat("OR LEN({0}DeleteCol) != 0 ", edgeColumn.Item1);
            }

            try
            {
                using (var command = Conn.CreateCommand())
                {
                    command.Transaction = tx;
                    command.CommandText = String.Format(CultureInfo.CurrentCulture, @"
                UPDATE {0}.{1}
                SET {2}
                WHERE {3}
                ",
                        tableSchema,
                        tableName,
                        setSb,
                        whereSb);
                    command.ExecuteNonQuery();
                }
            }
            catch (SqlException e)
            {
                throw new SqlExecutionException(
                    string.Format("An error occurred when merging the deleted column(s) in node table '{0}'", tableName),
                    e);
            }
        }

        /// <summary>
        /// Merges all "delta" columns in a node table and frees the space.
        /// </summary>
        /// <param name="tableSchema">The schema of the table to be updated</param>
        /// <param name="tableName">The table name</param>
        public void MergeAllDeleteColumn(string tableSchema, string tableName)
        {
            //var edgeColumns = GetGraphEdgeColumns(tableSchema, tableName).Select(x => x.Item1).ToArray();
            var edgeColumns =
                GetGraphEdgeColumns(tableSchema, tableName).Select(x => Tuple.Create(x.Item1, x.Item4)).ToList();
            //if (edgeColumns.Length == 0)
            if (edgeColumns.Count == 0)
                throw new GraphViewException(string.Format("Node table '{0}' does not exists", tableName));
            SqlTransaction tx = Conn.BeginTransaction();
            try
            {
                MergeDeleteColumn(tableSchema, tableName, edgeColumns, tx);
                tx.Commit();
            }
            catch (Exception)
            {
                tx.Rollback();
                throw;
            }
        }

        /// <summary>
        /// Releases the unmanaged resources used by GraphViewConnection and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources. </param>
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


        /// <summary>
        /// Releases all resources used by GraphViewConnection.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public SqlDataReader ExecuteReader(string queryString, int timeout = 0 )
        {
            using (var command = new GraphViewCommand(queryString, this))
            {
                command.CommandTimeOut = timeout;
                return command.ExecuteReader();
            }

        }

        public int ExecuteNonQuery(string queryString)
        {
            using (var command = new GraphViewCommand(queryString, this))
            {
                return command.ExecuteNonQuery();
            }
        }

        public bool DropNodeTableFunctionV100(string tableSchema, string tableName, SqlTransaction externalTransaction = null)
        {
            SqlTransaction tran;
            tran = externalTransaction ?? Conn.BeginTransaction();
            try
            {
                // delete metadata
                using (var command = new SqlCommand(null, Conn, tran))
                {
                    var edgeColumns = GetGraphEdgeColumns(tableSchema, tableName, tran);

                    command.Parameters.AddWithValue("@tableName", tableName);
                    command.Parameters.AddWithValue("@tableSchema", tableSchema);

                    var assemblyName = tableSchema + '_' + tableName;
                    foreach (var edgeColumn in edgeColumns.Where(e=>!e.Item3))
                    {
                        if (edgeColumn.Item2)
                        {
                            command.CommandText = string.Format(
                                @"DROP FUNCTION [{0}_{1}_Decoder];
                                  DROP FUNCTION [{0}_{1}_Recycle];
                                  DROP FUNCTION [{0}_{1}_PathDecoder];
                                  DROP FUNCTION [{0}_{1}_bfs];
                                  DROP AGGREGATE [{0}_{1}_Encoder];",
                                assemblyName,
                                edgeColumn.Item1);
                        }
                        else
                        {
                            command.CommandText = string.Format(
                                @"DROP FUNCTION [{0}_{1}_Decoder];
                                  DROP FUNCTION [{0}_{1}_Recycle];
                                  DROP AGGREGATE [{0}_{1}_Encoder];",
                                assemblyName,
                                edgeColumn.Item1);
                        }
                        command.ExecuteNonQuery();
                    }
                    if (edgeColumns.Any())
                    {
                        command.CommandText = @"DROP ASSEMBLY [" + assemblyName + "_Assembly]";
                        command.ExecuteNonQuery();
                    }

                    if (externalTransaction == null)
                    {
                        tran.Commit();
                    }
                    return true;
                }
            }
            catch (SqlException e)
            {
                if (externalTransaction == null)
                {
                    tran.Rollback();
                }
                throw new SqlExecutionException("An error occurred when dropping the node table function.", e);
            }
        }

//        public void UpgradeGraphViewFunctionV100(SqlTransaction externalTransaction = null)
//        {
//            SqlTransaction tx;
//            if (externalTransaction == null)
//            {
//                tx = Conn.BeginTransaction();
//            }
//            else
//            {
//                tx = externalTransaction;
//            }
//            try
//            {
//                using (var command = Conn.CreateCommand())
//                {
//                    command.Transaction = tx;
//                    command.CommandText = string.Format(@"
//                    select nt.TableId, nt.TableSchema, nt.TableName, ntc.ColumnName, ntc.ColumnId, ec.AttributeName, ec.AttributeType,ntc.ColumnRole
//                    from
//                    {0} as nt
//                    join
//                    {1} as ntc
//                    on ntc.TableId = nt.TableId
//                    left join
//                    {2} as ec
//                    on ec.ColumnId = ntc.ColumnId
//                    where ntc.ColumnRole = @role1 or ntc.ColumnRole = @role2
//                    order by ntc.TableId", MetadataTables[0], MetadataTables[1], MetadataTables[2]);
//                        command.Parameters.AddWithValue("@role1", WNodeTableColumnRole.Edge);
//                        command.Parameters.AddWithValue("@role2", WNodeTableColumnRole.NodeId);


//                    string tableSchema = null;
//                    string tableName = null;
//                    string columnName = null;
//                    Dictionary<long, Tuple<string, List<Tuple<string, string>>>> edgeDict =
//                        new Dictionary<long, Tuple<string, List<Tuple<string, string>>>>();
//                    long tableId = -1;
//                    Dictionary<long, Dictionary<long, Tuple<string, List<Tuple<string, string>>>>>
//                        tableColDict =
//                            new Dictionary
//                                <long, Dictionary<long, Tuple<string, List<Tuple<string, string>>>>>();
//                    Dictionary<long, Tuple<string, string, string>> tableInfoDict =
//                        new Dictionary<long, Tuple<string, string, string>>();
//                    using (var reader = command.ExecuteReader())
//                    {
//                        while (reader.Read())
//                        {
//                            long curTableId = (long) reader["TableId"];
//                            if (tableId == -1)
//                            {
//                                tableId = curTableId;
//                                tableSchema = reader["TableSchema"].ToString();
//                                tableName = reader["TableName"].ToString();
//                            }
//                            else if (curTableId != tableId)
//                            {

//                                tableColDict[tableId] = edgeDict;
//                                tableInfoDict[tableId] = new Tuple<string, string, string>(tableSchema, tableName,
//                                    columnName);
//                                tableSchema = reader["TableSchema"].ToString();
//                                tableName = reader["TableName"].ToString();
//                                columnName = null;
//                                edgeDict = new Dictionary<long, Tuple<string, List<Tuple<string, string>>>>();
//                                tableId = curTableId;
//                            }
//                            var role = (WNodeTableColumnRole) reader["ColumnRole"];
//                            if (role == WNodeTableColumnRole.NodeId)
//                            {
//                                columnName = reader["ColumnName"].ToString();
//                                //continue;
//                            }
//                            else if (role == WNodeTableColumnRole.Edge)
//                            {
//                                long colId = (long) reader["ColumnId"];
//                                if (!reader.IsDBNull(5) && !reader.IsDBNull(6))
//                                {
//                                    Tuple<string, List<Tuple<string, string>>> tuple;
//                                    if (edgeDict.TryGetValue(colId, out tuple))
//                                    {
//                                        tuple.Item2.Add(new Tuple<string, string>(reader["AttributeName"].ToString(),
//                                            reader["AttributeType"].ToString().ToLower()));
//                                    }
//                                    else
//                                    {
//                                        edgeDict[colId] =
//                                            new Tuple<string, List<Tuple<string, string>>>(
//                                                reader["ColumnName"].ToString(),
//                                                new List<Tuple<string, string>>
//                                                {
//                                                    new Tuple<string, string>(reader["AttributeName"].ToString(),
//                                                        reader["AttributeType"].ToString().ToLower())
//                                                });
//                                    }
//                                }
//                                else
//                                {
//                                    edgeDict[colId] =
//                                        new Tuple<string, List<Tuple<string, string>>>(reader["ColumnName"].ToString(),
//                                            new List<Tuple<string, string>>());
//                                }
//                            }
//                        }
//                        tableColDict[tableId] = edgeDict;
//                        tableInfoDict[tableId] = new Tuple<string, string, string>(tableSchema, tableName,
//                            columnName);
//                    }
//                    //List<Tuple<string, long, List<Tuple<string, string>>>> edgeList = new List<Tuple<string, long, List<Tuple<string, string>>>>();

//                    foreach (var item in tableColDict)
//                    {
//                        var edgeList =
//                            item.Value.Select(
//                                e =>
//                                    new Tuple<string, int, List<Tuple<string, string>>>(e.Value.Item1, (int)e.Key,
//                                        e.Value.Item2)).ToList();
//                        if (edgeList.Any())
//                        {
//                            var tableInfo = tableInfoDict[item.Key];
//                            var assemblyName = tableInfo.Item1 + '_' + tableInfo.Item2;
//                            GraphViewDefinedFunctionRegister register = new NodeTableRegister(assemblyName, tableInfo.Item2, edgeList,
//                                tableInfo.Item3);
//                            register.Register(Conn, tx);
//                        }

//                    }
//                }

//                if (externalTransaction == null)
//                {
//                    tx.Commit();
//                }
//            }

//            catch (SqlException e)
//            {
//                if (externalTransaction == null)
//                {
//                    tx.Rollback();
//                }
//                throw new SqlExecutionException("An error occurred when upgrading the node table function.", e);
//            }
               
//        }

        public void UpgradeMetaTableV100(SqlTransaction externalTransaction = null)
        {
            SqlTransaction tran;
            if (externalTransaction == null)
            {
                tran = Conn.BeginTransaction();
            }
            else
            {
                tran = externalTransaction;
            }

            const string upgradeScript = @"
            --_NodeTableColumnCollection
            alter table _NodeTableColumnCollection
            add TableId bigint
            go
            update _NodeTableColumnCollection
            set TableId = tid
            from
            (
            select n.TableId as tid, n.TableSchema as ts, n.TableName as tn
            from _NodeTableCollection as n
            ) as ntc
            where ntc.ts = TableSchema and ntc.tn = TableName
            go
            alter table _NodeTableColumnCollection
            alter column TableId bigint not null
            go
            -- _EdgeAttributeCollection
            alter table _EdgeAttributeCollection
            add ColumnId bigint
            go
            update _EdgeAttributeCollection
            set ColumnId = cid
            from
            (
            select n.ColumnId as cid, n.TableSchema as ts, n.TableName as tn, n.ColumnName as cn
            from _NodeTableColumnCollection as n
            ) as ntc
            where ntc.ts = TableSchema and ntc.tn = TableName and ntc.cn = ColumnName
            go
            alter table _EdgeAttributeCollection
            alter column ColumnId bigint not null
            go
            -- _EdgeAverageDegreeCollection
            alter table _EdgeAverageDegreeCollection
            add SampleRowCount int default(1000), ColumnId bigint
            go
            update _EdgeAverageDegreeCollection
            set ColumnId = cid
            from
            (
            select n.ColumnId as cid, n.TableSchema as ts, n.TableName as tn, n.ColumnName as cn
            from _NodeTableColumnCollection as n
            ) as ntc
            where ntc.ts = TableSchema and ntc.tn = TableName and ntc.cn = ColumnName
            go
            alter table _EdgeAverageDegreeCollection
            alter column ColumnId bigint not null
            go";

            try
            {
                using (var command = Conn.CreateCommand())
                {

                    var upgradeQuery = upgradeScript.Split(new string[] {"go"}, StringSplitOptions.None);

                    command.Connection = Conn;
                    command.Transaction = tran;

                    foreach (var query in upgradeQuery)
                    {
                        if (query == "") continue;
                        command.CommandText = query;
                        command.ExecuteNonQuery();

                    }
                }
                if (externalTransaction == null)
                {
                    tran.Commit();
                }

            }
            catch (Exception e)
            {
                if (externalTransaction == null)
                {
                    tran.Rollback();
                }
                throw new SqlExecutionException("An error occurred when upgrading the meta tables.", e);
            }
        }

        public void UpgradeMetaTableV111(SqlTransaction externalTransaction = null)
        {
            SqlTransaction tran = externalTransaction ?? Conn.BeginTransaction();

            const string upgradeScript = @"
            --_NodeTableColumnCollection
            alter table _NodeTableColumnCollection
            add RefTableSchema nvarchar(128) null
            go
            alter table _NodeTableColumnCollection
            add HasReversedEdge bit default 0
            go
            alter table _NodeTableColumnCollection
            add IsReversedEdge bit default 0
            go
            alter table _NodeTableColumnCollection
            add EdgeUdfPrefix nvarchar(512) null
            go
            ";

            try
            {
                using (var command = Conn.CreateCommand())
                {
                    var upgradeQuery = upgradeScript.Split(new string[] { "go" }, StringSplitOptions.None);

                    command.Connection = Conn;
                    command.Transaction = tran;

                    foreach (var query in upgradeQuery)
                    {
                        if (query == "") continue;
                        command.CommandText = query;
                        command.ExecuteNonQuery();
                    }

                    command.CommandText = string.Format(@"
                        UPDATE [{0}]
                        SET [EdgeUdfPrefix] = [TableSchema] + '_' + [TableName] + '_' + [ColumnName]
                        WHERE [ColumnRole] = 1
                    ", MetadataTables[1]);
                    command.ExecuteNonQuery();

                    command.CommandText = string.Format(@"
                        UPDATE [{0}]
                        SET [HasReversedEdge] = 0, [IsReversedEdge] = 0
                    ", MetadataTables[1]);
                    command.ExecuteNonQuery();
                }
                if (externalTransaction == null)
                {
                    tran.Commit();
                }

            }
            catch (Exception e)
            {
                if (externalTransaction == null)
                {
                    tran.Rollback();
                }
                throw new SqlExecutionException("An error occurred when upgrading the meta tables.", e);
            }
        }

        public void UpdateVersionNumber(string versionNumber, SqlTransaction externalTransaction = null)
        {
            SqlTransaction tran;
            if (externalTransaction == null)
            {
                tran = Conn.BeginTransaction();
            }
            else
            {
                tran = externalTransaction;
            }
            try
            {
                using (var command = Conn.CreateCommand())
                {
                    command.Transaction = tran;
                    command.CommandText = string.Format("UPDATE {0} SET VERSION = {1}", VersionTable, versionNumber);
                    command.ExecuteNonQuery();
                }
                currentVersion = versionNumber;
                if (externalTransaction == null)
                {
                    tran.Commit();
                }
            }
            catch (Exception e)
            {
                if (externalTransaction == null)
                {
                    tran.Rollback();
                }
                throw new SqlExecutionException("An error occurred when updateing the version table.", e);
            }
            
        }

        public void UpgradeNodeTableFunction(SqlTransaction externalTransaction = null)
        {
            SqlTransaction tx = externalTransaction ?? Conn.BeginTransaction();

            try
            {
                using (var command = Conn.CreateCommand())
                {
                    command.Transaction = tx;
                    command.CommandText = string.Format(@"
                    select nt.TableId, nt.TableSchema, nt.TableName, ntc.ColumnName, ntc.ColumnId, ec.AttributeName, ec.AttributeType,ntc.ColumnRole
                    from
                    {0} as nt
                    join
                    {1} as ntc
                    on ntc.TableId = nt.TableId
                    left join
                    {2} as ec
                    on ec.ColumnId = ntc.ColumnId
                    where ntc.ColumnRole = @role1 or ntc.ColumnRole = @role2
                    order by ntc.TableId", MetadataTables[0], MetadataTables[1], MetadataTables[2]);
                    command.Parameters.AddWithValue("@role1", WNodeTableColumnRole.Edge);
                    command.Parameters.AddWithValue("@role2", WNodeTableColumnRole.NodeId);


                    string tableSchema = null;
                    string tableName = null;
                    string columnName = null;

                    // [colId, [columnName, [attributeName, attributeType]]]
                    var edgeDict =
                        new Dictionary<long, Tuple<string, List<Tuple<string, string>>>>();
                    long tableId = -1;

                    // [tableId, edgeDict]
                    var tableColDict =
                        new Dictionary
                            <long, Dictionary<long, Tuple<string, List<Tuple<string, string>>>>>();

                    // [tableId, [schema, tableName, columnName]]
                    var tableInfoDict =
                        new Dictionary<long, Tuple<string, string, string>>();

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var curTableId = (long)reader["TableId"];
                            if (tableId == -1)
                            {
                                tableId = curTableId;
                                tableSchema = reader["TableSchema"].ToString();
                                tableName = reader["TableName"].ToString();
                            }
                            else if (curTableId != tableId)
                            {

                                tableColDict[tableId] = edgeDict;
                                tableInfoDict[tableId] = new Tuple<string, string, string>(tableSchema, tableName,
                                    columnName);
                                tableSchema = reader["TableSchema"].ToString();
                                tableName = reader["TableName"].ToString();
                                columnName = null;
                                edgeDict = new Dictionary<long, Tuple<string, List<Tuple<string, string>>>>();
                                tableId = curTableId;
                            }
                            var role = (WNodeTableColumnRole)reader["ColumnRole"];
                            if (role == WNodeTableColumnRole.NodeId)
                            {
                                columnName = reader["ColumnName"].ToString();
                                //continue;
                            }
                            else if (role == WNodeTableColumnRole.Edge)
                            {
                                long colId = (long)reader["ColumnId"];
                                if (!reader.IsDBNull(5) && !reader.IsDBNull(6))
                                {
                                    Tuple<string, List<Tuple<string, string>>> tuple;
                                    if (edgeDict.TryGetValue(colId, out tuple))
                                    {
                                        tuple.Item2.Add(new Tuple<string, string>(reader["AttributeName"].ToString(),
                                            reader["AttributeType"].ToString().ToLower()));
                                    }
                                    else
                                    {
                                        edgeDict[colId] =
                                            new Tuple<string, List<Tuple<string, string>>>(
                                                reader["ColumnName"].ToString(),
                                                new List<Tuple<string, string>>
                                                {
                                                    new Tuple<string, string>(reader["AttributeName"].ToString(),
                                                        reader["AttributeType"].ToString().ToLower())
                                                });
                                    }
                                }
                                else
                                {
                                    edgeDict[colId] =
                                        new Tuple<string, List<Tuple<string, string>>>(reader["ColumnName"].ToString(),
                                            new List<Tuple<string, string>>());
                                }
                            }
                        }
                        tableColDict[tableId] = edgeDict;
                        tableInfoDict[tableId] = new Tuple<string, string, string>(tableSchema, tableName,
                            columnName);
                    }

                    foreach (var item in tableColDict)
                    {
                        var edgeList =
                            item.Value.Select(
                                e =>
                                    new Tuple<string, int, List<Tuple<string, string>>>(e.Value.Item1, (int)e.Key,
                                        e.Value.Item2)).ToList();
                        if (edgeList.Any())
                        {
                            var tableInfo = tableInfoDict[item.Key];
                            var assemblyName = tableInfo.Item1 + '_' + tableInfo.Item2;
                            GraphViewDefinedFunctionRegister register = new NodeTableRegister(assemblyName, tableInfo.Item2, edgeList,
                                tableInfo.Item3);
                            register.Register(Conn, tx);
                        }

                    }
                }

                if (externalTransaction == null)
                {
                    tx.Commit();
                }
            }

            catch (SqlException e)
            {
                if (externalTransaction == null)
                {
                    tx.Rollback();
                }
                throw new SqlExecutionException("An error occurred when upgrading the node table function.", e);
            }

        }
    }
}
