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
using Microsoft.SqlServer.TransactSql.ScriptDom;

// For debugging
using System.Diagnostics;
using System.Security.Authentication.ExtendedProtection;
using System.Text;
using Microsoft.Win32.SafeHandles;
using IsolationLevel = Microsoft.SqlServer.TransactSql.ScriptDom.IsolationLevel;


namespace GraphView
{
    /// <summary>
    /// Graph Database class, providing framework with basic operations on database.
    /// </summary>
    public partial class GraphViewConnection : IDisposable
    {
        /// <summary>
        /// Sampling percent for checking average degree. Set to 100 by default.
        /// </summary>
        public double GraphDbAverageDegreeSamplingRate { get; set; }

        /// <summary>
        /// Sampling percent for edge columns. Set to 100 by default.
        /// </summary>
        public double GraphDbEdgeColumnSamplingRate { get; set; }

        /// <summary>
        /// Connection to database
        /// </summary>
        public SqlConnection Conn { get; private set; }

        /// <summary>
        /// When set to true, database will check validity if DbInit is set to false.
        /// </summary>
        public bool Overwrite { get; set; }

        private bool _disposed;

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
                "_NodeViewCollection",
            };

        private static readonly string Version = "VERSION";

        private static readonly string version = "1.00";
        internal const string GraphViewUdfAssemblyName = "GraphViewUDF";

        /// <summary>
        /// Initializes a new instance of the GraphViewConnection class.
        /// </summary>
        public GraphViewConnection()
        {
            Overwrite = false;
            _disposed = false;
            Conn = new SqlConnection();
        }

        /// <summary>
        /// Initializes a new connection to a graph database.
        /// The database could be a SQL Server instance or Azure SQL Database, as specified by the connection string.
        /// </summary>
        /// <param name="connectionString">The connection string of the SQL database.</param>
        public GraphViewConnection(string connectionString)
        {
            _disposed = false;
            Conn = new SqlConnection(connectionString);
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
        public SqlTransaction BeginTransaction(System.Data.IsolationLevel level, string tranName)
        {
            return Conn.BeginTransaction(level, tranName);
        }

        /// <summary>
        /// Starts a database transaction with the specified isolation level.
        /// </summary>
        /// <param name="level"></param>
        /// <returns></returns>
        public SqlTransaction BeginTransaction(System.Data.IsolationLevel level)
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
        /// Initialize a graph database, including table ID, graph column and edge attribute information.
        /// </summary>
        internal void CreateMetadata()
        {
            var tx = Conn.BeginTransaction();
            try
            {
                using (var command = new SqlCommand(null, Conn))
                {
                    command.Transaction = tx;
                    command.CommandText = string.Format(@"
                        CREATE TABLE [{0}] (
                            [ColumnId] [bigint] NOT NULL IDENTITY(0, 1),
                            [TableSchema] [nvarchar](128) NOT NULL,
                            [TableName] [nvarchar](128) NOT NULL,
                            [ColumnName] [nvarchar](128) NOT NULL,
                            [ColumnRole] [int] NOT NULL,
                            [Reference] [nvarchar](128) NULL,
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
                            [AverageDegree] [float] DEFAULT(5),
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
                        INSERT INTO [{0}] VALUES({1})";
                    command.CommandText = string.Format(createVersionTable, Version, version);
                    command.ExecuteNonQuery();
                }
                const string assemblyName = GraphViewUdfAssemblyName;
                //var edgeDictionary = new List<Tuple<string, bool, List<Tuple<string, string>>>>
                //{
                //    new Tuple<string, bool, List<Tuple<string, string>>>("GlobalNodeId",false, new List<Tuple<string, string>>())
                //};
                GraphViewDefinedFunctionGenerator.MetaRegister(assemblyName, Conn, tx);
                tx.Commit();
            }
            catch (SqlException e)
            {
                tx.Rollback();
                throw new SqlExecutionException("Failed to create necessary meta-data or system-reserved functions.", e);
            }
        }

        /// <summary>
        /// Clears all the node table data in GraphView.
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
                //dropEdgeView
                const string dropEdgeView = @"
                SELECT TableSchema, TableName, ColumnName
                FROM [dbo].[{0}]
                WHERE ColumnRole = 3";
                command.CommandText = string.Format(dropEdgeView, MetadataTables[1]);

                var edgeViewList = new List<Tuple<string, string, string>>();
                //The list of procedure with schema, table and Column name
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableSchema = reader["TableSchema"].ToString();
                        var tableName = reader["TableName"].ToString();
                        var columnName = reader["ColumnName"].ToString();
                        edgeViewList.Add(Tuple.Create(tableSchema, tableName, columnName));
                    }
                }

                foreach (var it in edgeViewList)
                {
                    DropEdgeView(it.Item1, it.Item2, it.Item3, transaction);
                }

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
                command.CommandText = string.Format(dropVersionTable, Version);
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
                const string dropAssembly = @"
                DROP AGGREGATE GraphViewUDFGlobalNodeIdEncoder
                DROP AGGREGATE GraphViewUDFEdgeIdEncoder
                DROP FUNCTION SingletonTable
                DROP FUNCTION DownSizeFunction
                DROP FUNCTION UpSizeFunction
                DROP ASSEMBLY GraphViewUDFAssembly";
                command.CommandText = dropAssembly;
                command.ExecuteNonQuery();
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
            try
            {
                Conn.Open();
                if (!CheckDatabase())
                {
                    CreateMetadata();
                }
            }
            catch (SqlException e)
            {
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
            }
            catch (SqlException e)
            {
                throw new SqlExecutionException("An error occurred when closing a database connection", e);
            }
        }

        /// <summary>
        /// Validates graph database by checking if metadata table exists.
        /// </summary>
        /// <returns>true if graph database is valid; otherwise false.</returns>
        private bool CheckDatabase()
        {
            var tableString = String.Join(", ", MetadataTables.Select(x => "'" + x + "'"));
            using (var command = Conn.CreateCommand())
            {

                command.CommandText = String.Format(CultureInfo.CurrentCulture, @"
                    SELECT COUNT([name]) cnt
                    FROM sysobjects
                    WHERE [type] = @type AND [category] = @category AND
                    [name] IN ({0})
                ", tableString);

                command.Parameters.Add("@type", SqlDbType.NVarChar, 2);
                command.Parameters.Add("@category", SqlDbType.Int);
                command.Parameters["@type"].Value = "U";
                command.Parameters["@category"].Value = 0;

                using (var reader = command.ExecuteReader())
                {
                    if (!reader.Read())
                        return false;
                    return Convert.ToInt32(reader["cnt"], CultureInfo.CurrentCulture) == MetadataTables.Count;
                }
            }
        }

        /// <summary>
        /// Creates node table and inserts related metadata.
        /// </summary>
        /// <param name="sqlStr">A CREATE TABLE statement with metadata.</param>
        /// <param name="externalTransaction">An existing SqlTransaction instance under which the create node table will occur.</param>
        /// <returns>Returns true if the statement is successfully executed.</returns>
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
                        var tableId = Convert.ToInt64(reader["TableId"], CultureInfo.CurrentCulture) << 48;
                        tableIdentitySeed = new WValueExpression(tableId.ToString(CultureInfo.InvariantCulture), false);
                    }

                    // create graph table
                    var wColumnDefinition = statement.Definition.ColumnDefinitions
                        .FirstOrDefault(x => x.ColumnIdentifier.Value == "GlobalNodeId");
                    if (wColumnDefinition != null)
                        wColumnDefinition.IdentityOptions.IdentitySeed = tableIdentitySeed;
                    command.CommandText = statement.ToString();
                    command.ExecuteNonQuery();
                }

                using (var command = new SqlCommand(null, Conn))
                {
                    command.Transaction = tx;
                    // insert graph column
                    command.CommandText = string.Format(@"
                    INSERT INTO [{0}]
                    ([TableSchema], [TableName], [ColumnName], [ColumnRole], [Reference])
                    VALUES (@tableSchema, @tableName, @columnName, @columnRole, @ref)", MetadataTables[1]);

                    command.Parameters.AddWithValue("@tableSchema", tableSchema);
                    command.Parameters.AddWithValue("@tableName", tableName);

                    command.Parameters.Add("@columnName", SqlDbType.NVarChar, 128);
                    command.Parameters.Add("@columnRole", SqlDbType.Int);
                    command.Parameters.Add("@ref", SqlDbType.NVarChar, 128);

                    //command.Parameters["@columnName"].Value = "NodeId";
                    //command.Parameters["@columnRole"].Value = (int) WGraphTableColumnRole.NodeId;
                    //command.Parameters["@ref"].Value = SqlChars.Null;
                    //command.ExecuteNonQuery();


                    foreach (var column in columns)
                    {
                        command.Parameters["@columnName"].Value = column.ColumnName.Value;
                        command.Parameters["@columnRole"].Value = (int) column.ColumnRole;
                        var edgeColumn = column as WGraphTableEdgeColumn;
                        if (edgeColumn != null)
                        {
                            command.Parameters["@ref"].Value =
                                (edgeColumn.TableReference as WNamedTableReference).ExposedName.Value;
                        }
                        else
                        {
                            command.Parameters["@ref"].Value = SqlChars.Null;
                        }

                        command.ExecuteNonQuery();
                    }

                    command.CommandText = string.Format(@"
                    INSERT INTO [{0}]
                    ([TableSchema], [TableName], [ColumnName], [AverageDegree])
                    VALUES (@tableSchema, @tableName, @columnName, @AverageDegree)", MetadataTables[3]);
                    command.Parameters.Add("@AverageDegree", SqlDbType.Int);
                    command.Parameters["@AverageDegree"].Value = 5;
                    foreach (var column in columns.OfType<WGraphTableEdgeColumn>())
                    {
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
                    ([TableSchema], [TableName], [ColumnName], [AttributeName], [AttributeType], [AttributeEdgeId])
                    VALUES (@tableSchema, @tableName, @columnName, @attrName, @attrType, @attrId)", MetadataTables[2]);
                    command.Parameters.AddWithValue("@tableSchema", tableSchema);
                    command.Parameters.AddWithValue("@tableName", tableName);

                    command.Parameters.Add("@columnName", SqlDbType.NVarChar, 128);
                    command.Parameters.Add("@attrName", SqlDbType.NVarChar, 128);
                    command.Parameters.Add("@attrType", SqlDbType.NVarChar, 128);
                    command.Parameters.Add("@attrId", SqlDbType.Int);

                    var createOrder = 1;
                    foreach (var column in columns.OfType<WGraphTableEdgeColumn>())
                    {
                        command.Parameters["@columnName"].Value = column.ColumnName.Value;
                        foreach (var attr in column.Attributes)
                        {
                            command.Parameters["@attrName"].Value = attr.Item1.Value;
                            command.Parameters["@attrType"].Value = attr.Item2.ToString();
                            command.Parameters["@attrId"].Value = (createOrder++).ToString();
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
                                new Tuple<string, bool, List<Tuple<string, string>>>(col.ColumnName.Value,
                                    String.Equals(tableName,
                                        (col.TableReference as WNamedTableReference).ExposedName.Value,
                                        StringComparison.CurrentCultureIgnoreCase),
                                    col.Attributes.Select(
                                        x =>
                                            new Tuple<string, string>(x.Item1.Value,
                                                x.Item2.ToString().ToLower(CultureInfo.CurrentCulture)))
                                        .ToList())).ToList();
                if (edgeDict.Count > 0)
                {
                    var assemblyName = tableSchema + '_' + tableName;
                    GraphViewDefinedFunctionGenerator.NodeTableRegister(assemblyName, tableName, edgeDict, Conn, tx);
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
                throw new SqlExecutionException("An error occurred when creating the node table.", e);
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

                    foreach (var obj in statement.Objects)
                    {
                        var tableName = obj.BaseIdentifier.Value;
                        var tableSchema = obj.SchemaIdentifier != null
                            ? obj.SchemaIdentifier.Value
                            : "dbo";
                        var edgeColumns = GetGraphEdgeColumns(tableSchema, tableName, tran);

                        command.Parameters.AddWithValue("@tableName", tableName);
                        command.Parameters.AddWithValue("@tableSchema", tableSchema);

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

                        foreach (var edgeColumn in edgeColumns)
                        {
                            command.CommandText = String.Format(CultureInfo.CurrentCulture, @"
                                DROP TABLE [{0}_{1}_{2}_Sampling]",
                                tableSchema, tableName, edgeColumn.Item1);
                            command.ExecuteNonQuery();
                        }

                        var assemblyName = tableSchema + '_' + tableName;
                        foreach (var edgeColumn in edgeColumns)
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

                        if (edgeColumns.Count == 0)
                            continue;
                        command.CommandText = @"DROP ASSEMBLY [" + assemblyName + "_Assembly]";
                        command.ExecuteNonQuery();

                    }

                    // drop node table
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
                throw new SqlExecutionException("An error occurred when dropping the node table.", e);
            }
        }

        /// <summary>
        /// Create procedure and related metadata.
        /// </summary>
        /// <param name="sqlStr"> A create procedure statement with metadata.</param>
        /// <param name="externalTransaction">An existing SqlTransaction instance under which the create procedure will occur.</param>
        /// <returns>Returns true if the statement is successfully executed.</returns>
        public bool CreateProcedure(string sqlStr, SqlTransaction externalTransaction = null)
        {
            // get syntax tree of CREATE Procedure command
            var parser = new GraphViewParser();
            var sr = new StringReader(sqlStr);
            IList<ParseError> errors;
            var script = parser.Parse(sr, out errors) as WSqlScript;
            if (errors.Count > 0)
                throw new SyntaxErrorException(errors);

            // Translation
            var modVisitor = new TranslateDataModificationVisitor(Conn);
            modVisitor.Invoke(script);
            var matchVisitor = new TranslateMatchClauseVisitor(Conn);
            matchVisitor.Invoke(script);
            if (script == null)
                return false;
            var statement = script.Batches[0].Statements[0] as WCreateProcedureStatement;
            if (statement == null)
                return false;
            var procName = statement.ProcedureReference.Name;
            if (procName.SchemaIdentifier == null)
                procName.Identifiers.Insert(0, new Identifier {Value = "dbo"});
            bool exists = false;

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
        /// Drops procedure and related metadata.
        /// </summary>
        /// <param name="sqlStr"> Name of procedure to be dropped.</param>
        /// <param name="externalTransaction">An existing SqlTransaction instance under which the drop procedure will occur.</param>
        /// <returns>Returns true if the statement is successfully executed.</returns>
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
        /// Gets names of edge columns of a table
        /// </summary>
        /// <param name="tableSchema">Schema of table</param>
        /// <param name="tableName">Name of table</param>
        /// <returns>List of names of edge columns and booleans indicate whether the edge has source and sink in same node table or not</returns>
        public IList<Tuple<string, bool>> GetGraphEdgeColumns(string tableSchema, string tableName,
            SqlTransaction tx = null)
        {
            var edgeColumns = new List<Tuple<string, Boolean>>();
            using (var command = Conn.CreateCommand())
            {
                command.Transaction = tx;
                command.CommandText = string.Format(
                    @"SELECT ColumnName, TableName, Reference
                  FROM [{0}]
                  WHERE TableSchema = @tableSchema AND TableName = @tableName
                  AND ColumnRole = @columnRole", MetadataTables[1]);
                command.Parameters.AddWithValue("@columnRole", (int) WNodeTableColumnRole.Edge);
                command.Parameters.AddWithValue("@tableSchema", tableSchema);
                command.Parameters.AddWithValue("@tableName", tableName);
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        edgeColumns.Add(Tuple.Create(reader["ColumnName"].ToString(),
                            reader["Reference"].ToString().ToLower() == reader["TableName"].ToString().ToLower()));
                    }
                }
                return edgeColumns;
            }
        }

        /// <summary>
        /// Get all node tables in the graph database.
        /// </summary>
        /// <returns>List of tuples of table schema and table name.</returns>
        public IList<Tuple<string, string>> GetNodeTables()
        {
            var tables = new List<Tuple<string, string>>();
            using (var command = Conn.CreateCommand())
            {

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
                return tables;
            }
        }

        /// <summary>
        /// Update table Statistics
        /// </summary>
        /// <param name="tableSchema">Schema of table to be updated.</param>
        /// <param name="tableName">Name of table to be updated.</param>
        public void UpdateTableStatistics(string tableSchema, string tableName)
        {
            SqlTransaction tx = Conn.BeginTransaction();
            try
            {
                var edgeColumns = GetGraphEdgeColumns(tableSchema, tableName, tx).Select(x => x.Item1);
                foreach (var edgeColumn in edgeColumns)
                {
                    UpdateTableEdgeSampling(tableSchema, tableName, edgeColumn, tx);
                    UpdateEdgeAverageDegree(tableSchema, tableName, edgeColumn, tx);
                }
                tx.Commit();
            }
            catch (Exception)
            {
                tx.Rollback();
                throw;
            }

        }

        /// <summary>
        /// Updates table edges average degree statistics
        /// </summary>
        /// <param name="tableSchema">Schema of the table</param>
        /// <param name="tableName">Name of the table</param>
        /// <param name="edgeColumn">Name of edge in the table</param>
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
                    SET AverageDegree = (
                        SELECT ISNULL(AVG(CAST(Cnt AS FLOAT)), 0)
                        FROM (
            	            SELECT COUNT(src) Cnt
            	            FROM [{0}].[{0}_{1}_{2}_Sampling]
                            GROUP BY src
              	        ) DEGREE
                    )
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
                throw new SqlExecutionException("An error occurred when updating statistics on edge average degree", e);
            }

        }

        /// <summary>
        /// Updates table edges average degree statistics
        /// </summary>
        /// <param name="tableSchema">Schema of the table</param>
        /// <param name="tableName">Name of the table</param>
        /// <param name="edgeColumn">Name of edge in the table</param>
        /// <param name="tx"></param>
        public void UpdateTableEdgeSampling(string tableSchema, string tableName, string edgeColumn,
            SqlTransaction tx = null)
        {

            try
            {
                using (var command = Conn.CreateCommand())
                {
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
                TRUNCATE TABLE [{0}_{1}_{2}_Sampling];
                INSERT INTO [{0}_{1}_{2}_Sampling]
            		SELECT [GlobalNodeID] [src], Edge.*
            		FROM [{0}].[{1}]
            		TABLESAMPLE ({3} rows) WITH (NOLOCK)
            		CROSS APPLY {0}_{1}_{2}_Decoder([{2}],[{2}DeleteCol]) As Edge
                ",
                        tableSchema,
                        tableName,
                        edgeColumn,
                        GraphDbEdgeColumnSamplingRate);
                    command.ExecuteNonQuery();
                }
            }
            catch (SqlException e)
            {
                throw new SqlExecutionException("An error occurred when updating edge sampling statistics", e);
            }

        }

        /// <summary>
        /// Merge Specific Delete Columns in a table
        /// </summary>
        /// <param name="tableSchema">Schema of table to be updated.</param>
        /// <param name="tableName">Name of table to be updated.</param>
        /// <param name="edgeColumns">Edge columns to be merged</param>
        /// <param name="delReverse">If it is set to true,merge the reversedEdge Column</param>
        public void MergeDeleteColumn(string tableSchema, string tableName, string[] edgeColumns,
            SqlTransaction tx = null)
        {
            //var edgeColumns = GetGraphEdgeColumns(tableSchema, tableName);
            int len = 0;
            if (edgeColumns != null)
                len = edgeColumns.Length;
            if (len == 0)
                throw new GraphViewException(
                    "Merged edge columns should be specified");
            var setSb = new StringBuilder(1024);
            var whereSb = new StringBuilder(1024);
            int i = 0;
            if (len > 0)
            {
                setSb.AppendFormat(
                    "{2} = ISNULL(dbo.{0}_{1}_{2}_Recycle({2},{2}DeleteCol),0x), {2}DeleteCol = 0x ",
                    tableSchema,
                    tableName, edgeColumns[0]);
                whereSb.AppendFormat("LEN({0}DeleteCol) != 0 ", edgeColumns[0]);
                i = 1;
            }
            for (; i < len; i++)
            {
                var edgeColumn = edgeColumns[i];
                setSb.AppendFormat(
                    ",{2} = ISNULL(dbo.{0}_{1}_{2}_Recycle({2},{2}DeleteCol),0x), {2}DeleteCol = 0x",
                    tableSchema,
                    tableName, edgeColumn);
                whereSb.AppendFormat("OR LEN({0}DeleteCol) != 0 ", edgeColumn);
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
        /// Merge All Delete Columns in a table
        /// </summary>
        /// <param name="tableSchema">Schema of table to be updated.</param>
        /// <param name="tableName">Name of table to be updated.</param>
        public void MergeAllDeleteColumn(string tableSchema, string tableName)
        {
            var edgeColumns = GetGraphEdgeColumns(tableSchema, tableName).Select(x => x.Item1).ToArray();
            if (edgeColumns.Length == 0)
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

    }
}
