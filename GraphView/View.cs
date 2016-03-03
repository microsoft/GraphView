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
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Mime;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Win32.SafeHandles;

namespace GraphView
{
    public partial class GraphViewConnection : IDisposable
    {
        private string _nodeName;
        private string _tableSet;

        private static readonly List<string> ColumnList =
            new List<string>
            {
                "GlobalNodeId",
                "InDegree",
                "LocalNodeId"
            };
        //For node View
        private Dictionary<string, Int64> _dictionaryTableId; // <Table Name> => <Table Id>
        private Dictionary<string, int> _dictionaryTableOffsetId; // <Table Name> => <Table Offset Id> (counted from 0)

        private Dictionary<Tuple<string, string>, Int64> _dictionaryColumnId;
        // <Table Name, Column Name> => <Column Id>

        private List<Tuple<string, List<Tuple<string, string>>>> _propertymapping;


        //For edge view
        private Dictionary<Tuple<string, string>, long> _edgeColumnToColumnId; //<NodeTable, Edge> => ColumnId
        private Dictionary<string, List<long>> _dictionaryAttribute; //<EdgeViewAttributeName> => List<AttributeId>
        private Dictionary<string, string> _attributeType; //<EdgeViewAttribute> => <Type>

        //For dropping edge view
        private static readonly List<string> EdgeViewFunctionList =
            new List<string>()
            {
                "Decoder",
                "ExclusiveEdgeGenerator",
                "bfsPath",
                "bfsPathWithMessage",
                "PathMessageEncoder",
                "PathMessageDecoder"
            };

        /// <summary>
        /// Creates node view.
        /// This operation creates view on nodes, mapping native node table propeties into node view 
        /// and merging all the edge columns relevant to the nodes in the view.
        /// </summary>
        /// <param name="tableSchema"> The Schema name of node table. Default(null or "") by "dbo".</param>
        ///  <param name="nodeViewName"> The name of supper node. </param>
        /// <param name="nodes"> The list of the names of native nodes. </param>
        /// <param name="propertymapping"> 
        /// Type is List<Tuple<string, List<Tuple<string, string>>>>
        /// That is, List<Tuple<nodeViewpropety, List<Tuple<native node table name, native node table propety>>>> 
        /// </param>
        /// <param name="externalTransaction">An existing SqlTransaction instance under which the create node view will occur.</param>
        public void CreateNodeView(string tableSchema, string nodeViewName, List<string> nodes,
            List<Tuple<string, List<Tuple<string, string>>>> propertymapping = null,
            SqlTransaction externalTransaction = null)
        {
            //Check validity of input
            if (string.IsNullOrEmpty(tableSchema))
            {
                tableSchema = "dbo";
            }
            if (string.IsNullOrEmpty(nodeViewName))
            {
                throw new EdgeViewException("The string of supper node name is null or empty.");
            }
            if (nodes == null || !nodes.Any())
            {
                throw new EdgeViewException("The list of nodes is null or empty.");
            }

            var transaction = externalTransaction ?? Conn.BeginTransaction();
            try
            {
                bool byDefault = propertymapping == null;
                CreateNodeViewWithoutRecord(tableSchema, nodeViewName, nodes, propertymapping, transaction);
                UpdateNodeViewMetatable(tableSchema, nodeViewName, nodes, _propertymapping, transaction);
                if (byDefault)
                {
                    CreateEdgeViewByDefault(tableSchema, nodeViewName, transaction);
                }
                if (externalTransaction == null)
                {
                    transaction.Commit();
                }
            }
            catch (Exception error)
            {
                if (externalTransaction == null)
                {
                    transaction.Rollback();
                }
                throw new NodeViewException("Create node view:" + error.Message);
            }
        }

        /// <summary>
        /// Creates node view without updating metadatatable.
        /// </summary>
        /// <param name="tableSchema"> The Schema name of node table. Default(null or "") by "dbo".</param>
        ///  <param name="nodeViewName"> The name of supper node. </param>
        /// <param name="nodes"> The list of the names of native nodes. </param>
        /// <param name="propertymapping"> 
        /// Type is List<Tuple<string, List<Tuple<string, string>>>>
        /// That is, List<Tuple<nodeViewpropety, List<Tuple<native node table name, native node table propety>>>> 
        /// </param>
        /// <param name="externalTransaction">An existing SqlTransaction instance under which the create node view will occur.</param>
        private void CreateNodeViewWithoutRecord(string tableSchema, string nodeViewName, List<string> nodes,
            List<Tuple<string, List<Tuple<string, string>>>> propertymapping = null,
            SqlTransaction externalTransaction = null)
        {
            var transaction = externalTransaction ?? Conn.BeginTransaction();
            var command = Conn.CreateCommand();
            command.Transaction = transaction;


            _dictionaryTableId = new Dictionary<string, long>(); //Also for searching table record in meta table
            _dictionaryTableOffsetId = new Dictionary<string, int>(); //Also for searching user-given node table
            _dictionaryColumnId = new Dictionary<Tuple<string, string>, long>();
            //Also for searching user-given node view's property

            try
            {
                _tableSet = "#" + RandomString();
                //Deal with the null propertymapping
                if (propertymapping == null)
                {
                    var columnToType = new Dictionary<string, Tuple<string, string>>(); //node view column => <datatype, length>
                    var columnToColumns = new Dictionary<string, List<Tuple<string, string>>>(); //node view column => <table, column>
                    string createTempTable = @"create table {0} (TableName varchar(4000))";
                    command.Parameters.Clear();
                    command.CommandText = string.Format(createTempTable, _tableSet);
                    command.ExecuteNonQuery();

                    DataTable table = new DataTable(_tableSet); //_NodeViewColumnCollection
                    DataColumn column;
                    DataRow row;
                    column = new DataColumn("TableName", Type.GetType("System.String"));
                    table.Columns.Add(column);

                    foreach (var it in nodes)
                    {
                        row = table.NewRow();
                        row["TableName"] = it;
                        table.Rows.Add(row);
                    }

                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Conn, SqlBulkCopyOptions.Default, transaction))
                    {
                        bulkCopy.DestinationTableName = _tableSet;
                        bulkCopy.WriteToServer(table);
                    }

                    const string getColumnList = @"
                    Select TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                    From {0} NTC
                    Join {1} NTCC
                    On NTC.TableId = NTCC.TableId
                    Join INFORMATION_SCHEMA.COLUMNS IC
                    On IC.TABLE_SCHEMA = NTC.TableSchema and IC.TABLE_NAME = NTC.TableName and IC.COLUMN_NAME = NTCC.ColumnName
                    Join {2} Temp 
                    On TABLE_NAME = Temp.TableName
                    Where NTC.TableSchema = @schema and (ColumnRole = @role1 or ColumnRole = @role2)";

                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("schema", tableSchema);
                    command.Parameters.AddWithValue("role1", WNodeTableColumnRole.Property);
                    command.Parameters.AddWithValue("role2", WNodeTableColumnRole.NodeId);
                    command.CommandText = string.Format(getColumnList, MetadataTables[0], MetadataTables[1], _tableSet);

                    Tuple<string, string> wrongTuple = Tuple.Create("wrong", "wrong");
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var tableName = reader["TABLE_NAME"].ToString().ToLower();
                            var columnName = reader["COLUMN_NAME"].ToString().ToLower();
                            var dataType = reader["DATA_TYPE"].ToString().ToLower();
                            var maximumLength = reader["CHARACTER_MAXIMUM_LENGTH"].ToString();
                            var type = Tuple.Create(dataType, maximumLength);
                            var newColumnName = columnName;
                            if (columnToType.ContainsKey(columnName))
                            {
                                if (!columnToType[columnName].Equals(type))
                                {
                                    if (!columnToType[columnName].Equals(wrongTuple))
                                    {
                                        var tempType = columnToType[columnName];
                                        newColumnName = columnName + "_" + tempType.Item1 + (string.IsNullOrEmpty(tempType.Item2)
                                        ? ""
                                        : ("_" + (tempType.Item2 == "-1" ? "max" : tempType.Item2)));
;
                                        columnToColumns[newColumnName] = columnToColumns[columnName];
                                        columnToType[newColumnName] = tempType;

                                        columnToType[columnName] = Tuple.Create("wrong", "wrong");
                                        columnToColumns[columnName] = new List<Tuple<string, string>>();
                                        columnToColumns[columnName].Add(wrongTuple);
                                    }
                                    var typeName = type.Item1 + (string.IsNullOrEmpty(maximumLength)
                                        ? ""
                                        : ("_" + (maximumLength == "-1" ? "max" : maximumLength)));
                                    newColumnName = columnName + "_" + typeName;
                                    if (!columnToType.ContainsKey(newColumnName))
                                    {
                                        columnToType[newColumnName] = type;
                                        columnToColumns[newColumnName] = new List<Tuple<string, string>>();
                                    }
                                    //var warning =
                                    //    new WarningException(
                                    //        string.Format(
                                    //            "Warning: The column \"{0}\" in node view has different datatypes in at least two base tables",
                                    //            columnName));
                                    //Console.WriteLine(warning.Message);
                                }
                            }
                            else
                            {
                                columnToType[newColumnName] = type;
                                columnToColumns[newColumnName] = new List<Tuple<string, string>>();
                            }
                            columnToColumns[newColumnName].Add(Tuple.Create(tableName, columnName));
                        }
                    }
                    propertymapping =
                        columnToColumns.Where(x => x.Value[0] != wrongTuple)
                            .Select(x => Tuple.Create(x.Key, x.Value.Select(y => Tuple.Create(y.Item1, y.Item2)).ToList()))
                            .ToList();
                }

                //Check validity of the list of node tables and get their table ID.
                const string getTableId = @"
                Select TableName, TableId
                From [dbo].{0}
                Where TableSchema = @schema";
                command.Parameters.Clear();
                command.CommandText = String.Format(getTableId, MetadataTables[0]); //GraphTable
                command.Parameters.Add("schema", SqlDbType.NVarChar, 128);
                command.Parameters["schema"].Value = tableSchema;

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableName = reader["TableName"].ToString().ToLower();
                        var tableId = Convert.ToInt64(reader["TableId"].ToString());
                        if (nodes.Exists(x => (x.ToLower() == tableName)))
                        {
                            _dictionaryTableId[tableName] = tableId;
                        }
                    }
                }

                if (_dictionaryTableId.Count() != nodes.Count())
                {
                    foreach (var it in nodes)
                    {
                        if (!_dictionaryTableId.ContainsKey(it))
                        {
                            throw new NodeViewException(string.Format("The graph table [{0}].[{1}] is not found",
                                tableSchema, it));
                        }
                    }
                }

                var count = 0;
                foreach (var it in nodes)
                {
                    if (_dictionaryTableOffsetId.ContainsKey(it.ToLower()))
                    {
                        throw new NodeViewException(string.Format("Node table {0} is given twice", it));
                    }
                    _dictionaryTableOffsetId[it.ToLower()] = count;
                    count++;
                }

                //Check validity of the proper mapping and get their column ID.
                foreach (var it in propertymapping)
                {
                    foreach (var VARIABLE in it.Item2)
                    {
                        _dictionaryColumnId[Tuple.Create(VARIABLE.Item1.ToLower(), VARIABLE.Item2.ToLower())] = -1;
                    }
                }

                const string getColumnId = @"
                Select ColumnId, TableName, ColumnName, ColumnRole
                From {0}
                Where TableSchema = @schema";
                command.CommandText = string.Format(getColumnId, MetadataTables[1]); //_NodeTableColumnCollection

                var nodeTableToUserId = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var edgeCount = 0;
                var edgeList = new List<Tuple<string, string>>[nodes.Count];
                for (int i = 0; i < nodes.Count; i++)
                {
                    edgeList[i] = new List<Tuple<string, string>>();
                }

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableName = reader["TableName"].ToString().ToLower();
                        var columnId = Convert.ToInt64(reader["ColumnId"].ToString());
                        var columnName = reader["ColumnName"].ToString().ToLower();
                        var columnRole = Convert.ToInt32(reader["ColumnRole"].ToString());

                        if (columnRole == (int) WNodeTableColumnRole.NodeId || columnRole == (int) WNodeTableColumnRole.Property)
                        {
                            var columnTuple = Tuple.Create(tableName, columnName);
                            if (_dictionaryColumnId.ContainsKey(columnTuple))
                            {
                                _dictionaryColumnId[columnTuple] = columnId;
                            }
                        }
                        else
                        {
                            if (columnRole == (int) WNodeTableColumnRole.Edge && _dictionaryTableOffsetId.ContainsKey(tableName))
                            {
                                edgeList[_dictionaryTableOffsetId[tableName]].Add(Tuple.Create(tableName, columnName));
                                edgeCount++;
                            }
                        }
                        if (columnRole == (int) WNodeTableColumnRole.NodeId)
                        {
                            nodeTableToUserId[tableName] = columnName;
                        }
                    }
                }

                foreach (var it in _dictionaryColumnId)
                {
                    if (it.Value == -1)
                    {
                        throw new EdgeViewException(string.Format("The column [{0}].[{1}].[{2}] is not found.",
                            tableSchema, it.Key.Item1, it.Key.Item2));
                    }
                }

                //Generates create view string.
                var mapping2DArrayTuples = new Tuple<string, string>[nodes.Count()][];
                for (int i = 0; i < nodes.Count; i++)
                {
                    mapping2DArrayTuples[i] = new Tuple<string, string>[propertymapping.Count];
                }

                count = 0;
                foreach (var it in propertymapping)
                {
                    foreach (var variable in it.Item2)
                    {
                        if (!_dictionaryTableOffsetId.ContainsKey(variable.Item1.ToLower()))
                        {
                            throw new Exception(
                                string.Format("The table column [{0}].[{1}] in propety mapping is not found.",
                                    tableSchema, variable.Item1));
                        }
                        mapping2DArrayTuples[_dictionaryTableOffsetId[variable.Item1.ToLower()]][count] =
                            Tuple.Create(variable.Item2, it.Item1);
                    }
                    for (int i = 0; i < nodes.Count(); i++)
                    {
                        if (mapping2DArrayTuples[i][count] == null)
                        {
                            mapping2DArrayTuples[i][count] = Tuple.Create("null", it.Item1);
                        }
                    }
                    count++;
                }
                var selectStringList = new string[nodes.Count];
                const string selectTemplate = "Select {0}, {1} {2}\n" +
                                              "From {3}\n";
                int edgeColumnOffset = 0;
                var edgeNameList = new string[edgeCount];
                edgeCount = 0;
                for (int i = 0; i < nodes.Count; i++)
                {
                    foreach (var it in edgeList[i])
                    {
                        edgeNameList[edgeCount] = it.Item1 + "_" + it.Item2;
                        edgeCount++;
                    }
                }

                int rowCount = 0;
                string appendColumn;//Edge type and user id 
                string selectElement;
                foreach (var it in nodes)
                {
                    var elementList =
                        mapping2DArrayTuples[rowCount].Select(
                            item => item.Item1.ToString() + " as " + item.Item2).ToList();

                    for (int i = 0; i < edgeColumnOffset; i++)
                    {
                        elementList.Add("null as " + edgeNameList[i]);
                        elementList.Add(string.Format("null as {0}DeleteCol", edgeNameList[i]));
                        elementList.Add(string.Format("null as {0}OutDegree", edgeNameList[i]));
                    }

                    foreach (var variable in edgeList[rowCount])
                    {
                        elementList.Add(variable.Item2 + " as " + variable.Item1 + '_' + variable.Item2);
                        elementList.Add(string.Format("{0}DeleteCol as {1}DeleteCol", variable.Item2,
                            variable.Item1 + '_' + variable.Item2));
                        elementList.Add(string.Format("{0}OutDegree as {1}OutDegree", variable.Item2,
                            variable.Item1 + '_' + variable.Item2));
                    }

                    edgeColumnOffset += edgeList[rowCount].Count;

                    for (int i = edgeColumnOffset; i < edgeCount; i++)
                    {
                        elementList.Add("null as " + edgeNameList[i]);
                        elementList.Add(string.Format("null as {0}DeleteCol", edgeNameList[i]));
                        elementList.Add(string.Format("null as {0}OutDegree", edgeNameList[i]));
                    }

                    selectElement = string.Join(",", elementList);
                    if (!string.IsNullOrEmpty(selectElement))
                    {
                        selectElement = ", " + selectElement;
                    }

                    string nodeId = nodeTableToUserId.ContainsKey(it) ? nodeTableToUserId[it] : null;
                    appendColumn = "'" + it + "' as _NodeType,";
                    if (!string.IsNullOrEmpty(nodeId))
                    {
                        appendColumn += "convert(nvarchar(max), " + nodeId + ") as _NodeId";
                    }
                    else
                    {
                        appendColumn += "convert(nvarchar(max), null) as _NodeId";
                    }
                    selectStringList[rowCount] = string.Format(selectTemplate, string.Join(", ", ColumnList),
                        appendColumn, selectElement, it);
                    rowCount++;
                }


                const string createView = "Create View {2}.{0} as(\n" +
                                          "{1}" +
                                          ")\n";
                command.Parameters.Clear();
                command.CommandText = string.Format(createView, nodeViewName,
                    string.Join("Union all\n", selectStringList), tableSchema);
                command.ExecuteNonQuery();

                _propertymapping = propertymapping;
                if (externalTransaction == null)
                {
                    transaction.Commit();
                }
            }
            catch (Exception e)
            {
                if (externalTransaction == null)
                {
                    transaction.Rollback();
                }
                throw new NodeViewException(e.Message);
            }
        }

        /// <summary>
        /// Updates metatable when creating node view.
        /// </summary>
        /// <param name="tableSchema"> The Schema name of node table. Default(null or "") by "dbo".</param>
        ///  <param name="nodeViewName"> The name of supper node. </param>
        /// <param name="nodes"> The list of the names of native nodes. </param>
        /// <param name="propertymapping"> 
        /// Type is List<Tuple<string, List<Tuple<string, string>>>>
        /// That is, List<Tuple<nodeViewpropety, List<Tuple<native node table name, native node table propety>>>> 
        /// </param>
        /// <param name="externalTransaction">An existing SqlTransaction instance under which the create node view will occur.</param>
        private void UpdateNodeViewMetatable(string tableSchema, string nodeViewName, List<string> nodes,
            List<Tuple<string, List<Tuple<string, string>>>> propertymapping,
            SqlTransaction externalTransaction = null)
        {
            var transaction = externalTransaction ?? Conn.BeginTransaction();
            var command = Conn.CreateCommand();
            command.Transaction = transaction;
            try
            {
                //Update metaTable
                Int64 nodeviewTableId = 0;
                string updateGraphTable =
                    string.Format(@"INSERT INTO {0} (TableSchema, TableName, TableRole) OUTPUT [Inserted].[TableId] VALUES (@schema, @nodeviewname, @role)",
                        MetadataTables[0]);
                command.Parameters.AddWithValue("schema", tableSchema);
                command.Parameters.AddWithValue("nodeviewname", nodeViewName);
                command.Parameters.AddWithValue("role", 1);
                command.CommandText = updateGraphTable;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        nodeviewTableId = Convert.ToInt64(reader["tableId"].ToString());
                    }
                }

                var nodeViewPropertycolumnId = new List<Int64>();
                string updateTableColumn =
                    string.Format(@"
                    INSERT INTO {0}(TableId,TableSchema,TableName,ColumnName,ColumnRole,Reference) OUTPUT [Inserted].[ColumnId]
                    VALUES (@tableId, @schema, @nodeviewname, @columnname, @columnrole, null)",
                        MetadataTables[1]);
                command.Parameters.AddWithValue("tableId", nodeviewTableId);
                command.Parameters.AddWithValue("columnrole", 4);
                command.Parameters.Add("columnname", SqlDbType.NVarChar);
                command.CommandText = updateTableColumn;
                foreach (var it in propertymapping)
                {
                    command.Parameters["columnname"].Value = it.Item1;
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            nodeViewPropertycolumnId.Add(Convert.ToInt64(reader["ColumnId"].ToString()));
                        }
                    }
                }

                string updateNodeView = string.Format(@"INSERT INTO {0} (NodeViewTableId, TableId) VALUES (@nodeviewtableid, @tableid)",
                    MetadataTables[7]);
                command.Parameters.Clear();
                command.Parameters.AddWithValue("nodeviewtableid", nodeviewTableId);
                command.Parameters.Add("tableid", SqlDbType.NVarChar);
                command.CommandText = updateNodeView;
                foreach (var it in nodes)
                {
                    command.Parameters["tableid"].Value = _dictionaryTableId[it.ToLower()];
                    command.ExecuteNonQuery();
                }

                DataTable table = new DataTable(MetadataTables[5]); //_NodeViewColumnCollection
                DataColumn column;
                DataRow row;
                column = new DataColumn("NodeViewColumnId", Type.GetType("System.Int64"));
                table.Columns.Add(column);
                column = new DataColumn("ColumnId", Type.GetType("System.Int64"));
                table.Columns.Add(column);

                int posi = 0;
                foreach (var it in propertymapping)
                {
                    foreach (var variable in it.Item2)
                    {
                        row = table.NewRow();
                        row["NodeViewColumnId"] = nodeViewPropertycolumnId[posi];
                        var columnTuple = Tuple.Create(variable.Item1.ToLower(), variable.Item2.ToLower());
                        row["ColumnId"] = _dictionaryColumnId[columnTuple];
                        table.Rows.Add(row);
                    }
                    posi++;
                }
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Conn, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = MetadataTables[5]; //_NodeViewColumnCollection
                    bulkCopy.WriteToServer(table);
                }

                if (externalTransaction == null)
                {
                    transaction.Commit();
                }
            }
            catch (Exception e)
            {
                if (externalTransaction == null)
                {
                    transaction.Rollback();
                }
                throw new NodeViewException(e.Message);
            }
        }


        private void CreateEdgeViewByDefault(string tableSchema, string nodeViewName,
            SqlTransaction externalTransaction = null)
        {
            var transaction = externalTransaction ?? Conn.BeginTransaction();

            try
            {
                using (var command = Conn.CreateCommand())
                {
                    command.Transaction = transaction;
                    //Creates edge view bydefault
                    var edgeToEdges = new Dictionary<string, List<string>>(); //edge view => list<table>
                    const string getEdgeColumnName = @"
                    Select NTC.TableName, ColumnName
                    From {0} NTC
                    Join {1} NTCC
                    On NTC.TableId = NTCC.TableId
                    Join {2} Temp
                    On NTC.TableName = Temp.TableName
                    Where NTC.TableSchema = @schema and NTCC.ColumnRole = @role";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("schema", tableSchema);
                    command.Parameters.AddWithValue("role", WNodeTableColumnRole.Edge);
                    command.CommandText = string.Format(getEdgeColumnName, MetadataTables[0], MetadataTables[1],
                        _tableSet);
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var tableName = reader["TableName"].ToString().ToLower();
                            var columnName = reader["columnName"].ToString().ToLower();
                            if (edgeToEdges.ContainsKey(columnName))
                            {
                                edgeToEdges[columnName].Add(tableName);
                            }
                            else
                            {
                                edgeToEdges[columnName] = new List<string>();
                                edgeToEdges[columnName].Add(tableName);
                            }
                        }
                    }

                    foreach (var it in edgeToEdges)
                    {
                        if (it.Value.Count >= 2)
                        {
                            CreateEdgeView(tableSchema, nodeViewName, it.Key,
                                it.Value.Select(x => Tuple.Create(x, it.Key)).ToList(), null, transaction);
                        }
                    }
                }
                if (externalTransaction == null)
                {
                    transaction.Commit();
                }
            }
            catch (Exception e)
            {
                if (externalTransaction == null)
                {
                    transaction.Rollback();
                }
                throw new EdgeViewException(e.Message);
            }
        }


        public void CreateNodeView(string query)
        {
            IList<ParseError> errors;
            var parser = new GraphViewParser();
            var script = parser.ParseCreateNodeEdgeViewStatement(query, out errors) as WSqlScript;
            if (errors.Count > 0)
                throw new SyntaxErrorException(errors);

            if (script == null || script.Batches.Count == 0)
            {
                throw new SyntaxErrorException("Invalid CREATE VIEW statement.");
            }

            var statement = script.Batches[0].Statements[0] as WCreateViewStatement;
            if (statement == null)
                throw new SyntaxErrorException("Not a CREATE VIEW statement");
            var nodeViewObjectName = statement.SchemaObjectName;
            string schema = nodeViewObjectName.SchemaIdentifier == null
                ? "dbo"
                : nodeViewObjectName.SchemaIdentifier.Value;
            string nodeViewName = nodeViewObjectName.BaseIdentifier.Value;
            var visitor = new NodeViewSelectStatementVisitor();
            List<string> tableObjList;
            List<Tuple<string, List<Tuple<string, string>>>> propertymapping;
            visitor.Invoke(schema, statement.SelectStatement, out tableObjList, out propertymapping);
            CreateNodeView(schema, nodeViewName, tableObjList, propertymapping);
        }

        /// <summary>
        /// Drops node view with edge view on it
        /// </summary>
        /// <param name="nodeViewSchema">The name of node view. Default(null or "") by "dbo".</param>
        /// <param name="nodeViewName">The name of node view.</param>
        /// <param name="externalTransaction">An existing SqlTransaction instance under which the drop edge view will occur.</param>
        public void DropNodeView(string nodeViewSchema, string nodeViewName, SqlTransaction externalTransaction = null)
        {
            SqlTransaction transaction = externalTransaction ?? Conn.BeginTransaction();
            var command = Conn.CreateCommand();
            command.Transaction = transaction;
            try
            {
                const string getSubEdgeView = @"
                Select ColumnName
                From {0} NTC
                join {1} NTCC
                on NTC.TableId = NTCC.TableId
                Where NTC.TableSchema = @schema and NTC.TableName = @nodeview and NTCC.ColumnRole = @role;";
                command.Parameters.AddWithValue("schema", nodeViewSchema);
                command.Parameters.AddWithValue("nodeview", nodeViewName);
                command.Parameters.AddWithValue("role", WNodeTableColumnRole.EdgeView);
                command.CommandText = string.Format(getSubEdgeView, MetadataTables[0], MetadataTables[1]);

                var edgeViewList = new List<string>();
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        edgeViewList.Add(reader["columnName"].ToString().ToLower());
                    }
                }

                foreach (var it in edgeViewList)
                {
                    DropEdgeView(nodeViewSchema, nodeViewName, it, transaction);
                }

                //drop view
                const string dropView = @"drop view [{0}]";
                command.Parameters.Clear();
                command.CommandText = string.Format(dropView, nodeViewName);
                command.ExecuteNonQuery();

                //update metatable
                const string deleteNodeViewColumn = @"
                Delete from [{0}]  
                where [NodeViewColumnId] in
                (
                    Select columnId
                    From [{1}] NodeTableColumn
                    Where TableSchema = @schema and  TableName = @tablename and ColumnRole = @role
                )
                Delete from [{1}]
                Where TableSchema = @schema and  TableName = @tablename and ColumnRole = @role";
                command.Parameters.AddWithValue("schema", nodeViewSchema);
                command.Parameters.AddWithValue("tablename", nodeViewName);
                command.Parameters.AddWithValue("role", WNodeTableColumnRole.NodeViewProperty);
                command.CommandText = string.Format(deleteNodeViewColumn, MetadataTables[5], MetadataTables[1]);
                command.ExecuteNonQuery();

                const string deleteNodeView = @"
                Delete from [{0}]
                where NodeViewTableId in
                (
                    select tableid 
                    from [{1}]
                    where TableRole = @role and TableSchema = @schema and TableName = @tablename
                )
                delete from [{1}]
                where TableRole = @role and TableSchema = @schema and TableName = @tablename";
                command.Parameters["role"].Value = WNodeTableColumnRole.Edge;
                command.CommandText = string.Format(deleteNodeView, MetadataTables[7], MetadataTables[0]);
                command.ExecuteNonQuery();

                if (externalTransaction == null)
                {
                    transaction.Commit();
                }
            }
            catch (Exception error)
            {
                if (externalTransaction == null)
                {
                    transaction.Rollback();
                }
                throw new NodeViewException("Drop node view:" + error.Message);
            }
        }

        /// <summary>
        /// Creates view on edges
        /// </summary>
        /// <param name="tableSchema"> The Schema name of node table. Default(null or "") by "dbo".</param>
        ///  <param name="nodeName"> The name of supper node. </param>
        /// <param name="edgeViewName"> The name of supper edge. </param>
        /// <param name="edges"> The list of message of edges for merging.
        /// The message is stored in tuple, containing (node table name, edge column name).</param>
        /// <param name="edgeAttribute"> The attributes' names in the supper edge.</param>
        /// <param name="externalTransaction">An existing SqlTransaction instance under which create edge view will occur.</param>
        /// <param name="attributeMapping"> User-supplied attribute-mapping.
        ///  Type is List<Tuple<string, List<Tuple<string, string, string>>>>.
        ///  That is, every attribute in supper edge is mapped into a list of attributes,
        ///  with the message of <node table name, edge column name, attribute name>
        ///
        ///  When "attributeMapping" is empty or null, the program will map the atrributes of edge view
        ///  to all the same-name attributes of all the user-supplied edges.
        ///  When "attributeMapping" is empty or null and "edgeAttribute" is null,
        ///  the program will map all the attributes of edges into edge View(merging attributes with same name)
        ///  </param>
        public void CreateEdgeView(string tableSchema, string nodeName, string edgeViewName,
            List<Tuple<string, string>> edges, List<string> edgeAttribute = null,
            SqlTransaction externalTransaction = null,
            List<Tuple<string, List<Tuple<string, string, string>>>> attributeMapping = null)
        {
            var transaction = externalTransaction ?? Conn.BeginTransaction();

            try
            {
                CreateEdgeViewWithoutRecord(tableSchema, nodeName, edgeViewName, edges, edgeAttribute, transaction,
                    attributeMapping);
                UpdateEdgeViewMetaData(tableSchema, edgeViewName, transaction);

                if (externalTransaction == null)
                {
                    transaction.Commit();
                }
            }
            catch (Exception error)
            {

                if (externalTransaction == null)
                {
                    transaction.Rollback();
                }
                throw new EdgeViewException(error.Message);
            }
        }

        /// <summary>
        /// Creates Edge View using CREATE EDGE VIEW statement
        /// </summary>
        /// <param name="query"></param>
        public void CreateEdgeView(string query)
        {
            IList<ParseError> errors;
            var parser = new GraphViewParser();
            var script = parser.ParseCreateNodeEdgeViewStatement(query, out errors) as WSqlScript;
            if (errors.Count > 0)
                throw new SyntaxErrorException(errors);

            if (script == null || script.Batches.Count == 0)
            {
                throw new SyntaxErrorException("Invalid CREATE VIEW statement.");
            }

            var statement = script.Batches[0].Statements[0] as WCreateViewStatement;
            if (statement == null)
                throw new SyntaxErrorException("Not a CREATE VIEW statement");
            var edgeViewObjectName = statement.SchemaObjectName;
            string schema = edgeViewObjectName.DatabaseIdentifier == null
                ? "dbo"
                : edgeViewObjectName.DatabaseIdentifier.Value;
            if (edgeViewObjectName.SchemaIdentifier == null)
                throw new SyntaxErrorException(
                    "Source node type should be specified. Format: <Node name>.<Edgeview Name>");
            string nodeName = edgeViewObjectName.SchemaIdentifier.Value;
            string edgeViewName = edgeViewObjectName.BaseIdentifier.Value;
            var visitor = new EdgeViewSelectStatementVisitor();
            List<Tuple<string, string>> edges;
            List<string> edgeAttribute;
            List<Tuple<string, List<Tuple<string, string, string>>>> attributeMapping;
            visitor.Invoke(schema, statement.SelectStatement, out edges, out edgeAttribute, out attributeMapping);
            CreateEdgeView(schema, nodeName, edgeViewName, edges, edgeAttribute, null, attributeMapping);
            statement.SchemaObjectName =
                new WSchemaObjectName(new Identifier
                {
                    Value = string.Format("{0}_{1}_{2}_Sampling", schema, nodeName, edgeViewName)
                });
            string a = statement.ToString();
            //ExecuteNonQuery(statement.ToString());

        }

        ///  <summary>
        ///  Edge View:create edge view decoder function
        ///  </summary>
        ///  <param name="tableSchema"> The Schema name of node table. Default(null or "") by "dbo".</param>
        ///  <param name="nodeName"> The name of supper node. </param>
        ///  <param name="edgeViewName"> The name of supper edge. </param>
        ///  <param name="edges"> The list of message of edges for merging.
        ///  The message is stored in tuple, containing (node table name, edge column name).</param>
        ///  <param name="edgeAttribute"> The attributes' names in the supper edge.</param>
        /// <param name="externalTransaction">An existing SqlTransaction instance under which create edge view will occur.</param>
        /// <param name="attributeMapping"> User-supplied attribute-mapping.
        ///  Type is List<Tuple<string, List<Tuple<string, string, string>>>>.
        ///  That is, every attribute in supper edge is mapped into a list of attributes,
        ///  with the message of <node table name, edge column name, attribute name>
        ///
        ///  When "attributeMapping" is empty or null, the program will map the atrributes of edge view
        ///  to all the same-name attributes of all the user-supplied edges.
        ///  When "attributeMapping" is empty or null and "edgeAttribute" is null,
        ///  the program will map all the attributes of edges into edge View(merging attributes with same name)
        ///  </param>
        private void CreateEdgeViewWithoutRecord(string tableSchema, string nodeName, string edgeViewName,
            List<Tuple<string, string>> edges,
            List<string> edgeAttribute = null, SqlTransaction externalTransaction = null,
            List<Tuple<string, List<Tuple<string, string, string>>>> attributeMapping = null)
        {
            _nodeName = nodeName;
            //Check validity of input
            if (string.IsNullOrEmpty(tableSchema))
            {
                tableSchema = "dbo";
            }
            if (string.IsNullOrEmpty(edgeViewName))
            {
                throw new EdgeViewException("The string of edge view name is null or empty.");
            }
            if (edges == null || !edges.Any())
            {
                throw new EdgeViewException("The list of edges is null or empty.");
            }
            if (edgeAttribute == null && (attributeMapping != null && attributeMapping.Any()))
            {
                throw new EdgeViewException("The edgeattribute is null but attributeMapping function is not empty.");
            }

            SqlTransaction transaction = externalTransaction ?? Conn.BeginTransaction();
            var command = Conn.CreateCommand();
            command.Transaction = transaction;
            command.CommandTimeout = 0;

            try
            {
                //Checke the validity of node and edge.
                const string checkNodeTableName =
                    @"SELECT [TableId], [TableName], [TableRole] 
                  FROM [{0}]
                  where TableSchema = @schema;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("schema", tableSchema);
                command.CommandText = string.Format(checkNodeTableName, MetadataTables[0]);
                var tableToRole = new Dictionary<string, int>();
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableName = reader["TableName"].ToString().ToLower();
                        var tableRole = Convert.ToInt32(reader["TableRole"].ToString());
                        tableToRole[tableName] = tableRole;
                    }
                }
                if (!tableToRole.ContainsKey(_nodeName.ToLower()))
                {
                    throw new EdgeViewException(
                        string.Format("Cannot find the node or node view \"{0}\" in schame \"{1}\".", _nodeName,
                            tableSchema));
                }

                //Check edge view name hasn't been created.
                const string checkEdge = @"
                Select NTCC.ColumnName
                From {0} NTC
                Join {1} NTCC
                on NTC.TableId = NTCC.TableId
                Where NTC.TableSchema = @schema and NTC.TableName = @name";
                command.CommandText = string.Format(checkEdge, MetadataTables[0], MetadataTables[1]);
                command.Parameters.Clear();
                command.Parameters.AddWithValue("schema", tableSchema);
                command.Parameters.AddWithValue("name", nodeName);
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var edgeName = reader["ColumnName"].ToString().ToLower();
                        if (edgeName == edgeViewName.ToLower())
                        {
                            throw new EdgeViewException(string.Format(
                                "The column name \"{0}\" already exists in node \"{1}\".", edgeViewName, nodeName));
                        }
                    }
                }

                edges = edges.Select(x => Tuple.Create(x.Item1.ToLower(), x.Item2.ToLower())).ToList();
                _edgeColumnToColumnId = edges.ToDictionary(x => Tuple.Create(x.Item1, x.Item2), x => (long)-1);
                //<NodeTable, Edge> => ColumnId

                var subViewToEdges = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase); //Subview is made up of the tables with non-zero indegree. <table name> => list<edge column>

                //Check validity of table name in metaDataTable and get table's column id
                command.Parameters.Clear();
                const string checkEdgeColumn = @"
                Select NTC.TableName, NTCC.ColumnName, NTCC.ColumnId, NTCC.Reference
                From {0} NTC
                Join {1} NTCC
                on NTC.TableId = NTCC.TableId
                where NTC.TableSchema = @tableschema and NTCC.ColumnRole = @role";
                command.CommandText = string.Format(checkEdgeColumn, MetadataTables[0], MetadataTables[1]); //_NodeTableColumnCollection
                command.Parameters.Add("tableschema", SqlDbType.NVarChar, 128);
                command.Parameters["tableschema"].Value = tableSchema;
                command.Parameters.Add("role", SqlDbType.Int);
                command.Parameters["role"].Value = WNodeTableColumnRole.Edge;
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableName = reader["TableName"].ToString().ToLower();
                        var edgeColumnName = reader["ColumnName"].ToString().ToLower();
                        int columnId = Convert.ToInt32(reader["ColumnId"].ToString());
                        var reference = reader["Reference"].ToString();

                        var edgeTuple = Tuple.Create(tableName, edgeColumnName);
                        if (_edgeColumnToColumnId.ContainsKey(edgeTuple))
                        {
                            _edgeColumnToColumnId[edgeTuple] = columnId;
                            if (!subViewToEdges.ContainsKey(reference))
                            {
                                subViewToEdges[reference] = new List<string>();
                            }
                        }
                    }
                }

                //sort it
                _edgeColumnToColumnId = _edgeColumnToColumnId.OrderBy(x => x.Value).ToDictionary(x => x.Key, x => x.Value);
                edges = edges.OrderBy(x => _edgeColumnToColumnId[Tuple.Create(x.Item1, x.Item2)]).ToList();

                var edgeNotInMetaTable = _edgeColumnToColumnId.Where(x => x.Value == -1).Select(x => x.Key).ToArray();
                if (edgeNotInMetaTable.Any())
                {
                    throw new EdgeViewException(string.Format("There doesn't exist edge column \"{0}.{1}\"",
                        edgeNotInMetaTable[0].Item1, edgeNotInMetaTable[0].Item2));
                }

                //Check validity of edge view name
                const string checkEdgeViewName = @"
                Select * 
                From {0} NTC
                Join {1} NTCC
                on NTC.TableId = NTCC.TableId
                where NTC.TableSchema = @schema and NTC.TableName = @tablename and NTCC.ColumnName = @columnname
                    and NTCC.ColumnRole = @role and NTCC.Reference = @ref";

                command.Parameters.Clear();
                command.Parameters.AddWithValue("schema", tableSchema);
                command.Parameters.AddWithValue("tablename", _nodeName);
                command.Parameters.AddWithValue("columnname", edgeViewName);
                command.Parameters.AddWithValue("role", WNodeTableColumnRole.EdgeView);
                command.Parameters.AddWithValue("ref", _nodeName);

                command.CommandText = String.Format(checkEdgeViewName, MetadataTables[0], MetadataTables[1]); //_NodeTableColumnCollection
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        throw new EdgeViewException(string.Format("Edge view name \"{0}\" alreadly existed.",
                            edgeViewName));
                    }
                }

                if (edgeAttribute == null)
                {
                    edgeAttribute = new List<string>();
                    var edgeColumnSet =
                        new HashSet<Tuple<string, string>>(
                            edges.Select(x => Tuple.Create(x.Item1, x.Item2)));
                    const string findEdgeAttribute = @"
                    select NTC.TableName, NTCC.ColumnName, EAC.AttributeName
                    from {0} NTC
                    join {1} NTCC
                    on NTC.TableId = NTCC.TableId
                    join {2} EAC
                    on EAC.columnid = NTCC.ColumnId
                    where NTC.TableSchema = @schema and columnRole = @role
                    order by EAC.AttributeId";
                    command.CommandText = string.Format(findEdgeAttribute, MetadataTables[0], MetadataTables[1],
                        MetadataTables[2]);
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("schema", tableSchema);
                    command.Parameters.AddWithValue("role", WNodeTableColumnRole.Edge);
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var tableName = reader["TableName"].ToString().ToLower();
                            var columnName = reader["ColumnName"].ToString().ToLower();
                            var attributeName = reader["AttributeName"].ToString().ToLower();
                            var edgeColumn = Tuple.Create(tableName.ToLower(), columnName.ToLower());
                            if (edgeColumnSet.Contains(edgeColumn))
                            {
                                edgeAttribute.Add(attributeName);
                            }
                        }
                    }
                    edgeAttribute = edgeAttribute.Distinct().ToList();
                }

                //Get the attribute's ids which refer to user-supplied attribute in meta data table
                _dictionaryAttribute = edgeAttribute.ToDictionary(x => x.ToLower(), x => new List<Int64>()); //<EdgeViewAttributeName> => List<AttributeId>

                _attributeType = edgeAttribute.ToDictionary(x => x.ToLower(), x => "");
                //<EdgeViewAttribute> => <Type>

                var edgeColumnToAttributeInfo =
                    edges.ToDictionary(x => Tuple.Create(x.Item1, x.Item2),
                        x => new List<Tuple<string, string>>());
                //<nodeTable, edgeName> => list<Tuple<Type, EdgeViewAttributeName>>

                const string getAttributeId = @"
                Select *
                From {0} NTC
                Join {1} NTCC
                on NTC.TableId = NTCC.TableId
                Join {2} EAC
                on EAC.ColumnId = NTCC.ColumnId
                Where NTC.TableSchema = @schema
                Order by AttributeEdgeId";
                command.Parameters.Clear();
                command.Parameters.Add("schema", SqlDbType.NVarChar, 128);
                command.Parameters["schema"].Value = tableSchema;
                command.CommandText = String.Format(getAttributeId, MetadataTables[0], MetadataTables[1], MetadataTables[2]); //_EdgeAttributeCollection

                //User supplies attribute mapping or not.
                if (attributeMapping == null)
                {
                    var ignoreAttribute = new HashSet<string>();
                    var attributeMappingDict = new Dictionary<string, List<Tuple<string, string, string>>>();
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var tableName = reader["TableName"].ToString().ToLower();
                            var edgeColumnName = reader["ColumnName"].ToString().ToLower();
                            var edgeTuple = Tuple.Create(tableName, edgeColumnName);
                            if (_edgeColumnToColumnId.ContainsKey(edgeTuple))
                            {
                                var type = reader["AttributeType"].ToString().ToLower();
                                var attributeMappingTo = "";

                                var attributeName = reader["AttributeName"].ToString().ToLower();
                                if (_dictionaryAttribute.ContainsKey(attributeName))
                                {
                                    var attributeId = Convert.ToInt64(reader["AttributeId"].ToString());
                                    _dictionaryAttribute[attributeName].Add(attributeId);
                                    attributeMappingTo = attributeName;
                                    if (_attributeType[attributeName] == "")
                                    {
                                        _attributeType[attributeName] = type;
                                    }
                                    else if (_attributeType[attributeName] != type)
                                    {
                                        //throw new EdgeViewException(
                                        //    string.Format(
                                        //        "There exist two edge attributes \"{0}\" with different type in different edges.",
                                        //        attributeName));
                                        //var warning =
                                        //    new WarningException(
                                        //        string.Format(
                                        //            "Warning: There exist two edge attributes \"{0}\" with different type in different edges.",
                                        //            attributeName));
                                        //Console.WriteLine(warning.Message);

                                        if (!ignoreAttribute.Contains(attributeName))
                                        {
                                            var newType = _attributeType[attributeName];
                                            var newAttributeName = attributeName + "_" + newType;

                                            //edgeColumnToAttributeInfo[edgeTuple].Add(Tuple.Create(newType, newAttributeName));
                                            _attributeType[newAttributeName] = _attributeType[attributeName];
                                            _attributeType[attributeName] = "wrong";

                                            ignoreAttribute.Add(attributeName.ToLower());
                                        }

                                        attributeMappingTo = attributeMappingTo + "_" + type;
                                        _attributeType[attributeMappingTo] = type;
                                    }
                                }
                                edgeColumnToAttributeInfo[edgeTuple].Add(Tuple.Create(type, attributeMappingTo));
                                if (!attributeMappingDict.ContainsKey(attributeMappingTo))
                                {
                                    attributeMappingDict[attributeMappingTo] = new List<Tuple<string, string, string>>();
                                }
                                attributeMappingDict[attributeMappingTo].Add(Tuple.Create(tableName, edgeColumnName,
                                    attributeName));
                            }
                        }
                    }
                    attributeMapping =
                        attributeMappingDict.Select(x => Tuple.Create(x.Key, x.Value))
                            .Where(x => !ignoreAttribute.Contains(x.Item1.ToLower())).ToList();

                    var temp = new List<int>();
                    foreach (var it in edgeColumnToAttributeInfo)
                    {
                        int count = 0;
                        temp.Clear();
                        foreach (var iterator in it.Value)
                        {
                            if (ignoreAttribute.Contains(iterator.Item2))
                            {
                                temp.Add(count);
                            }
                            count++;
                        }
                        foreach (var VARIABLE in temp)
                        {
                            it.Value[VARIABLE] = Tuple.Create(it.Value[VARIABLE].Item1, it.Value[VARIABLE].Item2 + "_" + it.Value[VARIABLE].Item1);
                        }
                    }
                    _attributeType =
                        _attributeType.Where(x => !ignoreAttribute.Contains(x.Key.ToLower()))
                            .ToDictionary(x => x.Key, x => x.Value);
                    _dictionaryAttribute =
                        _attributeType.Where(x => !ignoreAttribute.Contains(x.Key.ToLower()))
                            .ToDictionary(x => x.Key.ToLower(), x => new List<Int64>());
                }
                else
                {
                    var attributeMappingIntoDictionary = new Dictionary<Tuple<string, string, string>, string>();
                    // <TableName, EdgeName, Attribute> => <EdgeViewAttribute>

                    foreach (var it  in attributeMapping)
                    {
                        if (it.Item2 != null && it.Item2.Any())
                        {
                            foreach (var itAttributeRef in it.Item2)
                            {
                                var tempAttributeRef = Tuple.Create(itAttributeRef.Item1.ToLower(),
                                    itAttributeRef.Item2.ToLower(),
                                    itAttributeRef.Item3.ToLower());
                                if (attributeMappingIntoDictionary.ContainsKey(tempAttributeRef))
                                {
                                    throw new EdgeViewException(
                                        string.Format(
                                            "The attribute \"{0}.{1}.{2}\" maps into edge view's attribute twice.",
                                            itAttributeRef.Item1, itAttributeRef.Item2, itAttributeRef.Item3));
                                }
                                attributeMappingIntoDictionary[tempAttributeRef] = it.Item1.ToLower();
                            }
                        }
                        else
                        {
                            foreach (var itAttributeRef in edges)
                            {
                                var tempAttributeRef = Tuple.Create(itAttributeRef.Item1.ToLower(),
                                    itAttributeRef.Item2.ToLower(),
                                    it.Item1.ToLower());
                                if (attributeMappingIntoDictionary.ContainsKey(tempAttributeRef))
                                {
                                    throw new EdgeViewException(
                                        string.Format(
                                            "The attribute \"{0}.{1}.{2}\" maps into edge view's attribute twice.",
                                            itAttributeRef.Item1, itAttributeRef.Item2, it.Item1));
                                }
                                attributeMappingIntoDictionary[tempAttributeRef] = it.Item1.ToLower();
                            }
                        }
                    }

                    //Records information from metadatatable
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var tableName = reader["TableName"].ToString().ToLower();
                            var edgeColumnName = reader["ColumnName"].ToString().ToLower();
                            var edgeTuple = Tuple.Create(tableName, edgeColumnName);

                            if (_edgeColumnToColumnId.ContainsKey(edgeTuple))
                            {
                                var type = reader["AttributeType"].ToString().ToLower();
                                var attributeMappingTo = "";
                                var attributeName = reader["AttributeName"].ToString().ToLower();

                                var attributeTuple = Tuple.Create(tableName, edgeColumnName, attributeName);

                                if (attributeMappingIntoDictionary.ContainsKey(attributeTuple))
                                {
                                    var edgeViewAttributeName = attributeMappingIntoDictionary[attributeTuple];
                                    var attributeId = Convert.ToInt64(reader["AttributeId"].ToString());
                                    _dictionaryAttribute[edgeViewAttributeName].Add(attributeId);
                                    if (_attributeType[edgeViewAttributeName] == "")
                                    {
                                        _attributeType[edgeViewAttributeName] = type;
                                    }
                                    else if (_attributeType[edgeViewAttributeName] != type)
                                    {
                                        throw new EdgeViewException(
                                            string.Format(
                                                "There exist two edge attributes \"{0}\" with different type in different edges.",
                                                edgeViewAttributeName));
                                    }
                                    attributeMappingTo = attributeMappingIntoDictionary[attributeTuple];
                                }
                                edgeColumnToAttributeInfo[edgeTuple].Add(Tuple.Create(type, attributeMappingTo));
                            }
                        }
                    }
                }
                var emptyAttribute = _attributeType.Select(x => x).Where(x => (x.Value == "")).ToArray();
                if (emptyAttribute.Any())
                {
                    throw new EdgeViewException(string.Format("There is not attribute for \"{0}\" to map into",
                        emptyAttribute[0].Key));
                }

                //Creates subview
                foreach (var it in edges)
                {
                    if (subViewToEdges.ContainsKey(it.Item1))
                    {
                        subViewToEdges[it.Item1].Add(it.Item2);
                    }
                }

                int offset = 0;
                var subViewColumn = new List<Tuple<string, string>>();
                foreach (var it in subViewToEdges)
                {
                    foreach (var item in it.Value)
                    {
                       subViewColumn.Add(Tuple.Create(it.Key.ToLower(), item.ToLower()));
                    }
                }

                var nodeTableToNodeId = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                const string getNodeIdName = @"
                Select NTC.TableName, NTCC.ColumnName
                From {0} NTC
                Join {1} NTCC
                On  NTC.TableId = NTCC.TableId
                Where NTCC.ColumnRole = @role and NTC.TableSchema = @schema";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("role", WNodeTableColumnRole.NodeId);
                command.Parameters.AddWithValue("schema", tableSchema);
                command.CommandText = string.Format(getNodeIdName, MetadataTables[0], MetadataTables[1]);
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableName = reader["TableName"].ToString().ToLower();
                        var nodeId = reader["ColumnName"].ToString().ToLower();
                        nodeTableToNodeId[tableName] = nodeId;
                    }
                }

                int columnCal = subViewColumn.Count;
                var subQueryList = new List<string>();
                var subQuerytemplate = @"
                Select GlobalNodeId, {0}
                From {1}";
                var elementList = new List<string>(); 
                foreach (var it in subViewToEdges)
                {
                    if (it.Value.Count == 0)
                    {
                        continue;
                    }
                    elementList.Clear();
                    if (nodeTableToNodeId.ContainsKey(it.Key))
                    {
                        elementList.Add("convert(nvarchar(max), " + nodeTableToNodeId[it.Key] + ") as _NodeId");
                    }
                    else
                    {
                        elementList.Add("convert(nvarchar(max), null) as _NodeId");
                    }
                    elementList.Add("'" + it.Key + "' as _NodeType");
                    int i;
                    for (i = 0; i < offset; i++)
                    {
                        elementList.Add("null as " + subViewColumn[i].Item1 + "_" + subViewColumn[i].Item2);
                        elementList.Add("null as " + subViewColumn[i].Item1 + "_" + subViewColumn[i].Item2 + "DeleteCol");
                    }
                    foreach (var item in it.Value)
                    {
                        elementList.Add(item + " as " + subViewColumn[i].Item1 + "_" + subViewColumn[i].Item2);
                        elementList.Add(item + "DeleteCol" + " as " + subViewColumn[i].Item1 + "_" + subViewColumn[i].Item2 + "DeleteCol");
                        i++;
                        offset++;
                    }
                    for (; i < columnCal; i++)
                    {
                        elementList.Add("null as " + subViewColumn[i].Item1 + "_" + subViewColumn[i].Item2);
                        elementList.Add("null as " + subViewColumn[i].Item1 + "_" + subViewColumn[i].Item2 + "DeleteCol");
                    }
                    subQueryList.Add(String.Format(subQuerytemplate, string.Join(", ", elementList), it.Key));
                }
                const string createSubView = @"
                create view [{0}].[{1}] as (
                    {2}
                )";
                command.Parameters.Clear();
                if (subQueryList.Count != 0)
                {
                    command.CommandText = string.Format(createSubView, tableSchema,
                        tableSchema + "_" + nodeName + "_" + edgeViewName + "_SubView", string.Join("\nUNION ALL\n", subQueryList));
                }
                else
                {
                    command.CommandText = string.Format(createSubView, tableSchema,
                        tableSchema + "_" + nodeName + "_" + edgeViewName + "_SubView", "select -1 as globalnodeid, 'none' as _NodeType, 'none' as _NodeId");
                }
                command.ExecuteNonQuery();

                //Registers function
                GraphViewDefinedFunctionRegister register = new EdgeViewRegister(_nodeName, tableSchema, edgeViewName,
                    _attributeType, edgeColumnToAttributeInfo, _edgeColumnToColumnId);
                register.Register(Conn, transaction);
                
                var subViewEdgeColumnSet = subViewColumn.ToLookup(x => x);
                var nullValue = Tuple.Create("", "");
                var edgeColumnForBfs = edges.Select(x => subViewEdgeColumnSet.Contains(x) ? x : nullValue).ToList();
                register = new EdgeViewBfsRegister(tableSchema, nodeName, edgeViewName,
                    _attributeType.Select(x => Tuple.Create(x.Key, x.Value)).ToList(), edgeColumnForBfs);
                register.Register(Conn, transaction);


                //Prepares the select element 2D array
                Dictionary<string, int> attributeToColumnOffset =
                    _attributeType.Select(x => x.Key).Select((x, i) => new {x, i}).ToDictionary(x => x.x, x => x.i);
                Dictionary<Tuple<string, string>, int> edgeColumnToRowOffset =
                    edges.Select((x, i) => new {x, i})
                        .ToDictionary(x => Tuple.Create(x.x.Item1.ToLower(), x.x.Item2.ToLower()), x => x.i);
                string[][] attributeView2DArray = new string[edges.Count][];
                for (int i = 0; i < edges.Count; i++)
                {
                    attributeView2DArray[i] = new string[_attributeType.Count];
                }

                foreach (var it in attributeMapping)
                {
                    string edgeViewAttribute = it.Item1.ToLower();
                    int columnOffset = attributeToColumnOffset[edgeViewAttribute];
                    foreach (var iterator in it.Item2)
                    {
                        int rowOffset =
                            edgeColumnToRowOffset[Tuple.Create(iterator.Item1.ToLower(), iterator.Item2.ToLower())];
                        attributeView2DArray[rowOffset][columnOffset] = iterator.Item3;
                    }
                }

                int rowCount = 0;
                for (rowCount = 0; rowCount < edges.Count; rowCount++)
                {
                    var array = attributeView2DArray[rowCount];
                    int i = 0;
                    foreach (var VARIABLE in _attributeType)
                    {
                        if (string.IsNullOrEmpty(array[i]))
                        {
                            array[i] = "null";
                        }
                        array[i] = array[i] + " as " + VARIABLE.Key;
                        i++;
                    }
                }

                ////Create Edge View decode function
                //string createEdgeviewDecoder = @"
                //    CREATE FUNCTION {0} ({1})
                //    RETURNS TABLE
                //    AS
                //    RETURN 
                //    (
                //        {2}
                //    )";
                //string selectTemplate = @"
                //    Select Sink{0}
                //    From {1}({2}, {3})";
                //string decoderFunctionName = tableSchema + "_" + nodeName + "_" + edgeViewName + "_Decoder";
                //string parameterList = string.Join(", ",
                //    edges.Select((x, i) => "@edge" + i + " varbinary(max), " + "@del" + i + " varbinary(max)"));
                //var subQueryList = new List<string>();
                //rowCount = 0;
                //foreach (var it in edges)
                //{
                //    var array = attributeView2DArray[rowCount];
                //    string elementlist = string.Join(", ", array);
                //    if (!string.IsNullOrEmpty(elementlist))
                //    {
                //        elementlist = ", " + elementlist;
                //    }
                //    string subQuery = string.Format(selectTemplate, elementlist,
                //        tableSchema + "_" + it.Item1 + "_" + it.Item2 + "_Decoder", "@edge" + rowCount,
                //        "@del" + rowCount);
                //    subQueryList.Add(subQuery);
                //    rowCount++;
                //}
                //command.Parameters.Clear();
                //command.CommandText = string.Format(createEdgeviewDecoder, decoderFunctionName, parameterList,
                //    string.Join("UNION ALL\n", subQueryList));
                //command.ExecuteNonQuery();

                //Create sampling table
                var samplingTableName = tableSchema + "_" + nodeName + "_" + edgeViewName + "_Sampling";
                const string createEdgeSampling = @"
                CREATE VIEW {0} as
                (
                    {1}
                )";
                var selectTemplate = @"
                Select Src, Sink{0}
                From {1} ";

                subQueryList = new List<string>();

                rowCount = 0;
                foreach (var it in edges)
                {
                    var array = attributeView2DArray[rowCount];
                    string elementlist = string.Join(", ", array);
                    if (!string.IsNullOrEmpty(elementlist))
                    {
                        elementlist = ", " + elementlist;
                    }
                    string subQuery = string.Format(selectTemplate, elementlist,
                        tableSchema + "_" + it.Item1 + "_" + it.Item2 + "_Sampling");
                    subQueryList.Add(subQuery);
                    rowCount++;
                }

                command.Parameters.Clear();
                command.CommandText = string.Format(createEdgeSampling, samplingTableName,
                    string.Join("UNION ALL\n", subQueryList));
                command.ExecuteNonQuery();

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
                throw new EdgeViewException(e.Message);
            }
        }

        /// <summary>
        /// Updates metatable when creating edge view.
        /// </summary>
        /// <param name="tableSchema"> The Schema name of node table. Default(null or "") by "dbo".</param>
        /// <param name="edgeViewName"> The name of supper edge. </param>
        /// <param name="externalTransaction">An existing SqlTransaction instance under which create edge view will occur.</param>
        private void UpdateEdgeViewMetaData(string tableSchema, string edgeViewName,
            SqlTransaction externalTransaction = null)
        {
            SqlTransaction transaction = externalTransaction ?? Conn.BeginTransaction();
            var command = Conn.CreateCommand();
            command.Transaction = transaction;

            try
            {
                Int64 tableId = 0;
                const string getTableId = @"
                select TableId 
                From {0}
                Where TableSchema = @tableschema and TableName = @tablename";
                command.CommandText = string.Format(getTableId, MetadataTables[0]);
                command.Parameters.Clear();
                command.Parameters.AddWithValue("tableschema", tableSchema);
                command.Parameters.AddWithValue("tablename", _nodeName);
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        tableId = Convert.ToInt64(reader["tableId"].ToString());
                    }
                }

                //Insert edge view message into "_NodeTableColumnCollection" MetaDataTable
                const string insertGraphEdgeView = @"
                INSERT INTO [{0}] ([TableSchema], [TableName], [TableId], [ColumnName], [ColumnRole], [Reference])
                OUTPUT [Inserted].[ColumnId]
                VALUES (@tableschema, @TableName, @tableid, @columnname, @columnrole, @reference)";
                command.Parameters.AddWithValue("columnname", edgeViewName);
                command.Parameters.AddWithValue("columnrole", 3);
                command.Parameters.AddWithValue("reference", _nodeName);
                command.Parameters.AddWithValue("tableid", tableId);

                command.CommandText = string.Format(insertGraphEdgeView, MetadataTables[1]);
                //_NodeTableColumnCollection
                Int64 edgeViewId;
                using (var reader = command.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        throw new EdgeViewException(string.Format("MetaDataTable \"{0}\" can not be inserted",
                            MetadataTables[0]));
                    }
                    edgeViewId = Convert.ToInt64(reader["ColumnId"], CultureInfo.CurrentCulture);
                }

                //Insert into edge degree meta table
                const string insertEdgeViewAveDegree = @"
                INSERT INTO [{0}] ([ColumnId], [TableSchema], [TableName], [ColumnName])
                VALUES (@columnId, @tableschema, @TableName, @columnname)";
                command.CommandText = string.Format(insertEdgeViewAveDegree, MetadataTables[3]);
                command.Parameters.Clear();
                command.Parameters.AddWithValue("tableschema", tableSchema);
                command.Parameters.AddWithValue("tablename", _nodeName);
                command.Parameters.AddWithValue("columnname", edgeViewName);
                command.Parameters.AddWithValue("columnId", edgeViewId);
                command.ExecuteNonQuery();

                //Insert the edges's message into "_NodeViewColumnCollection" MetaDataTable
                DataTable table = new DataTable(MetadataTables[5]); //_EdgeViewCollection
                DataColumn column;
                DataRow row;
                column = new DataColumn("NodeViewColumnId", Type.GetType("System.Int64"));
                table.Columns.Add(column);
                column = new DataColumn("ColumnId", Type.GetType("System.Int64"));
                table.Columns.Add(column);

                foreach (var it in _edgeColumnToColumnId)
                {
                    row = table.NewRow();
                    row["NodeViewColumnId"] = edgeViewId;
                    row["ColumnId"] = it.Value;
                    table.Rows.Add(row);
                }
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Conn, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = MetadataTables[5]; //_NodeViewColumnCollection
                    bulkCopy.WriteToServer(table);
                }

                //Insert the edge view's attribute's message into "_EdgeAttributeCollection" MetaDataTable
                //Insert the message of edges which refer to user-supplied attribute into "_EdgeViewAttributeCollection" MetaDataTable
                const string insertEdgeViewAttribute = @"
                INSERT INTO [{0}] ([TableSchema], [TableName], [ColumnName], [ColumnId], [AttributeName], [AttributeType], [AttributeEdgeId])
                OUTPUT [Inserted].[AttributeId]
                VALUES (@schema, @tablename, @columnname, @columnid, @attributename, @type, @edgeid)";
                int count = 0;
                foreach (var it  in _dictionaryAttribute)
                {
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("schema", tableSchema);
                    command.Parameters.AddWithValue("tablename", _nodeName);
                    command.Parameters.AddWithValue("columnname", edgeViewName);
                    command.Parameters.AddWithValue("attributename", it.Key);
                    command.Parameters.AddWithValue("columnid", edgeViewId);
                    command.CommandText = string.Format(insertEdgeViewAttribute, MetadataTables[2]);

                    //_EdgeAttributeCollection
                    command.Parameters.AddWithValue("type", _attributeType[it.Key].ToLower());
                    command.Parameters.AddWithValue("edgeid", count++);
                    Int64 edgeViewAttributeId;
                    using (var reader = command.ExecuteReader())
                    {
                        if (!reader.Read())
                        {
                            throw new EdgeViewException("MetaDataTable \"Edge View AttributeId \" can not be inserted");
                        }
                        edgeViewAttributeId = Convert.ToInt64(reader["AttributeId"], CultureInfo.CurrentCulture);
                    }
                    table = new DataTable(MetadataTables[6]);
                    column = new DataColumn("EdgeViewAttributeId", Type.GetType("System.Int64"));
                    table.Columns.Add(column);
                    column = new DataColumn("EdgeAttributeId", Type.GetType("System.Int64"));
                    table.Columns.Add(column);

                    //_EdgeViewAttributeCollection
                    foreach (var variable in it.Value)
                    {
                        row = table.NewRow();
                        row["EdgeAttributeId"] = variable;
                        row["EdgeViewAttributeId"] = edgeViewAttributeId;
                        table.Rows.Add(row);
                    }
                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Conn, SqlBulkCopyOptions.Default, transaction))
                    {
                        bulkCopy.DestinationTableName = MetadataTables[6]; //_EdgeViewAttributeCollection
                        bulkCopy.WriteToServer(table);
                    }
                }



                if (externalTransaction == null)
                {
                    transaction.Commit();
                }
            }
            catch (Exception e)
            {
                if (externalTransaction == null)
                {
                    transaction.Rollback();
                }
                throw new NodeViewException(e.Message);
            }
        }

        /// <summary>
        /// Drop Edge View
        /// </summary>
        /// <param name="tableSchema">The name of schema. Default(null or "") by "dbo".</param>
        /// <param name="tableName">The name of node table.</param>
        /// <param name="edgeView">The name of Edge View</param>
        /// <param name="externalTransaction">An existing SqlTransaction instance under which the drop edge view will occur.</param>
        public void DropEdgeView(string tableSchema, string tableName, string edgeView,
            SqlTransaction externalTransaction = null)
        {
            var transaction = externalTransaction ?? Conn.BeginTransaction();
            var command = Conn.CreateCommand();
            command.Transaction = transaction;
            command.CommandTimeout = 0;
#if DEBUG
            if (externalTransaction == null)
            {
                transaction.Commit();
            }
#endif

            if (string.IsNullOrEmpty(tableSchema))
            {
                tableSchema = "dbo";
            }

            if (string.IsNullOrEmpty(tableName))
            {
                throw new EdgeViewException("The string of table name is null or empty.");
            }

            if (string.IsNullOrEmpty(edgeView))
            {
                throw new EdgeViewException("The string of edge view name is null or empty.");
            }
            try
            {
                _nodeName = tableName;
                //Check validity of edge view name
                const string checkViewName = @"
                select *
                from {0}
                where TableSchema = @schema and TableName = @tablename and ColumnName = @columnname and ColumnRole = @role and Reference = @ref";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("schema", tableSchema);
                command.Parameters.AddWithValue("tablename", _nodeName);
                command.Parameters.AddWithValue("columnname", edgeView);
                command.Parameters.AddWithValue("role", WNodeTableColumnRole.EdgeView);
                command.Parameters.AddWithValue("ref", _nodeName);
                command.CommandText = String.Format(checkViewName, MetadataTables[1]); //_NodeTableColumnCollection
                using (var reader = command.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        throw new EdgeViewException(string.Format("Edge view name \"{0}.{1}.{2}\" is not existed.",
                            tableSchema, _nodeName, edgeView));
                    }
                }
                const string dropAttribtueRef = @"
                Delete from [{1}]
                Where [EdgeViewAttributeId] in
                (select GEA.[AttributeId]
                From [{0}] GEA
                Where [TableSchema] = @schema and [TableName] = @table and [ColumnName] = @column);";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("schema", tableSchema);
                command.Parameters.AddWithValue("table", _nodeName);
                command.Parameters.AddWithValue("column", edgeView);
                command.CommandText = string.Format(dropAttribtueRef, MetadataTables[2], MetadataTables[6]);
                command.ExecuteNonQuery();

                const string dropAttribtueAndDegree = @"
                Delete From [{0}]
                Where [TableSchema] = @schema and [TableName] = @table and [ColumnName] = @column;";
                command.CommandText = string.Format(dropAttribtueAndDegree, MetadataTables[2]);
                command.ExecuteNonQuery();

                command.CommandText = string.Format(dropAttribtueAndDegree, MetadataTables[3]);
                command.ExecuteNonQuery();

                const string dropEdgeViewCollection = @"
                Delete From [{1}]
                Where NodeViewColumnId in
                (Select [ColumnId]
                From [{0}]
                Where [TableSchema] = @schema and [TableName] = @table and [ColumnName] = @column);";
                command.CommandText = string.Format(dropEdgeViewCollection, MetadataTables[1], MetadataTables[5]);
                command.ExecuteNonQuery();

                const string dropNodeTableColumnCollection = @"
                Delete From [{0}]
                Where [TableSchema] = @schema and [TableName] = @table and [ColumnName] = @column;";
                command.CommandText = string.Format(dropNodeTableColumnCollection, MetadataTables[1]);
                command.ExecuteNonQuery();

                const string dropFunction = @"
                Drop function [{0}]";
                command.Parameters.Clear();

                foreach (var it in EdgeViewFunctionList)
                {
                    command.CommandText = string.Format(dropFunction,
                        tableSchema + '_' + _nodeName + '_' + edgeView + '_' + it);
                    command.ExecuteNonQuery();
                }

                const string dropAssembly = @"
                Drop Assembly [{0}_Assembly]";
                command.CommandText = string.Format(dropAssembly, tableSchema + '_' + _nodeName + '_' + edgeView);
                command.ExecuteNonQuery();

                const string dropSamplingView = @"
                Drop View [{0}_Sampling]
                Drop View [{0}_SubView]";
                command.CommandText = string.Format(dropSamplingView, tableSchema + '_' + _nodeName + '_' + edgeView);
                command.ExecuteNonQuery();
#if !DEBUG
                if (externalTransaction == null)
                    transaction.Commit();
#endif
            }
            catch (Exception error)
            {
                if (externalTransaction == null)
                {
                    transaction.Rollback();
                }
                throw new EdgeViewException("Drop edge view:" + error.Message);
            }
        }

        public void updateGlobalNodeView(string schema = "dbo", SqlTransaction externalTransaction = null)
        {
            SqlTransaction transaction = externalTransaction ?? Conn.BeginTransaction();
            var command = Conn.CreateCommand();
            command.Transaction = transaction;

            try
            {
                //Drops GlobalNodeView if exists
                string globalViewName = "GlobalNodeView";
                const string checkGlobalView = @"
                select VIEWS.name
                from {0} SCH
                join {1} OBJ
                on SCH.schema_id = OBJ.schema_id
                join {2} VIEWS
                on OBJ.object_id = VIEWS.object_id
                where SCH.name = @schema and VIEWS.name = @name";
                bool delete = false;
                command.CommandText = string.Format(checkGlobalView,"sys.schemas", "sys.objects", "sys.all_views");
                command.Parameters.AddWithValue("name", globalViewName);
                command.Parameters.AddWithValue("schema", schema);
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        delete = true;
                    }
                }
                if (delete)
                {
                    DropNodeView(schema, globalViewName, transaction);
                }

                //Gets nodes list and creates global node view on it.
                var nodes = new List<string>();
                const string getTableName = @"
                Select TableName
                From {0}
                Where TableSchema = @schema and TableRole = @role;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("schema", schema);
                command.Parameters.AddWithValue("role", WNodeTableColumnRole.Property);
                command.CommandText = string.Format(getTableName, MetadataTables[0]);
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        nodes.Add(reader["TableName"].ToString().ToLower());
                    }
                }
                if (nodes.Any())
                {
                    CreateNodeView(schema, globalViewName, nodes, null, transaction);
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
                    transaction.Commit();
                }
                throw new NodeViewException(e.Message);
            }
        }
    }
}
