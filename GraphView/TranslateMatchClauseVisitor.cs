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
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    /// <summary>
    /// When an edge in the MATCH clause is not given an alias, this edge can still be referenced 
    /// by the edge column name. During translation, an edge without an explicit alias will be
    /// assigned a default alias, and as a result, the edge column name must be replaced by 
    /// the assigned alias. 
    /// </summary>
    internal class ReplaceEdgeReferenceVisitor : WSqlFragmentVisitor
    {
        /// <summary>
        /// Edge column name-> List of the candidate edge alias
        /// </summary>
        private Dictionary<string, List<string>> _edgeTableReferenceDict;

        public void Invoke(WSqlFragment node, Dictionary<string, List<string>> edgeTableReferenceDict)
        {
            _edgeTableReferenceDict = edgeTableReferenceDict;
            node.Accept(this);
        }

        public override void Visit(WColumnReferenceExpression node)
        {
            if (node.MultiPartIdentifier == null)
                return;
            var column = node.MultiPartIdentifier.Identifiers;
            if (column.Count >= 2)
            {
                var columnName = column[column.Count - 2].Value;
                if (_edgeTableReferenceDict.ContainsKey(columnName))
                {
                    if (_edgeTableReferenceDict[columnName].Count > 1)
                        throw new GraphViewException("Ambiguious Table Reference");
                    column[column.Count - 2].Value = _edgeTableReferenceDict[columnName].First();
                }
            }
            
            base.Visit(node);
        }
    }

    /// <summary>
    /// When a table in the FROM clause is not given an alias, this table will be assigned its 
    /// table name as its alias, and all references with schema name corresponding to this table
    /// should be replaced with the assigned alias by removing the schema name since it is invalid
    /// to have a schema identifier before an alias.
    /// </summary>
    internal class RemoveSchemanameInIdentifersVisitor : WSqlFragmentVisitor
    {
        public override void Visit(WSelectStarExpression node)
        {
            if (node.Qulifier != null && node.Qulifier.Count > 1)
                node.Qulifier = new WMultiPartIdentifier(node.Qulifier.Identifiers.Last());
        }

        public override void Visit(WColumnReferenceExpression node)
        {
            if (node.MultiPartIdentifier == null)
            {
                return;
            }
            var column = node.MultiPartIdentifier.Identifiers;
            var number = column.Count;
            if (number >= 3)
            {
                var tableName = column[number - 2];
                var columName = column[number - 1];
                column.Clear();
                column.Insert(0, tableName);
                column.Insert(1, columName);
            }
        }
    }

    public class NodeColumns
    {
        public WNodeTableColumnRole Role;
        public EdgeInfo EdgeInfo;
    }

    public class EdgeInfo
    {
        public HashSet<string> SinkNodes;
        public List<Tuple<string, string>> EdgeColumns;
        public IList<string> ColumnAttributes;
        public bool HasReversedEdge;
        public bool IsReversedEdge;
        public string EdgeUdfPrefix;
    }

    public class GraphMetaData
    {
        // Columns of each node table. For edge columns, edge attributes are attached.
        // (Schema name, Table name) -> (Column name -> Column Info)
        public readonly Dictionary<Tuple<string, string>, Dictionary<string, NodeColumns>> ColumnsOfNodeTables =
            new Dictionary<Tuple<string, string>, Dictionary<string, NodeColumns>>();

        // Node tables included in the node view.
        // (Schema name, Table name) -> set of the node table name included in the node view
        public readonly Dictionary<Tuple<string, string>, HashSet<string>> NodeViewMapping =
            new Dictionary<Tuple<string, string>, HashSet<string>>();

    }

    /// <summary>
    /// Translate match clause and add it to the from clause.
    /// Check validity -> Divide the graph into connected sub-graph -> Retrive the estimation
    /// -> Use DP-like algorithem to get the component -> add the component(s) into from clause
    /// </summary>
    internal class TranslateMatchClauseVisitor : WSqlFragmentVisitor
    {
        private WSqlTableContext _context;

        // A list of variables defined in a GraphView script and used in a SELECT statement.
        // When translating a GraphView SELECT statement, the optimizer sends a T-SQL SELECT query
        // to the SQL engine to estimate table cardinalities. The variables must be defined 
        // at the outset so that the SQL engine is able to parse and estimate the T-SQL SELECT query 
        // successfully. 
        private IList<DeclareVariableElement> _variables;

        // Upper Bound of the State number
        private const int MaxStates =
            //1000;
            100;
        //5000;
        //8000;
        //10000;
        //int.MaxValue;



        private GraphMetaData _graphMetaData;
        

        public SqlTransaction Tx { get; private set; }

        public TranslateMatchClauseVisitor(SqlTransaction tx)
        {
            this.Tx = tx;
            Init();
        }

        /// <summary>
        /// Retrieve the metadata
        /// </summary>
        /// <param name="conn"></param>
        private void Init()
        {
            _graphMetaData = new GraphMetaData();
            var columnsOfNodeTables = _graphMetaData.ColumnsOfNodeTables;
            var nodeViewMapping = _graphMetaData.NodeViewMapping;

            using (var command = Tx.Connection.CreateCommand())
            {
                command.Transaction = Tx;
                command.CommandText = string.Format(
                    @"
                    SELECT [TableSchema] as [Schema], [TableName] as [Name1], [ColumnName] as [Name2], 
                           [ColumnRole] as [Role], [Reference] as [Name3], [HasReversedEdge] as [HasRevEdge], [IsReversedEdge] as [IsRevEdge], [EdgeUdfPrefix] as [UdfPrefix], null as [EdgeViewTable], null as [ColumnId]
                    FROM [{0}]
                    UNION ALL
                    SELECT [TableSchema] as [Schema], [TableName] as [Name1], [ColumnName] as [Name2], 
                           -1 as [Role], [AttributeName] as [Name3], 0, 0, null, null, [AttributeId]
                    FROM [{1}]
                    UNION ALL
                    SELECT [NV].[TableSchema] as [Schema], [NV].[TableName] as [Name1], [NT].[TableName] as [Name2], 
                           -2 as [Role], null as [Name3], 0, 0, null, null, null
                    FROM 
                        [{2}] as [NV_NT_Mapping]
                        JOIN
                        [{3}] as [NV]
                        ON NV_NT_Mapping.NodeViewTableId = NV.TableId
                        JOIN 
                        [{3}] as [NT]
                        ON NV_NT_Mapping.TableId = NT.TableId
                    UNION ALL
                    SELECT [EV].[TableSchema] as [Schema], [EV].[ColumnName] as [Name1], [ED].[ColumnName]as [Name2],
                           -3 as [Role], [ED].[TableName] as [Name3], 0, 0, null, [EV].[TableName] as [EdgeViewTable], [ED].[ColumnId] as [ColumnId]
                    FROM 
                        [{4}] as [EV_ED_Mapping]
                        JOIN
                        [{0}] as [EV]
                        ON [EV_ED_Mapping].[NodeViewColumnId] = [EV].[ColumnId] and [EV].[ColumnRole] = 3
                        JOIN
                        [{0}] as [ED]
                        ON [EV_ED_Mapping].[ColumnId] = [ED].[ColumnId]
                        ORDER BY [ColumnId]", GraphViewConnection.MetadataTables[1],
                    GraphViewConnection.MetadataTables[2], GraphViewConnection.MetadataTables[7],
                    GraphViewConnection.MetadataTables[0], GraphViewConnection.MetadataTables[5]);

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        int tag = (int) reader["Role"];
                        string schema = reader["Schema"].ToString().ToLower(CultureInfo.CurrentCulture);
                        string name1 = reader["Name1"].ToString().ToLower(CultureInfo.CurrentCulture);
                        string name2 = reader["Name2"].ToString().ToLower(CultureInfo.CurrentCulture);
                        bool hasRevEdge = reader["HasRevEdge"].ToString().Equals("1");
                        bool isRevEdge = reader["IsRevEdge"].ToString().Equals("1");
                        string udfPrefix = reader["UdfPrefix"].ToString().ToLower(CultureInfo.CurrentCulture);

                        // Retrieve columns of node tables
                        var tableTuple = new Tuple<string, string>(schema, name1);
                        if (tag >= 0)
                        {
                            Dictionary<string, NodeColumns> columnDict;
                            if (!columnsOfNodeTables.TryGetValue(tableTuple, out columnDict))
                            {
                                columnDict = new Dictionary<string, NodeColumns>(StringComparer.OrdinalIgnoreCase);
                                columnsOfNodeTables.Add(tableTuple, columnDict);
                            }
                            var role = (WNodeTableColumnRole) tag;
                            EdgeInfo edgeInfo = null;
                            // Edge column
                            if (role == WNodeTableColumnRole.Edge || role == WNodeTableColumnRole.EdgeView)
                            {
                                edgeInfo = new EdgeInfo
                                {
                                    ColumnAttributes = new List<string>(),
                                    SinkNodes = role == WNodeTableColumnRole.Edge
                                        ? new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                                        {
                                            reader["Name3"].ToString().ToLower(CultureInfo.CurrentCulture)
                                        }
                                        : new HashSet<string>(StringComparer.OrdinalIgnoreCase),
                                    HasReversedEdge = hasRevEdge,
                                    IsReversedEdge = isRevEdge,
                                    EdgeUdfPrefix = udfPrefix,
                                };

                            }
                            columnDict.Add(name2,
                                new NodeColumns
                                {
                                    EdgeInfo = edgeInfo,
                                    Role = role,
                                });
                        }
                        // Retrieve edge attributes
                        else if (tag == -1)
                        {
                            var columnDict = columnsOfNodeTables[tableTuple];
                            columnDict[name2].EdgeInfo.ColumnAttributes.Add(reader["Name3"].ToString()
                                .ToLower(CultureInfo.CurrentCulture));
                        }
                        // Retrieve node view mapping
                        else if (tag == -2)
                        {
                            HashSet<string> nodeTableSet;
                            if (!nodeViewMapping.TryGetValue(tableTuple, out nodeTableSet))
                            {
                                nodeTableSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                                nodeViewMapping.Add(tableTuple, nodeTableSet);
                            }
                            nodeTableSet.Add(name2);
                        }
                        // Retrieve edge view mapping
                        else if (tag == -3)
                        {
                            string edgeViewSourceTableName =
                                reader["EdgeViewTable"].ToString().ToLower(CultureInfo.CurrentCulture);
                            string sourceTableName = reader["Name3"].ToString().ToLower(CultureInfo.CurrentCulture);
                            string sinkTableName =
                                columnsOfNodeTables[new Tuple<string, string>(schema, sourceTableName)][name2]
                                    .EdgeInfo.SinkNodes.First();
                            var edgeViewInfo =
                                columnsOfNodeTables[new Tuple<string, string>(schema, edgeViewSourceTableName)][
                                    name1].EdgeInfo;

                            if (!edgeViewInfo.SinkNodes.Contains(sourceTableName))
                                edgeViewInfo.SinkNodes.Add(sinkTableName);
                            if (edgeViewInfo.EdgeColumns == null)
                                edgeViewInfo.EdgeColumns = new List<Tuple<string, string>>();
                            edgeViewInfo.EdgeColumns.Add(new Tuple<string, string>(sourceTableName, name2));
                        }
                    }

                }
            }
        }

        public void Invoke(WSqlFragment node)
        {
            node.Accept(this);
        }
 
    }
}
