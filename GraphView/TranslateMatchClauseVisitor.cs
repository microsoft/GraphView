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
    /// Add Table name into the column reference with only column name & 
    /// Replace edge column name with internal edge alias (if any)
    /// </summary>
    internal class ReplaceTableRefVisitor : WSqlFragmentVisitor
    {
        /// <summary>
        /// Edge column name-> List of the candidate edge alias
        /// </summary>
        private Dictionary<string, List<string>> _edgeTableReferenceDict;

        /// <summary>
        /// Column name -> Table alias
        /// </summary>
        //private Dictionary<string, string> _columnTableDict;

        public void Invoke(WSqlFragment node, Dictionary<string, List<string>> edgeTableReferenceDict /*,
            Dictionary<string, string> columnTableDcit*/)
        {
            _edgeTableReferenceDict = edgeTableReferenceDict;
            //_columnTableDict = columnTableDcit;
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
            //else
            //{
            //    var columnName = column.Last().Value;
            //    if (_columnTableDict.ContainsKey(columnName))
            //    {
            //        column.Insert(0, new Identifier { Value = _columnTableDict[columnName] });
            //    }
            //}
            base.Visit(node);
        }
    }

    internal class DeleteSchemanameInSelectVisitor : WSqlFragmentVisitor
    {
        public void Invoke(WSelectQueryBlock node)
        {
            node.Accept(this);
        }

        public override void Visit(WColumnReferenceExpression node)
        {
            if (node.MultiPartIdentifier == null)
            {
                return;
            }
            var column = node.MultiPartIdentifier.Identifiers;
            var number = column.Count();
            if (number >= 3)
            {
                var TableName = column[number - 2];
                var ColumName = column[number - 1];
                column.Clear();
                column.Insert(0, TableName);
                column.Insert(1, ColumName);
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
        //public HashSet<string> SourceNodes;
        public HashSet<string> SinkNodes;
        public List<Tuple<string, string>> EdgeColumns;
        public IList<string> ColumnAttributes;
        public bool IsEdgeView;
    }

    public class MetaData
    {
        /// Columns of each node table. For edge columns, edge attributes are attached.
        /// (Schema name, Table name) -> (Column name -> Column Info)
        public readonly Dictionary<Tuple<string, string>, Dictionary<string, NodeColumns>> ColumnsOfNodeTables =
            new Dictionary<Tuple<string, string>, Dictionary<string, NodeColumns>>();

        // Node tables included in the node view.
        // (Schema name, Table name) -> set of the node table name included in the node view
        public readonly Dictionary<Tuple<string, string>, HashSet<string>> NodeViewMapping =
            new Dictionary<Tuple<string, string>, HashSet<string>>();

        // Source node tables, sink node tables and edge columns referenced by the edge view.
        // (Schema name, Edgeview name) -> (EdgeViewInfo:Source node tables, Sink node tables, Edge columns)
        //public readonly Dictionary<Tuple<string, string>, Dictionary<string,EdgeViewInfo>> EdgeViewMapping =
        //    new Dictionary<Tuple<string, string>, Dictionary<string, EdgeViewInfo>>();

        /// Density value of the GlobalNodeId Column in each node table.
        /// Table name -> Density value
        public readonly Dictionary<string, double> TableIdDensity =
            new Dictionary<string, double>(StringComparer.CurrentCultureIgnoreCase);
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

        // Upper Bound of the Bucket number
        private const int BucketNum = 200;

        private MetaData _metaData;

        // Set Selectivity Calculation Method
        private readonly IMatchJoinStatisticsCalculator _statisticsCalculator = new HistogramCalculator();

        // Set Pruning Strategy
        private readonly IMatchJoinPruning _pruningStrategy = new PruneJointEdge();



        public SqlConnection Conn { get; private set; }
        public WSqlTableContext Context { get; set; }


        public TranslateMatchClauseVisitor(SqlConnection conn)
        {

            Init(conn);
        }

        /// <summary>
        /// Retrieve the metadata
        /// </summary>
        /// <param name="conn"></param>
        private void Init(SqlConnection conn)
        {
            _metaData = new MetaData();
            var columnsOfNodeTables = _metaData.ColumnsOfNodeTables;
            var nodeViewMapping = _metaData.NodeViewMapping;

            Conn = conn;
            using (var command = Conn.CreateCommand())
            {
                command.CommandText = string.Format(
                    @"
                    SELECT [TableSchema], [TableName], [ColumnName], [ColumnRole], [Reference] as RefOrAtr, null as EdgeViewTable, null as ColumnId
                    FROM [{0}]
                    UNION ALL
                    SELECT [TableSchema], [TableName] ,[ColumnName], -1, [AttributeName] as RefOrAtr, null, null
                    FROM [{1}]
                    UNION ALL
                    SELECT [NodeTable1].[TableSchema], [NodeTable1].[TableName], [NodeTable2].[TableName], -2, null, null, null
                    FROM 
                        [{2}] as NodeViewMapping
                        JOIN
                        [{3}] as NodeTable1
                        ON NodeViewMapping.NodeViewTableId = NodeTable1.TableId
                        JOIN 
                        [{3}] as NodeTable2
                        ON NodeViewMapping.TableId = NodeTable2.TableId
                    UNION ALL
                    SELECT [EdgeViewCol].[TableSchema], [EdgeViewCol].[ColumnName], [EdgeCol].[ColumnName],-3, [EdgeCol].[TableName], [EdgeViewCol].[TableName], [EdgeCol].[ColumnId]
                          FROM 
                          [{4}] as [EdgeView]
                          JOIN
                          [{0}] as [EdgeViewCol]
                          ON [EdgeView].[NodeViewColumnId] = [EdgeViewCol].[ColumnId] and [EdgeViewCol].[ColumnRole] = 3
                          JOIN
                          [{0}] as [EdgeCol]
                          ON [EdgeView].[ColumnId] = [EdgeCol].[ColumnId]
                          ORDER BY [ColumnId]", GraphViewConnection.MetadataTables[1],
                    GraphViewConnection.MetadataTables[2], GraphViewConnection.MetadataTables[7],
                    GraphViewConnection.MetadataTables[0], GraphViewConnection.MetadataTables[5]);

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        int tag = (int) reader["ColumnRole"];
                        string tableSchema = reader["TableSchema"].ToString().ToLower(CultureInfo.CurrentCulture);
                        string tableName = reader["TableName"].ToString().ToLower(CultureInfo.CurrentCulture);
                        string columnName = reader["ColumnName"].ToString().ToLower(CultureInfo.CurrentCulture);
                        // Retrieve columns of node tables
                        if (tag >= 0)
                        {
                            var columnDict = columnsOfNodeTables.GetOrCreate(
                                new Tuple<string, string>(tableSchema, tableName));
                            var role = (WNodeTableColumnRole) reader["ColumnRole"];
                            EdgeInfo edgeInfo = null;
                            // Edge column
                            if (role == WNodeTableColumnRole.Edge || role == WNodeTableColumnRole.EdgeView)
                            {
                                edgeInfo = new EdgeInfo
                                {
                                    ColumnAttributes = new List<string>(),
                                    EdgeColumns = new List<Tuple<string, string>>(),
                                    SinkNodes = role == WNodeTableColumnRole.Edge
                                        ? new HashSet<string>(StringComparer.CurrentCultureIgnoreCase)
                                        {
                                            reader["RefOrAtr"].ToString().ToLower(CultureInfo.CurrentCulture)
                                        }
                                        : new HashSet<string>(StringComparer.CurrentCultureIgnoreCase),
                                    IsEdgeView = false
                                };
                               
                            }
                            columnDict.Add(columnName,
                                new NodeColumns
                                {
                                    EdgeInfo = edgeInfo,
                                    Role = (WNodeTableColumnRole) reader["ColumnRole"],
                                });
                        }
                        // Retrieve edge attributes
                        else if (tag == -1)
                        {
                            var columnDict = columnsOfNodeTables[new Tuple<string, string>(tableSchema, tableName)];
                            columnDict[columnName].EdgeInfo.ColumnAttributes.Add(reader["RefOrAtr"].ToString()
                                .ToLower(CultureInfo.CurrentCulture));
                        }
                        // Retrieve node view mapping
                        else if (tag == -2)
                        {
                            var nodeTableList =
                                nodeViewMapping.GetOrCreate(
                                    new Tuple<string, string>(tableSchema, tableName));
                            nodeTableList.Add(columnName);
                        }
                        // Retrieve edge view mapping
                        else if (tag == -3)
                        {
                            string edgeViewSourceTableName =
                                reader["EdgeViewTable"].ToString().ToLower(CultureInfo.CurrentCulture);
                            string sourceTableName = reader["RefOrAtr"].ToString().ToLower(CultureInfo.CurrentCulture);
                            string sinkTableName =
                                columnsOfNodeTables[new Tuple<string, string>(tableSchema, sourceTableName)][columnName]
                                    .EdgeInfo.SinkNodes.First();
                            var edgeViewInfo =
                                columnsOfNodeTables[new Tuple<string, string>(tableSchema, edgeViewSourceTableName)][
                                    tableName].EdgeInfo;


                            if (!edgeViewInfo.SinkNodes.Contains(sourceTableName))
                                edgeViewInfo.SinkNodes.Add(sinkTableName);
                            edgeViewInfo.EdgeColumns.Add(new Tuple<string, string>(sourceTableName, columnName));
                            edgeViewInfo.IsEdgeView = true;

                        }
                    }

                }
            }
        }

        public void Invoke(WSqlFragment node)
        {
            node.Accept(this);
        }

        /// <summary>
        /// Uses the union-Find algorithm to decompose the graph pattern into fully-connected components
        /// </summary>
        private class UnionFind
        {
            public Dictionary<string, string> Parent;

            public string Find(string x)
            {
                string k, j, r;
                r = x;
                while (Parent[r] != r)
                {
                    r = Parent[r];
                }
                k = x;
                while (k != r)
                {
                    j = Parent[k];
                    Parent[k] = r;
                    k = j;
                }
                return r;
            }

            public void Union(string a, string b)
            {
                string aRoot = Find(a);
                string bRoot = Find(b);
                if (aRoot == bRoot)
                    return;
                Parent[aRoot] = bRoot;
            }
        }

        /// <summary>
        /// Check whether the table is a valid node table
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        private bool IsNodeTable(WTableReferenceWithAlias table)
        {
            var namedTable = table as WNamedTableReference;
            if (namedTable == null)
                return false;
            var tableschema = namedTable.TableObjectName.SchemaIdentifier != null
                ? namedTable.TableObjectName.SchemaIdentifier.Value
                : "dbo";
            var tableName = namedTable.TableObjectName.BaseIdentifier.Value;
            return
                _metaData.ColumnsOfNodeTables.Keys.Contains(
                    new Tuple<string, string>(tableschema.ToLower(CultureInfo.CurrentCulture),
                        tableName.ToLower(CultureInfo.CurrentCulture)));
        }


        /// <summary>
        /// Check validity of the match clause
        /// </summary>
        /// <param name="node"></param>
        private void CheckValidity(WSelectQueryBlock node)
        {
            if (node.MatchClause == null)
                return;
            if (node.MatchClause.Paths.All(
                path => path.PathNodeList.All(
                    part => _context.CheckTable(part.Item1.BaseIdentifier.Value) &&
                            IsNodeTable(_context[part.Item1.BaseIdentifier.Value])
                    )
                ))
            {
                foreach (var path in node.MatchClause.Paths)
                {
                    var index = 0;
                    for (var count = path.PathNodeList.Count; index < count; ++index)
                    {
                        var pathNode = path.PathNodeList[index];
                        var table = _context[pathNode.Item1.BaseIdentifier.Value] as WNamedTableReference;
                        var edge =
                            pathNode.Item2.MultiPartIdentifier.Identifiers.Last().Value.ToLower();
                        var nodeTableTuple = WNamedTableReference.SchemaNameToTuple(table.TableObjectName);
                        var schema = nodeTableTuple.Item1;

                        // Bind edge/edge view to node/node view and check validity
                        string bindNode = _context.BindEdgeToNode(schema, edge, nodeTableTuple.Item2, _metaData);
                        if (string.IsNullOrEmpty(bindNode))
                            throw new GraphViewException(string.Format("Edge/EdgeView {0} cannot be bind to {1}.{2}", edge,
                                nodeTableTuple.Item1, nodeTableTuple.Item2));


                        HashSet<string> edgeSinkNodes =
                            _metaData.ColumnsOfNodeTables[new Tuple<string, string>(schema, bindNode)][edge].EdgeInfo
                                .SinkNodes;
                        HashSet<string> sinkNodes;


                        if (
                            !edgeSinkNodes.All(
                                e =>
                                    _metaData.ColumnsOfNodeTables.ContainsKey(new Tuple<string, string>(
                                        schema.ToLower(), e))))
                            throw new GraphViewException(String.Format(CultureInfo.CurrentCulture,
                                "Node Table Referenced by the Edge {0} not exists", edge));

                        var nextNode = index != count - 1
                            ? path.PathNodeList[index + 1].Item1
                            : path.Tail;
                        var getNextTable = _context[nextNode.BaseIdentifier.Value];
                        if (!IsNodeTable(getNextTable))
                            throw new GraphViewException("Node table expected in MATCH clause");
                        var nextTable = getNextTable as WNamedTableReference;
                        if (nextTable == null ||
                            !_metaData.NodeViewMapping.TryGetValue(
                                WNamedTableReference.SchemaNameToTuple(nextTable.TableObjectName), out sinkNodes))
                            sinkNodes = new HashSet<string> {nextTable.TableObjectName.BaseIdentifier.Value};
                        if (sinkNodes.All(e => !edgeSinkNodes.Contains(e)))
                        {
                            throw new GraphViewException(String.Format(CultureInfo.CurrentCulture,
                                "Wrong Reference Table {0}", nextTable.TableObjectName.BaseIdentifier.Value));
                        }

                    }
                }
            }
            else
            {
                throw new GraphViewException("Node table/view expected in MATCH clause");
            }
        }



        /// <summary>
        /// Construct Graph from the match clause. The Graph can consist of multiple connected SubGraph.
        /// Not supported in this version
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        private MatchGraph ConstructGraph(WSelectQueryBlock query)
        {
            var unionFind = new UnionFind();
            if (query.MatchClause == null)
                return null;
            var edgeTableReferenceDict = new Dictionary<string, List<string>>(StringComparer.CurrentCultureIgnoreCase);
            var matchClause = query.MatchClause;
            var nodes = new Dictionary<string, MatchNode>(StringComparer.CurrentCultureIgnoreCase);
            var connectedSubGraphs = new List<ConnectedComponent>();
            var subGrpahMap = new Dictionary<string, ConnectedComponent>(StringComparer.CurrentCultureIgnoreCase);
            var parent = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
            unionFind.Parent = parent;
            HashSet<Tuple<string, string>> nodeTypes = new HashSet<Tuple<string, string>>();

            //Construct Graph from Match Pattern
            foreach (var path in matchClause.Paths)
            {
                var index = 0;
                MatchEdge preEdge = null;
                for (var count = path.PathNodeList.Count; index < count; ++index)
                {
                    var currentNode = path.PathNodeList[index].Item1;
                    var currentEdge = path.PathNodeList[index].Item2;
                    var currentNodeExposedName = currentNode.BaseIdentifier.Value;
                    var nextNode = index != count - 1
                        ? path.PathNodeList[index + 1].Item1
                        : path.Tail;
                    var nextNodeExposedName = nextNode.BaseIdentifier.Value;
                    var node = nodes.GetOrCreate(currentNodeExposedName);
                    if (node.NodeAlias == null)
                    {
                        node.NodeAlias = currentNodeExposedName;
                        node.Neighbors = new List<MatchEdge>();
                        node.External = false;
                        var nodeTable = _context[currentNodeExposedName] as WNamedTableReference;
                        if (nodeTable != null)
                        {
                            node.NodeTableObjectName = nodeTable.TableObjectName;
                            if (node.NodeTableObjectName.SchemaIdentifier == null)
                                node.NodeTableObjectName.Identifiers.Insert(0, new Identifier {Value = "dbo"});
                            var nodeTypeTuple = WNamedTableReference.SchemaNameToTuple(node.NodeTableObjectName);
                            if (!nodeTypes.Contains(nodeTypeTuple))
                                nodeTypes.Add(nodeTypeTuple);
                            node.IncludedNodeNames = _metaData.NodeViewMapping.ContainsKey(nodeTypeTuple)
                                ? _metaData.NodeViewMapping[nodeTypeTuple]
                                : null;
                        }
                    }

                    if (currentEdge.AliasRole == AliasType.Default)
                    {
                        var currentEdgeName = currentEdge.MultiPartIdentifier.Identifiers.Last().Value;
                        if (edgeTableReferenceDict.ContainsKey(currentEdgeName))
                        {
                            edgeTableReferenceDict[currentEdgeName].Add(currentEdge.Alias);
                        }
                        else
                        {
                            edgeTableReferenceDict.Add(currentEdgeName, new List<string> {currentEdge.Alias});
                        }
                    }

                    Identifier edgeIdentifier = currentEdge.MultiPartIdentifier.Identifiers.Last();
                    string schema = node.NodeTableObjectName.SchemaIdentifier.Value.ToLower();
                    string nodeTableName = node.NodeTableObjectName.BaseIdentifier.Value;
                    string bindTableName =
                        _context.EdgeNodeBinding[
                            new Tuple<string, string>(nodeTableName.ToLower(), edgeIdentifier.Value.ToLower())].ToLower();
                    var edgeInfo = _metaData.ColumnsOfNodeTables[
                        new Tuple<string, string>(schema, bindTableName)][edgeIdentifier.Value.ToLower()]
                        .EdgeInfo;
                    var edge = new MatchEdge
                    {
                        SourceNode = node,
                        EdgeColumn = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier = new WMultiPartIdentifier
                            {
                                Identifiers = new List<Identifier>
                                {
                                    new Identifier {Value = node.NodeAlias},
                                    edgeIdentifier
                                }
                            }
                        },
                        EdgeAlias = currentEdge.Alias,
                        BindNodeTableObjName =
                            new WSchemaObjectName(
                                new Identifier {Value = schema},
                                new Identifier {Value = bindTableName}
                                ),
                        IncludedEdgeNames =
                            edgeInfo.IsEdgeView
                                ? edgeInfo.EdgeColumns
                                : null
                    };

                    if (preEdge != null)
                    {
                        preEdge.SinkNode = node;
                    }
                    preEdge = edge;

                    if (!parent.ContainsKey(currentNodeExposedName))
                        parent[currentNodeExposedName] = currentNodeExposedName;
                    if (!parent.ContainsKey(nextNodeExposedName))
                        parent[nextNodeExposedName] = nextNodeExposedName;

                    unionFind.Union(currentNodeExposedName, nextNodeExposedName);


                    node.Neighbors.Add(edge);


                    _context.AddEdgeReference(currentEdge.Alias, edge.SourceNode.NodeTableObjectName, currentEdge);
                }
                var tailExposedName = path.Tail.BaseIdentifier.Value;
                var tailNode = nodes.GetOrCreate(tailExposedName);
                if (tailNode.NodeAlias == null)
                {
                    tailNode.NodeAlias = tailExposedName;
                    tailNode.Neighbors = new List<MatchEdge>();
                    var nodeTable = _context[tailExposedName] as WNamedTableReference;
                    if (nodeTable != null)
                    {
                        tailNode.NodeTableObjectName = nodeTable.TableObjectName;
                        if (tailNode.NodeTableObjectName.SchemaIdentifier == null)
                            tailNode.NodeTableObjectName.Identifiers.Insert(0, new Identifier {Value = "dbo"});
                        var nodeTypeTuple = WNamedTableReference.SchemaNameToTuple(tailNode.NodeTableObjectName);
                        if (!nodeTypes.Contains(nodeTypeTuple))
                            nodeTypes.Add(nodeTypeTuple);
                    }
                }
                if (preEdge != null)
                    preEdge.SinkNode = tailNode;
            }

            // Put nodes into subgraphs
            foreach (var node in nodes)
            {
                string root = unionFind.Find(node.Key);
                if (!subGrpahMap.ContainsKey(root))
                {
                    var subGraph = new ConnectedComponent();
                    subGraph.Nodes[node.Key] = node.Value;
                    foreach (var edge in node.Value.Neighbors)
                    {
                        subGraph.Edges[edge.EdgeAlias] = edge;
                    }
                    subGrpahMap[root] = subGraph;
                    connectedSubGraphs.Add(subGraph);
                    subGraph.IsTailNode[node.Value] = false;
                }
                else
                {
                    var subGraph = subGrpahMap[root];
                    subGraph.Nodes[node.Key] = node.Value;
                    foreach (var edge in node.Value.Neighbors)
                    {
                        subGraph.Edges[edge.EdgeAlias] = edge;
                    }
                    subGraph.IsTailNode[node.Value] = false;
                }
            }

            // Replace Edge name alias with proper alias in the query
            var replaceTableRefVisitor = new ReplaceTableRefVisitor();
            replaceTableRefVisitor.Invoke(query, edgeTableReferenceDict);


            // If a table alias in the MATCH clause is defined in an upper-level context, 
            // to be able to translate this MATCH clause, this table alias must be re-materialized 
            // in the FROM clause of the current context and joined with the corresponding table
            // in the upper-level context. 
            var tableRefs = query.FromClause.TableReferences;
            var tableSet = new HashSet<string>(StringComparer.CurrentCultureIgnoreCase);
            var newTableRefs = new List<WTableReference>();
            for (int index = 0; index < tableRefs.Count; ++index)
            {
                var table = tableRefs[index] as WNamedTableReference;
                if (table == null)
                {
                    newTableRefs.Add(tableRefs[index]);
                    continue;
                }
                var tableTuple = WNamedTableReference.SchemaNameToTuple(table.TableObjectName);
                if (!nodeTypes.Contains(tableTuple))
                {
                    newTableRefs.Add(table);
                }
                else
                {
                    tableSet.Add(table.ExposedName.Value);
                }
            }
            query.FromClause = new WFromClause
            {
                TableReferences = newTableRefs,
            };
            WBooleanExpression whereCondiction = null;
            foreach (var node in nodes)
            {
                if (!tableSet.Contains(node.Key))
                {
                    node.Value.External = true;
                    var newWhereCondition = new WBooleanComparisonExpression
                    {
                        ComparisonType = BooleanComparisonType.Equals,
                        FirstExpr = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier = new WMultiPartIdentifier(
                                new Identifier {Value = node.Key},
                                new Identifier {Value = "GlobalNodeId"})
                        },
                        SecondExpr = new WColumnReferenceExpression
                        {
                            MultiPartIdentifier = new WMultiPartIdentifier(
                                new Identifier {Value = node.Value.RefAlias},
                                new Identifier {Value = "GlobalNodeId"})
                        },
                    };
                    whereCondiction = WBooleanBinaryExpression.Conjunction(whereCondiction, newWhereCondition);
                }
            }
            if (whereCondiction != null)
            {
                if (query.WhereClause == null)
                {
                    query.WhereClause = new WWhereClause {SearchCondition = whereCondiction};
                }
                else
                {
                    if (query.WhereClause.SearchCondition == null)
                    {
                        query.WhereClause.SearchCondition = whereCondiction;
                    }
                    else
                    {
                        query.WhereClause.SearchCondition = new WBooleanBinaryExpression
                        {
                            BooleanExpressionType = BooleanBinaryExpressionType.And,
                            FirstExpr = new WBooleanParenthesisExpression
                            {
                                Expression = query.WhereClause.SearchCondition
                            },
                            SecondExpr = new WBooleanParenthesisExpression
                            {
                                Expression = whereCondiction
                            }
                        };
                    }
                }
            }

            var graph = new MatchGraph
            {
                ConnectedSubGraphs = connectedSubGraphs,
                NodeTypesSet = nodeTypes,
            };
            unionFind.Parent = null;
            return graph;
        }

        /// <summary>
        /// Replace the Select * expression with all visible columns
        /// </summary>
        /// <param name="node"></param>
        /// <param name="graph"></param>
        private void ChangeSelectStarExpression(WSelectQueryBlock node, MatchGraph graph)
        {
            var newSelectElements = new List<WSelectElement>();
            Dictionary<string, List<WSelectElement>> starReplacement = null;
            foreach (var element in node.SelectElements)
            {
                var starElement = element as WSelectStarExpression;
                if (starElement != null)
                {
                    if (starReplacement == null)
                    {
                        starReplacement =
                            new Dictionary<string, List<WSelectElement>>(StringComparer.CurrentCultureIgnoreCase);
                        // Fetch table in order
                        foreach (var table in _context.NodeTableDictionary)
                        {
                            var alias = table.Key;
                            var namedTable = table.Value as WNamedTableReference;
                            if (namedTable != null)
                            {
                                foreach (
                                    var column in
                                        _metaData.ColumnsOfNodeTables[
                                            WNamedTableReference.SchemaNameToTuple(namedTable.TableObjectName)].Where(
                                                e => e.Value.Role != WNodeTableColumnRole.Edge).Select(e => e.Key))
                                {
                                    var elementList = starReplacement.GetOrCreate(alias);
                                    elementList.Add(new WSelectScalarExpression
                                    {
                                        SelectExpr = new WColumnReferenceExpression
                                        {
                                            MultiPartIdentifier = new WMultiPartIdentifier
                                            {
                                                Identifiers = new List<Identifier>
                                                {
                                                    new Identifier {Value = alias},
                                                    new Identifier {Value = column}
                                                }
                                            }
                                        }

                                    });
                                }
                                if (graph == null) continue;
                                foreach (var subGraph in graph.ConnectedSubGraphs)
                                {
                                    if (subGraph.Nodes.ContainsKey(alias))
                                    {
                                        var matchNode = subGraph.Nodes[alias];
                                        foreach (var edge in matchNode.Neighbors)
                                        {
                                            var schemaName = edge.SourceNode.NodeTableObjectName.SchemaIdentifier ==
                                                             null
                                                ? "dbo"
                                                : edge.SourceNode.NodeTableObjectName.SchemaIdentifier.Value.ToLower();
                                            var nodeTuple = new Tuple<string, string>(schemaName,
                                                edge.SourceNode.NodeTableObjectName.BaseIdentifier.Value.ToLower());
                                            var edgeColumnName =
                                                edge.EdgeColumn.MultiPartIdentifier.Identifiers.Last().Value.ToLower();
                                            if (!_metaData.ColumnsOfNodeTables[nodeTuple].ContainsKey(edgeColumnName))
                                            {
                                                throw new GraphViewException("Invalid Edge Alias");
                                            }
                                            foreach (
                                                var column in
                                                    _metaData.ColumnsOfNodeTables[nodeTuple][edgeColumnName].EdgeInfo
                                                        .ColumnAttributes)
                                            {
                                                var elementList = starReplacement.GetOrCreate(edge.EdgeAlias);
                                                elementList.Add(new WSelectScalarExpression
                                                {
                                                    SelectExpr = new WColumnReferenceExpression
                                                    {
                                                        MultiPartIdentifier = new WMultiPartIdentifier
                                                        {
                                                            Identifiers = new List<Identifier>
                                                            {
                                                                new Identifier {Value = edge.EdgeAlias},
                                                                new Identifier {Value = column}
                                                            }
                                                        }
                                                    }

                                                });
                                            }
                                        }
                                    }
                                }
                            }
                            else
                            {
                                var derivedTable = table.Value as WQueryDerivedTable;
                                if (derivedTable == null)
                                    continue;
                                var elementList = starReplacement.GetOrCreate(alias);
                                elementList.Add(new WSelectStarExpression
                                {
                                    Qulifier = new WMultiPartIdentifier
                                    {
                                        Identifiers = new List<Identifier> {new Identifier {Value = alias}}
                                    }
                                });

                            }
                        }
                    }
                    if (starElement.Qulifier != null)
                    {
                        newSelectElements.AddRange(starReplacement[starElement.Qulifier.Identifiers.Last().Value]);
                    }
                    else
                    {
                        foreach (var value in starReplacement.Values)
                        {
                            newSelectElements.AddRange(value);
                        }
                    }
                }
                else
                {
                    newSelectElements.Add(element);
                }
            }
            if (newSelectElements.Any())
                node.SelectElements = newSelectElements;
        }

        /// <summary>
        /// Remove the unnecessary tail node table
        /// </summary>
        /// <param name="query"></param>
        /// <param name="connectedSubGraph"></param>
        private void OptimizeTail(WSelectQueryBlock query, MatchGraph graph)
        {
            var visitor = new CheckTableReferencingVisitor();
            foreach (var connectedSubGraph in graph.ConnectedSubGraphs)
            {
                var toRemove = connectedSubGraph.Nodes.Where(
                    node => node.Value.Neighbors.Count == 0 &&
                            !visitor.Invoke(query, node.Key, _context, _metaData.ColumnsOfNodeTables)
                    )
                    .ToArray();
                foreach (var item in toRemove)
                {
                    connectedSubGraph.IsTailNode[item.Value] = true;
                }

            }

        }

        /// <summary>
        /// Update the statistics histogram for the edge given the sink id list.
        /// Bucket size is pre-defined
        /// </summary>
        /// <param name="edge"></param>
        /// <param name="sinkList"></param>
        private void UpdateEdgeHistogram(MatchEdge edge, List<long> sinkList)
        {
            sinkList.Sort();
            var rowCount = sinkList.Count;
            var statistics = new ColumnStatistics
            {
                RowCount = rowCount
            };
            var height = (int) (rowCount/BucketNum);
            var popBucketCount = 0;
            var popValueCount = 0;
            var bucketCount = 0;
            // If number in each bucket is very small, then generate a Frequency Histogram
            if (height < 2)
            {
                bucketCount = rowCount;
                long preValue = sinkList[0];
                int count = 1;
                int distCount = 1;
                for (int i = 1; i < rowCount; i++)
                {
                    var curValue = sinkList[i];
                    if (curValue == preValue)
                    {
                        count++;
                    }
                    else
                    {
                        if (count > 1)
                        {
                            popBucketCount += count;
                            popValueCount++;
                        }
                        statistics.Histogram.Add(preValue, new Tuple<double, bool>(count, count > 1));
                        count = 1;
                        preValue = curValue;
                        distCount++;
                    }
                }
                if (count > 1)
                {
                    popBucketCount += count;
                    popValueCount++;
                }
                statistics.Histogram.Add(preValue, new Tuple<double, bool>(count, count > 1));
                statistics.MaxValue = preValue;
                // Simple Denstity
                //statistics.Density = 1.0 / distCount;
                // Advanced Density
                statistics.Density = bucketCount == popBucketCount
                    ? 0
                    : 1.0*(bucketCount - popBucketCount)/bucketCount/(distCount - popValueCount);
            }

            // Generate a Height-balanced Histogram
            else
            {
                long preValue = sinkList[0];
                int count = 0;
                int distCount = 1;
                for (int i = 1; i < rowCount; i++)
                {
                    if (i%height == height - 1)
                    {
                        bucketCount++;
                        var curValue = sinkList[i];
                        if (curValue == preValue)
                            count += height;
                        else
                        {
                            distCount++;
                            if (count > height)
                            {
                                popBucketCount += count/height;
                                popValueCount++;
                            }
                            //count = count == 0 ? height : count;
                            statistics.Histogram.Add(preValue, new Tuple<double, bool>(count, count > height));
                            preValue = curValue;
                            count = height;
                        }
                    }
                }
                if (count > height)
                {
                    popBucketCount += count/height;
                    popValueCount++;
                }
                statistics.Histogram.Add(preValue, new Tuple<double, bool>(count, count > height));
                statistics.MaxValue = preValue;
                // Simple Density
                //statistics.Density = 1.0 / distCount;
                // Advanced Density
                statistics.Density = bucketCount == popBucketCount
                    ? 0
                    : 1.0*(bucketCount - popBucketCount)/bucketCount/(distCount - popValueCount);
            }
            _context.AddEdgeStatistics(edge, statistics);
        }

        /// <summary>
        /// Estimate number of rows of node table in graph.
        /// </summary>
        /// <param name="graph">Constructed node graph</param>
        private void EstimateRows(WSelectQueryBlock query, MatchGraph graph)
        {
            var declareParameter = "";
            if (_variables != null)
            {
                foreach (var parameter in _variables)
                {
                    declareParameter += "DECLARE " + parameter.VariableName.Value + " " +
                                        TsqlFragmentToString.DataType(parameter.DataType) + "\r\n";
                }
            }

            // Attach proper parts of the where clause into the Estimiation Query
            var attachPredicateVisitor = new AttachWhereClauseVisitor();
            var columnTableMapping = _context.GetColumnTableMapping(_metaData.ColumnsOfNodeTables);
            attachPredicateVisitor.Invoke(query.WhereClause, graph, columnTableMapping);

            // Attach predicates to nodes and edges
            var estimator = new TableSizeEstimator(Conn);
            bool first = true;
            var selectQuerySb = new StringBuilder(1024);
            foreach (var subGraph in graph.ConnectedSubGraphs)
            {
                foreach (var node in subGraph.Nodes)
                {
                    if (first)
                        first = false;
                    else
                        selectQuerySb.Append("\r\nUNION ALL\r\n");
                    var currentNode = node.Value;
                    selectQuerySb.AppendFormat("SELECT GlobalNodeId FROM {0} AS [{1}] WITH (ForceScan)",
                        currentNode.NodeTableObjectName,
                        currentNode.NodeAlias);
                    if (currentNode.Predicates != null && currentNode.Predicates.Count > 0)
                    {
                        selectQuerySb.AppendFormat("\r\nWHERE {0}", currentNode.Predicates[0]);
                        for (int i = 1; i < currentNode.Predicates.Count; i++)
                        {
                            var predicate = currentNode.Predicates[i];
                            selectQuerySb.AppendFormat(" AND {0}", predicate);
                        }
                    }
                }
            }

            // TODO: Can be distinguished between nodeview and node. E.g.:
            //             SELECT count(*) FROM [dbo].[EmployeeNode] AS [e1] WITH (ForceScan)
            //                                  ,[dbo].[EmployeeNode] AS [e2] WITH (ForceScan)
            //                                  , [dbo].[ClientNode] AS [c1] WITH (ForceScan)
            //                                  ,[dbo].[ClientNode] AS [c2] WITH (ForceScan)
            //             WHERE e1.WorkId!=e2.WorkId
            //             UNION ALL
            //             SELECT COUNT(*) FROM [dbo].[NV1] AS [NV1] WITH (ForceScan)
            //              WHERE [NV1].[id] = 10

            string nodeEstimationQuery = string.Format("{0}\r\n {1}\r\n", declareParameter,
                selectQuerySb);
            var estimateRows = estimator.GetUnionQueryTableEstimatedRows(nodeEstimationQuery);

            int j = 0;
            foreach (var subGraph in graph.ConnectedSubGraphs)
            {
                // Update Row Estimation for nodes
                foreach (var node in subGraph.Nodes)
                {
                    var currentNode = node.Value;
                    var tableSchema = currentNode.NodeTableObjectName.SchemaIdentifier.Value;
                    var tableName = currentNode.NodeTableObjectName.BaseIdentifier.Value;
                    var tableTuple = WNamedTableReference.SchemaNameToTuple(currentNode.NodeTableObjectName);
                    if (_metaData.NodeViewMapping.ContainsKey(tableTuple))
                    {
                        var nodeSet = _metaData.NodeViewMapping[tableTuple];
                        int n = nodeSet.Count;
                        double nodeViewEstRows = 0.0;
                        while (n > 0)
                        {
                            n--;
                            nodeViewEstRows += estimateRows[j];
                            j++;
                        }
                        currentNode.EstimatedRows = nodeViewEstRows;
                        currentNode.TableRowCount = nodeSet.Aggregate(0,
                            (cur, next) => cur + estimator.GetTableRowCount(tableSchema, next));
                    }
                    else
                    {
                        currentNode.EstimatedRows = estimateRows[j];
                        currentNode.TableRowCount = estimator.GetTableRowCount(tableSchema, tableName);
                        j++;
                    }
                }
            }


        }

        /// <summary>
        /// Estimate the average degree of the edges and retrieve density value.
        /// Send a query to retrieve the varbinary of the sink in the edge sampling table with edge predicates,
        /// then generate the statistics histogram for each edge
        /// </summary>
        /// <param name="subGraph"></param>
        private void EstimateAverageDegree(MatchGraph graph)
        {
            // Declare the parameters if any
            var declareParameter = "";
            if (_variables != null)
            {
                foreach (var parameter in _variables)
                {
                    declareParameter += "DECLARE " + parameter.VariableName.Value + " " +
                                        TsqlFragmentToString.DataType(parameter.DataType) + "\r\n";
                }
            }

            // Calculate the average degree
            var sb = new StringBuilder();
            bool first = true;
            sb.Append("SELECT [Edge].*, [EdgeDegrees].[SampleRowCount], [EdgeDegrees].[AverageDegree] FROM");
            sb.Append("(\n");
            foreach (var edge in graph.ConnectedSubGraphs.SelectMany(subGraph => subGraph.Edges.Values))
            {
                if (!first)
                    sb.Append("\nUNION ALL\n");
                else
                {
                    first = false;
                }
                var tableObjectName = edge.SourceNode.NodeTableObjectName;
                string schema = tableObjectName.SchemaIdentifier.Value.ToLower();
                string tableName = tableObjectName.BaseIdentifier.Value.ToLower();
                string edgeName = edge.EdgeColumn.MultiPartIdentifier.Identifiers.Last().Value.ToLower();
                string bindTableName = _context.EdgeNodeBinding[new Tuple<string, string>(tableName, edgeName)];
                sb.Append(
                    string.Format(@"
                            SELECT '{0}' as TableSchema, 
                                   '{1}' as TableName, 
                                   '{2}' as ColumnName,
                                   '{3}' as Alias, 
                                    [dbo].[GraphViewUDFGlobalNodeIdEncoder](Sink) as Sink
                            FROM [{0}_{1}_{2}_Sampling] as [{3}]", schema,
                        bindTableName,
                        edgeName,
                        edge.EdgeAlias));
                if (edge.Predicates != null)
                {
                    sb.Append("\n WHERE ");
                    bool fisrtPre = true;
                    foreach (var predicate in edge.Predicates)
                    {
                        if (fisrtPre)
                            fisrtPre = false;
                        else
                        {
                            sb.Append(" AND ");
                        }
                        sb.Append(predicate);
                    }
                }
            }
            sb.Append("\n) as Edge \n");
            sb.Append(String.Format(@"
                        INNER JOIN
                            [{0}] as [EdgeDegrees]
                        ON 
                            [EdgeDegrees].[TableSchema] = [Edge].[TableSchema] 
                        AND [EdgeDegrees].[TableName] = [Edge].[TableName] 
                        AND [EdgeDegrees].[ColumnName] = [Edge].[ColumnName]", GraphViewConnection.MetadataTables[3]));

            // Retrieve density value for each node table
            var tableIdDensity = _metaData.TableIdDensity;
            tableIdDensity.Clear();
            string tempTableName = Path.GetRandomFileName().Replace(".", "").Substring(0, 8);
            var dbccDensityQuery = new StringBuilder();
            dbccDensityQuery.Append(string.Format(@"CREATE TABLE #{0} (Density float, Len int, Col sql_variant);
                                                    INSERT INTO #{0} EXEC('", tempTableName));
            foreach (var nodeTable in graph.NodeTypesSet)
            {
                tableIdDensity[string.Format("[{0}].[{1}]", nodeTable.Item1, nodeTable.Item2)] =
                    ColumnStatistics.DefaultDensity;
                if (
                    !_metaData.NodeViewMapping.ContainsKey(new Tuple<string, string>(nodeTable.Item1.ToLower(),
                        nodeTable.Item2.ToLower())))
                    dbccDensityQuery.Append(string.Format(
                        "DBCC SHOW_STATISTICS (\"{0}.{1}\", [{0}{1}_PK_GlobalNodeId]) with DENSITY_VECTOR;\n",
                        nodeTable.Item1,
                        nodeTable.Item2));
            }
            dbccDensityQuery.Append("');\n");
            dbccDensityQuery.Append(string.Format("SELECT Density FROM #{0} WHERE Col = 'GlobalNodeId'", tempTableName));

            using (var command = Conn.CreateCommand())
            {
                command.CommandText = declareParameter + sb.ToString();
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        MatchEdge edge;
                        if (!graph.TryGetEdge(reader["Alias"].ToString(), out edge))
                            throw new GraphViewException(string.Format("Edge {0} not exists", reader["Alias"].ToString()));
                        var sinkBytes = reader["Sink"] as byte[];
                        if (sinkBytes == null)
                        {
                            _context.AddEdgeStatistics(edge, new ColumnStatistics
                            {
                                Density = 0,
                                Histogram = new Dictionary<long, Tuple<double, bool>>(),
                                MaxValue = 0,
                                RowCount = 0,
                                Selectivity = 1.0
                            });
                            continue;
                        }
                        List<long> sinkList = new List<long>();
                        var cursor = 0;
                        while (cursor < sinkBytes.Length)
                        {
                            var sink = BitConverter.ToInt64(sinkBytes, cursor);
                            cursor += 8;
                            sinkList.Add(sink);
                        }
                        UpdateEdgeHistogram(edge, sinkList);
                        edge.AverageDegree = Convert.ToDouble(reader["AverageDegree"])*sinkList.Count*1.0/
                                             Convert.ToInt64(reader["SampleRowCount"]);
                    }
                }

                var tableKey = tableIdDensity.Keys.ToArray();
                command.CommandText = dbccDensityQuery.ToString();
                using (var reader = command.ExecuteReader())
                {

                    foreach (var key in tableKey)
                    {
                        if (!reader.Read())
                            break;
                        double density = Convert.ToDouble(reader["Density"]);
                        if (Math.Abs(density - 1.0) < 0.0001)
                            density = ColumnStatistics.DefaultDensity;
                        tableIdDensity[key] = density;
                    }

                }
            }
            tableIdDensity = tableIdDensity.OrderBy(e => e.Key).ToDictionary(e => e.Key, e => e.Value);
        }

        // TODO: Use Heap O(logN)
        /// <summary>
        /// Get the index and the average size per edege of the component with maximum average size
        /// </summary>
        /// <param name="components"></param>
        /// <returns></returns>
        public Tuple<int, double> GetMostExpensiveMatchComponent(List<MatchComponent> components)
        {
            int index = 0;
            int edgeCount = components[0].EdgeMaterilizedDict.Count;
            edgeCount = edgeCount == 0 ? 1 : edgeCount;
            double maxValue = components[0].Cost/edgeCount;
            for (int i = 1; i < components.Count; i++)
            {
                edgeCount = components[i].EdgeMaterilizedDict.Count;
                edgeCount = edgeCount == 0 ? 1 : edgeCount;
                if (components[i].Cost/edgeCount > maxValue)
                {
                    index = i;
                    maxValue = components[i].Cost/edgeCount;
                }
            }
            return new Tuple<int, double>(index, maxValue);
        }

        /// <summary>
        /// Get a full one height tree with joint edges and unmaterlized edges,
        /// returns a tuple whose first item is the one height tree and the second item
        /// indicates whether the one height tree only joins with the component's materialized
        /// node on its root.
        /// </summary>
        /// <param name="graph"></param>
        /// <param name="component"></param>
        /// <returns></returns>
        public IEnumerable<Tuple<OneHeightTree, bool>> GetNodeUnits(ConnectedComponent graph, MatchComponent component)
        {
            var nodes = graph.Nodes;
            foreach (var node in nodes.Values.Where(e => !graph.IsTailNode[e]))
            {
                //var newJoinUnitList = new List<MatchJoinUnit>()
                bool joint = false;
                var jointEdges = new List<MatchEdge>();
                var nodeEdgeDict = node.Neighbors.ToDictionary(e => e,
                    e => component.EdgeMaterilizedDict.ContainsKey(e));

                // Edge to component node
                foreach (var edge in node.Neighbors.Where(e => !nodeEdgeDict[e]))
                {
                    if (component.Nodes.Contains(edge.SinkNode))
                    {
                        joint = true;
                        nodeEdgeDict[edge] = true;
                        jointEdges.Add(edge);
                    }
                }


                // Component edge to node
                if (!joint && component.UnmaterializedNodeMapping.ContainsKey(node))
                {
                    joint = true;
                }


                // Add unpopulated edges
                var nodeUnpopulatedEdges = nodeEdgeDict.Where(e => !e.Value).Select(e => e.Key).ToList();

                if (joint)
                    yield return new Tuple<OneHeightTree, bool>(new OneHeightTree
                    {
                        TreeRoot = node,
                        MaterializedEdges = jointEdges,
                        UnmaterializedEdges = nodeUnpopulatedEdges,
                    }, false);

                // Single node edge
                else if (nodeUnpopulatedEdges.Count > 0 && component.MaterializedNodeSplitCount.Count > 1 &&
                         component.MaterializedNodeSplitCount.ContainsKey(node))
                {
                    yield return new Tuple<OneHeightTree, bool>(new OneHeightTree
                    {
                        TreeRoot = node,
                        MaterializedEdges = jointEdges,
                        UnmaterializedEdges = nodeUnpopulatedEdges,
                    }, true);
                }
            }
        }



        /// <summary>
        /// Get the optimal join component for the given connected graph
        /// 1. Generate the initial states
        /// 2. DP, Iterate on each states: 
        ///     Get smallest join units -> Enumerate on all possible combination of the join units
        ///     -> Join to the current component to get the next states 
        ///     -> Those components with the largest average size per edge will be eliminate if exceeding the upper bound
        /// 3. If all the components has reached its end states, return the component with the smallest join cost
        /// </summary>
        /// <param name="subGraph"></param>
        /// <returns></returns>
        public MatchComponent ConstructComponent(ConnectedComponent subGraph)
        {
            var componentStates = new List<MatchComponent>();
            var nodes = subGraph.Nodes;
            var edges = subGraph.Edges;
            int nodeCount = subGraph.IsTailNode.Count(e => !e.Value);
            MatchComponent finishedComponent = null;

            //Init
            int maxIndex = -1;
            double maxValue = Double.MinValue;
            foreach (var node in nodes)
            {
                if (!subGraph.IsTailNode[node.Value])
                {
                    // Enumerate on each edge for a node to generate the intial states
                    var edgeCount = node.Value.Neighbors.Count;
                    int eNum = (int) Math.Pow(2, edgeCount) - 1;
                    while (eNum > 0)
                    {
                        var nodeInitialEdges = new List<MatchEdge>();
                        for (int i = 0; i < edgeCount; i++)
                        {
                            int index = (1 << i);
                            if ((eNum & index) != 0)
                            {
                                nodeInitialEdges.Add(node.Value.Neighbors[i]);
                            }
                        }
                        componentStates.Add(new MatchComponent(node.Value, nodeInitialEdges, _context));
                        eNum--;
                    }
                }
            }


            // DP
            while (componentStates.Any())
            {
                maxIndex = -1;
                var nextCompnentStates = new List<MatchComponent>();

                // Iterate on current components
                foreach (var curComponent in componentStates)
                {
                    var nodeUnits = GetNodeUnits(subGraph, curComponent).ToList();
                    if (!nodeUnits.Any())
                    {
                        if (finishedComponent == null || curComponent.Cost < finishedComponent.Cost)
                        {
                            finishedComponent = curComponent;
                        }
                        continue;
                    }


                    var candidateUnits = _pruningStrategy.GetCandidateUnits(nodeUnits, curComponent);

                    // Iterate on the candidate node units & add it to the current component to generate next states
                    foreach (var candidateUnit in candidateUnits)
                    {
                        // Pre-filter
                        if (finishedComponent != null &&
                            (curComponent.Size +
                             candidateUnit.TreeRoot.EstimatedRows*
                             candidateUnit.UnmaterializedEdges.Select(e => e.AverageDegree)
                                 .Aggregate(1.0, (cur, next) => cur*next)*
                             candidateUnit.MaterializedEdges.Select(e => e.AverageDegree)
                                 .Aggregate(1.0, (cur, next) => cur*next) >
                             finishedComponent.Cost))
                        {
                            continue;
                        }

                        // TODO : redundant work if newSize>maxvalue
                        var newComponent = curComponent.GetNextState(candidateUnit, _metaData, _statisticsCalculator);
                        if (nextCompnentStates.Count >= MaxStates)
                        {
                            if (maxIndex < 0)
                            {
                                var tuple = GetMostExpensiveMatchComponent(nextCompnentStates);
                                maxIndex = tuple.Item1;
                                maxValue = tuple.Item2;
                            }
                            else
                            {
                                int edgeCount = newComponent.EdgeMaterilizedDict.Count;
                                edgeCount = edgeCount == 0 ? 1 : edgeCount;
                                if (newComponent.Cost/edgeCount < maxValue)
                                {
                                    var temp = nextCompnentStates[maxIndex];
                                    nextCompnentStates[maxIndex] = newComponent;
                                    var tuple = GetMostExpensiveMatchComponent(nextCompnentStates);
                                    maxIndex = tuple.Item1;
                                    maxValue = tuple.Item2;
                                }
                                continue;
                            }
                        }
                        nextCompnentStates.Add(newComponent);
                    }
                }
                componentStates = nextCompnentStates;
            }

            return finishedComponent;

        }

        /// <summary>
        /// Update from clause in the query using optimal component of each connected sub-graph
        /// </summary>
        /// <param name="node"></param>
        /// <param name="components"></param>
        private void UpdateQuery(WSelectQueryBlock node, List<MatchComponent> components)
        {
            string newWhereString = "";
            foreach (var component in components)
            {
                // Add down size predicates
                foreach (var joinTableTuple in component.FatherListofDownSizeTable)
                {
                    var joinTable = joinTableTuple.Item1;
                    joinTable.JoinCondition = WBooleanBinaryExpression.Conjunction(joinTable.JoinCondition,
                        new WBooleanComparisonExpression
                        {
                            ComparisonType = BooleanComparisonType.Equals,
                            FirstExpr = new WFunctionCall
                            {
                                CallTarget = new WMultiPartIdentifierCallTarget
                                {
                                    Identifiers = new WMultiPartIdentifier(new Identifier {Value = "dbo"})
                                },
                                FunctionName = new Identifier {Value = "DownSizeFunction"},
                                Parameters = new List<WScalarExpression>
                                {
                                    new WColumnReferenceExpression
                                    {
                                        MultiPartIdentifier = new WMultiPartIdentifier
                                        {
                                            Identifiers = new List<Identifier>
                                            {
                                                new Identifier {Value = joinTableTuple.Item2},
                                                new Identifier {Value = "LocalNodeid"}
                                            }
                                        }
                                    }
                                }
                            },
                            SecondExpr = new WValueExpression("1", false)
                        });
                }

                // Update from clause
                node.FromClause.TableReferences.Add(component.TableRef);

                // Add predicates for split nodes
                var component1 = component;
                foreach (
                    var compNode in
                        component.MaterializedNodeSplitCount.Where(
                            e => e.Value > 0 && e.Key.Predicates != null && e.Key.Predicates.Any()))
                {
                    var matchNode = compNode.Key;

                    WBooleanExpression newExpression = null;
                    foreach (var predicate in matchNode.Predicates)
                    {
                        newExpression = WBooleanBinaryExpression.Conjunction(newExpression, predicate);
                    }
                    string predicateString = newExpression.ToString();
                    var nodeCount = component1.MaterializedNodeSplitCount[matchNode];

                    while (nodeCount > 0)
                    {
                        newWhereString += " AND ";
                        string tempStr = predicateString.Replace(string.Format("[{0}]", matchNode.RefAlias.ToUpper()),
                            string.Format("[{0}_{1}]", matchNode.RefAlias.ToUpper(), nodeCount));
                        tempStr = tempStr.Replace(string.Format("[{0}]", matchNode.RefAlias.ToLower()),
                            string.Format("[{0}_{1}]", matchNode.RefAlias.ToLower(), nodeCount));
                        newWhereString += tempStr;
                        //newWhereString += predicateString.Replace(string.Format("[{0}]", matchNode.RefAlias.ToLower()),
                        //    string.Format("[{0}_{1}]", matchNode.RefAlias, nodeCount));
                        nodeCount--;
                    }
                }
            }
            if (newWhereString.Any())
            {
                node.WhereClause.SearchCondition = new WBooleanParenthesisExpression
                {
                    Expression = node.WhereClause.SearchCondition
                };
                node.WhereClause.GhostString = newWhereString;
            }

            var visitor2 = new DeleteSchemanameInSelectVisitor();
            visitor2.Invoke(node);
        }

        /// <summary>
        /// Recorde the declared parameter in the Store Procedure Statement
        /// </summary>
        /// <param name="node"></param>
        public override void Visit(WProcedureStatement node)
        {
            var upperPar = _variables;
            if (node.Parameters != null)
            {
                if (_variables == null)
                {
                    _variables = new List<DeclareVariableElement>();
                }
                else
                {
                    _variables = new List<DeclareVariableElement>(_variables);
                }
                foreach (var parameter in node.Parameters)
                {
                    _variables.Add(parameter);
                }
            }
            base.Visit(node);
            _variables = upperPar;
        }

        public override void Visit(WDeclareVariableStatement node)
        {
            if (node.Statement.Declarations != null)
            {
                if (_variables == null)
                {
                    _variables = new List<DeclareVariableElement>();
                }
                foreach (var parameter in node.Statement.Declarations)
                {
                    _variables.Add(parameter);
                }
            }
        }

        /// <summary>
        /// The entry of the optimizer, activated when visting each Select Query Block
        /// </summary>
        /// <param name="node"></param>
        public override void Visit(WSelectQueryBlock node)
        {
            var checkVarVisitor = new CollectVariableVisitor();
            var currentContext = checkVarVisitor.Invoke(node.FromClause, _metaData.ColumnsOfNodeTables.Keys);
            currentContext.UpperLevel = _context;
            _context = currentContext;

            base.Visit(node);

            _statisticsCalculator.Context = _context;
            CheckValidity(node);
            var graph = ConstructGraph(node);
            //ChangeSelectStarExpression(node, graph);

            if (graph != null)
            {
                OptimizeTail(node, graph);
                EstimateRows(node, graph);
                EstimateAverageDegree(graph);

                var components = new List<MatchComponent>();
                foreach (var subGraph in graph.ConnectedSubGraphs)
                {


                    components.Add(ConstructComponent(subGraph));
#if DEBUG
                    foreach (var matchNode in subGraph.Nodes.Values)
                    {
                        Trace.WriteLine(matchNode.NodeAlias);
                        Trace.WriteLine(string.Format("  RowCount:{0}", matchNode.TableRowCount));
                        Trace.WriteLine(string.Format("  EstiRow:{0}", matchNode.EstimatedRows));
                    }
#endif

                }

                UpdateQuery(node, components);

#if DEBUG
                Trace.WriteLine(string.Format("Rows:{0}", components[0].Size));
                Trace.WriteLine(string.Format("Cost:{0}", components[0].Cost));
                Trace.WriteLine(string.Format("Estimated Rows:{0}", components[0].EstimateSize));

#endif
                node.MatchClause = null;
            }

            _context = _context.UpperLevel;
        }

    }
}
