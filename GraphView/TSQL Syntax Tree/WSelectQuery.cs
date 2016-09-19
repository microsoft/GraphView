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
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualBasic;

namespace GraphView
{
    /// <summary>
    /// The base class of a SELECT statement
    /// </summary>
    public partial class WSelectStatement : WStatementWithCtesAndXmlNamespaces
    {
        // The table name of the INTO clause
        internal WSchemaObjectName Into { set; get; }

        // The body of the SELECT statement
        internal WSelectQueryExpression QueryExpr { set; get; }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);

            if (Into != null)
            {
                sb.AppendFormat("{0}SELECT INTO {1}\r\n", indent, Into);
            }
            sb.Append(QueryExpr.ToString(indent));
            sb.Append(OptimizerHintListToString(indent));

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Into != null)
                Into.Accept(visitor);
            if (QueryExpr != null)
                QueryExpr.Accept(visitor);
            base.AcceptChildren(visitor);


        }
    }

    /// <summary>
    /// The base class of the SELECT query hierarchy
    /// </summary>
    public abstract partial class WSelectQueryExpression : WSqlStatement
    {
        // Omit ForClause and OffsetClause

        internal WOrderByClause OrderByClause { set; get; }
        internal WSchemaObjectName Into { set; get; }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (OrderByClause != null)
                OrderByClause.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    /// <summary>
    /// SELECT query within a parenthesis
    /// </summary>
    public partial class WQueryParenthesisExpression : WSelectQueryExpression
    {
        internal WSelectQueryExpression QueryExpr { get; set; }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);

            sb.AppendFormat("{0}(\r\n", indent);
            sb.AppendFormat("{0}\r\n", QueryExpr.ToString(indent));
            sb.AppendFormat("{0})", indent);

            return sb.ToString();
        }


        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (QueryExpr != null)
                QueryExpr.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    /// <summary>
    /// Represents the union/except/intersect of SELECT queries.
    /// </summary>
    public partial class WBinaryQueryExpression : WSelectQueryExpression
    {
        // Indicates whether the ALL keyword is used in the binary SQL espression.
        internal bool All { set; get; }

        // The binary operation type: union, except or intersect
        internal BinaryQueryExpressionType BinaryQueryExprType { get; set; }

        internal WSelectQueryExpression FirstQueryExpr { get; set; }
        internal WSelectQueryExpression SecondQueryExpr { get; set; }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);

            sb.AppendFormat("{0}\r\n", FirstQueryExpr.ToString(indent));

            sb.AppendFormat(All ? "{0}{1} ALL\r\n" : "{0}{1}\r\n", indent,
                TsqlFragmentToString.BinaryQueryExpressionType(BinaryQueryExprType));

            sb.AppendFormat("{0}", SecondQueryExpr.ToString(indent));

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (FirstQueryExpr != null)
                FirstQueryExpr.Accept(visitor);
            if (SecondQueryExpr != null)
                SecondQueryExpr.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    /// <summary>
    /// The body of the SELECT query, including a list of selected elements, FROM and WHERE clauses
    /// </summary>
    public partial class WSelectQueryBlock : WSelectQueryExpression
    {
        internal IList<WSelectElement> SelectElements { get; set; }
        internal WFromClause FromClause { get; set; }
        internal WWhereClause WhereClause { get; set; }
        internal WTopRowFilter TopRowFilter { get; set; }
        internal WGroupByClause GroupByClause { get; set; }
        internal WHavingClause HavingClause { get; set; }
        internal WMatchClause MatchClause { get; set; }
        internal WLimitClause LimitClause { get; set; }
        internal WWithPathClause WithPathClause { get; set; }
        internal UniqueRowFilter UniqueRowFilter { get; set; }
        internal bool OutputPath { get; set; }
        public WSelectQueryBlock()
        {
            FromClause = new WFromClause();
            WhereClause = new WWhereClause();
        }

        internal override bool OneLine()
        {
            if (FromClause == null &&
                WhereClause == null &&
                OrderByClause == null &&
                GroupByClause == null)
            {
                return SelectElements.All(sel => sel.OneLine());
            }
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(1024);

            sb.AppendFormat("{0}SELECT ", indent);

            if (TopRowFilter != null)
            {
                if (TopRowFilter.OneLine())
                {
                    sb.AppendFormat("{0} ", TopRowFilter.ToString(""));
                }
                else
                {
                    sb.Append("\r\n");
                    sb.AppendFormat("{0} ", TopRowFilter.ToString(indent));
                }
            }

            switch (UniqueRowFilter)
            {
                case UniqueRowFilter.All:
                    sb.Append("ALL ");
                    break;
                case UniqueRowFilter.Distinct:
                    sb.Append("DISTINCT ");
                    break;
            }

            for (var i = 0; i < SelectElements.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(", ");
                }

                if (SelectElements[i].OneLine())
                {
                    sb.Append(SelectElements[i].ToString(""));
                }
                else
                {
                    sb.Append("\r\n");
                    sb.Append(SelectElements[i].ToString(indent + " "));
                }
            }

            if (Into != null)
            {
                sb.AppendFormat(" INTO {0} ", Into);
            }

            if (FromClause.TableReferences != null)
            {
                sb.Append("\r\n");
                sb.Append(FromClause.ToString(indent));
            }

            if (MatchClause != null)
            {
                sb.Append("\r\n");
                sb.Append(MatchClause.ToString(indent));
            }

            if (WhereClause.SearchCondition != null || !string.IsNullOrEmpty(WhereClause.GhostString))
            {
                sb.Append("\r\n");
                sb.Append(WhereClause.ToString(indent));
            }

            if (GroupByClause != null)
            {
                sb.Append("\r\n");
                sb.Append(GroupByClause.ToString(indent));
            }

            if (HavingClause != null)
            {
                sb.Append("\r\n");
                sb.Append(HavingClause.ToString(indent));
            }

            if (OrderByClause != null)
            {
                sb.Append("\r\n");
                sb.Append(OrderByClause.ToString(indent));
            }

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (FromClause != null)
                FromClause.Accept(visitor);
            if (WhereClause != null)
                WhereClause.Accept(visitor);
            if (TopRowFilter != null)
                TopRowFilter.Accept(visitor);
            if (GroupByClause != null)
                GroupByClause.Accept(visitor);
            if (HavingClause != null)
                HavingClause.Accept(visitor);

            if (SelectElements != null)
            {
                var index = 0;
                for (var count = SelectElements.Count; index < count; ++index)
                    SelectElements[index].Accept(visitor);
            }

            base.AcceptChildren(visitor);
        }

        internal override GraphViewOperator Generate(GraphViewConnection pConnection)
        {
            if (WithPathClause != null) WithPathClause.Generate(pConnection);
            // Construct Match graph for later use
            MatchGraph graph = ConstructGraph();
            // Construct a header for the processor it will generate to interpret its result
            List<string> header = ConstructHeader(graph);
            // Attach pre-generated docDB script to the node on Match graph
            List<BooleanFunction> Functions = AttachScriptSegment(graph, header);
            // Generate proper processor for the current syntax element
            return ConstructOperator(graph, header, pConnection, Functions);
        }

        private MatchGraph ConstructGraph()
        {
            Dictionary<string, List<string>> EdgeColumnToAliasesDict = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, MatchPath> pathDictionary = new Dictionary<string, MatchPath>(StringComparer.OrdinalIgnoreCase);

            UnionFind UnionFind = new UnionFind();
            Dictionary<string, MatchNode> Nodes = new Dictionary<string, MatchNode>(StringComparer.OrdinalIgnoreCase);
            List<ConnectedComponent> ConnectedSubGraphs = new List<ConnectedComponent>();
            Dictionary<string, ConnectedComponent> SubGrpahMap = new Dictionary<string, ConnectedComponent>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, string> Parent = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            UnionFind.Parent = Parent;

            // Retrive information from the SelectQueryBlcok
            if (FromClause != null)
            {
                foreach (WTableReferenceWithAlias FromReference in FromClause.TableReferences)
                {
                    Nodes.GetOrCreate(FromReference.Alias.Value);
                    if (!Parent.ContainsKey(FromReference.Alias.Value))
                        Parent[FromReference.Alias.Value] = FromReference.Alias.Value;
                }
            }

            // Consturct nodes and edges of a match graph defined by the SelectQueryBlock
            if (MatchClause != null)
            {
                if (MatchClause.Paths.Count > 0)
                {
                    foreach (var path in MatchClause.Paths)
                    {
                        var index = 0;
                        // Consturct the source node of a path in MatchClause.Paths
                        MatchEdge EdgeToSrcNode = null;
                        for (var count = path.PathEdgeList.Count; index < count; ++index)
                        {
                            var CurrentNodeTableRef = path.PathEdgeList[index].Item1;
                            var CurrentEdgeColumnRef = path.PathEdgeList[index].Item2;
                            var CurrentNodeExposedName = CurrentNodeTableRef.BaseIdentifier.Value;
                            var nextNodeTableRef = index != count - 1
                                ? path.PathEdgeList[index + 1].Item1
                                : path.Tail;
                            var nextNodeExposedName = nextNodeTableRef.BaseIdentifier.Value;
                            var SrcNode = Nodes.GetOrCreate(CurrentNodeExposedName);
                            if (SrcNode.NodeAlias == null)
                            {
                                SrcNode.NodeAlias = CurrentNodeExposedName;
                                SrcNode.Neighbors = new List<MatchEdge>();
                                SrcNode.ReverseNeighbors = new List<MatchEdge>();
                                SrcNode.External = false;
                                SrcNode.Predicates = new List<WBooleanExpression>();
                            }

                            // Consturct the edge of a path in MatchClause.Paths
                            string EdgeAlias = CurrentEdgeColumnRef.Alias;
                            if (EdgeAlias == null)
                            {
                                bool isReversed = path.IsReversed;
                                var CurrentEdgeName = CurrentEdgeColumnRef.MultiPartIdentifier.Identifiers.Last().Value;
                                string originalEdgeName = null;

                                EdgeAlias = string.Format("{0}_{1}_{2}", CurrentNodeExposedName, CurrentEdgeName,
                                    nextNodeExposedName);

                                // when current edge is a reversed edge, the key should still be the original edge name
                                var edgeNameKey = isReversed ? originalEdgeName : CurrentEdgeName;
                                if (EdgeColumnToAliasesDict.ContainsKey(edgeNameKey))
                                {
                                    EdgeColumnToAliasesDict[edgeNameKey].Add(EdgeAlias);
                                }
                                else
                                {
                                    EdgeColumnToAliasesDict.Add(edgeNameKey, new List<string> { EdgeAlias });
                                }
                            }

                            MatchEdge EdgeFromSrcNode;
                            if (CurrentEdgeColumnRef.MinLength == 1 && CurrentEdgeColumnRef.MaxLength == 1)
                            {
                                EdgeFromSrcNode = new MatchEdge
                                {
                                    SourceNode = SrcNode,
                                    EdgeColumn = CurrentEdgeColumnRef,
                                    EdgeAlias = EdgeAlias,
                                    Predicates = new List<WBooleanExpression>(),
                                    BindNodeTableObjName =
                                        new WSchemaObjectName(
                                            ),
                                };
                            }
                            else
                            {
                                MatchPath matchPath = new MatchPath
                                {
                                    SourceNode = SrcNode,
                                    EdgeColumn = CurrentEdgeColumnRef,
                                    EdgeAlias = EdgeAlias,
                                    Predicates = new List<WBooleanExpression>(),
                                    BindNodeTableObjName =
                                        new WSchemaObjectName(
                                            ),
                                    MinLength = CurrentEdgeColumnRef.MinLength,
                                    MaxLength = CurrentEdgeColumnRef.MaxLength,
                                    ReferencePathInfo = false,
                                    AttributeValueDict = CurrentEdgeColumnRef.AttributeValueDict
                                };
                                pathDictionary[EdgeAlias] = matchPath;
                                EdgeFromSrcNode = matchPath;
                            }

                            if (EdgeToSrcNode != null)
                            {
                                EdgeToSrcNode.SinkNode = SrcNode;
                                //Add ReverseEdge
                                MatchEdge reverseEdge;
                                reverseEdge = new MatchEdge
                                {
                                    SourceNode = EdgeToSrcNode.SinkNode,
                                    SinkNode = EdgeToSrcNode.SourceNode,
                                    EdgeColumn = EdgeToSrcNode.EdgeColumn,
                                    EdgeAlias = EdgeToSrcNode.EdgeAlias,
                                    Predicates = EdgeToSrcNode.Predicates,
                                    BindNodeTableObjName =
                                       new WSchemaObjectName(
                                           ),
                                };
                                SrcNode.ReverseNeighbors.Add(reverseEdge);
                            }

                            EdgeToSrcNode = EdgeFromSrcNode;

                            if (!Parent.ContainsKey(CurrentNodeExposedName))
                                Parent[CurrentNodeExposedName] = CurrentNodeExposedName;
                            if (!Parent.ContainsKey(nextNodeExposedName))
                                Parent[nextNodeExposedName] = nextNodeExposedName;

                            UnionFind.Union(CurrentNodeExposedName, nextNodeExposedName);

                            SrcNode.Neighbors.Add(EdgeFromSrcNode);


                        }
                        // Consturct destination node of a path in MatchClause.Paths
                        var tailExposedName = path.Tail.BaseIdentifier.Value;
                        var DestNode = Nodes.GetOrCreate(tailExposedName);
                        if (DestNode.NodeAlias == null)
                        {
                            DestNode.NodeAlias = tailExposedName;
                            DestNode.Neighbors = new List<MatchEdge>();
                            DestNode.ReverseNeighbors = new List<MatchEdge>();
                            DestNode.Predicates = new List<WBooleanExpression>();
                        }
                        if (EdgeToSrcNode != null)
                        {
                            EdgeToSrcNode.SinkNode = DestNode;
                            //Add ReverseEdge
                            MatchEdge reverseEdge;
                            reverseEdge = new MatchEdge
                            {
                                SourceNode = EdgeToSrcNode.SinkNode,
                                SinkNode = EdgeToSrcNode.SourceNode,
                                EdgeColumn = EdgeToSrcNode.EdgeColumn,
                                EdgeAlias = EdgeToSrcNode.EdgeAlias,
                                Predicates = EdgeToSrcNode.Predicates,
                                BindNodeTableObjName =
                                   new WSchemaObjectName(
                                       ),
                            };
                            DestNode.ReverseNeighbors.Add(reverseEdge);
                        }
                    }

                }
            }
            // Use union find algorithmn to define which subgraph does a node belong to and put it into where it belongs to.
            foreach (var node in Nodes)
            {
                string root;

                root = UnionFind.Find(node.Key);  // put them into the same graph

                var patternNode = node.Value;

                if (patternNode.NodeAlias == null)
                {
                    patternNode.NodeAlias = node.Key;
                    patternNode.Neighbors = new List<MatchEdge>();
                    patternNode.ReverseNeighbors = new List<MatchEdge>();
                    patternNode.External = false;
                    patternNode.Predicates = new List<WBooleanExpression>();
                }

                if (!SubGrpahMap.ContainsKey(root))
                {
                    var subGraph = new ConnectedComponent();
                    subGraph.Nodes[node.Key] = node.Value;
                    foreach (var edge in node.Value.Neighbors)
                    {
                        subGraph.Edges[edge.EdgeAlias] = edge;
                    }
                    SubGrpahMap[root] = subGraph;
                    ConnectedSubGraphs.Add(subGraph);
                    subGraph.IsTailNode[node.Value] = false;
                }
                else
                {
                    var subGraph = SubGrpahMap[root];
                    subGraph.Nodes[node.Key] = node.Value;
                    foreach (var edge in node.Value.Neighbors)
                    {
                        subGraph.Edges[edge.EdgeAlias] = edge;
                    }
                    subGraph.IsTailNode[node.Value] = false;
                }
            }

            // Combine all subgraphs into a complete match graph and return it
            MatchGraph Graph = new MatchGraph
            {
                ConnectedSubGraphs = ConnectedSubGraphs,
            };

            return Graph;
        }

        private List<BooleanFunction> AttachScriptSegment(MatchGraph graph, List<string> header)
        {
            AttachWhereClauseVisitor AttachPredicateVistor = new AttachWhereClauseVisitor();
            WSqlTableContext Context = new WSqlTableContext();
            GraphMetaData GraphMeta = new GraphMetaData();
            Dictionary<string, string> ColumnTableMapping = Context.GetColumnToAliasMapping(GraphMeta.ColumnsOfNodeTables);
            AttachPredicateVistor.Invoke(WhereClause, graph, ColumnTableMapping);
            List<BooleanFunction> BooleanList = new List<BooleanFunction>();
            foreach (var predicate in AttachPredicateVistor.FailedToAssign)
            {
                if (predicate is WBooleanComparisonExpression)
                {
                    string FirstExpr = (predicate as WBooleanComparisonExpression).FirstExpr.ToString();
                    string SecondExpr = (predicate as WBooleanComparisonExpression).SecondExpr.ToString();
                    header.Add(FirstExpr);
                    header.Add(SecondExpr);
                    FieldComparisonFunction NewCBF = null;
                    if ((predicate as WBooleanComparisonExpression).ComparisonType == BooleanComparisonType.Equals)
                        NewCBF = new FieldComparisonFunction(header.Count - 1, header.Count - 2,
                            ComparisonBooleanFunction.ComparisonType.eq);
                    if ((predicate as WBooleanComparisonExpression).ComparisonType ==
                        BooleanComparisonType.NotEqualToExclamation)
                        NewCBF = new FieldComparisonFunction(header.Count - 1, header.Count - 2,
                            ComparisonBooleanFunction.ComparisonType.neq);
                    if ((predicate as WBooleanComparisonExpression).ComparisonType ==
    BooleanComparisonType.LessThan)
                        NewCBF = new FieldComparisonFunction(header.Count - 1, header.Count - 2,
                            ComparisonBooleanFunction.ComparisonType.lt);
                    if ((predicate as WBooleanComparisonExpression).ComparisonType ==
    BooleanComparisonType.GreaterThan)
                        NewCBF = new FieldComparisonFunction(header.Count - 1, header.Count - 2,
                            ComparisonBooleanFunction.ComparisonType.gt);
                    if ((predicate as WBooleanComparisonExpression).ComparisonType ==
    BooleanComparisonType.GreaterThanOrEqualTo)
                        NewCBF = new FieldComparisonFunction(header.Count - 1, header.Count - 2,
                            ComparisonBooleanFunction.ComparisonType.gte);
                    if ((predicate as WBooleanComparisonExpression).ComparisonType ==
    BooleanComparisonType.LessThanOrEqualTo)
                        NewCBF = new FieldComparisonFunction(header.Count - 1, header.Count - 2,
                            ComparisonBooleanFunction.ComparisonType.lte);
                    BooleanList.Add(NewCBF);
                }
            }
            // Calculate how much nodes the whole match graph has.
            int StartOfResult = 0;
            foreach (var subgraph in graph.ConnectedSubGraphs)
                StartOfResult += subgraph.Nodes.Count() * 2;
            foreach (var subgraph in graph.ConnectedSubGraphs)
            {
                // Use Topological Sort to give a sorted node list.
                // Note that if there's a cycle in the match graph, a random node will be chose as the start.
                Stack<Tuple<string, string, string>> SortedNodeList = TopoSorting.TopoSort(subgraph.Nodes);
                // Marking down which node has been processed for later reverse checking.  
                List<string> ProcessedNodeList = new List<string>();
                while (SortedNodeList.Count != 0)
                {
                    MatchNode CurrentProcessingNode = null;
                    string TargetNode = SortedNodeList.Pop().Item3;
                    foreach (var x in subgraph.Nodes)
                        if (x.Key == TargetNode)
                            CurrentProcessingNode = x.Value;
                    BuildQuerySegementOnNode(ProcessedNodeList, CurrentProcessingNode, header, StartOfResult);
                    ProcessedNodeList.Add(CurrentProcessingNode.NodeAlias);
                }
            }
            return BooleanList;
        }

        private List<string> ConstructHeader(MatchGraph graph)
        {
            List<string> header = new List<string>();
            // Construct the first part of the head which is defined as 
            // |Node's Alias|Node's Adjacent list|Node's Alias|Node's Adjacent list|...
            // |   "NODE1"  |   "NODE1_ADJ"      |  "NODE2"   |   "NODE2_ADJ"      |...
            foreach (var subgraph in graph.ConnectedSubGraphs)
            {
                foreach (var node in subgraph.Nodes)
                {
                    header.Add(node.Key);
                    header.Add(node.Key + "_ADJ");
                }
            }
            // Construct the second part of the head which is defined as 
            // |Select element|Select element|Select element|...
            // |  "ELEMENT1"  ||  "ELEMENT2" ||  "ELEMENT3" |...
            foreach (var element in SelectElements)
            {
                if (element is WSelectScalarExpression)
                {
                    if ((element as WSelectScalarExpression).SelectExpr is WValueExpression) continue;
                    var expr = (element as WSelectScalarExpression).SelectExpr as WColumnReferenceExpression;
                    header.Add(expr.MultiPartIdentifier.ToString());
                }
            }
            header.Add("PATH");
            return header;
        }
        private GraphViewOperator ConstructOperator(MatchGraph graph, List<string> header, GraphViewConnection pConnection, List<BooleanFunction> functions)
        {
            Record RecordZero = new Record(header.Count);

            List<GraphViewOperator> ChildrenProcessor = new List<GraphViewOperator>();
            List<GraphViewOperator> RootProcessor = new List<GraphViewOperator>();
            List<int> FunctionVaildalityCheck = new List<int>();
            foreach (var i in functions)
            {
                FunctionVaildalityCheck.Add(0);
            }
            int StartOfResult = 0;
            // Generate processor subgraph by subgraph 
            foreach (var subgraph in graph.ConnectedSubGraphs)
            {
                // Use Topological Sorting to define the order of nodes it will travel.
                Stack<Tuple<string, string, string>> SortedNodes = TopoSorting.TopoSort(subgraph.Nodes);
                StartOfResult += subgraph.Nodes.Count * 2;
                bool FirstNodeFlag = true;
                int LastDest = -1;
                while (SortedNodes.Count != 0)
                {
                    MatchNode TempNode = null;
                    Tuple<string, string, string> CurrentProcessingNode = SortedNodes.Pop();
                    // If it is the first node of a sub graph, the node will be dealed by a FetchNodeOperator.
                    // Otherwise it will be dealed by a TraversalOperator.
                    if (FirstNodeFlag)
                    {
                        int node = header.IndexOf(CurrentProcessingNode.Item3);
                        foreach (var x in subgraph.Nodes)
                            if (x.Key == CurrentProcessingNode.Item3)
                                ChildrenProcessor.Add(new FetchNodeOperator(pConnection, x.Value.AttachedQuerySegment, node, header, StartOfResult, 50));
                        FirstNodeFlag = false;
                    }
                    else
                    {
                        Dictionary<int, string> ReverseCheckList = new Dictionary<int, string>();
                        int src = header.IndexOf(CurrentProcessingNode.Item1);
                        int dest = header.IndexOf(CurrentProcessingNode.Item3);
                        foreach (var x in subgraph.Nodes)
                            if (x.Key == CurrentProcessingNode.Item3)
                                TempNode = x.Value;
                        if (WithPathClause != null)
                        {
                            Tuple<string, GraphViewOperator, int> InternalOperator = null;
                            if (
                                (InternalOperator =
                                    WithPathClause.PathOperators.Find(p => p.Item1 == CurrentProcessingNode.Item2)) !=
                                null)
                                foreach (var neighbor in TempNode.ReverseNeighbors)
                                    ReverseCheckList.Add(header.IndexOf(neighbor.SinkNode.NodeAlias),
                                        neighbor.EdgeAlias + "_REV");
                            ChildrenProcessor.Add(new TraversalOperator(pConnection,ChildrenProcessor.Last(),TempNode.AttachedQuerySegment,src,dest,header, ReverseCheckList, StartOfResult, 50,50,InternalOperator.Item2));
                        }
                        else
                        {
                            foreach (var neighbor in TempNode.ReverseNeighbors)
                                ReverseCheckList.Add(header.IndexOf(neighbor.SinkNode.NodeAlias),
                                    neighbor.EdgeAlias + "_REV");
                            ChildrenProcessor.Add(new TraversalOperator(pConnection, ChildrenProcessor.Last(),
                                TempNode.AttachedQuerySegment, src, dest, header, ReverseCheckList, StartOfResult, 50,
                                50));
                        }
                    }
                    for (int i = 0; i < functions.Count; i++)
                    {
                        if (functions[i] is FieldComparisonFunction)
                        {
                            string lhs = header[(functions[i] as FieldComparisonFunction).LhsFieldIndex];
                            string rhs = header[(functions[i] as FieldComparisonFunction).RhsFieldIndex];
                            if (TempNode.AttachedQuerySegment.Contains(lhs))
                                FunctionVaildalityCheck[i]++;
                            if (TempNode.AttachedQuerySegment.Contains(rhs))
                                FunctionVaildalityCheck[i]++;
                            if (FunctionVaildalityCheck[i] == 2)
                            {
                                if ((ChildrenProcessor.Last() as TraversalOperator).BooleanCheck == null)
                                    (ChildrenProcessor.Last() as TraversalOperator).BooleanCheck = functions[i];
                                else
                                    (ChildrenProcessor.Last() as TraversalOperator).BooleanCheck =
                                        new BinaryFunction((ChildrenProcessor.Last() as TraversalOperator).BooleanCheck, functions[i], BinaryBooleanFunction.BinaryType.and);
                                FunctionVaildalityCheck[i] = 0;

                            }
                        }
                    }
                }
                // The last processor of a sub graph will be added to root processor list for later use.
                RootProcessor.Add(ChildrenProcessor.Last());
            }
            GraphViewOperator root = null;
            if (RootProcessor.Count == 1) root = RootProcessor[0];
            // A cartesian product will be made among all the result from the root processor in order to produce a complete result
            else root = new CartesianProductOperator(pConnection, RootProcessor, header, 100);
            if (OrderByClause != null && OrderByClause.OrderByElements != null)
            {
                if (OrderByClause.OrderByElements[0].SortOrder == SortOrder.Ascending)
                    root = new OrderbyOperator(pConnection, root, OrderByClause.OrderByElements[0].ToString(), root.header, OrderbyOperator.Order.Incr);
                if (OrderByClause.OrderByElements[0].SortOrder == SortOrder.Descending)
                    root = new OrderbyOperator(pConnection, root, OrderByClause.OrderByElements[0].ToString(), root.header, OrderbyOperator.Order.Decr);
                if (OrderByClause.OrderByElements[0].SortOrder == SortOrder.NotSpecified)
                    root = new OrderbyOperator(pConnection, root, OrderByClause.OrderByElements[0].ToString(), root.header, OrderbyOperator.Order.NotSpecified);
            }
            List<string> SelectedElement = new List<string>();
            foreach (var x in SelectElements)
            {
                if ((x as WSelectScalarExpression).SelectExpr is WColumnReferenceExpression)
                    SelectedElement.Add(x.ToString());
            }
            if (!OutputPath)
                root = new OutputOperator(root, pConnection, SelectedElement, root.header);
            else
                root = new OutputOperator(root,pConnection,true,header);
            return root;
        }

        private void BuildQuerySegementOnNode(List<string> ProcessedNodeList, MatchNode node, List<string> header, int pStartOfResultField)
        {
            // Node predicates will be attached here.
            string FromClauseString = node.NodeAlias;
            string WhereClauseString = "";
            //string AttachedClause = "From " + node.NodeAlias;
            string PredicatesOnReverseEdge = "";
            string PredicatesOnNodes = "";
            foreach (var edge in node.ReverseNeighbors.Concat(node.Neighbors))
            {
                if (node.ReverseNeighbors.Contains(edge))
                    FromClauseString += " Join " + edge.EdgeAlias + " in " + node.NodeAlias + "._reverse_edge ";
                else
                    FromClauseString += " Join " + edge.EdgeAlias + " in " + node.NodeAlias + "._edge ";
                if (edge != node.ReverseNeighbors.Concat(node.Neighbors).Last())
                    foreach (var predicate in edge.Predicates)
                    {
                        PredicatesOnReverseEdge += predicate + " AND ";
                    }
                else
                    foreach (var predicate in edge.Predicates)
                    {
                        if (predicate != edge.Predicates.Last())
                            PredicatesOnReverseEdge += predicate + " AND ";
                        else PredicatesOnReverseEdge += predicate;
                    }
            }

            FromClauseString = " FROM " + FromClauseString;

            foreach (var predicate in node.Predicates)
            {
                if (predicate != node.Predicates.Last())
                    PredicatesOnNodes += predicate + " AND ";
                else
                    PredicatesOnNodes += predicate;
            }
            if (PredicatesOnNodes != "" || PredicatesOnReverseEdge != "")
            {
                WhereClauseString += " WHERE ";
                if (PredicatesOnNodes != "" && PredicatesOnReverseEdge != "")
                    WhereClauseString += PredicatesOnNodes + " AND " + PredicatesOnReverseEdge;
                else WhereClauseString += PredicatesOnNodes + PredicatesOnReverseEdge;
            }

            // Select elements that related to current node will be attached here.

            List<string> ResultIndexToAppend = new List<string>();
            foreach (string ResultIndex in header.GetRange(pStartOfResultField, header.Count - pStartOfResultField))
            {
                int CutPoint = ResultIndex.Length;
                if (ResultIndex.IndexOf('.') != -1) CutPoint = ResultIndex.IndexOf('.');
                if (ResultIndex.Substring(0, CutPoint) == node.NodeAlias)
                    ResultIndexToAppend.Add(ResultIndex);
                foreach (var edge in node.ReverseNeighbors)
                {
                    if (ResultIndex.Substring(0, CutPoint) == edge.EdgeAlias)
                        ResultIndexToAppend.Add(ResultIndex);
                }
            }

            string ResultIndexString = ",";
            foreach (string ResultIndex in ResultIndexToAppend)
            {
                if (ResultIndex.Length > 3 && ResultIndex.Substring(ResultIndex.Length - 3, 3) == "doc")
                    ResultIndexString += ResultIndex.Substring(0, ResultIndex.Length - 4) + " AS " + ResultIndex.Replace(".", "_") + ",";
                else ResultIndexString += ResultIndex + " AS " + ResultIndex.Replace(".", "_") + ",";
            }
            if (ResultIndexString == ",") ResultIndexString = "";
            ResultIndexString = CutTheTail(ResultIndexString);

            // Reverse checking related script will be attached here.
            string ReverseCheckString = ",";
            foreach (var ReverseEdge in node.ReverseNeighbors.Concat(node.Neighbors))
            {
                if (ProcessedNodeList.Contains(ReverseEdge.SinkNode.NodeAlias))
                    ReverseCheckString += ReverseEdge.EdgeAlias + " AS " + ReverseEdge.EdgeAlias + "_REV,";
            }
            if (ReverseCheckString == ",") ReverseCheckString = "";
            ReverseCheckString = CutTheTail(ReverseCheckString);

            // The DocDb script that related to the giving node will be assembled here.
            string ScriptBase = "SELECT {\"id\":node.id, \"edge\":node._edge, \"reverse\":node._reverse_edge} AS NodeInfo";
            string QuerySegment = QuerySegment = ScriptBase.Replace("node", node.NodeAlias) + ResultIndexString + " " + ReverseCheckString;
            QuerySegment += FromClauseString + WhereClauseString;
            node.AttachedQuerySegment = QuerySegment;
        }

        // Cut the last character of a string.
        private string CutTheTail(string InRangeScript)
        {
            if (InRangeScript.Length == 0) return "";
            return InRangeScript.Substring(0, InRangeScript.Length - 1);
        }
        // The implementation of Union find algorithmn.
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

        // The implementation of topological sorting using DFS
        // Note that if is there's a cycle, a random node in the cycle will be pick as the start.
        private class TopoSorting
        {
            static internal Stack<Tuple<string, string, string>> TopoSort(Dictionary<string, MatchNode> graph)
            {
                Dictionary<MatchNode, int> state = new Dictionary<MatchNode, int>();
                Stack<Tuple<string, string, string>> list = new Stack<Tuple<string, string, string>>();
                foreach (var node in graph)
                    state.Add(node.Value, 0);
                foreach (var node in graph)
                    visit(graph, node.Value, list, state, node.Value.NodeAlias, "");
                return list;
            }
            static private void visit(Dictionary<string, MatchNode> graph, MatchNode node, Stack<Tuple<string, string, string>> list, Dictionary<MatchNode, int> state, string ParentAlias, string EdgeAlias)
            {
                if (state[node] == 1)
                    return;
                if (state[node] == 2)
                    return;
                state[node] = 2;
                foreach (var neighbour in node.Neighbors)
                    visit(graph, neighbour.SinkNode, list, state, node.NodeAlias, neighbour.EdgeAlias);
                state[node] = 1;
                list.Push(new Tuple<string, string, string>(ParentAlias, EdgeAlias, node.NodeAlias));
            }
        }
    }

    public partial class WSelectQueryBlockWithMatchClause : WSelectQueryBlock
    {

    }

    public partial class WTopRowFilter : WSqlFragment
    {
        internal bool Percent { set; get; }
        internal bool WithTies { get; set; }
        internal WScalarExpression Expression { get; set; }

        internal override bool OneLine()
        {
            return Expression.OneLine();
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(32);

            sb.AppendFormat("{0}TOP ", indent);

            if (Expression.OneLine())
            {
                sb.Append(Expression.ToString(""));
            }
            else
            {
                sb.Append("\r\n");
                sb.Append(Expression.ToString(indent + "  "));
            }

            if (Percent)
            {
                sb.Append(" PERCENT");
            }

            if (WithTies)
            {
                sb.Append(" WITH TIES");
            }

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Expression != null)
                Expression.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WWithPathClause : WSqlStatement
    {
        // Definition of a path: 
        // item1 is the binding name
        // item2 is the path description
        // item3 is the length limitation of it (-1 for no limitation)
        internal List<Tuple<string, WSelectQueryBlock, int>> Paths;
        internal List<Tuple<string, GraphViewOperator, int>> PathOperators;

        public WWithPathClause(List<Tuple<string, WSelectQueryBlock, int>> pPaths)
        {
            Paths = pPaths;
            PathOperators = new List<Tuple<string, GraphViewOperator, int>>();
        }

        public WWithPathClause(Tuple<string, WSelectQueryBlock, int> path)
        {
            PathOperators = new List<Tuple<string, GraphViewOperator, int>>();
            if (Paths == null) Paths = new List<Tuple<string, WSelectQueryBlock, int>>();
            Paths.Add(path);
        }
        internal override GraphViewOperator Generate(GraphViewConnection dbConnection)
        {
            foreach (var path in Paths)
                PathOperators.Add(new Tuple<string, GraphViewOperator, int>(path.Item1, path.Item2.Generate(dbConnection), path.Item3));
            if (PathOperators.Count != 0) return PathOperators.First().Item2;
            else return null;
        }
    }

}

