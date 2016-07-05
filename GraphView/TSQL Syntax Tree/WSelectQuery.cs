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
using System.Text.RegularExpressions;
using Microsoft.SqlServer.TransactSql.ScriptDom;

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
        internal MatchGraph Graph { get; set; }
        internal UniqueRowFilter UniqueRowFilter { get; set; }
        internal List<TraversalProcessor> ChildrenProcessor;
        internal Record RecordZero;
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

        public override GraphViewOperator Generate(GraphViewConnection pConnection)
        {
            ConstructGraph();
            List<string> header = ConstructHeader();
            foreach (var node in Graph.MainSubGraph.Nodes) BuildQuerySegementOnNode(node.Value, header, Graph.MainSubGraph.Nodes.Count * 3);
            return GenerateTraversalProcessor(header, pConnection);
        }

        private void ConstructGraph()
        {
            Dictionary<string, List<string>> EdgeColumnToAliasesDict = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, MatchPath> pathDictionary = new Dictionary<string, MatchPath>(StringComparer.OrdinalIgnoreCase);

            UnionFind UnionFind = new UnionFind();
            Dictionary<string, MatchNode> Nodes = new Dictionary<string, MatchNode>(StringComparer.OrdinalIgnoreCase);
            List<ConnectedComponent> ConnectedSubGraphs = new List<ConnectedComponent>();
            Dictionary<string, ConnectedComponent> SubGrpahMap = new Dictionary<string, ConnectedComponent>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, string> Parent = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            UnionFind.Parent = Parent;

            foreach (var cnt in SelectElements)
            {
                if (cnt is WSelectStarExpression) continue;
                if (cnt == null) continue;
                var cnt2 = (cnt as WSelectScalarExpression).SelectExpr as WColumnReferenceExpression;
                if (cnt2 == null) continue;
            }
            if (FromClause != null)
            {
                foreach (WTableReferenceWithAlias cnt in FromClause.TableReferences)
                {
                    Nodes.GetOrCreate(cnt.Alias.Value);
                }
            }

            if (MatchClause != null)
            {
                if (MatchClause.Paths.Count > 0)
                {
                    foreach (var path in MatchClause.Paths)
                    {
                        var index = 0;
                        MatchEdge PreEdge = null;
                        for (var count = path.PathEdgeList.Count; index < count; ++index)
                        {
                            var CurrentNodeTableRef = path.PathEdgeList[index].Item1;
                            var CurrentEdgeColumnRef = path.PathEdgeList[index].Item2;
                            var CurrentNodeExposedName = CurrentNodeTableRef.BaseIdentifier.Value;
                            var nextNodeTableRef = index != count - 1
                                ? path.PathEdgeList[index + 1].Item1
                                : path.Tail;
                            var nextNodeExposedName = nextNodeTableRef.BaseIdentifier.Value;
                            var PatternNode = Nodes.GetOrCreate(CurrentNodeExposedName);
                            if (PatternNode.NodeAlias == null)
                            {
                                PatternNode.NodeAlias = CurrentNodeExposedName;
                                PatternNode.Neighbors = new List<MatchEdge>();
                                PatternNode.ReverseNeighbors = new List<MatchEdge>();
                                PatternNode.External = false;
                            }

                            string pEdgeAlias = CurrentEdgeColumnRef.Alias;
                            if (pEdgeAlias == null)
                            {
                                bool isReversed = path.IsReversed;
                                var CurrentEdgeName = CurrentEdgeColumnRef.MultiPartIdentifier.Identifiers.Last().Value;
                                string originalEdgeName = null;

                                pEdgeAlias = string.Format("{0}_{1}_{2}", CurrentNodeExposedName, CurrentEdgeName,
                                    nextNodeExposedName);

                                // when current edge is a reversed edge, the key should still be the original edge name
                                var edgeNameKey = isReversed ? originalEdgeName : CurrentEdgeName;
                                if (EdgeColumnToAliasesDict.ContainsKey(edgeNameKey))
                                {
                                    EdgeColumnToAliasesDict[edgeNameKey].Add(pEdgeAlias);
                                }
                                else
                                {
                                    EdgeColumnToAliasesDict.Add(edgeNameKey, new List<string> { pEdgeAlias });
                                }
                            }

                            MatchEdge edge;
                            if (CurrentEdgeColumnRef.MinLength == 1 && CurrentEdgeColumnRef.MaxLength == 1)
                            {
                                edge = new MatchEdge
                                {
                                    SourceNode = PatternNode,
                                    EdgeColumn = CurrentEdgeColumnRef,
                                    EdgeAlias = pEdgeAlias,
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
                                    SourceNode = PatternNode,
                                    EdgeColumn = CurrentEdgeColumnRef,
                                    EdgeAlias = pEdgeAlias,
                                    Predicates = new List<WBooleanExpression>(),
                                    BindNodeTableObjName =
                                        new WSchemaObjectName(
                                            ),
                                    MinLength = CurrentEdgeColumnRef.MinLength,
                                    MaxLength = CurrentEdgeColumnRef.MaxLength,
                                    ReferencePathInfo = false,
                                    AttributeValueDict = CurrentEdgeColumnRef.AttributeValueDict
                                };
                                pathDictionary[pEdgeAlias] = matchPath;
                                edge = matchPath;
                            }

                            if (PreEdge != null)
                            {
                                PreEdge.SinkNode = PatternNode;
                                //Add ReverseEdge
                                MatchEdge reverseEdge;
                                reverseEdge = new MatchEdge
                                {
                                    SourceNode = PreEdge.SinkNode,
                                    SinkNode = PreEdge.SourceNode,
                                    EdgeColumn = PreEdge.EdgeColumn,
                                    EdgeAlias = PreEdge.EdgeAlias,
                                    Predicates = PreEdge.Predicates,
                                    BindNodeTableObjName =
                                       new WSchemaObjectName(
                                           ),
                                };
                                PatternNode.ReverseNeighbors.Add(reverseEdge);
                            }
                            PreEdge = edge;
                            if (!Parent.ContainsKey(CurrentNodeExposedName))
                                Parent[CurrentNodeExposedName] = CurrentNodeExposedName;
                            if (!Parent.ContainsKey(nextNodeExposedName))
                                Parent[nextNodeExposedName] = nextNodeExposedName;

                            UnionFind.Union(CurrentNodeExposedName, nextNodeExposedName);


                            PatternNode.Neighbors.Add(edge);

                        }
                        var tailExposedName = path.Tail.BaseIdentifier.Value;
                        var tailNode = Nodes.GetOrCreate(tailExposedName);
                        if (tailNode.NodeAlias == null)
                        {
                            tailNode.NodeAlias = tailExposedName;
                            tailNode.Neighbors = new List<MatchEdge>();
                            tailNode.ReverseNeighbors = new List<MatchEdge>();
                        }
                        if (PreEdge != null)
                        {
                            PreEdge.SinkNode = tailNode;
                            //Add ReverseEdge
                            MatchEdge reverseEdge;
                            reverseEdge = new MatchEdge
                            {
                                SourceNode = PreEdge.SinkNode,
                                SinkNode = PreEdge.SourceNode,
                                EdgeColumn = PreEdge.EdgeColumn,
                                EdgeAlias = PreEdge.EdgeAlias,
                                Predicates = PreEdge.Predicates,
                                BindNodeTableObjName =
                                   new WSchemaObjectName(
                                       ),
                            };
                            tailNode.ReverseNeighbors.Add(reverseEdge);
                        }
                    }

                }
            }
            // Puts nodes into subgraphs
            foreach (var node in Nodes)
            {
                string root;

                root = "DocDB_graph";  // put them into the same graph

                var patternNode = node.Value;

                if (patternNode.NodeAlias == null)
                {
                    patternNode.NodeAlias = node.Key;
                    patternNode.Neighbors = new List<MatchEdge>();
                    patternNode.ReverseNeighbors = new List<MatchEdge>();
                    patternNode.External = false;
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

            Graph = new MatchGraph
            {
                ConnectedSubGraphs = ConnectedSubGraphs,
                MainSubGraph = ConnectedSubGraphs[0]
            };

            AttachWhereClauseVisitor AttachPredicateVistor = new AttachWhereClauseVisitor();
            WSqlTableContext Context = new WSqlTableContext();
            GraphMetaData GraphMeta = new GraphMetaData();
            Dictionary<string, string> ColumnTableMapping = Context.GetColumnToAliasMapping(GraphMeta.ColumnsOfNodeTables);
            AttachPredicateVistor.Invoke(WhereClause, Graph, ColumnTableMapping);
        }
        private List<string> ConstructHeader()
        {
            List<string> header = new List<string>();
            foreach(var node in Graph.MainSubGraph.Nodes)
            {
                header.Add(node.Key);
                header.Add(node.Key + "_ADJ");
                header.Add(node.Key + "_SEG");
            }
            foreach (var element in SelectElements)
            {
                if (element is WSelectScalarExpression)
                {
                    if ((element as WSelectScalarExpression).SelectExpr is WValueExpression) continue;
                    var expr = (element as WSelectScalarExpression).SelectExpr as WColumnReferenceExpression;
                    header.Add(expr.MultiPartIdentifier.ToString());
                }
            }
            return header;
        }
        private TraversalProcessor GenerateTraversalProcessor(List<string> header, GraphViewConnection pConnection)
        {
            RecordZero = new Record(header.Count);
            foreach (var node in Graph.MainSubGraph.Nodes)
            {
                RecordZero.field[header.IndexOf(node.Key + "_SEG")] = node.Value.AttachedQuerySegment;
            }

            ChildrenProcessor = new List<TraversalProcessor>();
            Queue<string> NodeList = new Queue<string>();
            string src = "";
            string dest = "";
            int StartOfResult = Graph.MainSubGraph.Nodes.Count * 3;
            foreach (var SrcNode in Graph.MainSubGraph.Nodes)
            {
                if (!NodeList.Contains(SrcNode.Key) && ChildrenProcessor.Count == 0) ChildrenProcessor.Add(new TraversalProcessor(pConnection, null, "", SrcNode.Key, header, StartOfResult, 50, 50));
                else if (!NodeList.Contains(SrcNode.Key)) ChildrenProcessor.Add(new TraversalProcessor(pConnection, ChildrenProcessor.Last(), "", SrcNode.Key, header, StartOfResult, 50, 50));
                foreach (var DestNode in SrcNode.Value.Neighbors)
                    {
                        ChildrenProcessor.Add(new TraversalProcessor(pConnection, ChildrenProcessor.Last(), SrcNode.Key, DestNode.SinkNode.NodeAlias, header, StartOfResult, 50, 50));
                        NodeList.Enqueue(DestNode.SinkNode.NodeAlias);
                    }
            }
            TraversalProcessor.RecordZero = RecordZero;
            return ChildrenProcessor.Last();
        }
        private void BuildQuerySegementOnNode(MatchNode node, List<string> header, int pStartOfResultField)
        {
            string AttachedClause = "From " + node.NodeAlias;
            string PredicatesOnReverseEdge = "";
            int NumberOfPredicates = 0;
            foreach (var edge in node.ReverseNeighbors)
            {
                    AttachedClause += " Join " + edge.EdgeAlias + " in " + node.NodeAlias + "._reverse_edge ";
                    if (edge.Predicates != null)
                    {
                        if (NumberOfPredicates != 0) PredicatesOnReverseEdge += " And ";
                        NumberOfPredicates++;
                        PredicatesOnReverseEdge += " (";
                        for (int i = 0; i < edge.Predicates.Count(); i++)
                        {
                            if (i != 0)
                                PredicatesOnReverseEdge += " And ";
                            PredicatesOnReverseEdge += "(" + edge.Predicates[i] + ")";
                        }
                        PredicatesOnReverseEdge += ") ";
                    }
                
            }
            AttachedClause += " Where ";
            if (node.Predicates != null)
            {
                for (int i = 0; i < node.Predicates.Count(); i++)
                {
                    if (i != 0)
                        AttachedClause += " And ";
                    AttachedClause += node.Predicates[i];
                }
                if (PredicatesOnReverseEdge != "")
                    AttachedClause += " And ";
            }
            AttachedClause += PredicatesOnReverseEdge;

            List<string> ResultIndexToAppend = new List<string>();
            string ResultIndexString = " ,";
            foreach (string ResultIndex in header.GetRange(pStartOfResultField, header.Count - pStartOfResultField))
            {
                int CutPoint = ResultIndex.Length;
                if (ResultIndex.IndexOf('.') != -1) CutPoint = ResultIndex.IndexOf('.');
                if (ResultIndex.Substring(0, CutPoint) == node.NodeAlias)
                    ResultIndexToAppend.Add(ResultIndex);
            }
            foreach (string ResultIndex in ResultIndexToAppend)
            {
                ResultIndexString += ResultIndex + " AS " + ResultIndex.Replace(".", "_") + ",";
            }
            if (ResultIndexString == " ,") ResultIndexString = "";
            ResultIndexString = CutTheTail(ResultIndexString);

            string ScriptBase = "SELECT {\"id\":node.id, \"edge\":node._edge, \"reverse\":node._reverse_edge} AS NodeInfo";
            string QuerySegment = ScriptBase.Replace("node", node.NodeAlias) + ResultIndexString;
            if (HasWhereClause(AttachedClause))
                QuerySegment += " " + AttachedClause;
            else QuerySegment += " From " + node.NodeAlias;

            node.AttachedQuerySegment = QuerySegment;
        }
        string CutTheTail(string InRangeScript)
        {
            if (InRangeScript.Length == 0) return "";
            return InRangeScript.Substring(0, InRangeScript.Length - 1);
        }
        private bool HasWhereClause(string SelectClause)
        {
            return !(SelectClause.Length < 6 || SelectClause.Substring(SelectClause.Length - 6, 5) == "Where");
        }

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
}
