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
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public class MatchEdge
    {
        public MatchNode SourceNode { get; set; }
        public WColumnReferenceExpression EdgeColumn { get; set; }
        public string EdgeAlias { get; set; }
        public MatchNode SinkNode { get; set; }
        public bool IsReversed { get; set; }
        public WEdgeType EdgeType { get; set; }
        public bool IsDanglingEdge { get; set; }

        /// <summary>
        /// Schema Object of the node table/node view which the edge is bound to.
        /// It is an instance in the syntax tree.
        /// </summary>
        public WSchemaObjectName BindNodeTableObjName { get; set; }
        public double AverageDegree { get; set; }
        public IList<WBooleanExpression> Predicates { get; set; }
        public List<string> Properties { get; set; }
        public int Low { get; set; }
        public int High { get; set; }
        public Statistics Statistics { get; set; }
        public override int GetHashCode()
        {
            return EdgeAlias.GetHashCode();
        }

        /// <summary>
        /// Converts edge attribute predicates into a boolean expression, which is used for
        /// constructing queries for retrieving edge statistics
        /// </summary>
        /// <returns></returns>
        public virtual WBooleanExpression RetrievePredicatesExpression()
        {
            if (Predicates != null)
            {
                WBooleanExpression res = null;
                foreach (var expression in Predicates)
                {
                    res = WBooleanBinaryExpression.Conjunction(res, expression);
                }
                return res;
            }
            return null;
        }
    }

    internal class MatchPath : MatchEdge
    {
        // The minimal length constraint for the path
        public int MinLength { get; set; }
        // The maximal length constraint for the path. Represents max when the value is set to -1.
        public int MaxLength { get; set; }
        /// <summary>
        /// True, the path is referenced in the SELECT clause and path information should be displayed
        /// False, path information can be neglected
        /// </summary>
        public bool ReferencePathInfo { get; set; }
        
        // Predicates associated with the path constructs in the current context. 
        // Note that path predicates are defined as a part of path constructs, rather than
        // defined in the WHERE clause. The current supported predicates are only equality comparison,
        // and a predicate is in a pair of <edge_attribute, attribute_value>.
        public Dictionary<string, string> AttributeValueDict { get; set; }

        /// <summary>
        /// Converts edge attribute predicates into a boolean expression, which is used for
        /// constructing queries for retrieving edge statistics
        /// </summary>
        /// <returns></returns>
        public override WBooleanExpression RetrievePredicatesExpression()
        {
            if (AttributeValueDict != null)
            {
                WBooleanExpression res = null;
                foreach (var tuple in AttributeValueDict)
                {
                    res = WBooleanBinaryExpression.Conjunction(res, new WBooleanComparisonExpression
                    {
                        ComparisonType = BooleanComparisonType.Equals,
                        FirstExpr =
                            new WColumnReferenceExpression
                            {
                                MultiPartIdentifier =
                                    new WMultiPartIdentifier(new Identifier {Value = EdgeAlias},
                                        new Identifier {Value = tuple.Key})
                            },
                        SecondExpr = new WValueExpression {Value = tuple.Value}
                    });
                }
                return res;
            }
            return null;
        }
    }

    public class MatchNode
    {
        public string NodeAlias { get; set; }
        public WSchemaObjectName NodeTableObjectName { get; set; }
        public IList<MatchEdge> Neighbors { get; set; }
        public IList<MatchEdge> ReverseNeighbors { get; set; }
        public IList<MatchEdge> DanglingEdges { get; set; } 
        public double EstimatedRows { get; set; }
        public int TableRowCount { get; set; }
        internal JsonQuery AttachedJsonQuery { get; set; }
        public HashSet<string> Properties { get; set; }

        /// <summary>
        /// The density value of the GlobalNodeId Column of the corresponding node table.
        /// This value is used to estimate the join selectivity of A-->B. 
        /// </summary>
        public double GlobalNodeIdDensity { get;set; }

        /// <summary>
        /// Conjunctive predicates from the WHERE clause that 
        /// can be associated with this node variable. 
        /// </summary>
        public IList<WBooleanExpression> Predicates { get; set; }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj)) return true;

            MatchNode rhs = obj as MatchNode;
            if (rhs == null) {
                return false;
            }

            return this.NodeAlias.Equals(rhs.NodeAlias);
        }

        public override int GetHashCode()
        {
            return NodeAlias.GetHashCode();
        }

        public MatchNode() { }

        public MatchNode(MatchNode rhs)
        {
            this.NodeAlias = rhs.NodeAlias;
            this.NodeTableObjectName = rhs.NodeTableObjectName;
            this.Neighbors = rhs.Neighbors;
            this.ReverseNeighbors = rhs.ReverseNeighbors;
            this.DanglingEdges = rhs.DanglingEdges;
            this.EstimatedRows = rhs.EstimatedRows;
            this.TableRowCount = rhs.TableRowCount;
            this.AttachedJsonQuery = rhs.AttachedJsonQuery;
            this.Properties = new HashSet<string>(rhs.Properties);
            this.GlobalNodeIdDensity = rhs.GlobalNodeIdDensity;
            this.Predicates = rhs.Predicates;
        }
    }

    public class ConnectedComponent
    {
        public Dictionary<string, MatchNode> Nodes { get; set; }
        public Dictionary<string, MatchEdge> Edges { get; set; }
        public Dictionary<MatchNode, bool> IsTailNode { get; set; }

        /// Item1: current node. A query will be sent to the server to fetch this node if this is the first time it appears in the whole list.
        /// Item2: the traversalEdge whose sink is current node.
        /// Item3: traversalEdges whose source is currentNode. 
        ///        This list will either contain 0 or 1 traversal edge in the current version, and it will be pushed to server if possible.
        /// Item4: backwardMatchingEdges.
        /// Item5: forwardMatchingEdges.
        public List<Tuple<MatchNode, MatchEdge, List<MatchEdge>, List<MatchEdge>, List<MatchEdge>>> TraversalOrder { get; set; }

        public ConnectedComponent()
        {
            Nodes = new Dictionary<string, MatchNode>(StringComparer.OrdinalIgnoreCase);
            Edges = new Dictionary<string, MatchEdge>(StringComparer.OrdinalIgnoreCase);
            IsTailNode = new Dictionary<MatchNode, bool>();
        }

        public int ActiveNodeCount
        {
            get { return IsTailNode.Count(e => !e.Value); }
        }

        public int EdgeCount
        {
            get { return Edges.Count; }
        }
    }

    public class MatchGraph
    {
        // Fully-connected components in the graph pattern 
        public IList<ConnectedComponent> ConnectedSubGraphs { get; set; }
        public ConnectedComponent MainSubGraph;
        // Mapping between an original edge and its corresponding reversed edge
        public Dictionary<string, MatchEdge> ReversedEdgeDict { get; set; }
        public bool ContainsNode(string key)
        {
            return ConnectedSubGraphs.Any(e => e.Nodes.ContainsKey(key) && !e.IsTailNode[e.Nodes[key]]);
        }

        public bool TryGetNode(string key, out MatchNode node)
        {
            foreach (var subGraph in ConnectedSubGraphs)
            {
                if (subGraph.Nodes.TryGetValue(key, out node))
                {
                    return true;
                }
            }
            node = null;
            return false;
        }

        public bool TryGetEdge(string key, out MatchEdge edge)
        {
            foreach (var subGraph in ConnectedSubGraphs)
            {
                if (subGraph.Edges.TryGetValue(key, out edge))
                {
                    return true;
                }
            }
            edge = null;
            return false;
        }

    }
}
