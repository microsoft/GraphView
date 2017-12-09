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
using GraphView.GraphViewDBPortal;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class ConnectedComponent
    {
        public Dictionary<string, MatchNode> Nodes { get; set; }
        public Dictionary<string, MatchEdge> Edges { get; set; }
        public Dictionary<MatchNode, bool> IsTailNode { get; set; }

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

    internal class MatchGraph
    {
        // Fully-connected components in the graph pattern 
        public List<ConnectedComponent> ConnectedSubgraphs { get; set; }
        public MatchGraph()
        {
            this.ConnectedSubgraphs = new List<ConnectedComponent>();
        }

        public MatchGraph(List<ConnectedComponent> connectedSubgraphs)
        {
            this.ConnectedSubgraphs = connectedSubgraphs;
        }

        public bool TryGetNode(string key, out MatchNode node)
        {
            foreach (var subGraph in ConnectedSubgraphs)
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
            foreach (var subGraph in ConnectedSubgraphs)
            {
                if (subGraph.Edges.TryGetValue(key, out edge))
                {
                    return true;
                }
            }
            edge = null;
            return false;
        }

        public void AttachProperties(Dictionary<string, HashSet<string>> tableColumnReferences)
        {
            MatchEdge edge;
            MatchNode node;

            foreach (var tableColumnReference in tableColumnReferences)
            {
                var tableName = tableColumnReference.Key;
                var properties = tableColumnReference.Value;

                if (TryGetEdge(tableName, out edge))
                {
                    if (edge.Properties == null)
                        edge.Properties = new List<string>();
                    foreach (var property in properties)
                    {
                        if (!edge.Properties.Contains(property))
                            edge.Properties.Add(property);
                    }
                }
                else if (TryGetNode(tableName, out node))
                {
                    if (node.Properties == null)
                        node.Properties = new HashSet<string>();
                    foreach (var property in properties)
                    {
                        node.Properties.Add(property);
                    }
                }
            }
        }

        public bool TryAttachPredicate(WBooleanExpression predicate, Dictionary<string, HashSet<string>> tableColumnReferences)
        {
            // Attach fail if it is a cross-table predicate
            if (tableColumnReferences.Count > 1)
            {
                return false;
            }

            MatchEdge edge;
            MatchNode node;
            bool attachFlag = false;

            foreach (var tableColumnReference in tableColumnReferences)
            {
                var tableName = tableColumnReference.Key;
                var properties = tableColumnReference.Value;

                if (TryGetEdge(tableName, out edge))
                {
                    if (edge.Predicates == null)
                        edge.Predicates = new List<WBooleanExpression>();
                    edge.Predicates.Add(predicate);
                    // Attach edge's propeties for later runtime evaluation
                    AttachProperties(new Dictionary<string, HashSet<string>> { { tableName, properties } });
                    attachFlag = true;
                }
                else if (TryGetNode(tableName, out node))
                {
                    if (node.Predicates == null)
                        node.Predicates = new List<WBooleanExpression>();
                    node.Predicates.Add(predicate);
                    AttachProperties(new Dictionary<string, HashSet<string>> { { tableName, properties } });
                    attachFlag = true;
                }
            }

            return attachFlag;
        }

        public HashSet<string> GetNodesAndEdgesAliases()
        {
            HashSet<string> aliases = new HashSet<string>();
            foreach (ConnectedComponent connectedComponent in ConnectedSubgraphs)
            {
                aliases.UnionWith(connectedComponent.Nodes.Keys);
                aliases.UnionWith(connectedComponent.Edges.Keys);
            }
            return aliases;
        }
    }
}
