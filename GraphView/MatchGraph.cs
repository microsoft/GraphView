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
using System.Threading.Tasks;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class MatchEdge
    {
        public MatchNode SourceNode { get; set; }
        public WColumnReferenceExpression EdgeColumn { get; set; }
        public string EdgeAlias { get; set; }
        public MatchNode SinkNode { get; set; }


        /// <summary>
        /// Schema Object of the node table/node view which the edge is bound to.
        /// It is an instance in the syntax tree.
        /// </summary>
        public WSchemaObjectName BindNodeTableObjName { get; set; }
        public double AverageDegree { get; set; }
        public IList<WBooleanExpression> Predicates { get; set; }
        public Statistics Statistics { get; set; }
        public override int GetHashCode()
        {
            return EdgeAlias.GetHashCode();
        }

        /// <summary>
        /// Constructs parameters for the edge table-valued function when translation the MATCH clause
        /// </summary>
        /// <param name="nodeAlias">Source node alias</param>
        /// <param name="nodeTableNameSet">Node table names mapping to the source node of the edge. 
        /// If null, the source node is mapped to a physical table in the syntax tree. Otherewise, 
        /// the source node is mapped to a node view</param>
        /// <param name="edgeNameTuples">A tuple (Node table name, Edge column name) mapping to the edge.
        /// If null, the edge is mapped to an edge column in a physical node table. Ohterwise,
        /// the edge is mapped to an edge view</param>
        /// <returns>Parameters in the table-valued function</returns>
        protected List<WScalarExpression> ConstructEdgeTvfParameters(string nodeAlias, 
            HashSet<string> nodeTableNameSet = null, List<Tuple<string, string>> edgeNameTuples=null)
        {
            var edgeIdentifiers = EdgeColumn.MultiPartIdentifier.Identifiers;
            var edgeColIdentifier = edgeIdentifiers.Last();
            Identifier srcNodeIdentifier = new Identifier { Value = nodeAlias };
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            // The source is a physical node
            if (nodeTableNameSet==null)
            {
                // The edge is a physical edge
                if (edgeNameTuples == null)
                {
                    parameters.Add(new WColumnReferenceExpression
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier(srcNodeIdentifier, edgeColIdentifier)
                    });
                    parameters.Add(new WColumnReferenceExpression
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier(srcNodeIdentifier,
                                new Identifier { Value = edgeColIdentifier.Value + "DeleteCol" })
                    });
                }
                // The edge is an edge view
                else
                {
                    foreach (var column in edgeNameTuples)
                    {
                        Identifier includedEdgeColumnIdentifier = new Identifier { Value = column.Item2 };
                        parameters.Add(new WColumnReferenceExpression
                        {
                            MultiPartIdentifier =
                                new WMultiPartIdentifier(srcNodeIdentifier, includedEdgeColumnIdentifier)
                        });
                        parameters.Add(new WColumnReferenceExpression
                        {
                            MultiPartIdentifier =
                                new WMultiPartIdentifier(srcNodeIdentifier,
                                    new Identifier { Value = includedEdgeColumnIdentifier.Value + "DeleteCol" })
                        });
                    }
                }
            }
            // The source is a node view
            else
            {
                // The edge is a physical edge
                if (edgeNameTuples==null)
                {
                    string srcTableName = BindNodeTableObjName.BaseIdentifier.Value;
                    Identifier nodeViewEdgeColIdentifier = new Identifier
                    {
                        Value = srcTableName + "_" + edgeColIdentifier.Value
                    };
                    parameters.Add(new WColumnReferenceExpression
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier(srcNodeIdentifier, nodeViewEdgeColIdentifier)
                    });
                    parameters.Add(new WColumnReferenceExpression
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier(srcNodeIdentifier,
                                new Identifier { Value = nodeViewEdgeColIdentifier.Value + "DeleteCol" })
                    });

                }
                // The edge is an edge view
                else
                {
                    foreach (var column in edgeNameTuples)
                    {
                        if (nodeTableNameSet.Contains(column.Item1))
                        {
                            Identifier includedEdgeColumnIdentifier = new Identifier
                            {
                                Value = column.Item1 + "_" + column.Item2
                            };
                            parameters.Add(new WColumnReferenceExpression
                            {
                                MultiPartIdentifier =
                                    new WMultiPartIdentifier(srcNodeIdentifier, includedEdgeColumnIdentifier)
                            });
                            parameters.Add(new WColumnReferenceExpression
                            {
                                MultiPartIdentifier =
                                    new WMultiPartIdentifier(srcNodeIdentifier,
                                        new Identifier { Value = includedEdgeColumnIdentifier.Value + "DeleteCol" })
                            });
                        }
                        else
                        {
                            parameters.Add(new WValueExpression { Value = "null" });
                            parameters.Add(new WValueExpression { Value = "null" });
                        }
                    }
                }
            }
            return parameters;
        }

        /// <summary>
        /// Converts the edge to the table-valued function
        /// </summary>
        /// <param name="nodeAlias">Source node alias</param>
        /// <param name="metaData">Meta data</param>
        /// <returns>A syntax tree node representing the table-valued function</returns>
        public virtual WSchemaObjectFunctionTableReference ToSchemaObjectFunction(string nodeAlias, GraphMetaData metaData)
        {
            var edgeIdentifiers = EdgeColumn.MultiPartIdentifier.Identifiers;
            var edgeColIdentifier = edgeIdentifiers.Last();

            HashSet<string> nodeSet;
            if (!metaData.NodeViewMapping.TryGetValue(
                WNamedTableReference.SchemaNameToTuple(SourceNode.NodeTableObjectName), out nodeSet))
                nodeSet = null;
            EdgeInfo edgeInfo =
                metaData.ColumnsOfNodeTables[WNamedTableReference.SchemaNameToTuple(BindNodeTableObjName)][
                    edgeColIdentifier.Value].EdgeInfo;
            List<Tuple<string, string>> edgeTuples = edgeInfo.EdgeColumns;
            var parameters = ConstructEdgeTvfParameters(nodeAlias, nodeSet, edgeTuples);

            var decoderFunction = new Identifier
            {
                Value = BindNodeTableObjName.SchemaIdentifier.Value + '_' +
                        BindNodeTableObjName.BaseIdentifier.Value + '_' +
                        EdgeColumn.MultiPartIdentifier.Identifiers.Last().Value + '_' +
                        "Decoder"
            };
            return new WSchemaObjectFunctionTableReference
            {
                SchemaObject = new WSchemaObjectName(
                    new Identifier { Value = "dbo" },
                    decoderFunction),
                Parameters = parameters,
                Alias = new Identifier
                {
                    Value = EdgeAlias,
                }
            };
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
        /// Converts the edge to the table-valued function
        /// </summary>
        /// <param name="nodeAlias">Source node alias</param>
        /// <param name="metaData">Meta data</param>
        /// <returns>A syntax tree node representing the table-valued function</returns>
        public override WSchemaObjectFunctionTableReference ToSchemaObjectFunction(string nodeAlias, GraphMetaData metaData)
        {
            var edgeIdentifiers = EdgeColumn.MultiPartIdentifier.Identifiers;
            var edgeColIdentifier = edgeIdentifiers.Last();
            HashSet<string> nodeSet;
            if (!metaData.NodeViewMapping.TryGetValue(
                WNamedTableReference.SchemaNameToTuple(SourceNode.NodeTableObjectName), out nodeSet))
                nodeSet = null;
            var sourceNodeColumns =
                metaData.ColumnsOfNodeTables[WNamedTableReference.SchemaNameToTuple(BindNodeTableObjName)];
            var edgeInfo = sourceNodeColumns[edgeColIdentifier.Value].EdgeInfo;
            List<Tuple<string, string>> edgeTuples = edgeInfo.EdgeColumns;
            var parameters = ConstructEdgeTvfParameters(nodeAlias, nodeSet, edgeTuples);

            Identifier decoderFunction;
            if (ReferencePathInfo)
            {
                decoderFunction = new Identifier
                {
                    Value = BindNodeTableObjName.SchemaIdentifier.Value + '_' +
                            BindNodeTableObjName.BaseIdentifier.Value + '_' +
                            EdgeColumn.MultiPartIdentifier.Identifiers.Last().Value + '_' +
                            "bfsPathWithMessage"
                };
                // Node view
                if (nodeSet!=null)
                {
                    parameters.Insert(0,new WColumnReferenceExpression
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier(new Identifier() { Value = SourceNode.RefAlias },
                                new Identifier() { Value = "_NodeId" })
                    });
                    parameters.Insert(0,new WColumnReferenceExpression
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier(new Identifier() { Value = SourceNode.RefAlias },
                                new Identifier() { Value = "_NodeType" })
                    });
                    
                }
                else
                {
                    string nodeIdName =
                    sourceNodeColumns.FirstOrDefault(e => e.Value.Role == WNodeTableColumnRole.NodeId).Key;
                    if (string.IsNullOrEmpty(nodeIdName))
                        parameters.Insert(0, new WValueExpression { Value = "null" });
                    else
                    {
                        parameters.Insert(0, new WColumnReferenceExpression
                        {
                            MultiPartIdentifier =
                                new WMultiPartIdentifier(new Identifier() { Value = SourceNode.RefAlias },
                                    new Identifier() { Value = nodeIdName })
                        });
                    }
                    parameters.Insert(0,
                        new WValueExpression { Value = BindNodeTableObjName.BaseIdentifier.Value, SingleQuoted = true });
                }
            }
            else
            {
                decoderFunction = new Identifier
                {
                    Value = BindNodeTableObjName.SchemaIdentifier.Value + '_' +
                            BindNodeTableObjName.BaseIdentifier.Value + '_' +
                            EdgeColumn.MultiPartIdentifier.Identifiers.Last().Value + '_' +
                            "bfsPath"
                };
            }
            parameters.Insert(0, new WValueExpression { Value = MaxLength.ToString() });
            parameters.Insert(0, new WValueExpression { Value = MinLength.ToString() });
            parameters.Insert(0,
                new WColumnReferenceExpression
                {
                    MultiPartIdentifier =
                        new WMultiPartIdentifier(new[] { new Identifier { Value = nodeAlias }, new Identifier { Value = "GlobalNodeId" }, })
                });
            var attributes = edgeInfo.ColumnAttributes;
            if (AttributeValueDict == null)
            {
                WValueExpression nullExpression = new WValueExpression { Value = "null" };
                for (int i = 0; i < attributes.Count; i++)
                    parameters.Add(nullExpression);
            }
            else
            {
                foreach (var attribute in attributes)
                {
                    string value;
                    var valueExpression = new WValueExpression
                    {
                        Value = AttributeValueDict.TryGetValue(attribute, out value) ? value : "null"
                    };

                    parameters.Add(valueExpression);
                }
            }
            return new WSchemaObjectFunctionTableReference
            {
                SchemaObject = new WSchemaObjectName(
                    new Identifier { Value = "dbo" },
                    decoderFunction),
                Parameters = parameters,
                Alias = new Identifier
                {
                    Value = EdgeAlias,
                }
            };
        }

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

    internal class MatchNode
    {
        public string NodeAlias { get; set; }
        public WSchemaObjectName NodeTableObjectName { get; set; }
        public IList<MatchEdge> Neighbors { get; set; }
        public double EstimatedRows { get; set; }
        public int TableRowCount { get; set; }
        /// <summary>
        /// True, if this node alias is defined in one of the parent query contexts;
        /// false, if the node alias is defined in the current query context.
        /// </summary>
        public bool External { get; set; }

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

        public string RefAlias
        {
            get { return NodeAlias + (External ? "Prime" : ""); }
        }

        public override int GetHashCode()
        {
            return NodeAlias.GetHashCode();
        }
    }

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
        public IList<ConnectedComponent> ConnectedSubGraphs { get; set; }

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
