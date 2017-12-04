using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GraphView
{
    public class AggregationBlock
    {
        internal string AggregationAlias { get; set; }
        internal List<string> FreeTableList { get; set; }
        internal List<string> TableList { get; set; }
        internal Dictionary<string, WTableReferenceWithAlias> TableDict { get; set; }
        internal Dictionary<string, HashSet<string>> TableInputDependency { get; set; }
        internal MatchGraph GraphPattern { get; set; }
        private int HashCode { get; set; }

        public AggregationBlock()
        {
            this.AggregationAlias = "dummy";
            this.FreeTableList = new List<string>();
            this.TableList = new List<string>();
            this.TableDict = new Dictionary<string, WTableReferenceWithAlias>();
            this.TableDict[this.AggregationAlias] = null;
            this.TableInputDependency = new Dictionary<string, HashSet<string>>();
            this.GraphPattern = null;
            this.HashCode = Int32.MaxValue;
        }

        public AggregationBlock(WSchemaObjectFunctionTableReference table)
        {
            this.AggregationAlias = table.Alias.Value;
            this.FreeTableList = new List<string>();
            this.TableList = new List<string>();
            this.TableDict = new Dictionary<string, WTableReferenceWithAlias>();
            this.TableDict[this.AggregationAlias] = table;
            this.TableInputDependency = new Dictionary<string, HashSet<string>>();
            this.GraphPattern = null;
            this.HashCode = Int32.MaxValue;
        }

        public AggregationBlock(WQueryDerivedTable table)
        {
            this.AggregationAlias = table.Alias.Value;
            this.FreeTableList = new List<string>();
            this.TableList = new List<string>();
            this.TableDict = new Dictionary<string, WTableReferenceWithAlias>();
            this.TableDict[this.AggregationAlias] = table;
            this.TableInputDependency = new Dictionary<string, HashSet<string>>();
            this.GraphPattern = null;
            this.HashCode = Int32.MaxValue;
        }

        internal string AddTable(WNamedTableReference table)
        {
            string alias = table.Alias.Value;
            this.TableList.Add(alias);
            this.TableDict[alias] = table;
            this.FreeTableList.Add(alias);
            this.TableInputDependency[alias] = new HashSet<string>();
            this.HashCode = Int32.MaxValue;
            return alias;
        }

        internal string AddTable(WQueryDerivedTable table)
        {
            string alias = table.Alias.Value;
            this.TableList.Add(alias);
            this.TableDict[alias] = table;
            this.TableInputDependency[alias] = new HashSet<string>();
            this.HashCode = Int32.MaxValue;
            return alias;
        }

        internal string AddTable(WVariableTableReference table)
        {
            string alias = table.Alias.Value;
            this.TableList.Add(alias);
            this.TableDict[alias] = table;
            this.TableInputDependency[alias] = new HashSet<string>();
            this.HashCode = Int32.MaxValue;
            return alias;
        }

        internal string AddTable(WSchemaObjectFunctionTableReference table)
        {
            string alias = table.Alias.Value;
            this.TableList.Add(alias);
            this.TableDict[alias] = table;
            this.TableInputDependency[alias] = new HashSet<string>();
            this.TableInputDependency[alias].Add("dummy");
            this.HashCode = Int32.MaxValue;
            return alias;
        }

        internal void CreateMatchGraph(WMatchClause matchClause)
        {
            Dictionary<string, MatchPath> pathDictionary = new Dictionary<string, MatchPath>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, MatchNode> vertexTableCollection = new Dictionary<string, MatchNode>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, MatchEdge> edgeTableCollection = new Dictionary<string, MatchEdge>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, ConnectedComponent> subGraphMap = new Dictionary<string, ConnectedComponent>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, string> parent = new Dictionary<string, string>();
            List<ConnectedComponent> connectedSubGraphs = new List<ConnectedComponent>();
            UnionFind unionFind = new UnionFind();

            foreach (string table in this.FreeTableList)
            {
                if (table.StartsWith(GremlinKeyword.VertexTablePrefix))
                {
                    vertexTableCollection[table] = new MatchNode()
                    {
                        NodeAlias = table,
                        Neighbors = new List<MatchEdge>(),
                        ReverseNeighbors = new List<MatchEdge>(),
                        DanglingEdges = new List<MatchEdge>(),
                        Predicates = new List<WBooleanExpression>(),
                        Properties = new HashSet<string>()
                    };
                }
                else
                {
                    edgeTableCollection[table] = new MatchEdge()
                    {
                        EdgeAlias = table,
                        Predicates = new List<WBooleanExpression>(),
                        BindNodeTableObjName =
                            new WSchemaObjectName(
                            ),
                        IsReversed = false,
                        Properties = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties)
                    };
                }
                parent[table] = table;
            }

            unionFind.Parent = parent;

            if (matchClause != null)
            {
                foreach (WMatchPath path in matchClause.Paths)
                {
                    int index = 0;
                    bool outOfBlock = false;
                    MatchEdge edgeToSrcNode = null;

                    for (int count = path.PathEdgeList.Count; index < count; ++index)
                    {
                        WSchemaObjectName currentNodeTableRef = path.PathEdgeList[index].Item1;
                        WEdgeColumnReferenceExpression currentEdgeColumnRef = path.PathEdgeList[index].Item2;
                        WSchemaObjectName nextNodeTableRef = index != count - 1
                            ? path.PathEdgeList[index + 1].Item1
                            : path.Tail;
                        string currentNodeExposedName = currentNodeTableRef.BaseIdentifier.Value;
                        string edgeAlias = currentEdgeColumnRef.Alias;
                        string nextNodeExposedName = nextNodeTableRef != null ? nextNodeTableRef.BaseIdentifier.Value : null;

                        if (!this.TableList.Contains(currentNodeExposedName))
                        {
                            continue;
                        }

                        // Consturct the source node of a path in MatchClause.Paths
                        MatchNode srcNode = vertexTableCollection[currentNodeExposedName];

                        // Consturct the edge of a path in MatchClause.Paths
                        MatchEdge edgeFromSrcNode;
                        edgeTableCollection[edgeAlias].SourceNode = srcNode;
                        edgeTableCollection[edgeAlias].EdgeColumn = currentEdgeColumnRef;
                        edgeTableCollection[edgeAlias].EdgeType = currentEdgeColumnRef.EdgeType;

                        if (currentEdgeColumnRef.MinLength == 1 && currentEdgeColumnRef.MaxLength == 1)
                        {
                            edgeFromSrcNode = new MatchEdge
                            {
                                SourceNode = srcNode,
                                EdgeColumn = currentEdgeColumnRef,
                                EdgeAlias = edgeAlias,
                                Predicates = edgeTableCollection[edgeAlias].Predicates,
                                BindNodeTableObjName =
                                    new WSchemaObjectName(
                                    ),
                                IsReversed = false,
                                EdgeType = currentEdgeColumnRef.EdgeType,
                                Properties = edgeTableCollection[edgeAlias].Properties
                            };
                        }
                        else
                        {
                            MatchPath matchPath = new MatchPath
                            {
                                SourceNode = srcNode,
                                EdgeColumn = currentEdgeColumnRef,
                                EdgeAlias = edgeAlias,
                                Predicates = edgeTableCollection[edgeAlias].Predicates,
                                BindNodeTableObjName =
                                    new WSchemaObjectName(
                                    ),
                                MinLength = currentEdgeColumnRef.MinLength,
                                MaxLength = currentEdgeColumnRef.MaxLength,
                                ReferencePathInfo = false,
                                AttributeValueDict = currentEdgeColumnRef.AttributeValueDict,
                                IsReversed = false,
                                EdgeType = currentEdgeColumnRef.EdgeType,
                                Properties = edgeTableCollection[edgeAlias].Properties
                            };
                            pathDictionary[edgeAlias] = matchPath;
                            edgeFromSrcNode = matchPath;
                        }


                        if (path.IsReversed)
                        {
                            unionFind.Union(edgeAlias, currentNodeExposedName);
                        }
                        else
                        {
                            unionFind.Union(currentNodeExposedName, edgeAlias);
                        }

                        if (edgeToSrcNode != null)
                        {
                            edgeToSrcNode.SinkNode = srcNode;
                            if (!(edgeToSrcNode is MatchPath))
                            {
                                // Construct reverse edge
                                MatchEdge reverseEdge = new MatchEdge
                                {
                                    SourceNode = edgeToSrcNode.SinkNode,
                                    SinkNode = edgeToSrcNode.SourceNode,
                                    EdgeColumn = edgeToSrcNode.EdgeColumn,
                                    EdgeAlias = edgeToSrcNode.EdgeAlias,
                                    Predicates = edgeToSrcNode.Predicates,
                                    BindNodeTableObjName =
                                        new WSchemaObjectName(
                                        ),
                                    IsReversed = true,
                                    EdgeType = edgeToSrcNode.EdgeType,
                                    Properties = edgeToSrcNode.Properties,
                                };
                                srcNode.ReverseNeighbors.Add(reverseEdge);
                            }
                        }

                        edgeToSrcNode = edgeFromSrcNode;

                        if (nextNodeExposedName != null)
                        {
                            if (!parent.ContainsKey(nextNodeExposedName))
                            {
                                parent[nextNodeExposedName] = nextNodeExposedName;
                            }

                            // unionFind.Union(currentNodeExposedName, nextNodeExposedName);
                            // unionFind.Union(edgeAlias, nextNodeExposedName);

                            if (path.IsReversed)
                            {
                                unionFind.Union(nextNodeExposedName, edgeAlias);
                            }
                            else
                            {
                                unionFind.Union(edgeAlias, nextNodeExposedName);
                            }

                            srcNode.Neighbors.Add(edgeFromSrcNode);
                        }
                        // Dangling edge without SinkNode
                        else
                        {
                            srcNode.DanglingEdges.Add(edgeFromSrcNode);
                        }
                    }

                    if (path.Tail == null)
                    {
                        continue;
                    }

                    // Consturct destination node of a path in MatchClause.Paths
                    string tailExposedName = path.Tail.BaseIdentifier.Value;

                    if (!this.FreeTableList.Contains(tailExposedName))
                    {
                        continue;
                    }

                    MatchNode destNode = vertexTableCollection[tailExposedName];

                    if (edgeToSrcNode != null)
                    {
                        edgeToSrcNode.SinkNode = destNode;
                        if (!(edgeToSrcNode is MatchPath))
                        {
                            // Construct reverse edge
                            MatchEdge reverseEdge = new MatchEdge
                            {
                                SourceNode = edgeToSrcNode.SinkNode,
                                SinkNode = edgeToSrcNode.SourceNode,
                                EdgeColumn = edgeToSrcNode.EdgeColumn,
                                EdgeAlias = edgeToSrcNode.EdgeAlias,
                                Predicates = edgeToSrcNode.Predicates,
                                BindNodeTableObjName =
                                    new WSchemaObjectName(
                                    ),
                                IsReversed = true,
                                EdgeType = edgeToSrcNode.EdgeType,
                                Properties = edgeToSrcNode.Properties,
                            };
                            destNode.ReverseNeighbors.Add(reverseEdge);
                        }

                    }
                }
            }

            // Use union find algorithmn to define which subgraph does a node belong to and put it into where it belongs to.
            foreach (var node in vertexTableCollection)
            {
                string root = unionFind.Find(node.Key);

                ConnectedComponent subGraph;
                if (!subGraphMap.ContainsKey(root))
                {
                    subGraph = new ConnectedComponent();
                    subGraphMap[root] = subGraph;
                    connectedSubGraphs.Add(subGraph);
                }
                else
                {
                    subGraph = subGraphMap[root];
                }

                subGraph.Nodes[node.Key] = node.Value;

                subGraph.IsTailNode[node.Value] = false;

                if (node.Value.Neighbors.Count + node.Value.ReverseNeighbors.Count + node.Value.DanglingEdges.Count > 0)
                {
                    node.Value.Properties.Add(GremlinKeyword.Star);
                }
            }

            foreach (var edge in edgeTableCollection)
            {
                string root = unionFind.Find(edge.Key);
                ConnectedComponent subGraph = subGraphMap[root];
                subGraph.Edges[edge.Key] = edge.Value;
            }

            this.GraphPattern = new MatchGraph() { ConnectedSubGraphs = connectedSubGraphs };
        }

        internal void AttachInputDependency(string alias, List<string> tables)
        {
            if (this.TableInputDependency.ContainsKey(alias))
            {
                foreach (string table in tables)
                {
                    this.TableInputDependency[alias].Add(table);
                }
            }
        }

        public override int GetHashCode()
        {
            if (this.HashCode != Int32.MaxValue)
            {
                return HashCode;
            }
            else
            {
                StringBuilder sb = new StringBuilder();
                sb.Append(this.AggregationAlias);
                foreach (string table in this.TableList)
                {
                    sb.Append(table);
                }
                HashCode = sb.ToString().GetHashCode();
                return HashCode;
            }
        }
    }

    // The implementation of Union find algorithmn.
    public class UnionFind
    {
        public Dictionary<string, string> Parent;

        public string Find(string x)
        {
            return x == Parent[x] ? x : Parent[x] = Find(Parent[x]);
        }

        public void Union(string x, string y)
        {
            string xRoot = Find(x);
            string yRoot = Find(y);
            if (xRoot == yRoot)
                return;
            Parent[xRoot] = yRoot;
        }
    }
}
