using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GraphView
{
    /// <summary>
    /// We use AggregationBlocks to separate swappable parts and unswappable parts.
    /// If some free nodes and TVFs are in one AggregationBlock, they support swap as long as they do not violate dependency.
    /// Generally, we separate tables according to 
    ///     side-effect TVFs (aggregate, store, group, subgraph, tree), 
    ///     global filters (coin, dedup(global), range(global)), 
    ///     global maps (order(global), select), barriers (barrier), 
    ///     modification TVFs (addV, addE, commit, drop, property) 
    ///     and some special TVFs (constant, inject, sample(global)).
    /// Given a SQL-like query, the AggregationBlocks can be certain. So we use the alias of this special table as the alias 
    /// of this Aggregation Block, and we call this special table "root table"
    /// Here, every AggregationBlock incudes 
    ///     one alias of the root table, 
    ///     a dictionary to map aliases to nonfree tables,
    ///     a MatchGraph,
    ///     a dictionary about input dependency except edges
    /// </summary>
    internal class AggregationBlock
    {
        // The alias of the root table
        // If this block is the first one, then the alias is "dummy"
        // Every time generating a new solution, the aggregation table must be the first in an sequence if it is not "dummy"
        internal string RootTableAlias { get; set; }

        // A dictionary to map aliases to NonFreeTables
        internal Dictionary<string, NonFreeTable> NonFreeTables { get; set; }

        // The MatchGraph of this AggregationBlock
        internal MatchGraph GraphPattern { get; set; }

        // A dictionary to record all input dependencies except edges'
        internal Dictionary<string, HashSet<string>> TableInputDependency { get; set; }

        public AggregationBlock()
        {
            this.RootTableAlias = "dummy";
            this.GraphPattern = new MatchGraph();
            this.NonFreeTables = new Dictionary<string, NonFreeTable>();
            this.NonFreeTables[RootTableAlias] = new NonFreeTable();
            this.TableInputDependency = new Dictionary<string, HashSet<string>>();
            this.TableInputDependency[RootTableAlias] = new HashSet<string>();
            this.NonFreeTables[RootTableAlias].Position = this.TableInputDependency.Count;
        }

        public AggregationBlock(WSchemaObjectFunctionTableReference table)
        {
            this.RootTableAlias = table.Alias.Value;
            this.GraphPattern = new MatchGraph();
            this.NonFreeTables = new Dictionary<string, NonFreeTable>();
            this.NonFreeTables[RootTableAlias] = new NonFreeTable(table);
            this.TableInputDependency = new Dictionary<string, HashSet<string>>();
            this.TableInputDependency[RootTableAlias] = new HashSet<string>();
            this.NonFreeTables[RootTableAlias].Position = this.TableInputDependency.Count;
        }

        public AggregationBlock(WQueryDerivedTable table)
        {
            this.RootTableAlias = table.Alias.Value;
            this.GraphPattern = new MatchGraph();
            this.NonFreeTables = new Dictionary<string, NonFreeTable>();
            this.NonFreeTables[RootTableAlias] = new NonFreeTable(table);
            this.TableInputDependency = new Dictionary<string, HashSet<string>>();
            this.TableInputDependency[RootTableAlias] = new HashSet<string>();
            this.NonFreeTables[RootTableAlias].Position = this.TableInputDependency.Count;
        }

        // We firstly think every node are isolated, and we will find subgraphs later
        internal string AddTable(WNamedTableReference table)
        {
            string alias = table.Alias.Value;
            this.TableInputDependency[alias] = new HashSet<string>();
            MatchNode matchNode = new MatchNode()
            {
                NodeAlias = alias,
                Neighbors = new List<MatchEdge>(),
                ReverseNeighbors = new List<MatchEdge>(),
                DanglingEdges = new List<MatchEdge>(),
                Predicates = new List<WBooleanExpression>(),
                Properties = new HashSet<string>()
            };
            ConnectedComponent subgraph = new ConnectedComponent();
            subgraph.Nodes[alias] = matchNode;
            this.GraphPattern.ConnectedSubgraphs.Add(subgraph);
            matchNode.Position = this.TableInputDependency.Count;
            return alias;
        }

        internal string AddTable(WQueryDerivedTable table)
        {
            string alias = table.Alias.Value;
            this.NonFreeTables[alias] = new NonFreeTable(table);
            this.TableInputDependency[alias] = new HashSet<string>();
            this.NonFreeTables[alias].Position = this.TableInputDependency.Count;
            return alias;
        }

        internal string AddTable(WVariableTableReference table)
        {
            string alias = table.Alias.Value;
            this.NonFreeTables[alias] = new NonFreeTable(table);
            this.TableInputDependency[alias] = new HashSet<string>();
            this.NonFreeTables[alias].Position = this.TableInputDependency.Count;
            return alias;
        }

        internal string AddTable(WSchemaObjectFunctionTableReference table)
        {
            string alias = table.Alias.Value;
            this.NonFreeTables[alias] = new NonFreeTable(table);
            this.TableInputDependency[alias] = new HashSet<string>();
            this.NonFreeTables[alias].Position = this.TableInputDependency.Count;
            return alias;
        }

        internal bool TryGetNode(string alias, out CompileNode node)
        {
            if (this.NonFreeTables.ContainsKey(alias))
            {
                node = this.NonFreeTables[alias];
                return true;
            }
            else
            {
                MatchNode matchNode;
                if (this.GraphPattern.TryGetNode(alias, out matchNode))
                {
                    node = matchNode;
                    return true;
                }
            }
            node = null;
            return false;
        }

        internal bool TryGetEdge(string alias, out MatchEdge edge)
        {
            return this.GraphPattern.TryGetEdge(alias, out edge);
        }

        // Greate the MatchGraph of this AggregationBlock. If some free nodes and free edges are connected, they are in the same ConnectedComponent
        internal HashSet<string> CreateMatchGraph(WMatchClause matchClause)
        {
            HashSet<string> freeNodesAndEdges = new HashSet<string>();
            Dictionary<string, MatchPath> pathCollection = new Dictionary<string, MatchPath>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, MatchNode> nodeCollection = new Dictionary<string, MatchNode>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, MatchEdge> edgeCollection = new Dictionary<string, MatchEdge>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, ConnectedComponent> subgraphCollection = new Dictionary<string, ConnectedComponent>(StringComparer.OrdinalIgnoreCase);

            // we use Disjoint-set data structure to determine whether tables are in the same component or not.
            UnionFind unionFind = new UnionFind();

            foreach (ConnectedComponent subgraph in this.GraphPattern.ConnectedSubgraphs)
            {
                foreach (KeyValuePair<string, MatchNode> pair in subgraph.Nodes)
                {
                    nodeCollection.Add(pair.Key, pair.Value);
                    unionFind.Add(pair.Key);
                }
            }

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

                        // Get the source node of a path
                        if (!nodeCollection.ContainsKey(currentNodeExposedName))
                        {
                            continue;
                        }
                        MatchNode srcNode = nodeCollection[currentNodeExposedName];

                        // Get the edge of a path, and set required attributes
                        // Because the sourceNode is relative, we need to construct new edges or paths
                        // But they need to share the same predicates and proerties
                        MatchEdge edgeFromSrcNode;
                        if (currentEdgeColumnRef.MinLength == 1 && currentEdgeColumnRef.MaxLength == 1)
                        {
                            if (!edgeCollection.ContainsKey(edgeAlias))
                            {
                                edgeCollection[edgeAlias] = new MatchEdge()
                                {
                                    LinkAlias = edgeAlias,
                                    SourceNode = srcNode,
                                    EdgeType = currentEdgeColumnRef.EdgeType,
                                    Predicates = new List<WBooleanExpression>(),
                                    BindNodeTableObjName = new WSchemaObjectName(),
                                    IsReversed = false,
                                    Properties = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties)
                                };
                                unionFind.Add(edgeAlias);
                            }

                            edgeFromSrcNode = new MatchEdge
                            {
                                LinkAlias = edgeAlias,
                                SourceNode = srcNode,
                                EdgeType = edgeCollection[edgeAlias].EdgeType,
                                Predicates = edgeCollection[edgeAlias].Predicates,
                                BindNodeTableObjName = edgeCollection[edgeAlias].BindNodeTableObjName,
                                IsReversed = false,
                                Properties = edgeCollection[edgeAlias].Properties
                            };
                        }
                        else
                        {
                            if (!pathCollection.ContainsKey(edgeAlias))
                            {
                                pathCollection[edgeAlias] = new MatchPath
                                {
                                    SourceNode = srcNode,
                                    LinkAlias = edgeAlias,
                                    Predicates = new List<WBooleanExpression>(),
                                    BindNodeTableObjName = new WSchemaObjectName(),
                                    MinLength = currentEdgeColumnRef.MinLength,
                                    MaxLength = currentEdgeColumnRef.MaxLength,
                                    ReferencePathInfo = false,
                                    AttributeValueDict = currentEdgeColumnRef.AttributeValueDict,
                                    IsReversed = false,
                                    EdgeType = currentEdgeColumnRef.EdgeType,
                                    Properties = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties)
                                };
                            }

                            edgeFromSrcNode = new MatchPath
                            {
                                SourceNode = srcNode,
                                LinkAlias = edgeAlias,
                                Predicates = pathCollection[edgeAlias].Predicates,
                                BindNodeTableObjName = pathCollection[edgeAlias].BindNodeTableObjName,
                                MinLength = pathCollection[edgeAlias].MinLength,
                                MaxLength = pathCollection[edgeAlias].MaxLength,
                                ReferencePathInfo = false,
                                AttributeValueDict = pathCollection[edgeAlias].AttributeValueDict,
                                IsReversed = false,
                                EdgeType = pathCollection[edgeAlias].EdgeType,
                                Properties = pathCollection[edgeAlias].Properties
                            };
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
                                    LinkAlias = edgeToSrcNode.LinkAlias,
                                    Predicates = edgeToSrcNode.Predicates,
                                    BindNodeTableObjName = edgeToSrcNode.BindNodeTableObjName,
                                    IsReversed = true,
                                    EdgeType = edgeToSrcNode.EdgeType,
                                    Properties = edgeToSrcNode.Properties,
                                };
                                srcNode.ReverseNeighbors.Add(reverseEdge);
                            }
                        }

                        edgeToSrcNode = edgeFromSrcNode;

                        // Add this edge to node's neightbors
                        if (nextNodeExposedName != null)
                        {
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
                        // Add this edge to node's dangling edges
                        else
                        {
                            srcNode.DanglingEdges.Add(edgeFromSrcNode);
                        }
                    }

                    if (path.Tail == null)
                    {
                        continue;
                    }

                    // Get destination node of a path
                    string tailExposedName = path.Tail.BaseIdentifier.Value;

                    if (!nodeCollection.ContainsKey(tailExposedName))
                    {
                        continue;
                    }

                    MatchNode destNode = nodeCollection[tailExposedName];

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
                                LinkAlias = edgeToSrcNode.LinkAlias,
                                Predicates = edgeToSrcNode.Predicates,
                                BindNodeTableObjName = edgeToSrcNode.BindNodeTableObjName,
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
            foreach (var node in nodeCollection)
            {
                freeNodesAndEdges.Add(node.Key);
                string root = unionFind.Find(node.Key);

                ConnectedComponent subGraph;
                if (!subgraphCollection.ContainsKey(root))
                {
                    subGraph = new ConnectedComponent();
                    subgraphCollection[root] = subGraph;
                }
                else
                {
                    subGraph = subgraphCollection[root];
                }

                subGraph.Nodes[node.Key] = node.Value;

                subGraph.IsTailNode[node.Value] = false;

                foreach (MatchEdge edge in node.Value.Neighbors)
                {
                    subGraph.Edges[edge.LinkAlias] = edge;
                    freeNodesAndEdges.Add(edge.LinkAlias);
                }

                foreach (MatchEdge edge in node.Value.DanglingEdges)
                {
                    subGraph.Edges[edge.LinkAlias] = edge;
                    edge.IsDanglingEdge = true;
                    freeNodesAndEdges.Add(edge.LinkAlias);
                }

                if (node.Value.Neighbors.Count + node.Value.ReverseNeighbors.Count + node.Value.DanglingEdges.Count > 0)
                {
                    node.Value.Properties.Add(GremlinKeyword.Star);
                }
            }
            
            this.GraphPattern = new MatchGraph(subgraphCollection.Values.ToList());

            return freeNodesAndEdges;
        }

        internal void CheckIsDummy()
        {
            foreach (ConnectedComponent component in this.GraphPattern.ConnectedSubgraphs)
            {
                foreach (MatchNode node in component.Nodes.Values)
                {
                    if (node.Neighbors.Count + node.ReverseNeighbors.Count == 0 &&
                        node.DanglingEdges.Count == 1 &&
                        node.DanglingEdges[0].EdgeType == WEdgeType.OutEdge &&
                        (node.Predicates == null || !node.Predicates.Any()))
                    {
                        node.IsDummyNode = true;
                        break;
                    }
                }
            }
        }
    }


    // The implementation of Union find algorithmn.
    public class UnionFind
    {
        private Dictionary<string, string> parent;

        public UnionFind()
        {
            this.parent = new Dictionary<string, string>();
        }

        public string Find(string x)
        {
            return x == this.parent[x] ? x : this.parent[x] = Find(this.parent[x]);
        }

        public void Union(string x, string y)
        {
            string xRoot = Find(x);
            string yRoot = Find(y);
            if (xRoot == yRoot)
                return;
            this.parent[xRoot] = yRoot;
        }

        public bool Contains(string x)
        {
            return this.parent.ContainsKey(x);
        }

        public void Add(string x)
        {
            this.parent[x] = x;
        }
    }
}
