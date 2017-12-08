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
    /// of this Aggregation Block
    /// Here, every AggregationBlock incudes 
    ///     one alias of the special table and the AggregationBlock, 
    ///     a list of free tables,
    ///     a list of all tables except for this special table,
    ///     a dictionary to map aliases to tables,
    ///     a dictionary about input dependency,
    ///     a MatchGraph,
    ///     and a HashCode which is used as the key to get the optimal solution that is stored in the QueryCompilationContext
    /// </summary>
    internal class AggregationBlock
    {
        // The alias of the AggregationBlock
        // If no special table in this block, this alias is "dummy"
        // Every time generating a new solution, the aggregation table must be the first in an sequence if it is not "dummy"
        internal string AggregationAlias { get; set; }

        // A list of aliases of tables, except for AggregationAlias. It is used to record the original order
        internal HashSet<string> TableAliases { get; set; }

        // A dictionary to map aliases to NonFreeTables
        internal Dictionary<string, NonFreeTable> NonFreeTables { get; set; }

        // The MatchGraph of this AggregationBlock
        internal MatchGraph GraphPattern { get; set; }
        
        internal Dictionary<string, HashSet<string>> TableInputDependency { get; set; }

        public AggregationBlock()
        {
            this.AggregationAlias = "dummy";
            this.GraphPattern = new MatchGraph();
            this.NonFreeTables = new Dictionary<string, NonFreeTable>();
            this.NonFreeTables[AggregationAlias] = new NonFreeTable();
            this.TableAliases = new HashSet<string>();
            this.TableAliases.Add(this.AggregationAlias);
            this.TableInputDependency = new Dictionary<string, HashSet<string>>();
            this.TableInputDependency[AggregationAlias] = new HashSet<string>();

            // TODO: find a better way to compute cardinality
            this.NonFreeTables[AggregationAlias].Cardinality = this.TableAliases.Count * this.TableAliases.Count;
        }

        public AggregationBlock(WSchemaObjectFunctionTableReference table)
        {
            this.AggregationAlias = table.Alias.Value;
            this.GraphPattern = new MatchGraph();
            this.NonFreeTables = new Dictionary<string, NonFreeTable>();
            this.NonFreeTables[AggregationAlias] = new NonFreeTable(table);
            this.TableAliases = new HashSet<string>();
            this.TableAliases.Add(this.AggregationAlias);
            this.TableInputDependency = new Dictionary<string, HashSet<string>>();
            this.TableInputDependency[AggregationAlias] = new HashSet<string>();

            // TODO: find a better way to compute cardinality
            this.NonFreeTables[AggregationAlias].Cardinality = this.TableAliases.Count * this.TableAliases.Count;
        }

        public AggregationBlock(WQueryDerivedTable table)
        {
            this.AggregationAlias = table.Alias.Value;
            this.GraphPattern = new MatchGraph();
            this.NonFreeTables = new Dictionary<string, NonFreeTable>();
            this.NonFreeTables[AggregationAlias] = new NonFreeTable(table);
            this.TableAliases = new HashSet<string>();
            this.TableAliases.Add(this.AggregationAlias);
            this.TableInputDependency = new Dictionary<string, HashSet<string>>();
            this.TableInputDependency[AggregationAlias] = new HashSet<string>();

            // TODO: find a better way to compute cardinality
            this.NonFreeTables[AggregationAlias].Cardinality = this.TableAliases.Count * this.TableAliases.Count;
        }

        // We firstly think every node are isolated, and we will find subgraphs later
        internal string AddTable(WNamedTableReference table)
        {
            string alias = table.Alias.Value;
            this.TableAliases.Add(alias);
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

            // TODO: find a better way to compute estimatedRows
            matchNode.EstimatedRows = this.TableAliases.Count * this.TableAliases.Count;
            return alias;
        }

        internal string AddTable(WQueryDerivedTable table)
        {
            string alias = table.Alias.Value;
            this.NonFreeTables[alias] = new NonFreeTable(table);
            this.TableAliases.Add(alias);
            this.TableInputDependency[alias] = new HashSet<string>();

            // TODO: find a better way to compute cardinality
            this.NonFreeTables[alias].Cardinality = this.TableAliases.Count * this.TableAliases.Count;
            return alias;
        }

        internal string AddTable(WVariableTableReference table)
        {
            string alias = table.Alias.Value;
            this.NonFreeTables[alias] = new NonFreeTable(table);
            this.TableAliases.Add(alias);
            this.TableInputDependency[alias] = new HashSet<string>();

            // TODO: find a better way to compute cardinality
            this.NonFreeTables[alias].Cardinality = this.TableAliases.Count * this.TableAliases.Count;
            return alias;
        }

        internal string AddTable(WSchemaObjectFunctionTableReference table)
        {
            string alias = table.Alias.Value;
            this.NonFreeTables[alias] = new NonFreeTable(table);
            this.TableAliases.Add(alias);
            this.TableInputDependency[alias] = new HashSet<string>();

            // TODO: find a better way to compute cardinality
            this.NonFreeTables[alias].Cardinality = this.TableAliases.Count * this.TableAliases.Count;
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
        internal HashSet<string> CreateGraph(WMatchClause matchClause)
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
                        MatchEdge edgeFromSrcNode;
                        if (edgeCollection.ContainsKey(edgeAlias))
                        {
                            edgeFromSrcNode = edgeCollection[edgeAlias];
                        }
                        else if (pathCollection.ContainsKey(edgeAlias))
                        {
                            edgeFromSrcNode = pathCollection[edgeAlias];
                        }
                        else if (currentEdgeColumnRef.MinLength == 1 && currentEdgeColumnRef.MaxLength == 1)
                        {
                            edgeFromSrcNode = edgeCollection[edgeAlias] = new MatchEdge
                            {
                                LinkAlias = edgeAlias,
                                SourceNode = srcNode,
                                EdgeType = currentEdgeColumnRef.EdgeType,
                                Predicates = new List<WBooleanExpression>(),
                                BindNodeTableObjName =
                                    new WSchemaObjectName(
                                    ),
                                IsReversed = false,
                                Properties = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties)
                            };
                        }
                        else
                        {
                            edgeFromSrcNode = pathCollection[edgeAlias] = new MatchPath
                            {
                                SourceNode = srcNode,
                                LinkAlias = edgeAlias,
                                Predicates = edgeCollection[edgeAlias].Predicates,
                                BindNodeTableObjName =
                                    new WSchemaObjectName(
                                    ),
                                MinLength = currentEdgeColumnRef.MinLength,
                                MaxLength = currentEdgeColumnRef.MaxLength,
                                ReferencePathInfo = false,
                                AttributeValueDict = currentEdgeColumnRef.AttributeValueDict,
                                IsReversed = false,
                                EdgeType = currentEdgeColumnRef.EdgeType,
                                Properties = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties)
                            };
                            unionFind.Add(edgeAlias);
                        }

                        
                        if (!unionFind.Contains(edgeAlias))
                        {
                            unionFind.Add(edgeAlias);
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

                if (node.Value.Neighbors.Count + node.Value.ReverseNeighbors.Count + node.Value.DanglingEdges.Count > 0)
                {
                    node.Value.Properties.Add(GremlinKeyword.Star);
                }
            }

            foreach (var edge in edgeCollection)
            {
                freeNodesAndEdges.Add(edge.Key);
                string root = unionFind.Find(edge.Key);
                ConnectedComponent subGraph = subgraphCollection[root];
                subGraph.Edges[edge.Key] = edge.Value;
            }
            
            this.GraphPattern = new MatchGraph(subgraphCollection.Values.ToList());

            return freeNodesAndEdges;
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
