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
        internal List<string> TableList { get; set; }

        // A dictionary to map aliases to NonMatchTables
        internal Dictionary<string, NonMatchTable> NonMatchTables { get; set; }

        // The MatchGraph of this AggregationBlock
        internal MatchGraph GraphPattern { get; set; }

        // The HashCode of this AggregationBlock, we can use it to get optimal solution if this block has been optimized.
        private int hashCode;

        public AggregationBlock()
        {
            this.AggregationAlias = "dummy";
            this.GraphPattern = null;
            this.hashCode = Int32.MaxValue;
            this.NonMatchTables = new Dictionary<string, NonMatchTable>();
            this.NonMatchTables[AggregationAlias] = null;
            this.TableList = new List<string>();
        }

        public AggregationBlock(WSchemaObjectFunctionTableReference table)
        {
            this.AggregationAlias = table.Alias.Value;
            this.GraphPattern = null;
            this.hashCode = Int32.MaxValue;
            this.NonMatchTables = new Dictionary<string, NonMatchTable>();
            this.NonMatchTables[AggregationAlias] = new NonMatchTable(table);
            this.TableList = new List<string>();
        }

        public AggregationBlock(WQueryDerivedTable table)
        {
            this.AggregationAlias = table.Alias.Value;
            this.GraphPattern = null;
            this.hashCode = Int32.MaxValue;
            this.NonMatchTables = new Dictionary<string, NonMatchTable>();
            this.NonMatchTables[AggregationAlias] = new NonMatchTable(table);
            this.TableList = new List<string>();
        }

        // Here, we do not construct MatchNode. We will create MatchGraph later
        internal string AddTable(WNamedTableReference table)
        {
            string alias = table.Alias.Value;
            this.hashCode = Int32.MaxValue;
            this.TableList.Add(alias);
            return alias;
        }

        internal string AddTable(WQueryDerivedTable table)
        {
            string alias = table.Alias.Value;
            this.hashCode = Int32.MaxValue;
            this.NonMatchTables[alias] = new NonMatchTable(table);
            this.TableList.Add(alias);
            return alias;
        }

        internal string AddTable(WVariableTableReference table)
        {
            string alias = table.Alias.Value;
            this.hashCode = Int32.MaxValue;
            this.NonMatchTables[alias] = new NonMatchTable(table);
            this.TableList.Add(alias);
            return alias;
        }

        internal string AddTable(WSchemaObjectFunctionTableReference table)
        {
            string alias = table.Alias.Value;
            this.hashCode = Int32.MaxValue;
            this.NonMatchTables[alias] = new NonMatchTable(table);
            this.TableList.Add(alias);
            return alias;
        }

        // Greate the MatchGraph of this AggregationBlock. If some free nodes and free edges are connected, they are in the same ConnectedComponent
        internal void CreateMatchGraph(WMatchClause matchClause)
        {
            Dictionary<string, MatchPath> pathCollection = new Dictionary<string, MatchPath>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, MatchNode> nodeCollection = new Dictionary<string, MatchNode>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, MatchEdge> edgeCollection = new Dictionary<string, MatchEdge>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, ConnectedComponent> subGraphMap = new Dictionary<string, ConnectedComponent>(StringComparer.OrdinalIgnoreCase);
            List<ConnectedComponent> connectedSubGraphs = new List<ConnectedComponent>();

            // we use Disjoint-set data structure to determine whether tables are in the same component or not.
            UnionFind unionFind = new UnionFind();

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
                        MatchNode srcNode;
                        if (nodeCollection.ContainsKey(currentNodeExposedName))
                        {
                            srcNode = nodeCollection[currentNodeExposedName];
                        }
                        else
                        {
                            srcNode = nodeCollection[currentNodeExposedName] = new MatchNode()
                            {
                                NodeAlias = currentNodeExposedName,
                                Neighbors = new List<MatchEdge>(),
                                ReverseNeighbors = new List<MatchEdge>(),
                                DanglingEdges = new List<MatchEdge>(),
                                Predicates = new List<WBooleanExpression>(),
                                Properties = new HashSet<string>()
                            };
                        }

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
                        }

                        if (!unionFind.Parent.ContainsKey(currentNodeExposedName))
                        {
                            unionFind.Parent[currentNodeExposedName] = currentNodeExposedName;
                        }
                        if (!unionFind.Parent.ContainsKey(edgeAlias))
                        {
                            unionFind.Parent[edgeAlias] = edgeAlias;
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
                            if (!unionFind.Parent.ContainsKey(nextNodeExposedName))
                            {
                                unionFind.Parent[nextNodeExposedName] = nextNodeExposedName;
                            }
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
                    
                    MatchNode destNode;
                    if (nodeCollection.ContainsKey(tailExposedName))
                    {
                        destNode = nodeCollection[tailExposedName];
                    }
                    else
                    {
                        destNode = nodeCollection[tailExposedName] = new MatchNode()
                        {
                            NodeAlias = tailExposedName,
                            Neighbors = new List<MatchEdge>(),
                            ReverseNeighbors = new List<MatchEdge>(),
                            DanglingEdges = new List<MatchEdge>(),
                            Predicates = new List<WBooleanExpression>(),
                            Properties = new HashSet<string>()
                        };
                    }

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

            foreach (var edge in edgeCollection)
            {
                string root = unionFind.Find(edge.Key);
                ConnectedComponent subGraph = subGraphMap[root];
                subGraph.Edges[edge.Key] = edge.Value;
            }

            this.GraphPattern = new MatchGraph(connectedSubGraphs);
        }

        public override int GetHashCode()
        {
            if (this.hashCode != Int32.MaxValue)
            {
                return this.hashCode;
            }
            else
            {
                StringBuilder sb = new StringBuilder();
                sb.Append(this.AggregationAlias);
                foreach (string table in this.TableList)
                {
                    sb.Append(table);
                }
                this.hashCode = sb.ToString().GetHashCode();
                return this.hashCode;
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
