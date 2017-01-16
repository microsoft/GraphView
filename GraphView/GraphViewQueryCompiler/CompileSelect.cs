using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using GraphView;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    partial class WSelectQueryBlock
    {
        internal override GraphViewExecutionOperator Generate(GraphViewConnection pConnection)
        {
            if (WithPathClause != null) WithPathClause.Generate(pConnection);
            // Construct Match graph for later use
            MatchGraph graph = ConstructGraph();
            // Construct the traversal chain
            ConstructTraversalChain(graph);
            // Construct a header for the operators.
            Dictionary<string, string> columnToAliasDict;
            Dictionary<string, DColumnReferenceExpression> headerToColumnRefDict;
            List<string> header = ConstructHeader(graph, out columnToAliasDict, out headerToColumnRefDict);
            // Attach pre-generated docDB script to the node on Match graph, 
            // and turn predicates that cannot be attached to one node into boolean function.
            List<BooleanFunction> Functions = AttachScriptSegment(graph, header, columnToAliasDict, headerToColumnRefDict);
            // Construct operators accroding to the match graph, header and boolean function list.
            return ConstructOperator(graph, header, columnToAliasDict, pConnection, Functions);
        }

        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            List<WTableReferenceWithAlias> nonVertexTableReferences = null;
            MatchGraph graphPattern = ConstructGraph2(context.TableReferences, out nonVertexTableReferences);

            // Vertex and edge aliases from the graph pattern, plus non-vertex table references.
            List<string> vertexAndEdgeAliases = new List<string>();

            foreach (var subGraph in graphPattern.ConnectedSubGraphs)
            {
                vertexAndEdgeAliases.AddRange(subGraph.Nodes.Keys);
                vertexAndEdgeAliases.AddRange(subGraph.Edges.Keys);
            }
            foreach (var nonVertexTableReference in nonVertexTableReferences)
            {
                vertexAndEdgeAliases.Add(nonVertexTableReference.Alias.Value);
            }

            // Normalizes the search condition into conjunctive predicates
            BooleanExpressionNormalizeVisitor booleanNormalize = new BooleanExpressionNormalizeVisitor();
            List<WBooleanExpression> conjunctivePredicates = 
                WhereClause != null && WhereClause.SearchCondition != null ?
                booleanNormalize.Invoke(WhereClause.SearchCondition) :
                new List<WBooleanExpression>();

            // A list of predicates and their accessed table references 
            // Predicates in this list are those that cannot be assigned to the match graph
            List<Tuple<WBooleanExpression, HashSet<string>>>
                predicatesAccessedTableReferences = new List<Tuple<WBooleanExpression, HashSet<string>>>();
            AccessedTableColumnVisitor columnVisitor = new AccessedTableColumnVisitor();

            foreach (WBooleanExpression predicate in conjunctivePredicates)
            {
                bool isOnlyTargetTableReferenced;
                Dictionary<string, HashSet<string>> tableColumnReferences = columnVisitor.Invoke(predicate,
                    vertexAndEdgeAliases, out isOnlyTargetTableReferenced);

                if (!isOnlyTargetTableReferenced || !TryAttachPredicate(graphPattern, predicate, tableColumnReferences))
                {
                    // Attach cross-table predicate's referencing properties for later runtime evaluation
                    AttachProperties(graphPattern, tableColumnReferences);
                    predicatesAccessedTableReferences.Add(
                        new Tuple<WBooleanExpression, HashSet<string>>(predicate,
                            new HashSet<string>(tableColumnReferences.Keys)));
                }
            }

            foreach (WSelectElement selectElement in SelectElements)
            {
                bool isOnlyTargetTableReferenced;
                Dictionary<string, HashSet<string>> tableColumnReferences = columnVisitor.Invoke(selectElement,
                    vertexAndEdgeAliases, out isOnlyTargetTableReferenced);
                // Attach referencing properties for later runtime evaluation or selection
                AttachProperties(graphPattern, tableColumnReferences);
            }

            foreach (var nonVertexTableReference in nonVertexTableReferences)
            {
                bool isOnlyTargetTableReferenced;
                Dictionary<string, HashSet<string>> tableColumnReferences = columnVisitor.Invoke(
                    nonVertexTableReference, vertexAndEdgeAliases, out isOnlyTargetTableReferenced);
                // Attach referencing properties for later runtime evaluation
                AttachProperties(graphPattern, tableColumnReferences);
            }

            ConstructTraversalChain2(graphPattern);

            ConstructJsonQueries(graphPattern);

            return ConstructOperator2(dbConnection, graphPattern, context, nonVertexTableReferences,
                predicatesAccessedTableReferences);
        }

        /// <summary>
        /// If a predicate is a cross-table one, return false
        /// Otherwise, attach the predicate to the corresponding node or edge and return true
        /// </summary>
        /// <param name="graphPattern"></param>
        /// <param name="predicate"></param>
        /// <param name="tableColumnReferences"></param>
        private bool TryAttachPredicate(MatchGraph graphPattern, WBooleanExpression predicate, Dictionary<string, HashSet<string>> tableColumnReferences)
        {
            // Attach fail if it is a cross-table predicate
            if (tableColumnReferences.Count > 1)
                return false;
            MatchEdge edge;
            MatchNode node;
            bool attachFlag = false;

            foreach (var tableColumnReference in tableColumnReferences)
            {
                var tableName = tableColumnReference.Key;
                var properties = tableColumnReference.Value;

                if (graphPattern.TryGetEdge(tableName, out edge))
                {
                    if (edge.Predicates == null)
                        edge.Predicates = new List<WBooleanExpression>();
                    edge.Predicates.Add(predicate);
                    // Attach edge's propeties for later runtime evaluation
                    AttachProperties(graphPattern, new Dictionary<string, HashSet<string>> {{tableName, properties}});
                    attachFlag = true;
                }
                else if (graphPattern.TryGetNode(tableName, out node))
                {
                    if (node.Predicates == null)
                        node.Predicates = new List<WBooleanExpression>();
                    node.Predicates.Add(predicate);
                    attachFlag = true;
                }
            }

            return attachFlag;
        }

        /// <summary>
        /// Attach referencing properties to corresponding nodes and edges
        /// for later runtime evaluation or selection.
        /// </summary>
        /// <param name="graphPattern"></param>
        /// <param name="tableColumnReferences"></param>
        private void AttachProperties(MatchGraph graphPattern, Dictionary<string, HashSet<string>> tableColumnReferences)
        {
            MatchEdge edge;
            MatchNode node;

            foreach (var tableColumnReference in tableColumnReferences)
            {
                var tableName = tableColumnReference.Key;
                var properties = tableColumnReference.Value;

                if (graphPattern.TryGetEdge(tableName, out edge))
                {
                    if (edge.Properties == null)
                        edge.Properties = new List<string>();
                    foreach (var property in properties)
                    {
                        if (!edge.Properties.Contains(property))
                            edge.Properties.Add(property);
                    }
                }
                else if (graphPattern.TryGetNode(tableName, out node))
                {
                    if (node.Properties == null)
                        node.Properties = new List<string>();
                    foreach (var property in properties)
                    {
                        if (!node.Properties.Contains(property))
                            node.Properties.Add(property);
                    }
                }
            }
        }

        internal static void ConstructJsonQueries(MatchGraph graphPattern)
        {
            foreach (var subGraph in graphPattern.ConnectedSubGraphs)
            {
                var processedNodes = new HashSet<MatchNode>();
                var traversalChain =
                    new Stack<Tuple<MatchNode, MatchEdge, MatchNode, List<MatchEdge>, List<MatchEdge>>>(
                        subGraph.TraversalChain2);
                while (traversalChain.Count != 0)
                {
                    var currentChain = traversalChain.Pop();
                    var sourceNode = currentChain.Item1;
                    var traversalEdge = currentChain.Item2;
                    if (!processedNodes.Contains(sourceNode))
                    {
                        ConstructJsonQueryOnNode(sourceNode);
                        processedNodes.Add(sourceNode);
                    }
                    if (traversalEdge != null)
                    {
                        var sinkNode = traversalEdge.SinkNode;
                        ConstructJsonQueryOnNode(sinkNode, currentChain.Item4);
                        processedNodes.Add(sinkNode);
                    }
                }
            }
        }

        /// <summary>
        /// This function works like the FillMetaField function in the AdjacencyListDecoder
        /// </summary>
        /// <param name="edge"></param>
        /// <returns></returns>
        internal static string ConstructMetaFieldSelectClauseOfEdge(MatchEdge edge)
        {
            var metaFieldSelectStringBuilder = new StringBuilder();
            var isStartVertexTheOriginVertex = edge.IsReversed;
            var isReversedAdjList = IsTraversalThroughPhysicalReverseEdge(edge);
            var nodeId = edge.SourceNode.NodeAlias + ".id";
            var edgeSink = edge.EdgeAlias + "._sink";
            var edgeId = edge.EdgeAlias + "._ID";
            var edgeReverseId = edge.EdgeAlias + "._reverse_ID";


            var sourceValue = isReversedAdjList ? edgeSink : nodeId;
            var sinkValue = isReversedAdjList ? nodeId : edgeSink;
            var otherValue = isStartVertexTheOriginVertex ? edgeSink : nodeId;
            var edgeIdValue = isReversedAdjList ? edgeReverseId : edgeId;

            metaFieldSelectStringBuilder.Append(", ").Append(string.Format("{0} AS {1}", sourceValue, edge.EdgeAlias + "_source"));
            metaFieldSelectStringBuilder.Append(", ").Append(string.Format("{0} AS {1}", sinkValue, edge.EdgeAlias + "_sink"));
            metaFieldSelectStringBuilder.Append(", ").Append(string.Format("{0} AS {1}", otherValue, edge.EdgeAlias + "_other"));
            metaFieldSelectStringBuilder.Append(", ").Append(string.Format("{0} AS {1}", edgeIdValue, edge.EdgeAlias + "_ID"));

            return metaFieldSelectStringBuilder.ToString();
        }

        internal static void ConstructJsonQueryOnNode(MatchNode node, List<MatchEdge> backwardMatchingEdges = null)
        {
            const int ReservedEdgeMetaFieldCount = 4;
            var nodeAlias = node.NodeAlias;
            var selectStrBuilder = new StringBuilder();
            var joinStrBuilder = new StringBuilder();
            var properties = new List<string>();
            WBooleanExpression searchCondition = null;

            for (var i = 0; i < node.Properties.Count; i++)
            {
                var selectName = nodeAlias;
                if (!"*".Equals(node.Properties[i], StringComparison.OrdinalIgnoreCase))
                {
                    selectName += "." + node.Properties[i];
                    properties.Add(node.Properties[i]);
                }
                else
                {
                    properties.Add(nodeAlias);
                }

                if (i > 0)
                    selectStrBuilder.Append(", ");
                selectStrBuilder.Append(selectName);
            }
                

            if (backwardMatchingEdges == null)
                backwardMatchingEdges = new List<MatchEdge>();

            foreach (var predicate in node.Predicates)
                searchCondition = WBooleanBinaryExpression.Conjunction(searchCondition, predicate);

            foreach (var edge in backwardMatchingEdges)
            {
                joinStrBuilder.Append(" Join ")
                    .Append(edge.EdgeAlias)
                    .Append(" in ")
                    .Append(node.NodeAlias)
                    .Append(IsTraversalThroughPhysicalReverseEdge(edge) ? "._reverse_edge" : "_edge");

                selectStrBuilder.Append(ConstructMetaFieldSelectClauseOfEdge(edge));
                properties.Add(edge.EdgeAlias + "_source");
                properties.Add(edge.EdgeAlias + "_sink");
                properties.Add(edge.EdgeAlias + "_other");
                properties.Add(edge.EdgeAlias + "_ID");

                for (var i = ReservedEdgeMetaFieldCount; i < edge.Properties.Count; i++)
                {
                    var property = edge.Properties[i];
                    var selectName = edge.EdgeAlias;
                    var selectAlias = edge.EdgeAlias;
                    if ("*".Equals(property, StringComparison.OrdinalIgnoreCase))
                    {
                        selectAlias += "_" + selectName;
                    }
                    else
                    {
                        selectName += "." + property;
                        selectAlias += "_" + property;
                    }
                        
                    selectStrBuilder.Append(", ").Append(string.Format("{0} AS {1}", selectName, selectAlias));
                    properties.Add(selectAlias);
                }   

                foreach (var predicate in edge.Predicates)
                    searchCondition = WBooleanBinaryExpression.Conjunction(searchCondition, predicate);
            }

            var booleanWValueExpressionVisitor = new BooleanWValueExpressionVisitor();
            booleanWValueExpressionVisitor.Invoke(searchCondition);

            var jsonQuery = new JsonQuery
            {
                Alias = nodeAlias,
                JoinClause = joinStrBuilder.ToString(),
                SelectClause = selectStrBuilder.ToString(),
                WhereSearchCondition = searchCondition != null ? searchCondition.ToString() : null,
                Properties = properties,
                // TODO: ProjectedColumns
                //ProjectedColumns = 
            };
            node.AttachedJsonQuery = jsonQuery;
        }

        private MatchGraph ConstructGraph()
        {
            Dictionary<string, List<string>> EdgeColumnToAliasesDict = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, MatchPath> pathDictionary = new Dictionary<string, MatchPath>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, MatchEdge> ReversedEdgeDict = new Dictionary<string, MatchEdge>();

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
                                SrcNode.ReverseCheckList = new Dictionary<int, int>();
                                SrcNode.HeaderLength = 0;
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
                                    IsReversed = false,
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
                                    AttributeValueDict = CurrentEdgeColumnRef.AttributeValueDict,
                                    IsReversed = false,
                                };
                                pathDictionary[EdgeAlias] = matchPath;
                                EdgeFromSrcNode = matchPath;
                            }

                            if (EdgeToSrcNode != null)
                            {
                                EdgeToSrcNode.SinkNode = SrcNode;
                                if (!(EdgeToSrcNode is MatchPath))
                                {
                                    //Add ReverseEdge
                                    MatchEdge reverseEdge = new MatchEdge
                                    {
                                        SourceNode = EdgeToSrcNode.SinkNode,
                                        SinkNode = EdgeToSrcNode.SourceNode,
                                        EdgeColumn = EdgeToSrcNode.EdgeColumn,
                                        EdgeAlias = EdgeToSrcNode.EdgeAlias,
                                        Predicates = EdgeToSrcNode.Predicates,
                                        BindNodeTableObjName =
                                            new WSchemaObjectName(
                                            ),
                                        IsReversed = true,
                                    };
                                    SrcNode.ReverseNeighbors.Add(reverseEdge);
                                    ReversedEdgeDict[EdgeToSrcNode.EdgeAlias] = reverseEdge;
                                }
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
                            DestNode.ReverseCheckList = new Dictionary<int, int>();
                            DestNode.HeaderLength = 0;
                        }
                        if (EdgeToSrcNode != null)
                        {
                            EdgeToSrcNode.SinkNode = DestNode;
                            if (!(EdgeToSrcNode is MatchPath))
                            {
                                //Add ReverseEdge
                                MatchEdge reverseEdge = new MatchEdge
                                {
                                    SourceNode = EdgeToSrcNode.SinkNode,
                                    SinkNode = EdgeToSrcNode.SourceNode,
                                    EdgeColumn = EdgeToSrcNode.EdgeColumn,
                                    EdgeAlias = EdgeToSrcNode.EdgeAlias,
                                    Predicates = EdgeToSrcNode.Predicates,
                                    BindNodeTableObjName =
                                        new WSchemaObjectName(
                                        ),
                                    IsReversed = true,
                                };
                                DestNode.ReverseNeighbors.Add(reverseEdge);
                                ReversedEdgeDict[EdgeToSrcNode.EdgeAlias] = reverseEdge;
                            }
                            
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
                ReversedEdgeDict = ReversedEdgeDict,
            };

            return Graph;
        }

        private MatchGraph ConstructGraph2(
            Dictionary<string, TableGraphType> outerContextTableReferences,
            out List<WTableReferenceWithAlias> nonVertexTableReferences)
        {
            nonVertexTableReferences = new List<WTableReferenceWithAlias>();

            Dictionary<string, MatchPath> pathDictionary = new Dictionary<string, MatchPath>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, MatchEdge> reversedEdgeDict = new Dictionary<string, MatchEdge>();

            UnionFind unionFind = new UnionFind();
            Dictionary<string, MatchNode> vertexTableCollection = new Dictionary<string, MatchNode>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, WNamedTableReference> vertexTableReferencesDict = new Dictionary<string, WNamedTableReference>();
            List<ConnectedComponent> connectedSubGraphs = new List<ConnectedComponent>();
            Dictionary<string, ConnectedComponent> subGraphMap = new Dictionary<string, ConnectedComponent>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, string> parent = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            unionFind.Parent = parent;

            // Goes through the FROM clause and extracts vertex table references and non-vertex table references
            if (FromClause != null)
            {
                List<WNamedTableReference> vertexTableList = new List<WNamedTableReference>();
                TableClassifyVisitor tcVisitor = new TableClassifyVisitor();
                tcVisitor.Invoke(FromClause, vertexTableList, nonVertexTableReferences);

                foreach (WNamedTableReference vertexTableRef in vertexTableList)
                {
                    vertexTableCollection.GetOrCreate(vertexTableRef.Alias.Value);
                    vertexTableReferencesDict[vertexTableRef.Alias.Value] = vertexTableRef;
                    if (!parent.ContainsKey(vertexTableRef.Alias.Value))
                        parent[vertexTableRef.Alias.Value] = vertexTableRef.Alias.Value;
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
                            MatchNode SrcNode = vertexTableCollection.GetOrCreate(CurrentNodeExposedName);
                            WNamedTableReference SrcNodeTableReference =
                                vertexTableReferencesDict[CurrentNodeExposedName];

                            // Check whether the vertex is defined in outer context
                            if (!vertexTableCollection.TryGetValue(CurrentNodeExposedName, out SrcNode))
                            {
                                if (!outerContextTableReferences.ContainsKey(CurrentNodeExposedName))
                                    throw new GraphViewException("Table " + CurrentNodeExposedName + " doesn't exist in the context.");
                                SrcNode = new MatchNode { IsFromOuterContext = true };
                                vertexTableCollection.Add(CurrentNodeExposedName, SrcNode);
                            }
                            if (SrcNode.NodeAlias == null)
                            {
                                SrcNode.NodeAlias = CurrentNodeExposedName;
                                SrcNode.Neighbors = new List<MatchEdge>();
                                SrcNode.ReverseNeighbors = new List<MatchEdge>();
                                SrcNode.DanglingEdges = new List<MatchEdge>();
                                SrcNode.External = false;
                                SrcNode.Predicates = new List<WBooleanExpression>();
                                SrcNode.ReverseCheckList = new Dictionary<int, int>();
                                SrcNode.HeaderLength = 0;
                                SrcNode.Properties = new List<string> {"id", "_edge", "_reverse_edge"};
                                SrcNode.Low = SrcNodeTableReference.Low;
                                SrcNode.High = SrcNodeTableReference.High;
                            }

                            // Consturct the edge of a path in MatchClause.Paths
                            string EdgeAlias = CurrentEdgeColumnRef.Alias;
                            //if (EdgeAlias == null)
                            //{
                            //    var CurrentEdgeName = CurrentEdgeColumnRef.MultiPartIdentifier.Identifiers.Last().Value;
                            //    EdgeAlias = string.Format("{0}_{1}_{2}", CurrentNodeExposedName, CurrentEdgeName,
                            //        nextNodeExposedName);
                            //}

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
                                    IsReversed = false,
                                    EdgeType = CurrentEdgeColumnRef.EdgeType,
                                    Properties = new List<string> { "_source", "_sink", "_other", "_ID" },
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
                                    AttributeValueDict = CurrentEdgeColumnRef.AttributeValueDict,
                                    IsReversed = false,
                                    EdgeType = CurrentEdgeColumnRef.EdgeType,
                                    Properties = new List<string> { "_source", "_sink", "_other", "_ID" },
                            };
                                pathDictionary[EdgeAlias] = matchPath;
                                EdgeFromSrcNode = matchPath;
                            }
                            // Check whether the edge is defined in the outer context
                            //TableGraphType tableGraphType;
                            //if (outerContextTableReferences.TryGetValue(EdgeAlias, out tableGraphType && 
                            //    tableGraphType == TableGraphType.Edge)
                            if (outerContextTableReferences.ContainsKey(EdgeAlias))
                                EdgeFromSrcNode.IsFromOuterContext = true;

                            if (EdgeToSrcNode != null)
                            {
                                EdgeToSrcNode.SinkNode = SrcNode;
                                if (!(EdgeToSrcNode is MatchPath))
                                {
                                    //Add ReverseEdge
                                    MatchEdge reverseEdge = new MatchEdge
                                    {
                                        SourceNode = EdgeToSrcNode.SinkNode,
                                        SinkNode = EdgeToSrcNode.SourceNode,
                                        EdgeColumn = EdgeToSrcNode.EdgeColumn,
                                        EdgeAlias = EdgeToSrcNode.EdgeAlias,
                                        Predicates = EdgeToSrcNode.Predicates,
                                        BindNodeTableObjName =
                                            new WSchemaObjectName(
                                            ),
                                        IsReversed = true,
                                        EdgeType = EdgeToSrcNode.EdgeType,
                                        Properties = new List<string> { "_source", "_sink", "_other", "_ID" },
                                };
                                    SrcNode.ReverseNeighbors.Add(reverseEdge);
                                    reversedEdgeDict[EdgeToSrcNode.EdgeAlias] = reverseEdge;
                                }
                            }

                            EdgeToSrcNode = EdgeFromSrcNode;

                            if (!parent.ContainsKey(CurrentNodeExposedName))
                                parent[CurrentNodeExposedName] = CurrentNodeExposedName;

                            var nextNodeExposedName = nextNodeTableRef != null ? nextNodeTableRef.BaseIdentifier.Value : null;
                            if (nextNodeExposedName != null)
                            {
                                if (!parent.ContainsKey(nextNodeExposedName))
                                    parent[nextNodeExposedName] = nextNodeExposedName;

                                unionFind.Union(CurrentNodeExposedName, nextNodeExposedName);

                                SrcNode.Neighbors.Add(EdgeFromSrcNode);

                            }
                            // Dangling edge without SinkNode
                            else
                            {
                                SrcNode.DanglingEdges.Add(EdgeFromSrcNode);
                            }
                        }
                        if (path.Tail == null) continue;
                        // Consturct destination node of a path in MatchClause.Paths
                        var tailExposedName = path.Tail.BaseIdentifier.Value;
                        MatchNode DestNode;
                        WNamedTableReference DestNodeTableReference =
                                vertexTableReferencesDict[tailExposedName];
                        // Check whether the vertex is defined in outer context
                        if (!vertexTableCollection.TryGetValue(tailExposedName, out DestNode))
                        {
                            if (!outerContextTableReferences.ContainsKey(tailExposedName))
                                throw new GraphViewException("Table " + tailExposedName + " doesn't exist in the context.");
                            DestNode = new MatchNode { IsFromOuterContext = true };
                            vertexTableCollection.Add(tailExposedName, DestNode);
                        }
                        if (DestNode.NodeAlias == null)
                        {
                            DestNode.NodeAlias = tailExposedName;
                            DestNode.Neighbors = new List<MatchEdge>();
                            DestNode.ReverseNeighbors = new List<MatchEdge>();
                            DestNode.DanglingEdges = new List<MatchEdge>();
                            DestNode.Predicates = new List<WBooleanExpression>();
                            DestNode.ReverseCheckList = new Dictionary<int, int>();
                            DestNode.HeaderLength = 0;
                            DestNode.Properties = new List<string> { "id", "_edge", "_reverse_edge" };
                            DestNode.Low = DestNodeTableReference.Low;
                            DestNode.High = DestNodeTableReference.High;
                        }
                        if (EdgeToSrcNode != null)
                        {
                            EdgeToSrcNode.SinkNode = DestNode;
                            if (!(EdgeToSrcNode is MatchPath))
                            {
                                //Add ReverseEdge
                                MatchEdge reverseEdge = new MatchEdge
                                {
                                    SourceNode = EdgeToSrcNode.SinkNode,
                                    SinkNode = EdgeToSrcNode.SourceNode,
                                    EdgeColumn = EdgeToSrcNode.EdgeColumn,
                                    EdgeAlias = EdgeToSrcNode.EdgeAlias,
                                    Predicates = EdgeToSrcNode.Predicates,
                                    BindNodeTableObjName =
                                        new WSchemaObjectName(
                                        ),
                                    IsReversed = true,
                                    EdgeType = EdgeToSrcNode.EdgeType,
                                    Properties = new List<string> { "_source", "_sink", "_other", "_ID" },
                            };
                                DestNode.ReverseNeighbors.Add(reverseEdge);
                                reversedEdgeDict[EdgeToSrcNode.EdgeAlias] = reverseEdge;
                            }

                        }
                    }

                }
            }
            // Use union find algorithmn to define which subgraph does a node belong to and put it into where it belongs to.
            foreach (var node in vertexTableCollection)
            {
                string root;

                root = unionFind.Find(node.Key);  // put them into the same graph

                var patternNode = node.Value;

                if (patternNode.NodeAlias == null)
                {
                    WNamedTableReference patternNodeTableReference =
                        vertexTableReferencesDict[node.Key];
                    patternNode.NodeAlias = node.Key;
                    patternNode.Neighbors = new List<MatchEdge>();
                    patternNode.ReverseNeighbors = new List<MatchEdge>();
                    patternNode.DanglingEdges = new List<MatchEdge>();
                    patternNode.External = false;
                    patternNode.Predicates = new List<WBooleanExpression>();
                    patternNode.Properties = new List<string> { "id", "_edge", "_reverse_edge" };
                    patternNode.Low = patternNodeTableReference.Low;
                    patternNode.High = patternNodeTableReference.High;
                }

                if (!subGraphMap.ContainsKey(root))
                {
                    var subGraph = new ConnectedComponent();
                    subGraph.Nodes[node.Key] = node.Value;
                    foreach (var edge in node.Value.Neighbors)
                    {
                        subGraph.Edges[edge.EdgeAlias] = edge;
                    }
                    foreach (var edge in node.Value.DanglingEdges)
                    {
                        subGraph.Edges[edge.EdgeAlias] = edge;
                    }
                    subGraphMap[root] = subGraph;
                    connectedSubGraphs.Add(subGraph);
                    subGraph.IsTailNode[node.Value] = false;
                }
                else
                {
                    var subGraph = subGraphMap[root];
                    subGraph.Nodes[node.Key] = node.Value;
                    foreach (var edge in node.Value.Neighbors)
                    {
                        subGraph.Edges[edge.EdgeAlias] = edge;
                    }
                    foreach (var edge in node.Value.DanglingEdges)
                    {
                        subGraph.Edges[edge.EdgeAlias] = edge;
                    }
                    subGraph.IsTailNode[node.Value] = false;
                }
            }

            // Combine all subgraphs into a complete match graph and return it
            MatchGraph graphPattern = new MatchGraph
            {
                ConnectedSubGraphs = connectedSubGraphs,
                ReversedEdgeDict = reversedEdgeDict,
            };

            return graphPattern;
        }

        private List<BooleanFunction> AttachScriptSegment(MatchGraph graph, List<string> header, Dictionary<string, string> columnToAliasDict, 
            Dictionary<string, DColumnReferenceExpression> headerToColumnRefDict)
        {
            // Call attach predicate visitor to attach predicates on nodes.
            AttachWhereClauseVisitor AttachPredicateVistor = new AttachWhereClauseVisitor();
            QueryCompilationContext Context = new QueryCompilationContext();
            // GraphMetaData GraphMeta = new GraphMetaData();
            // Dictionary<string, string> ColumnTableMapping = Context.GetColumnToAliasMapping(GraphMeta.ColumnsOfNodeTables);
            AttachPredicateVistor.Invoke(WhereClause, graph);
            List<BooleanFunction> BooleanList = new List<BooleanFunction>();

            // If some predictaes are failed to be assigned to one node, turn them into boolean functions
            foreach (var predicate in AttachPredicateVistor.FailedToAssign)
            {
                // Analyse what kind of predicates they are, and generate corresponding boolean functions.
                if (predicate is WBooleanComparisonExpression)
                {
                    var FirstColumnExpr = ((predicate as WBooleanComparisonExpression).FirstExpr) as WColumnReferenceExpression;
                    var SecondColumnExpr = ((predicate as WBooleanComparisonExpression).SecondExpr) as WColumnReferenceExpression;
                    if (FirstColumnExpr == null || SecondColumnExpr == null)
                        throw new GraphViewException("Cross documents predicate: " + predicate.ToString() + " not supported yet.");
                    string FirstExpr = FirstColumnExpr.ToString();
                    string SecondExpr = SecondColumnExpr.ToString();

                    var insertIdx = header.Count > 0 ? header.Count-1 : 0;
                    if (header.IndexOf(FirstExpr) == -1)
                    {
                        header.Insert(insertIdx++, FirstExpr);
                        columnToAliasDict.Add(FirstExpr, FirstExpr);
                        headerToColumnRefDict[FirstExpr] = new DColumnReferenceExpression
                        {
                            ColumnName = FirstExpr,
                            MultiPartIdentifier = new DMultiPartIdentifier(FirstColumnExpr.MultiPartIdentifier)
                        };
                    }
                    if (header.IndexOf(SecondExpr) == -1)
                    {
                        header.Insert(insertIdx, SecondExpr);
                        columnToAliasDict.Add(SecondExpr, SecondExpr);
                        headerToColumnRefDict[SecondExpr] = new DColumnReferenceExpression
                        {
                            ColumnName = SecondExpr,
                            MultiPartIdentifier = new DMultiPartIdentifier(SecondColumnExpr.MultiPartIdentifier)
                        };
                    }
                    var lhs = columnToAliasDict[FirstExpr];
                    var rhs = columnToAliasDict[SecondExpr];
                    FieldComparisonFunction NewCBF = null;
                    if ((predicate as WBooleanComparisonExpression).ComparisonType == BooleanComparisonType.Equals)
                        NewCBF = new FieldComparisonFunction(lhs, rhs,
                            ComparisonBooleanFunction.ComparisonType.eq);
                    if ((predicate as WBooleanComparisonExpression).ComparisonType ==
                        BooleanComparisonType.NotEqualToExclamation)
                        NewCBF = new FieldComparisonFunction(lhs, rhs,
                            ComparisonBooleanFunction.ComparisonType.neq);
                    if ((predicate as WBooleanComparisonExpression).ComparisonType ==
    BooleanComparisonType.LessThan)
                        NewCBF = new FieldComparisonFunction(lhs, rhs,
                            ComparisonBooleanFunction.ComparisonType.lt);
                    if ((predicate as WBooleanComparisonExpression).ComparisonType ==
    BooleanComparisonType.GreaterThan)
                        NewCBF = new FieldComparisonFunction(lhs, rhs,
                            ComparisonBooleanFunction.ComparisonType.gt);
                    if ((predicate as WBooleanComparisonExpression).ComparisonType ==
    BooleanComparisonType.GreaterThanOrEqualTo)
                        NewCBF = new FieldComparisonFunction(lhs, rhs,
                            ComparisonBooleanFunction.ComparisonType.gte);
                    if ((predicate as WBooleanComparisonExpression).ComparisonType ==
    BooleanComparisonType.LessThanOrEqualTo)
                        NewCBF = new FieldComparisonFunction(lhs, rhs,
                            ComparisonBooleanFunction.ComparisonType.lte);
                    BooleanList.Add(NewCBF);
                }
            }
            // Calculate the start index of select elements
            int StartOfResult =
                graph.ConnectedSubGraphs.Sum(
                    subgraph => subgraph.Nodes.Select(n => n.Value.HeaderLength).Aggregate(0, (cur, next) => cur + next));
            foreach (var subgraph in graph.ConnectedSubGraphs)
            {
                var SortedNodeList = new Stack<Tuple<MatchNode, MatchEdge>>(subgraph.TraversalChain);
                var NodeToMatEdgesDict = subgraph.NodeToMaterializedEdgesDict;
                // Marking which node has been processed for later reverse checking.  
                List<string> ProcessedNodeList = new List<string>();
                // Build query segment on both source node and dest node, 
                while (SortedNodeList.Count != 0)
                {
                    MatchNode CurrentProcessingNode = null;
                    var TargetNode = SortedNodeList.Pop();
                    if (!ProcessedNodeList.Contains(TargetNode.Item1.NodeAlias))
                    {
                        CurrentProcessingNode = TargetNode.Item1;
                        BuildQuerySegementOnNode(ProcessedNodeList, CurrentProcessingNode, header, NodeToMatEdgesDict, columnToAliasDict, headerToColumnRefDict, StartOfResult);
                        ProcessedNodeList.Add(CurrentProcessingNode.NodeAlias);
                    }
                    if (TargetNode.Item2 != null)
                    {
                        CurrentProcessingNode = TargetNode.Item2.SinkNode;
                        BuildQuerySegementOnNode(ProcessedNodeList, CurrentProcessingNode, header, NodeToMatEdgesDict, columnToAliasDict, headerToColumnRefDict, StartOfResult, TargetNode.Item2 is MatchPath);
                        ProcessedNodeList.Add(CurrentProcessingNode.NodeAlias);
                    }
                }
            }
            return BooleanList;
        }

        private List<string> ConstructHeader(MatchGraph graph, out Dictionary<string, string> columnToAliasDict, out Dictionary<string, DColumnReferenceExpression> headerToColumnRefDict)
        {
            List<string> header = new List<string>();
            HashSet<string> aliasSet = new HashSet<string>();
            columnToAliasDict = new Dictionary<string, string>();
            headerToColumnRefDict = new Dictionary<string, DColumnReferenceExpression>();
            // Construct the first part of the head which is defined as 
            // |    Node's Alias     {[|  Node's Adjacent list |       _SINK      ][|...n]}
            // |  "node.NodeAlias"   {[|   "edgeAlias_ADJ"     | "edgeAlias_SINK" ][|...n]}
            foreach (var subgraph in graph.ConnectedSubGraphs)
            {
                HashSet<MatchNode> ProcessedNode = new HashSet<MatchNode>();

                var SortedNodes = new Stack<Tuple<MatchNode, MatchEdge>>(subgraph.TraversalChain);
                var nodeToMatEdgesDict = subgraph.NodeToMaterializedEdgesDict;
                while (SortedNodes.Count != 0)
                {
                    var processingNodePair = SortedNodes.Pop();
                    var srcNode = processingNodePair.Item1;
                    var sinkNode = processingNodePair.Item2?.SinkNode;

                    if (!ProcessedNode.Contains(srcNode))
                    {
                        MatchNode node = srcNode;
                        header.Add(node.NodeAlias);

                        if (nodeToMatEdgesDict != null)
                        {
                            foreach (var t in nodeToMatEdgesDict[srcNode.NodeAlias])
                            {
                                var edge = t.Item1;
                                header.Add(edge.EdgeAlias + "_ADJ");
                                header.Add(edge.EdgeAlias + "_SINK");
                            }
                        }
                        // The meta header length of the node, consisting of node's id and node's outgoing edges
                        // Every edge will have a field as adjList and a field as single sink id
                        // | node id | edge1 | edge1.sink | edge2 | edge2.sink | ...
                        srcNode.HeaderLength = nodeToMatEdgesDict?[srcNode.NodeAlias].Count * 2 + 1 ?? 1;
                        ProcessedNode.Add(node);
                        aliasSet.Add(node.NodeAlias);
                    }
                    if (sinkNode != null && !ProcessedNode.Contains(sinkNode))
                    {
                        MatchNode node = sinkNode;
                        header.Add(node.NodeAlias);

                        if (nodeToMatEdgesDict != null)
                        {
                            foreach (var t in nodeToMatEdgesDict[sinkNode.NodeAlias])
                            {
                                var edge = t.Item1;
                                header.Add(edge.EdgeAlias + "_ADJ");
                                header.Add(edge.EdgeAlias + "_SINK");
                            }
                        }

                        sinkNode.HeaderLength = nodeToMatEdgesDict?[sinkNode.NodeAlias].Count * 2 + 1 ?? 1;
                        ProcessedNode.Add(node);
                        aliasSet.Add(node.NodeAlias);
                    }
                }

                foreach (var edge in subgraph.Edges)
                    aliasSet.Add(edge.Key);
            }
            // Construct the second part of the head which is defined as 
            // ...|Select element|Select element|Select element|...
            // ...|  "ELEMENT1"  |  "ELEMENT2"  |  "ELEMENT3"  |...
            for (var i = 0; i < SelectElements.Count; i++)
            {
                var element = SelectElements[i];
                if (element is WSelectStarExpression)
                {
                    if (FromClause.TableReferences != null && FromClause.TableReferences.Count > 1)
                        throw new GraphViewException("'SELECT *' is only valid with a single input set.");
                    var tr = FromClause.TableReferences[0] as WNamedTableReference;
                    var expr = tr.Alias.Value;
                    var alias = expr + ".doc";
                    header.Add(expr);
                    columnToAliasDict.Add(expr, alias);
                    headerToColumnRefDict[expr] = new DColumnReferenceExpression
                    {
                        ColumnName = alias,
                        MultiPartIdentifier = new DMultiPartIdentifier(expr),
                    };
                    var iden = new Identifier { Value = expr };
                    SelectElements[i] = new WSelectScalarExpression
                    {
                        ColumnName = alias,
                        SelectExpr = new WColumnReferenceExpression { MultiPartIdentifier = new WMultiPartIdentifier(iden) }
                    };
                }
                else if (element is WSelectScalarExpression)
                {
                    var scalarExpr = element as WSelectScalarExpression;
                    if (scalarExpr.SelectExpr is WValueExpression) continue;

                    var column = scalarExpr.SelectExpr as WColumnReferenceExpression;

                    var expr = column.MultiPartIdentifier.ToString();
                    var alias = scalarExpr.ColumnName ?? expr;
                    header.Add(expr);
                    // Add the mapping between the expr and its alias
                    columnToAliasDict.Add(expr, alias);
                    // Add the mapping between the expr and its DColumnReferenceExpr for later normalization
                    headerToColumnRefDict[expr] = new DColumnReferenceExpression
                    {
                        ColumnName = alias,
                        MultiPartIdentifier = new DMultiPartIdentifier(column.MultiPartIdentifier)
                    };
                }
            }

            if (OrderByClause != null && OrderByClause.OrderByElements != null)
            {
                foreach (var element in OrderByClause.OrderByElements)
                {
                    var expr = element.ScalarExpr.ToString();
                    // If true, the expr might need to be added to the header or could not be resolved
                    if (!columnToAliasDict.ContainsKey(expr) && !columnToAliasDict.ContainsValue(expr))
                    {
                        int cutPoint = expr.Length;
                        if (expr.IndexOf('.') != -1) cutPoint = expr.IndexOf('.');
                        var bindObject = expr.Substring(0, cutPoint);
                        if (aliasSet.Contains(bindObject))
                        {
                            header.Add(expr);
                            columnToAliasDict.Add(expr, expr);
                            headerToColumnRefDict[expr] = new DColumnReferenceExpression
                            {
                                ColumnName = expr,
                                MultiPartIdentifier = new DMultiPartIdentifier((element.ScalarExpr as WColumnReferenceExpression).MultiPartIdentifier)
                            };
                        }
                        else
                            throw new GraphViewException(string.Format("The identifier \"{0}\" could not be bound", expr));
                    }
                }
            }
            // Construct a slot for path 
            // ...|   PATH  |...
            // ...|xxx-->yyy|...
            header.Add("PATH");
            return header;
        }

        private void ConstructTraversalChain(MatchGraph graph)
        {
            var graphOptimizer = new DocDbGraphOptimizer(graph);
            foreach (var subGraph in graph.ConnectedSubGraphs)
            {
                // <node, node's edges which need to be pulled from the server>
                Dictionary<string, List<Tuple<MatchEdge, MaterializedEdgeType>>> nodeToMatEdgesDict;
                subGraph.TraversalChain = graphOptimizer.GetOptimizedTraversalOrder(subGraph, out nodeToMatEdgesDict);
                subGraph.NodeToMaterializedEdgesDict = nodeToMatEdgesDict;
            }
        }

        private void ConstructTraversalChain2(MatchGraph graphPattern)
        {
            var graphOptimizer = new DocDbGraphOptimizer(graphPattern);
            foreach (var subGraph in graphPattern.ConnectedSubGraphs)
            {
                subGraph.TraversalChain2 = graphOptimizer.GetOptimizedTraversalOrder2(subGraph);
            }
        }

        /// <summary>
        /// If using node._reverse_edge, return true.
        /// If using node._edge, return false
        /// </summary>
        /// <param name="edge"></param>
        /// <returns></returns>
        internal static bool IsTraversalThroughPhysicalReverseEdge(MatchEdge edge)
        {
            if ((edge.EdgeType == WEdgeType.OutEdge && edge.IsReversed)
                || edge.EdgeType == WEdgeType.InEdge && !edge.IsReversed)
                return true;
            return false;
        }

        /// <summary>
        /// Return adjacency list's index.
        /// Item1 is _edge's index and Item2 is _reverse_edge's index.
        /// They are set to -1 if not used.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="edge"></param>
        /// <returns></returns>
        private Tuple<int, int> LocateAdjacencyListIndexes(QueryCompilationContext context, MatchEdge edge)
        {
            var edgeIndex =
                context.LocateColumnReference(new WColumnReferenceExpression(edge.SourceNode.NodeAlias, "_edge"));
            var reverseEdgeIndex =
                context.LocateColumnReference(new WColumnReferenceExpression(edge.SourceNode.NodeAlias, "_reverse_edge"));
            if (edge.EdgeType == WEdgeType.BothEdge)
                return new Tuple<int, int>(edgeIndex, reverseEdgeIndex);

            if (IsTraversalThroughPhysicalReverseEdge(edge))
                return new Tuple<int, int>(-1, reverseEdgeIndex);
            else
                return new Tuple<int, int>(edgeIndex, -1);
        }

        /// <summary>
        /// Return the edge's traversal column reference
        /// </summary>
        /// <param name="edge"></param>
        /// <returns></returns>
        private static WColumnReferenceExpression GetAdjacencyListTraversalColumn(MatchEdge edge)
        {
            if (edge.EdgeType == WEdgeType.BothEdge)
                return new WColumnReferenceExpression(edge.EdgeAlias, "_other");
            return new WColumnReferenceExpression(edge.EdgeAlias, IsTraversalThroughPhysicalReverseEdge(edge) ? "_source" : "_sink");
        }

        /// <summary>
        /// Locate the edge's traversal column's index in the context
        /// </summary>
        /// <param name="context"></param>
        /// <param name="edge"></param>
        /// <returns></returns>
        private int LocateAdjacencyListTraversalIndex(QueryCompilationContext context, MatchEdge edge)
        {
            var adjListTraversalColumn = GetAdjacencyListTraversalColumn(edge);
            return context.LocateColumnReference(adjListTraversalColumn);
        }

        /// <summary>
        /// Generate a local context for edge's predicate evaluation
        /// </summary>
        /// <param name="edgeTableAlias"></param>
        /// <param name="projectedFields"></param>
        /// <returns></returns>
        internal QueryCompilationContext GenerateLocalContextForAdjacentListDecoder(string edgeTableAlias, List<string> projectedFields)
        {
            var localContext = new QueryCompilationContext();

            var localIndex = 0;
            foreach (var projectedField in projectedFields)
            {
                var columnReference = new WColumnReferenceExpression(edgeTableAlias, projectedField);
                localContext.RawRecordLayout.Add(columnReference, localIndex++);
            }

            return localContext;
        }

        /// <summary>
        /// Check whether all the tabls referenced by the cross-table predicate have been processed
        /// If so, embed the predicate in a filter operator and append it to the operator list
        /// </summary>
        /// <param name="context"></param>
        /// <param name="connection"></param>
        /// <param name="tableReferences"></param>
        /// <param name="remainingPredicatesAndTheirTableReferences"></param>
        /// <param name="childrenProcessor"></param>
        private void CheckRemainingPredicatesAndAppendFilterOp(QueryCompilationContext context, GraphViewConnection connection,
            HashSet<string> tableReferences,
            List<Tuple<WBooleanExpression, HashSet<string>>> remainingPredicatesAndTheirTableReferences,
            List<GraphViewExecutionOperator> childrenProcessor)
        {
            for (var i = remainingPredicatesAndTheirTableReferences.Count - 1; i >= 0; i--)
            {
                var predicate = remainingPredicatesAndTheirTableReferences[i].Item1;
                var tableRefs = remainingPredicatesAndTheirTableReferences[i].Item2;

                if (tableReferences.IsSupersetOf(tableRefs))
                {
                    childrenProcessor.Add(new FilterOperator(childrenProcessor.Last(),
                        predicate.CompileToFunction(context, connection)));
                    remainingPredicatesAndTheirTableReferences.RemoveAt(i);
                    context.CurrentExecutionOperator = childrenProcessor.Last();
                }
            }
        }

        /// <summary>
        /// Generate AdjacencyListDecoder and update context's layout for edges
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="context"></param>
        /// <param name="operatorChain"></param>
        /// <param name="edges"></param>
        /// <param name="predicatesAccessedTableReferences"></param>
        /// <param name="isForwardingEdges"></param>
        private void CrossApplyEdges(GraphViewConnection connection, QueryCompilationContext context, 
            List<GraphViewExecutionOperator> operatorChain, IList<MatchEdge> edges, 
            List<Tuple<WBooleanExpression, HashSet<string>>> predicatesAccessedTableReferences,
            bool isForwardingEdges = false)
        {
            var tableReferences = context.TableReferences;
            var rawRecordLayout = context.RawRecordLayout;
            foreach (var edge in edges)
            {
                var edgeIndexTuple = LocateAdjacencyListIndexes(context, edge);
                var localEdgeContext = GenerateLocalContextForAdjacentListDecoder(edge.EdgeAlias, edge.Properties);
                var edgePredicates = edge.RetrievePredicatesExpression();
                operatorChain.Add(new AdjacencyListDecoder2(
                    operatorChain.Last(),
                    context.LocateColumnReference(edge.SourceNode.NodeAlias, "id"),
                    edgeIndexTuple.Item1, edgeIndexTuple.Item2, !edge.IsReversed,
                    edgePredicates != null ? edgePredicates.CompileToFunction(localEdgeContext, connection) : null,
                    edge.Properties));

                // Update edge's context info
                tableReferences.Add(edge.EdgeAlias, TableGraphType.Edge);
                UpdateRawRecordLayout(edge.EdgeAlias, edge.Properties, rawRecordLayout);

                if (isForwardingEdges)
                {
                    var sinkNodeIdColumnReference = new WColumnReferenceExpression(edge.SinkNode.NodeAlias, "id");
                    // Add "forwardEdge.traversalColumn = sinkNode.id" filter
                    var edgeSinkColumnReference = GetAdjacencyListTraversalColumn(edge);
                    var edgeJoinPredicate = new WBooleanComparisonExpression
                    {
                        ComparisonType = BooleanComparisonType.Equals,
                        FirstExpr = edgeSinkColumnReference,
                        SecondExpr = sinkNodeIdColumnReference
                    };
                    operatorChain.Add(new FilterOperator(operatorChain.Last(),
                        edgeJoinPredicate.CompileToFunction(context, connection)));
                }

                CheckRemainingPredicatesAndAppendFilterOp(context, connection,
                    new HashSet<string>(tableReferences.Keys),
                    predicatesAccessedTableReferences,
                    operatorChain);
            }
        }

        /// <summary>
        /// Generate matching indexes for backwardMatchingEdges
        /// </summary>
        /// <param name="context"></param>
        /// <param name="backwardMatchingEdges"></param>
        /// <returns></returns>
        private List<Tuple<int, int>> GenerateMatchingIndexesForBackforwadMatchingEdges(QueryCompilationContext context, List<MatchEdge> backwardMatchingEdges)
        {
            if (backwardMatchingEdges.Count == 0) return null;
            var localContext = new QueryCompilationContext();
            var localRawRecordLayout = localContext.RawRecordLayout;
            var node = backwardMatchingEdges[0].SourceNode;
            UpdateRawRecordLayout(node.NodeAlias, node.Properties, localRawRecordLayout);

            var matchingIndexes = new List<Tuple<int, int>>();
            foreach (var backwardMatchingEdge in backwardMatchingEdges)
            {
                // backwardEdges.SinkNode.id = backwardEdges.traversalColumn
                var sourceMatchIndex =
                    context.RawRecordLayout[new WColumnReferenceExpression(backwardMatchingEdge.SinkNode.NodeAlias, "id")];

                UpdateRawRecordLayout(backwardMatchingEdge.EdgeAlias, backwardMatchingEdge.Properties, localRawRecordLayout);

                var edgeTraversalColumn = GetAdjacencyListTraversalColumn(backwardMatchingEdge);

                matchingIndexes.Add(new Tuple<int, int>(sourceMatchIndex, localContext.LocateColumnReference(edgeTraversalColumn)));
            }

            return matchingIndexes;
        } 

        /// <summary>
        /// Update the raw record layout when new properties are added
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="properties"></param>
        /// <param name="rawRecordLayout"></param>
        private void UpdateRawRecordLayout(string tableName, List<string> properties,
            Dictionary<WColumnReferenceExpression, int> rawRecordLayout)
        {
            var nextLayoutIndex = rawRecordLayout.Count;
            foreach (var property in properties)
            {
                var columnReference = new WColumnReferenceExpression(tableName, property);
                if (!rawRecordLayout.ContainsKey(columnReference))
                    rawRecordLayout.Add(columnReference, nextLayoutIndex++);
            }
        }

        private void CheckAndAppendRangeFilter(QueryCompilationContext context, List<GraphViewExecutionOperator> operatorChain,
            int low, int high)
        {
            if (low == Int32.MinValue && high == Int32.MaxValue) return;
            operatorChain.Add(new RangeOperator(context.CurrentExecutionOperator, low, high));
            context.CurrentExecutionOperator = operatorChain.Last();
        }

        private GraphViewExecutionOperator ConstructOperator2(GraphViewConnection connection, MatchGraph graphPattern,
            QueryCompilationContext context, List<WTableReferenceWithAlias> nonVertexTableReferences,
            List<Tuple<WBooleanExpression, HashSet<string>>> predicatesAccessedTableReferences)
        {
            var operatorChain = new List<GraphViewExecutionOperator>();
            var tableReferences = context.TableReferences;
            var rawRecordLayout = context.RawRecordLayout;

            if (context.OuterContextOp != null)
                context.CurrentExecutionOperator = context.OuterContextOp;

            foreach (var subGraph in graphPattern.ConnectedSubGraphs)
            {
                // For List<MatchEdge>, backwardMatchingEdges in item4 will be cross applied when GetVertices
                // and forwardMatchingEdges in item5 will be cross applied after the TraversalOp
                var traversalChain =
                    new Stack<Tuple<MatchNode, MatchEdge, MatchNode, List<MatchEdge>, List<MatchEdge>>>(
                        subGraph.TraversalChain2);
                var processedNodes = new HashSet<MatchNode>();
                while (traversalChain.Count != 0)
                {
                    var currentChain = traversalChain.Pop();
                    var sourceNode = currentChain.Item1;
                    var traversalEdge = currentChain.Item2;
                    var sinkNode = currentChain.Item3;
                    var backwardMatchingEdges = currentChain.Item4;
                    var forwardMatchingEdges = currentChain.Item5;

                    // The first node in a component
                    if (!processedNodes.Contains(sourceNode))
                    {
                        var fetchNodeOp = new FetchNodeOperator2(connection, sourceNode.AttachedJsonQuery);

                        // The graph contains more than one component
                        if (operatorChain.Any())
                            operatorChain.Add(new CartesianProductOperator2(operatorChain.Last(), fetchNodeOp));
                        else if (context.OuterContextOp != null)
                            operatorChain.Add(new CartesianProductOperator2(context.OuterContextOp, fetchNodeOp));
                        else
                            operatorChain.Add(fetchNodeOp);

                        context.CurrentExecutionOperator = operatorChain.Last();
                        UpdateRawRecordLayout(sourceNode.NodeAlias, sourceNode.Properties, rawRecordLayout);
                        processedNodes.Add(sourceNode);
                        tableReferences.Add(sourceNode.NodeAlias, TableGraphType.Vertex);

                        CheckAndAppendRangeFilter(context, operatorChain, sourceNode.Low, sourceNode.High);

                        CheckRemainingPredicatesAndAppendFilterOp(context, connection,
                            new HashSet<string>(tableReferences.Keys), predicatesAccessedTableReferences,
                            operatorChain);

                        // Cross apply dangling Edges
                        CrossApplyEdges(connection, context, operatorChain, sourceNode.DanglingEdges,
                            predicatesAccessedTableReferences);
                    }

                    if (sinkNode != null)
                    {
                        if (WithPathClause2 != null)
                        {
                            
                        }
                        else
                        {
                            // Cross apply the traversal edge and update context info
                            CrossApplyEdges(connection, context, operatorChain, new List<MatchEdge> {traversalEdge},
                                predicatesAccessedTableReferences);

                            var traversalEdgeSinkIndex = LocateAdjacencyListTraversalIndex(context, traversalEdge);
                            // Generate matching indexes for backwardMatchingEdges
                            var matchingIndexes = GenerateMatchingIndexesForBackforwadMatchingEdges(context, backwardMatchingEdges);

                            operatorChain.Add(new TraversalOperator2(operatorChain.Last(), connection,
                                traversalEdgeSinkIndex, sinkNode.AttachedJsonQuery, matchingIndexes));

                            // Update sinkNode's context info
                            processedNodes.Add(sinkNode);
                            UpdateRawRecordLayout(sinkNode.NodeAlias, sinkNode.Properties, rawRecordLayout);
                            tableReferences.Add(sinkNode.NodeAlias, TableGraphType.Vertex);

                            // Update backwardEdges' context info
                            foreach (var backwardMatchingEdge in backwardMatchingEdges)
                            {
                                tableReferences.Add(backwardMatchingEdge.EdgeAlias, TableGraphType.Edge);
                                UpdateRawRecordLayout(backwardMatchingEdge.EdgeAlias, backwardMatchingEdge.Properties, rawRecordLayout);
                            }

                            CheckRemainingPredicatesAndAppendFilterOp(context, connection,
                                new HashSet<string>(tableReferences.Keys), predicatesAccessedTableReferences,
                                operatorChain);

                            // Cross apply forwardMatchingEdges
                            CrossApplyEdges(connection, context, operatorChain, forwardMatchingEdges,
                                predicatesAccessedTableReferences, true);

                            // Cross apply dangling edges
                            CrossApplyEdges(connection, context, operatorChain, sinkNode.DanglingEdges,
                                predicatesAccessedTableReferences);

                            CheckAndAppendRangeFilter(context, operatorChain, sinkNode.Low, sinkNode.High);

                            CheckRemainingPredicatesAndAppendFilterOp(context, connection,
                                new HashSet<string>(tableReferences.Keys), predicatesAccessedTableReferences,
                                operatorChain);
                        }
                    }
                    context.CurrentExecutionOperator = operatorChain.Last();
                }
            }

            foreach (var tableReference in nonVertexTableReferences)
            {
                if (tableReference is WQueryDerivedTable)
                {
                    var derivedQueryExpr = (tableReference as WQueryDerivedTable).QueryExpr;
                    var derivedQueryContext = new QueryCompilationContext(context.TemporaryTableCollection);
                    var derivedQueryOp = derivedQueryExpr.Compile(derivedQueryContext, connection);

                    operatorChain.Add(operatorChain.Any()
                        ? new CartesianProductOperator2(operatorChain.Last(), derivedQueryOp)
                        : derivedQueryOp);

                    foreach (var pair in derivedQueryContext.RawRecordLayout.OrderBy(e => e.Value))
                    {
                        var tableAlias = tableReference.Alias.Value;
                        var columnName = pair.Key.ColumnName;
                        // TODO: Change to correct ColumnGraphType
                        context.AddField(tableAlias, columnName, ColumnGraphType.Value);
                    }

                    // TODO: Change to correct ColumnGraphType
                    tableReferences.Add(tableReference.Alias.Value, TableGraphType.Vertex);
                    context.CurrentExecutionOperator = operatorChain.Last();
                }
                else if (tableReference is WVariableTableReference)
                {
                    var variableTable = tableReference as WVariableTableReference;
                    var tableName = variableTable.Variable.Name;
                    var tableAlias = variableTable.Alias.Value;
                    Tuple<TemporaryTableHeader, GraphViewExecutionOperator> temporaryTableTuple;
                    if (!context.TemporaryTableCollection.TryGetValue(tableName, out temporaryTableTuple))
                        throw new GraphViewException("Table variable " + tableName + " doesn't exist in the context.");

                    var tableHeader = temporaryTableTuple.Item1;
                    var tableOp = temporaryTableTuple.Item2;
                    operatorChain.Add(operatorChain.Any()
                        ? new CartesianProductOperator2(operatorChain.Last(), tableOp)
                        : tableOp);

                    // Merge temporary table's header into current context
                    foreach (var pair in tableHeader.columnSet.OrderBy(e => e.Value.Item1))
                    {
                        var columnName = pair.Key;
                        var columnGraphType = pair.Value.Item2;

                        context.AddField(tableAlias, columnName, columnGraphType);
                    }
                    context.CurrentExecutionOperator = operatorChain.Last();
                }
                else if (tableReference is WSchemaObjectFunctionTableReference)
                {
                    var functionTableReference = tableReference as WSchemaObjectFunctionTableReference;
                    var functionName = functionTableReference.SchemaObject.Identifiers.Last().ToString();
                    var tableOp = functionTableReference.Compile(context, connection);

                    GraphViewEdgeTableReferenceEnum edgeTypeEnum;
                    GraphViewVertexTableReferenceEnum vertexTypeEnum;
                    if (Enum.TryParse(functionName, true, out edgeTypeEnum))
                        tableReferences.Add(functionTableReference.Alias.Value, TableGraphType.Edge);
                    else if (Enum.TryParse(functionName, true, out vertexTypeEnum))
                        tableReferences.Add(functionTableReference.Alias.Value, TableGraphType.Vertex);
                    // TODO: Change to correct ColumnGraphType
                    else
                        tableReferences.Add(functionTableReference.Alias.Value, TableGraphType.Value);

                    operatorChain.Add(tableOp);
                    context.CurrentExecutionOperator = operatorChain.Last();
                }
                else
                {

                }

                CheckAndAppendRangeFilter(context, operatorChain, tableReference.Low, tableReference.High);

                CheckRemainingPredicatesAndAppendFilterOp(context, connection,
                    new HashSet<string>(tableReferences.Keys), predicatesAccessedTableReferences,
                    operatorChain);
            }

            // TODO: groupBy operator

            if (OrderByClause != null && OrderByClause.OrderByElements != null)
            {
                var orderByOp = OrderByClause.Compile(context, connection);
                operatorChain.Add(orderByOp);
            }


            var selectScalarExprList = SelectElements.Select(e => e as WSelectScalarExpression).ToList();

            int aggregateCount = 0;

            foreach (var selectScalar in selectScalarExprList)
            {
                if (selectScalar.SelectExpr is WFunctionCall)
                {
                    WFunctionCall fcall = selectScalar.SelectExpr as WFunctionCall;
                    switch(fcall.FunctionName.Value.ToUpper())
                    {
                        case "COUNT":
                        case "FOLD":
                        case "TREE":
                        case "CAP":
                            aggregateCount++;
                            break;
                        default:
                            break;
                    }
                } 
            }

            if (aggregateCount == 0)
            {
                var projectOperator =
                    new ProjectOperator(operatorChain.Any()
                        ? operatorChain.Last()
                        : (context.OuterContextOp ?? new ConstantSourceOperator(new RawRecord())));

                foreach (var expr in selectScalarExprList)
                {
                    ScalarFunction scalarFunction = expr.SelectExpr.CompileToFunction(context, connection);
                    projectOperator.AddSelectScalarElement(scalarFunction);
                }

                // Rebuild the context's layout
                context.ClearField();
                var i = 0;
                foreach (var expr in selectScalarExprList)
                {
                    var alias = expr.ColumnName;
                    WColumnReferenceExpression columnReference;
                    if (alias == null)
                    {
                        columnReference = expr.SelectExpr as WColumnReferenceExpression;
                        if (columnReference == null)
                        {
                            var value = expr.SelectExpr as WValueExpression;
                            columnReference = new WColumnReferenceExpression("", value.Value);
                        }
                    }
                    else
                        columnReference = new WColumnReferenceExpression("", alias);
                    // TODO: Change to Addfield with correct ColumnGraphType
                    context.RawRecordLayout.Add(columnReference, i++);
                }

                operatorChain.Add(projectOperator);
                context.CurrentExecutionOperator = projectOperator;
            }
            else
            {
                ProjectAggregation projectAggregationOp = new ProjectAggregation(operatorChain.Last());

                foreach (var selectScalar in selectScalarExprList)
                {
                    WFunctionCall fcall = selectScalar.SelectExpr as WFunctionCall;

                    switch (fcall.FunctionName.Value.ToUpper())
                    {
                        case "COUNT":
                            projectAggregationOp.AddAggregateSpec(new CountFunction(), new List<int>());
                            break;
                        case "FOLD":
                            var foldedField = fcall.Parameters[0] as WColumnReferenceExpression;
                            var foldedFieldIndex = context.LocateColumnReference(foldedField);
                            projectAggregationOp.AddAggregateSpec(new FoldFunction(), new List<int>(foldedFieldIndex));
                            break;
                        case "TREE":
                            var pathField = fcall.Parameters[0] as WColumnReferenceExpression;
                            var pathFieldIndex = context.LocateColumnReference(pathField);
                            projectAggregationOp.AddAggregateSpec(new TreeFunction(), new List<int>(pathFieldIndex));
                            break;
                        case "CAP":
                            var capAggregate = new CapAggregate();
                            foreach (var expression in fcall.Parameters)
                            {
                                var capName = expression as WValueExpression;
                                IAggregateFunction sideEffectState;
                                if (!context.SideEffectStates.TryGetValue(capName.Value, out sideEffectState))
                                    throw new GraphViewException("SideEffect state " + capName + " doesn't exist in the context");
                                capAggregate.AddCapatureSideEffectState(capName.Value, sideEffectState);
                            }
                            projectAggregationOp.AddAggregateSpec(new CapAggregate(), new List<int>());
                            break;
                        default:
                            break;
                    }
                }

                // Rebuild the context's layout
                context.ClearField();
                foreach (var expr in selectScalarExprList)
                {
                    var alias = expr.ColumnName;
                    // TODO: Change to Addfield with correct ColumnGraphType
                    context.AddField("", alias ?? "_value", ColumnGraphType.Value);
                }

                operatorChain.Add(projectAggregationOp);
                context.CurrentExecutionOperator = projectAggregationOp;
            }

            return operatorChain.Last();
        }


        private GraphViewExecutionOperator ConstructOperator(MatchGraph graph, List<string> header, 
            Dictionary<string, string> columnToAliasDict, GraphViewConnection pConnection, List<BooleanFunction> functions)
        {
            // output and input buffer size is set here.
            const int OUTPUT_BUFFER_SIZE = 50;
            const int INPUT_BUFFER_SIZE = 50;
            List<GraphViewExecutionOperator> ChildrenProcessor = new List<GraphViewExecutionOperator>();
            List<GraphViewExecutionOperator> RootProcessor = new List<GraphViewExecutionOperator>();
            List<string> HeaderForOneOperator = new List<string>();
            // Init function validality cheking list. 
            // Whenever all the operands of a boolean check function appeared, attach the function to the operator.
            List<int> FunctionVaildalityCheck = new List<int>();
            foreach (var i in functions)
            {
                FunctionVaildalityCheck.Add(0);
            }
            int StartOfResult = 0, CurrentMetaHeaderLength = 0;
            // Generate operator for each subgraph.
            foreach (var subgraph in graph.ConnectedSubGraphs)
            {
                var SortedNodes = new Stack<Tuple<MatchNode, MatchEdge>>(subgraph.TraversalChain);
                StartOfResult += subgraph.Nodes.Select(n => n.Value.HeaderLength).Aggregate(0, (cur, next) => cur + next);
                HashSet<MatchNode> ProcessedNode = new HashSet<MatchNode>();
                while (SortedNodes.Count != 0)
                {
                    MatchNode TempNode = null;
                    var CurrentProcessingNode = SortedNodes.Pop();
                    // If a node is a source node and never appeared before, it will be consturcted to a fetchnode operator
                    // Otherwise it will be consturcted to a TraversalOperator.
                    if (!ProcessedNode.Contains(CurrentProcessingNode.Item1))
                    {
                        int node = header.IndexOf(CurrentProcessingNode.Item1.NodeAlias);
                        TempNode = CurrentProcessingNode.Item1;
                        CurrentMetaHeaderLength += TempNode.HeaderLength;
                        HeaderForOneOperator = header.GetRange(0, CurrentMetaHeaderLength);

                        for (int i = StartOfResult; i < header.Count; i++)
                            HeaderForOneOperator.Add(header[i]);
                        //if (ChildrenProcessor.Count == 0)
                        ChildrenProcessor.Add(new FetchNodeOperator(pConnection, CurrentProcessingNode.Item1.AttachedQuerySegment, node, HeaderForOneOperator, TempNode.HeaderLength, 50));
                        //else
                        //    ChildrenProcessor.Add(new FetchNodeOperator(pConnection, CurrentProcessingNode.Item1.AttachedQuerySegment, node, HeaderForOneOperator, ProcessedNode.Count, 50, ChildrenProcessor.Last()));
                        if (functions != null && functions.Count != 0)
                            CheckFunctionValidate(ref header, ref functions, ref TempNode, ref FunctionVaildalityCheck, ref ChildrenProcessor);
                        ProcessedNode.Add(CurrentProcessingNode.Item1);

                    }
                    if (CurrentProcessingNode.Item2 != null)
                    {
                        TempNode = CurrentProcessingNode.Item2.SinkNode;

                        int src = header.IndexOf(CurrentProcessingNode.Item2.SourceNode.NodeAlias);
                        int srcAdj = header.IndexOf(CurrentProcessingNode.Item2.EdgeAlias + "_ADJ");
                        int dest = header.IndexOf(CurrentProcessingNode.Item2.SinkNode.NodeAlias);

                        CurrentMetaHeaderLength += TempNode.HeaderLength;
                        HeaderForOneOperator = header.GetRange(0, CurrentMetaHeaderLength);

                        for (int i = StartOfResult; i < header.Count; i++)
                            HeaderForOneOperator.Add(header[i]);

                        Tuple<string, GraphViewExecutionOperator, int> InternalOperator = null;
                        if (WithPathClause != null && (InternalOperator =
                                    WithPathClause.PathOperators.Find(
                                        p => p.Item1 == CurrentProcessingNode.Item2.EdgeAlias)) !=
                                null)
                        {
                            // if WithPathClause != null, internal operator should be constructed for the traversal operator that deals with path.
                            ChildrenProcessor.Add(new TraversalOperator(pConnection, ChildrenProcessor.Last(),
                                TempNode.AttachedQuerySegment, src, srcAdj, dest, HeaderForOneOperator,
                                TempNode.HeaderLength, TempNode.ReverseCheckList, INPUT_BUFFER_SIZE,
                                OUTPUT_BUFFER_SIZE, false, InternalOperator.Item2));
                        }
                        else
                        {
                            ChildrenProcessor.Add(new TraversalOperator(pConnection, ChildrenProcessor.Last(),
                                TempNode.AttachedQuerySegment, src, srcAdj, dest, HeaderForOneOperator,
                                TempNode.HeaderLength, TempNode.ReverseCheckList, INPUT_BUFFER_SIZE,
                                OUTPUT_BUFFER_SIZE, CurrentProcessingNode.Item2.IsReversed));
                        }
                        ProcessedNode.Add(TempNode);
                        // Check if any boolean function should be attached to this operator.
                        if (functions != null && functions.Count != 0)
                            CheckFunctionValidate(ref header, ref functions, ref TempNode, ref FunctionVaildalityCheck, ref ChildrenProcessor);
                    }

                }
                // The last processor of a sub graph will be added to root processor list for later use.
                RootProcessor.Add(ChildrenProcessor.Last());

                for (int i = 0; i < FunctionVaildalityCheck.Count; i++)
                    if (FunctionVaildalityCheck[i] == 1) FunctionVaildalityCheck[i] = 0;
            }
            GraphViewExecutionOperator root = null;
            if (RootProcessor.Count == 1) root = RootProcessor[0];
            // A cartesian product will be made among all the result from the root processor in order to produce a complete result
            else
            {
                root = new CartesianProductOperator(RootProcessor, header);
                // If some boolean function cannot be attached in any single subgraph, it should either be attached to cartesian product operator.
                // or it cannot be attached anywhere.
                for (int i = 0; i < FunctionVaildalityCheck.Count; i++)
                {
                    if (FunctionVaildalityCheck[i] < 2)
                    {
                        if ((root as CartesianProductOperator).BooleanCheck == null)
                            (root as CartesianProductOperator).BooleanCheck = functions[i];
                        else (root as CartesianProductOperator).BooleanCheck = new BooleanBinaryFunction((root as CartesianProductOperator).BooleanCheck,
                                        functions[i], BooleanBinaryFunctionType.And);
                    }
                }
            }
            if (OrderByClause != null && OrderByClause.OrderByElements != null)
            {
                var orderByElements = new List<Tuple<string, SortOrder>>();
                foreach (var element in OrderByClause.OrderByElements)
                {
                    var sortOrder = element.SortOrder;
                    var expr = element.ScalarExpr.ToString();
                    string alias;
                    // if expr is a column name with an alias, use its alias
                    if (columnToAliasDict.TryGetValue(expr, out alias))
                        orderByElements.Add(new Tuple<string, SortOrder>(alias, sortOrder));
                    // if expr is already the alias, use the expr directly
                    else if (columnToAliasDict.ContainsValue(expr))
                        orderByElements.Add(new Tuple<string, SortOrder>(expr, sortOrder));
                    else
                        throw new GraphViewException(string.Format("Invalid column name '{0}'", expr));
                }
                //(from wExpressionWithSortOrder in OrderByClause.OrderByElements
                //    let expr = columnToAliasDict[wExpressionWithSortOrder.ScalarExpr.ToString()]
                //    let sortOrder = wExpressionWithSortOrder.SortOrder
                //    select new Tuple<string, SortOrder>(expr, sortOrder)).ToList();
                root = new OrderbyOperator(root, orderByElements, header);
            }

            List<string> SelectedElement = new List<string>();
            foreach (var x in SelectElements)
            {
                var expr = (x as WSelectScalarExpression).SelectExpr;
                if (expr is WColumnReferenceExpression)
                {
                    var columnName = (expr as WColumnReferenceExpression).MultiPartIdentifier.ToString();
                    string alias;
                    columnToAliasDict.TryGetValue(columnName, out alias);
                    if (alias == null) alias = columnName;
                    SelectedElement.Add(alias);
                }
            }
            if (!OutputPath)
                root = new OutputOperator(root, SelectedElement, root.header);
            else
                root = new OutputOperator(root, true, header);
            return root;
        }

        private void BuildQuerySegementOnNode(List<string> ProcessedNodeList, MatchNode node, List<string> header, 
            Dictionary<string, List<Tuple<MatchEdge, MaterializedEdgeType>>> nodeToMatEdgesDict, Dictionary<string, string> columnToAliasDict, Dictionary<string, DColumnReferenceExpression> headerToColumnRefDict,
            int pStartOfResultField, bool isPathTailNode = false)
        {
            DFromClause fromClause = new DFromClause();
            string FromClauseString = "";
            WBooleanExpression searchCondition = null;

            string scriptBase = string.Format("SELECT {{\"id\":{0}.id}} AS _nodeid", node.NodeAlias);
            const string edgeProjectBase = "{{\"_sink\": {0}._sink, \"_ID\": {0}._ID";
            // <edge, extra properties need to be pulled from the server besides _sink and _ID>
            var edgeProjection = new Dictionary<string, List<DColumnReferenceExpression>>();

            fromClause.TableReference = node.NodeAlias;

            if (!isPathTailNode && nodeToMatEdgesDict != null)
            {
                // Join every edge needs to be pulled
                foreach (var t in nodeToMatEdgesDict[node.NodeAlias])
                {
                    var edge = t.Item1;
                    FromClauseString += " Join " + edge.EdgeAlias + " in " + node.NodeAlias + 
                        (edge.IsReversed
                        ? "._reverse_edge "
                        : "._edge ");
                    // Add all the predicates on edges to the where clause.
                    foreach (var predicate in edge.Predicates)
                        searchCondition = WBooleanBinaryExpression.Conjunction(searchCondition, predicate);
                    edgeProjection[edge.EdgeAlias] = new List<DColumnReferenceExpression>();
                }
            }
            fromClause.FromClauseString = FromClauseString;

            // Add all the predicates on nodes to the where clause.
            foreach (var predicate in node.Predicates)
                searchCondition = WBooleanBinaryExpression.Conjunction(searchCondition, predicate);

            // Select elements that related to current node and its edges will be attached here.
            List<DColumnReferenceExpression> DSelectElements = new List<DColumnReferenceExpression>();
            for (var i = pStartOfResultField; i < header.Count; i++)
            {
                var str = header[i];
                int CutPoint = str.Length;
                if (str.IndexOf('.') != -1) CutPoint = str.IndexOf('.');
                if (str.Substring(0, CutPoint) == node.NodeAlias)
                {
                    // Replace the column name in header with its alias
                    header[i] = columnToAliasDict[str];
                    DSelectElements.Add(headerToColumnRefDict[str]);
                }
                if (nodeToMatEdgesDict != null)
                {
                    foreach (var t in nodeToMatEdgesDict[node.NodeAlias])
                    {
                        var edge = t.Item1;
                        if (str.Substring(0, CutPoint) == edge.EdgeAlias)
                        {
                            header[i] = columnToAliasDict[str];
                            edgeProjection[edge.EdgeAlias].Add(headerToColumnRefDict[str]);
                        }
                    }
                }
            }

            // Reverse checking pair generation
            if (!isPathTailNode && nodeToMatEdgesDict != null)
            {
                foreach (var t in nodeToMatEdgesDict[node.NodeAlias])
                {
                    var edge = t.Item1;
                    // <index of adj field, index of dest id field>
                    if (ProcessedNodeList.Contains(edge.SinkNode.NodeAlias))
                        node.ReverseCheckList.Add(header.IndexOf(edge.EdgeAlias + "_ADJ"), header.IndexOf(edge.SinkNode.NodeAlias));
                    else
                        edge.SinkNode.ReverseCheckList.Add(header.IndexOf(edge.EdgeAlias + "_ADJ"), header.IndexOf(edge.SinkNode.NodeAlias));
                }
            }

            foreach (var pair in edgeProjection)
            {
                var edgeAlias = pair.Key;
                var projects = pair.Value;
                scriptBase += ", " + string.Format(edgeProjectBase, edgeAlias);
                scriptBase = projects.Aggregate(scriptBase,
                    (current, project) =>
                        current + ", " +
                        string.Format("\"{0}\": {1}", project.ColumnName, project.MultiPartIdentifier.ToString()));
                scriptBase += string.Format("}} AS {0}_ADJ", edgeAlias);
            }

            // The DocDb script of the current node will be assembled here.
            WWhereClause whereClause = new WWhereClause {SearchCondition = searchCondition};
            DocDbScript script = new DocDbScript {ScriptBase = scriptBase, SelectElements = DSelectElements, FromClause = fromClause, WhereClause = whereClause, OriginalSearchCondition = searchCondition};
            node.AttachedQuerySegment = script;
        }

        // Check if any operand of the boolean functions appeared in the operator, increase the corresponding mark if so.
        // Whenever all the operands of a boolean check function appeared, attach the function to the operator.
        private void CheckFunctionValidate(ref List<string> header, ref List<BooleanFunction> functions, ref MatchNode TempNode, ref List<int> FunctionVaildalityCheck, ref List<GraphViewExecutionOperator> ChildrenProcessor)
        {
            for (int i = 0; i < functions.Count; i++)
            {
                if (functions[i] is FieldComparisonFunction)
                {
                    //string lhs = header[(functions[i] as FieldComparisonFunction).LhsFieldIndex];
                    //string rhs = header[(functions[i] as FieldComparisonFunction).RhsFieldIndex];
                    string lhs = (functions[i] as FieldComparisonFunction).LhsFieldName;
                    string rhs = (functions[i] as FieldComparisonFunction).RhsFieldName;
                    bool isLhsContained = false, isRhsContained = false;
                    var selectElements =
                        TempNode.AttachedQuerySegment.SelectElements.Select(expr => expr.ToSqlStyleString()).ToList();

                    foreach (var expr in selectElements)
                    {
                        if (expr.Contains(lhs))
                            isLhsContained = true;
                        if (expr.Contains(rhs))
                            isRhsContained = true;
                    }
                    if (isLhsContained)
                        FunctionVaildalityCheck[i]++;
                    if (isRhsContained)
                        FunctionVaildalityCheck[i]++;

                    if (FunctionVaildalityCheck[i] == 2)
                        {
                            functions[i].header = ChildrenProcessor.Last().header;
                            if (ChildrenProcessor.Last() != null && ChildrenProcessor.Last() is TraversalBaseOperator)
                            {
                                if ((ChildrenProcessor.Last() as TraversalBaseOperator).crossDocumentJoinPredicates ==
                                    null)
                                    (ChildrenProcessor.Last() as TraversalBaseOperator).crossDocumentJoinPredicates =
                                        functions[i];
                                else
                                    (ChildrenProcessor.Last() as TraversalBaseOperator).crossDocumentJoinPredicates =
                                        new BooleanBinaryFunction(
                                            (ChildrenProcessor.Last() as TraversalBaseOperator)
                                                .crossDocumentJoinPredicates,
                                            functions[i], BooleanBinaryFunctionType.And);
                            }
                            FunctionVaildalityCheck[i] = 0;
                        }
                }
            }
        }

        //private List<Tuple<int, string>> ConsturctReverseCheckList(MatchNode TempNode, ref HashSet<MatchNode> ProcessedNode, List<string> header)
        //{
        //    List<Tuple<int, string>> ReverseCheckList = new List<Tuple<int, string>>();
        //    foreach (var neighbor in TempNode.ReverseNeighbors)
        //        if (ProcessedNode.Contains(neighbor.SinkNode))
        //            ReverseCheckList.Add(new Tuple<int, string>(header.IndexOf(neighbor.SinkNode.NodeAlias),
        //                neighbor.EdgeAlias + "_REV"));
        //    foreach (var neighbor in TempNode.Neighbors)
        //        if (ProcessedNode.Contains(neighbor.SinkNode))
        //            ReverseCheckList.Add(new Tuple<int, string>(header.IndexOf(neighbor.SinkNode.NodeAlias),
        //                neighbor.EdgeAlias + "_REV"));
        //    return ReverseCheckList;
        //}

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
            static internal Stack<Tuple<MatchNode, MatchEdge>> TopoSort(Dictionary<string, MatchNode> graph)
            {
                Dictionary<MatchNode, int> state = new Dictionary<MatchNode, int>();
                Stack<Tuple<MatchNode, MatchEdge>> list = new Stack<Tuple<MatchNode, MatchEdge>>();
                foreach (var node in graph)
                    state.Add(node.Value, 0);
                foreach (var node in graph)
                    if (state[node.Value] == 0)
                        visit(graph, node.Value, list, state, node.Value.NodeAlias, null);
                if (graph.Count == 1) list.Push(new Tuple<MatchNode, MatchEdge>(graph.First().Value, null));
                return list;
            }
            static private void visit(Dictionary<string, MatchNode> graph, MatchNode node, Stack<Tuple<MatchNode, MatchEdge>> list, Dictionary<MatchNode, int> state, string ParentAlias, MatchEdge Edge)
            {
                state[node] = 2;
                foreach (var neighbour in node.Neighbors)
                {
                    if (state[neighbour.SinkNode] == 0)
                        visit(graph, neighbour.SinkNode, list, state, node.NodeAlias, neighbour);
                    if (state[neighbour.SinkNode] == 2)
                        foreach (var neighbour2 in neighbour.SinkNode.ReverseNeighbors)
                        {
                            foreach (var x in neighbour2.SinkNode.Neighbors)
                                if (x.SinkNode == node)
                                    list.Push(new Tuple<MatchNode, MatchEdge>(x.SourceNode, x));
                        }

                }
                state[node] = 1;
                foreach (var neighbour in node.ReverseNeighbors)
                {
                    foreach (var x in neighbour.SinkNode.Neighbors)
                        if (x.SinkNode == node)
                            list.Push(new Tuple<MatchNode, MatchEdge>(x.SourceNode, x));
                }
            }
        }
    }

    partial class WWithPathClause
    {
        internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        {
            foreach (var path in Paths)
            {
                //path.Item2.SelectElements = new List<WSelectElement>();
                PathOperators.Add(new Tuple<string, GraphViewExecutionOperator, int>(path.Item1,
                    path.Item2.Generate(dbConnection), path.Item3));
            }
            if (PathOperators.Count != 0) return PathOperators.First().Item2;
            else return null;
        }
    }

    partial class WChoose
    {
        internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        {
            List<GraphViewExecutionOperator> Source = new List<GraphViewExecutionOperator>();
            foreach (var x in InputExpr)
            {
                Source.Add(x.Generate(dbConnection));
            }
            return new ConcatenateOperator(Source);
        }
    }

    partial class WCoalesce
    {
        internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        {
            List<GraphViewExecutionOperator> Source = new List<GraphViewExecutionOperator>();
            foreach (var x in InputExpr)
            {
                Source.Add(x.Generate(dbConnection));
            }
            var op = new CoalesceOperator(Source, CoalesceNumber);
            return new OutputOperator(op, op.header, null);
        }
    }

    partial class WSqlBatch
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            QueryCompilationContext priorContext = new QueryCompilationContext();
            GraphViewExecutionOperator op = null;
            foreach (WSqlStatement st in Statements)
            {
                QueryCompilationContext statementContext = new QueryCompilationContext(priorContext.TemporaryTableCollection);
                op = st.Compile(statementContext, dbConnection);
                priorContext = statementContext;
            }

            // Returns the last execution operator
            // To consider: prior execution operators that have no links to the last operator will not be executed.
            return op;
        }
    }

    partial class WSetVariableStatement
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            if (_expression.GetType() != typeof(WScalarSubquery))
            {
                throw new NotImplementedException();
            }

            WSqlStatement subquery = (_expression as WScalarSubquery).SubQueryExpr;
            GraphViewExecutionOperator subqueryOp = subquery.Compile(context, dbConnection);
            TemporaryTableHeader tmpTableHeader = context.ToTableHeader();
            // Adds the table populated by the statement as a temporary table to the context
            context.TemporaryTableCollection[_variable.Name] = new Tuple<TemporaryTableHeader, GraphViewExecutionOperator>(tmpTableHeader, subqueryOp);

            return subqueryOp;
        }
    }

    partial class WOrderByClause
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {

            var orderByElements = new List<Tuple<int, SortOrder>>();
            if (OrderByElements != null)
            {
                foreach (var element in OrderByElements)
                {
                    var expr = element.ScalarExpr as WColumnReferenceExpression;
                    if (expr == null)
                        throw new SyntaxErrorException("The order by elements can only be WColumnReferenceExpression.");

                    orderByElements.Add(new Tuple<int, SortOrder>(context.LocateColumnReference(expr), element.SortOrder));
                }
            }

            var orderByOp = new OrderbyOperator2(context.CurrentExecutionOperator, orderByElements);
            context.CurrentExecutionOperator = orderByOp;
            return orderByOp;
        }
    }

    partial class WUnionTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            UnionOperator unionOp = new UnionOperator(context.CurrentExecutionOperator);

            WSelectQueryBlock firstSelectQuery = null;
            foreach (WScalarExpression parameter in Parameters)
            {
                WScalarSubquery scalarSubquery = parameter as WScalarSubquery;
                if (scalarSubquery == null)
                {
                    throw new SyntaxErrorException("The input of a union table reference must be one or more scalar subqueries.");
                }

                if (firstSelectQuery == null)
                {
                    firstSelectQuery = scalarSubquery.SubQueryExpr as WSelectQueryBlock;
                    if (firstSelectQuery == null)
                    {
                        throw new SyntaxErrorException("The input of a union table reference must be one or more select query blocks.");
                    }
                }

                QueryCompilationContext subcontext = new QueryCompilationContext(context);
                GraphViewExecutionOperator traversalOp = scalarSubquery.SubQueryExpr.Compile(subcontext, dbConnection);
                unionOp.AddTraversal(subcontext.OuterContextOp, traversalOp);
            }

            // Updates the raw record layout. The columns of this table-valued function 
            // are specified by the select elements of the input subqueries.
            foreach (WSelectElement selectElement in firstSelectQuery.SelectElements)
            {
                WSelectScalarExpression selectScalar = selectElement as WSelectScalarExpression;
                if (selectScalar == null)
                {
                    throw new SyntaxErrorException("The input subquery of a union table reference can only select scalar elements.");
                }
                WColumnReferenceExpression columnRef = selectScalar.SelectExpr as WColumnReferenceExpression;
                if (columnRef == null)
                {
                    throw new SyntaxErrorException("The input subquery of a union table reference can only select column epxressions.");
                }
                string selectElementAlias = selectScalar.ColumnName;
                context.AddField(Alias.Value, selectElementAlias ?? columnRef.ColumnName, columnRef.ColumnGraphType);
            }

            context.CurrentExecutionOperator = unionOp;
            return unionOp;
        }
    }

    partial class WCoalesceTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            CoalesceOperator2 coalesceOp = new CoalesceOperator2(context.CurrentExecutionOperator);

            WSelectQueryBlock firstSelectQuery = null;
            foreach (WScalarExpression parameter in Parameters)
            {
                WScalarSubquery scalarSubquery = parameter as WScalarSubquery;
                if (scalarSubquery == null)
                {
                    throw new SyntaxErrorException("The input of a coalesce table reference must be one or more scalar subqueries.");
                }

                if (firstSelectQuery == null)
                {
                    firstSelectQuery = scalarSubquery.SubQueryExpr as WSelectQueryBlock;
                    if (firstSelectQuery == null)
                    {
                        throw new SyntaxErrorException("The input of a coalesce table reference must be one or more select query blocks.");
                    }
                }

                QueryCompilationContext subcontext = new QueryCompilationContext(context);
                GraphViewExecutionOperator traversalOp = scalarSubquery.SubQueryExpr.Compile(subcontext, dbConnection);
                coalesceOp.AddTraversal(subcontext.OuterContextOp, traversalOp);
            }

            // Updates the raw record layout. The columns of this table-valued function 
            // are specified by the select elements of the input subqueries.
            foreach (WSelectElement selectElement in firstSelectQuery.SelectElements)
            {
                WSelectScalarExpression selectScalar = selectElement as WSelectScalarExpression;
                if (selectScalar == null)
                {
                    throw new SyntaxErrorException("The input subquery of a coalesce table reference can only select scalar elements.");
                }
                WColumnReferenceExpression columnRef = selectScalar.SelectExpr as WColumnReferenceExpression;
                if (columnRef == null)
                {
                    throw new SyntaxErrorException("The input subquery of a coalesce table reference can only select column epxressions.");
                }
                string selectElementAlias = selectScalar.ColumnName;
                context.AddField(Alias.Value, selectElementAlias ?? columnRef.ColumnName, columnRef.ColumnGraphType);
            }

            context.CurrentExecutionOperator = coalesceOp;
            return coalesceOp;
        }
    }

    partial class WOptionalTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WSelectQueryBlock contextSelect, optionalSelect;
            Split(out contextSelect, out optionalSelect);

            List<int> inputIndexes = new List<int>();
            List<Tuple<WColumnReferenceExpression, string>> columnList = new List<Tuple<WColumnReferenceExpression, string>>();

            foreach (WSelectElement selectElement in contextSelect.SelectElements)
            {
                WSelectScalarExpression selectScalar = selectElement as WSelectScalarExpression;
                if (selectScalar == null)
                {
                    throw new SyntaxErrorException("The SELECT elements of the sub-queries in an optional table reference must be select scalar elements.");
                }
                WColumnReferenceExpression columnRef = selectScalar.SelectExpr as WColumnReferenceExpression;
                if (columnRef == null)
                {
                    throw new SyntaxErrorException("The SELECT elements of the sub-queries in an optional table reference must be column references.");
                }

                int index = context.LocateColumnReference(columnRef);
                inputIndexes.Add(index);

                string selectElementAlias = selectScalar.ColumnName;
                columnList.Add(new Tuple<WColumnReferenceExpression, string>(columnRef, selectElementAlias));
            }

            QueryCompilationContext subcontext = new QueryCompilationContext(context);
            GraphViewExecutionOperator optionalTraversalOp = optionalSelect.Compile(subcontext, dbConnection);

            OptionalOperator optionalOp = new OptionalOperator(context.CurrentExecutionOperator, inputIndexes, optionalTraversalOp, subcontext.OuterContextOp);
            context.CurrentExecutionOperator = optionalOp;

            // Updates the raw record layout. The columns of this table-valued function 
            // are specified by the select elements of the input subqueries.
            foreach (var tuple in columnList)
            {
                var columnRef = tuple.Item1;
                var selectElementAlias = tuple.Item2;
                context.AddField(Alias.Value, selectElementAlias ?? columnRef.ColumnName, columnRef.ColumnGraphType);
            }

            return optionalOp;
        }
    }

    partial class WLocalTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WScalarSubquery localSubquery = Parameters[0] as WScalarSubquery;
            if (localSubquery == null)
            {
                throw new SyntaxErrorException("The input of a local table reference must be a scalar subquery.");
            }
            WSelectQueryBlock localSelect = localSubquery.SubQueryExpr as WSelectQueryBlock;
            if (localSelect == null)
            {
                throw new SyntaxErrorException("The sub-query must be a select query block.");
            }

            QueryCompilationContext subcontext = new QueryCompilationContext(context);
            GraphViewExecutionOperator localTraversalOp = localSelect.Compile(subcontext, dbConnection);

            LocalOperator localOp = new LocalOperator(context.CurrentExecutionOperator, localTraversalOp, subcontext.OuterContextOp);
            context.CurrentExecutionOperator = localOp;

            foreach (WSelectElement selectElement in localSelect.SelectElements)
            {
                WSelectScalarExpression selectScalar = selectElement as WSelectScalarExpression;
                if (selectScalar == null)
                {
                    throw new SyntaxErrorException("The SELECT elements of the sub-query in a local table reference must be select scalar elements.");
                }
                WColumnReferenceExpression columnRef = selectScalar.SelectExpr as WColumnReferenceExpression;
                if (columnRef == null)
                {
                    throw new SyntaxErrorException("The SELECT elements of the sub-query in a local table reference must be column references.");
                }
                string selectElementAlias = selectScalar.ColumnName;
                context.AddField(Alias.Value, selectElementAlias ?? columnRef.ColumnName, columnRef.ColumnGraphType);
            }

            return localOp;
        }
    }

    partial class WFlatMapTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WScalarSubquery flatMapSubquery = Parameters[0] as WScalarSubquery;
            if (flatMapSubquery == null)
            {
                throw new SyntaxErrorException("The input of a flatMap table reference must be a scalar subquery.");
            }
            WSelectQueryBlock flatMapSelect = flatMapSubquery.SubQueryExpr as WSelectQueryBlock;
            if (flatMapSelect == null)
            {
                throw new SyntaxErrorException("The sub-query must be a select query block.");
            }

            QueryCompilationContext subcontext = new QueryCompilationContext(context);
            GraphViewExecutionOperator flatMapTraversalOp = flatMapSelect.Compile(subcontext, dbConnection);

            FlatMapOperator flatMapOp = new FlatMapOperator(context.CurrentExecutionOperator, flatMapTraversalOp, subcontext.OuterContextOp);
            context.CurrentExecutionOperator = flatMapOp;

            foreach (WSelectElement selectElement in flatMapSelect.SelectElements)
            {
                WSelectScalarExpression selectScalar = selectElement as WSelectScalarExpression;
                if (selectScalar == null)
                {
                    throw new SyntaxErrorException("The SELECT elements of the sub-query in a flatMap table reference must be select scalar elements.");
                }
                WColumnReferenceExpression columnRef = selectScalar.SelectExpr as WColumnReferenceExpression;
                if (columnRef == null)
                {
                    throw new SyntaxErrorException("The SELECT elements of the sub-query in a flatMap table reference must be column references.");
                }
                string selectElementAlias = selectScalar.ColumnName;
                context.AddField(Alias.Value, selectElementAlias ?? columnRef.ColumnName, columnRef.ColumnGraphType);
            }

            return flatMapOp;
        }
    }

    partial class WBoundOutNodeTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            var sinkParameter = Parameters[0] as WColumnReferenceExpression;
            var sinkIndex = context.LocateColumnReference(sinkParameter);
            var nodeAlias = Alias.Value;
            var isSendQueryRequired = !(Parameters.Count == 2 && (Parameters[1] as WValueExpression).Value.Equals("id"));
            var matchNode = new MatchNode
            {
                AttachedJsonQuery = null,
                NodeAlias = nodeAlias,
                Predicates = new List<WBooleanExpression>(),
                Properties = new List<string>{ "id", "_edge", "_reverse_edge" },
            };

            // Construct JSON query
            if (isSendQueryRequired)
            {
                for (int i = 1; i < Parameters.Count; i++)
                {
                    var property = (Parameters[i] as WValueExpression).Value;
                    if (!matchNode.Properties.Contains(property))
                        matchNode.Properties.Add(property);
                }
                WSelectQueryBlock.ConstructJsonQueryOnNode(matchNode);
            }

            var traversalOp = new TraversalOperator2(context.CurrentExecutionOperator, dbConnection, sinkIndex,
                matchNode.AttachedJsonQuery, null);
            context.CurrentExecutionOperator = traversalOp;

            // Update context's record layout
            if (isSendQueryRequired)
            {
                // TODO: Change to correct ColumnGraphType
                foreach (var property in matchNode.Properties)
                    context.AddField(nodeAlias, property, ColumnGraphType.Value);
            }
            else
            {
                context.AddField(nodeAlias, "id", ColumnGraphType.VertexId);
            }

            return traversalOp;
        }
    }

    partial class WBoundBothNodeTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            var firstSinkParameter = Parameters[0] as WColumnReferenceExpression;
            var secondSinkParameter = Parameters[1] as WColumnReferenceExpression;
            var sinkIndexes = new List<int>
            {
                context.LocateColumnReference(firstSinkParameter),
                context.LocateColumnReference(secondSinkParameter)
            };
            var nodeAlias = Alias.Value;
            var isSendQueryRequired = !(Parameters.Count == 3 && (Parameters[2] as WValueExpression).Value.Equals("id"));
            var matchNode = new MatchNode
            {
                AttachedJsonQuery = null,
                NodeAlias = nodeAlias,
                Predicates = new List<WBooleanExpression>(),
                Properties = new List<string> { "id", "_edge", "_reverse_edge" },
            };

            // Construct JSON query
            if (isSendQueryRequired)
            {
                for (int i = 2; i < Parameters.Count; i++)
                {
                    var property = (Parameters[i] as WValueExpression).Value;
                    if (!matchNode.Properties.Contains(property))
                        matchNode.Properties.Add(property);
                }
                WSelectQueryBlock.ConstructJsonQueryOnNode(matchNode);
            }

            var bothVOp = new BothVOperator(context.CurrentExecutionOperator, dbConnection, sinkIndexes,
                matchNode.AttachedJsonQuery);
            context.CurrentExecutionOperator = bothVOp;

            // Update context's record layout
            if (isSendQueryRequired)
            {
                // TODO: Change to correct ColumnGraphType
                foreach (var property in matchNode.Properties)
                    context.AddField(nodeAlias, property, ColumnGraphType.Value);
            }
            else
            {
                context.AddField(nodeAlias, "id", ColumnGraphType.VertexId);
            }

            return bothVOp;
        }
    }

    partial class WBoundOutEdgeTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context,
            GraphViewConnection dbConnection)
        {
            var startVertexIdParameter = Parameters[0] as WColumnReferenceExpression;
            var adjListParameter = Parameters[1] as WColumnReferenceExpression;

            var startVertexIndex = context.LocateColumnReference(startVertexIdParameter);
            var adjListIndex = context.LocateColumnReference(adjListParameter);

            var edgeAlias = Alias.Value;
            var projectFields = new List<string> { "_source", "_sink", "_other", "_ID" };

            for (int i = 2; i < Parameters.Count; i++)
            {
                var field = (Parameters[i] as WValueExpression).Value;
                if (!projectFields.Contains(field))
                    projectFields.Add(field);
            }

            var adjListDecoder = new AdjacencyListDecoder2(context.CurrentExecutionOperator, startVertexIndex,
                adjListIndex, -1, true, null, projectFields);
            context.CurrentExecutionOperator = adjListDecoder;

            // Update context's record layout
            foreach (var projectField in projectFields)
            {
                // TODO: Change to correct ColumnGraphType
                context.AddField(edgeAlias, projectField, ColumnGraphType.Value);
            }

            return adjListDecoder;
        }
    }

    partial class WBoundInEdgeTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            var startVertexIdParameter = Parameters[0] as WColumnReferenceExpression;
            var revAdjListParameter = Parameters[1] as WColumnReferenceExpression;

            var startVertexIndex = context.LocateColumnReference(startVertexIdParameter);
            var revAdjListIndex = context.LocateColumnReference(revAdjListParameter);

            var edgeAlias = Alias.Value;
            var projectFields = new List<string> { "_source", "_sink", "_other", "_ID" };

            for (int i = 2; i < Parameters.Count; i++)
            {
                var field = (Parameters[i] as WValueExpression).Value;
                if (!projectFields.Contains(field))
                    projectFields.Add(field);
            }

            var adjListDecoder = new AdjacencyListDecoder2(context.CurrentExecutionOperator, startVertexIndex,
               -1, revAdjListIndex, true, null, projectFields);
            context.CurrentExecutionOperator = adjListDecoder;

            // Update context's record layout
            foreach (var projectField in projectFields)
            {
                // TODO: Change to correct ColumnGraphType
                context.AddField(edgeAlias, projectField, ColumnGraphType.Value);
            }

            return adjListDecoder;
        }
    }

    partial class WBoundBothEdgeTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            var startVertexIdParameter = Parameters[0] as WColumnReferenceExpression;
            var adjListParameter = Parameters[1] as WColumnReferenceExpression;
            var revAdjListParameter = Parameters[2] as WColumnReferenceExpression;

            var startVertexIndex = context.LocateColumnReference(startVertexIdParameter);
            var adjListIndex = context.LocateColumnReference(adjListParameter);
            var revAdjListIndex = context.LocateColumnReference(revAdjListParameter);

            var edgeAlias = Alias.Value;
            var projectFields = new List<string> { "_source", "_sink", "_other", "_ID" };

            for (int i = 3; i < Parameters.Count; i++)
            {
                var field = (Parameters[i] as WValueExpression).Value;
                if (!projectFields.Contains(field))
                    projectFields.Add(field);
            }

            var adjListDecoder = new AdjacencyListDecoder2(context.CurrentExecutionOperator, startVertexIndex,
                adjListIndex, revAdjListIndex, true, null, projectFields);
            context.CurrentExecutionOperator = adjListDecoder;

            // Update context's record layout
            foreach (var projectField in projectFields)
            {
                // TODO: Change to correct ColumnGraphType
                context.AddField(edgeAlias, projectField, ColumnGraphType.Value);
            }

            return adjListDecoder;
        }
    }

    partial class WValuesTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            List<int> valuesIdxList = new List<int>();

            foreach (var expression in Parameters)
            {
                var columnReference = expression as WColumnReferenceExpression;
                if (columnReference == null)
                    throw new SyntaxErrorException("Parameters of Values function can only be WColumnReference.");
                valuesIdxList.Add(context.LocateColumnReference(columnReference));
            }

            GraphViewExecutionOperator valuesOperator = new ValuesOperator(context.CurrentExecutionOperator, valuesIdxList);
            context.CurrentExecutionOperator = valuesOperator;
            context.AddField(Alias.Value, "_value", ColumnGraphType.Value);
            
            return valuesOperator;
        }
    }

    partial class WPropertiesTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            List<Tuple<string, int>> propertiesList = new List<Tuple<string, int>>();

            foreach (var expression in Parameters)
            {
                var columnReference = expression as WColumnReferenceExpression;
                if (columnReference == null)
                    throw new SyntaxErrorException("Parameters of Properties function can only be WColumnReference.");
                propertiesList.Add(new Tuple<string, int>(columnReference.ColumnName,
                    context.LocateColumnReference(columnReference)));
            }

            GraphViewExecutionOperator propertiesOp = new PropertiesOperator(context.CurrentExecutionOperator, propertiesList);
            context.CurrentExecutionOperator = propertiesOp;
            context.AddField(Alias.Value, "_value", ColumnGraphType.Value);

            return propertiesOp;
        }
    }

    partial class WDedupTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            var targetField = Parameters[0] as WColumnReferenceExpression;
            if (targetField == null)
                throw new SyntaxErrorException("The parameter of Dedup function can only be a WColumnReference");

            var targetFieldIndex = context.LocateColumnReference(targetField);
            var dedupOp = new DeduplicateOperator(context.CurrentExecutionOperator, targetFieldIndex);
            context.CurrentExecutionOperator = dedupOp;

            return dedupOp;
        }
    }

    partial class WConstantReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            List<string> constantValues = new List<string>();
            foreach (var parameter in Parameters)
            {
                var constantParameter = parameter as WValueExpression;
                if (constantParameter == null)
                    throw new SyntaxErrorException("The parameter of Constant function can only be a WValueExpression");
                constantValues.Add(constantParameter.Value);
            }

            var constantOp = new ConstantOperator(context.CurrentExecutionOperator, constantValues);
            context.CurrentExecutionOperator = constantOp;
            context.AddField(Alias.Value, "_value", ColumnGraphType.Value);

            return constantOp;
        }
    }

    partial class WProjectTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            var projectList = new List<Tuple<ScalarFunction, string>>();
            for (var i = 0; i < Parameters.Count; i += 2)
            {
                var scalarSubquery = Parameters[i] as WScalarSubquery;
                if (scalarSubquery == null)
                    throw new SyntaxErrorException("The parameter of ProjectTableReference at an odd position has to be a WScalarSubquery.");

                var projectName = Parameters[i + 1] as WValueExpression;
                if (projectName == null)
                    throw new SyntaxErrorException("The parameter of ProjectTableReference at an even position has to be a WValueExpression.");

                projectList.Add(
                    new Tuple<ScalarFunction, string>(scalarSubquery.CompileToFunction(context, dbConnection),
                        projectName.Value));
            }

            var projectByOp = new ProjectByOperator(context.CurrentExecutionOperator, projectList);
            context.CurrentExecutionOperator = projectByOp;
            context.AddField(Alias.Value, "_value", ColumnGraphType.Value);

            return projectByOp;
        }
    }

    partial class WRepeatTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WSelectQueryBlock contextSelect, repeatSelect;
            Split(out contextSelect, out repeatSelect);

            List<int> inputIndexes = new List<int>();
            QueryCompilationContext rTableContext = new QueryCompilationContext(context);
            rTableContext.ClearField();

            List<WColumnReferenceExpression> columnList = new List<WColumnReferenceExpression>();
            foreach (WSelectElement selectElement in contextSelect.SelectElements)
            {
                WSelectScalarExpression selectScalar = selectElement as WSelectScalarExpression;
                if (selectScalar == null)
                {
                    throw new SyntaxErrorException("The SELECT elements of the sub-queries in a repeat table reference must be select scalar elements.");
                }

                WColumnReferenceExpression columnRef = selectScalar.SelectExpr as WColumnReferenceExpression;
                if (columnRef == null)
                {
                    throw new SyntaxErrorException("The SELECT elements of the sub-queries in a repeat table reference must be column references.");
                }

                int index;
                if (!context.TryLocateColumnReference(columnRef, out index))
                    index = -1;
                inputIndexes.Add(index);

                string rColumnName = selectScalar.ColumnName ?? columnRef.ColumnName;
                rTableContext.AddField("R", rColumnName, columnRef.ColumnGraphType);

                columnList.Add(new WColumnReferenceExpression(Alias.Value, rColumnName, columnRef.ColumnGraphType));
            }

            GraphViewExecutionOperator innerOp = repeatSelect.Compile(rTableContext, dbConnection);

            WRepeatConditionExpression repeatCondition = Parameters[1] as WRepeatConditionExpression;
            if (repeatCondition == null)
                throw new SyntaxErrorException("The second parameter of a repeat table reference must be WRepeatConditionExpression");

            int repeatTimes = repeatCondition.RepeatTimes;
            BooleanFunction terminationCondition = repeatCondition.TerminationCondition?.CompileToFunction(rTableContext, dbConnection);
            bool startFromContext = repeatCondition.StartFromContext;
            BooleanFunction emitCondition = repeatCondition.EmitCondition?.CompileToFunction(rTableContext, dbConnection);
            bool emitContext = repeatCondition.EmitContext;

            RepeatOperator repeatOp;
            if (repeatTimes == -1)
                repeatOp = new RepeatOperator(context.CurrentExecutionOperator, inputIndexes, innerOp,
                    rTableContext.OuterContextOp, terminationCondition, startFromContext, emitCondition, emitContext);
            else
                repeatOp = new RepeatOperator(context.CurrentExecutionOperator, inputIndexes, innerOp,
                    rTableContext.OuterContextOp, repeatTimes, emitCondition, emitContext);

            context.CurrentExecutionOperator = repeatOp;


            // Updates the raw record layout. The columns of this table-valued function 
            // are specified by the select elements of the thrid parameter.
            foreach (WColumnReferenceExpression columnRef in columnList)
            {
                context.AddField(columnRef.TableReference, columnRef.ColumnName, columnRef.ColumnGraphType);
            }

            return repeatOp;
        }
    }

    partial class WUnfoldTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            var unfoldTargetColumn = Parameters[0] as WColumnReferenceExpression;
            var unfoldTargetColumnIndex = context.LocateColumnReference(unfoldTargetColumn);

            var unfoldOp = new UnfoldOperator(context.CurrentExecutionOperator, unfoldTargetColumnIndex);
            context.CurrentExecutionOperator = unfoldOp;
            context.AddField(Alias.Value, "_value", ColumnGraphType.Value);

            return unfoldOp;
        }
    }

    partial class WPathTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            //<field index, whether this field is a path list needed to be unfolded>
            var pathFieldList = new List<Tuple<int, bool>>();

            foreach (var expression in Parameters)
            {
                var columnReference = expression as WColumnReferenceExpression;
                if (columnReference == null) throw new SyntaxErrorException("Parameters in Path function can only be WColumnReference");

                pathFieldList.Add(new Tuple<int, bool>(context.LocateColumnReference(columnReference),
                    columnReference.ColumnName.Equals("_path")));
            }

            var pathOp = new PathOperator(context.CurrentExecutionOperator, pathFieldList);
            context.CurrentExecutionOperator = pathOp;
            context.AddField(Alias.Value, "_value", ColumnGraphType.Value);

            return pathOp;
        }
    }

    partial class WInjectTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            List<GraphViewExecutionOperator> subQueriesOps = new List<GraphViewExecutionOperator>();
            foreach (var expression in Parameters)
            {
                var subQuery = expression as WScalarSubquery;
                if (subQuery == null) throw new SyntaxErrorException("Parameters in Inject function can only be WScalarSubquery");

                var subContext = new QueryCompilationContext(context);
                // In g.Inject() case, the Inject operator itself is the first operator, so a not-null OuterContextOp is faked here
                if (context.CurrentExecutionOperator == null)
                    subContext.OuterContextOp.SetRef(new RawRecord());
                var subQueryOp = subQuery.SubQueryExpr.Compile(subContext, dbConnection);
                subQueriesOps.Add(subQueryOp);
            }

            InjectOperator injectOp = new InjectOperator(subQueriesOps, context.CurrentExecutionOperator);
            context.CurrentExecutionOperator = injectOp;

            // In g.Inject() case, the inject() step creates a new column in RawRecord
            if (context.RawRecordLayout.Count == 0)
                context.AddField(Alias.Value, "_value", ColumnGraphType.Value);

            return injectOp;
        }
    }

    partial class WStoreTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            var targetFieldParameter = Parameters[0] as WColumnReferenceExpression;
            var targetFieldIndex = context.LocateColumnReference(targetFieldParameter);

            var storedName = (Parameters[1] as WValueExpression).Value;
            var storeOp = new StoreOperator(context.CurrentExecutionOperator, targetFieldIndex);
            context.CurrentExecutionOperator = storeOp;
            context.SideEffectStates.Add(storedName, storeOp.StoreState);

            return storeOp;
        }
    }

    partial class WBarrierTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            var barrierOp = new BarrierOperator(context.CurrentExecutionOperator);
            context.CurrentExecutionOperator = barrierOp;

            return barrierOp;
        }
    }

    partial class WMapTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WScalarSubquery mapSubquery = Parameters[0] as WScalarSubquery;
            if (mapSubquery == null)
            {
                throw new SyntaxErrorException("The input of a map table reference must be a scalar subquery.");
            }
            WSelectQueryBlock mapSelect = mapSubquery.SubQueryExpr as WSelectQueryBlock;
            if (mapSelect == null)
            {
                throw new SyntaxErrorException("The sub-query must be a select query block.");
            }

            QueryCompilationContext subcontext = new QueryCompilationContext(context);
            GraphViewExecutionOperator mapTraversalOp = mapSelect.Compile(subcontext, dbConnection);

            MapOperator mapOp = new MapOperator(context.CurrentExecutionOperator, mapTraversalOp, subcontext.OuterContextOp);
            context.CurrentExecutionOperator = mapOp;

            foreach (WSelectElement selectElement in mapSelect.SelectElements)
            {
                WSelectScalarExpression selectScalar = selectElement as WSelectScalarExpression;
                if (selectScalar == null)
                {
                    throw new SyntaxErrorException("The SELECT elements of the sub-query in a map table reference must be select scalar elements.");
                }
                WColumnReferenceExpression columnRef = selectScalar.SelectExpr as WColumnReferenceExpression;
                if (columnRef == null)
                {
                    throw new SyntaxErrorException("The SELECT elements of the sub-query in a map table reference must be column references.");
                }
                string selectElementAlias = selectScalar.ColumnName;
                context.AddField(Alias.Value, selectElementAlias ?? columnRef.ColumnName, columnRef.ColumnGraphType);
            }

            return mapOp;
        }
    }

    partial class WSideEffectTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WScalarSubquery sideEffectSubquery = Parameters[0] as WScalarSubquery;
            if (sideEffectSubquery == null)
            {
                throw new SyntaxErrorException("The input of a sideEffect table reference must be a scalar subquery.");
            }
            WSelectQueryBlock sideEffectSelect = sideEffectSubquery.SubQueryExpr as WSelectQueryBlock;
            if (sideEffectSelect == null)
            {
                throw new SyntaxErrorException("The sub-query must be a select query block.");
            }

            QueryCompilationContext subcontext = new QueryCompilationContext(context);
            GraphViewExecutionOperator sideEffectTraversalOp = sideEffectSelect.Compile(subcontext, dbConnection);

            SideEffectOperator sideEffectOp = new SideEffectOperator(context.CurrentExecutionOperator, sideEffectTraversalOp, subcontext.OuterContextOp);
            context.CurrentExecutionOperator = sideEffectOp;

            return sideEffectOp;
        }
    }
}

