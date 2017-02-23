using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace GraphView
{
    partial class WSelectQueryBlock
    {
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
            GraphviewRuntimeFunctionCountVisitor runtimeFunctionCountVisitor = new GraphviewRuntimeFunctionCountVisitor();

            foreach (WBooleanExpression predicate in conjunctivePredicates)
            {
                bool isOnlyTargetTableReferenced;
                bool useGraphViewRuntimeFunction = runtimeFunctionCountVisitor.Invoke(predicate) > 0;
                Dictionary<string, HashSet<string>> tableColumnReferences = columnVisitor.Invoke(predicate,
                    vertexAndEdgeAliases, out isOnlyTargetTableReferenced);

                if (useGraphViewRuntimeFunction 
                    || !isOnlyTargetTableReferenced 
                    || !TryAttachPredicate(graphPattern, predicate, tableColumnReferences))
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
        [Obsolete]
        internal static string ConstructMetaFieldSelectClauseOfEdge(MatchEdge edge)
        {
            StringBuilder metaFieldSelectStringBuilder = new StringBuilder();
            bool isStartVertexTheOriginVertex = edge.IsReversed;
            bool isReversedAdjList = IsTraversalThroughPhysicalReverseEdge(edge);
            string nodeId = edge.SourceNode.NodeAlias + ".id";
            string edgeSink = edge.EdgeAlias + "._otherV";
            string edgeId = edge.EdgeAlias + "._offset";
            string edgeReverseId = edge.EdgeAlias + "._reverse_ID";

            string sourceValue = isReversedAdjList ? edgeSink : nodeId;
            string sinkValue = isReversedAdjList ? nodeId : edgeSink;
            string otherValue = isStartVertexTheOriginVertex ? edgeSink : nodeId;
            string edgeIdValue = isReversedAdjList ? edgeReverseId : edgeId;

            metaFieldSelectStringBuilder.Append(", ").Append($"{sourceValue} AS {edge.EdgeAlias}_source");
            metaFieldSelectStringBuilder.Append(", ").Append($"{sinkValue} AS {edge.EdgeAlias}_sink");
            metaFieldSelectStringBuilder.Append(", ").Append($"{otherValue} AS {edge.EdgeAlias}_other");
            metaFieldSelectStringBuilder.Append(", ").Append($"{edgeIdValue} AS {edge.EdgeAlias}_ID");
            metaFieldSelectStringBuilder.Append(", ").Append($"{edgeId} AS {edge.EdgeAlias}_physical_ID");
            metaFieldSelectStringBuilder.Append(", ").Append($"{(isReversedAdjList ? "_reverse_edge" : "_edge")} AS adjType");
            metaFieldSelectStringBuilder.Append(", ").Append($"{edge.EdgeAlias} AS {edge.EdgeAlias}");

            return metaFieldSelectStringBuilder.ToString();
        }

        internal static void ConstructJsonQueryOnNode(MatchNode node, List<MatchEdge> backwardMatchingEdges = null)
        {
            string nodeAlias = node.NodeAlias;
            StringBuilder selectStrBuilder = new StringBuilder();
            StringBuilder joinStrBuilder = new StringBuilder();
            List<string> properties = new List<string> { nodeAlias };
            List<ColumnGraphType> projectedColumnsType = new List<ColumnGraphType>();
           
            WBooleanExpression searchCondition = null;

            properties.Add("id");
            projectedColumnsType.Add(ColumnGraphType.VertexId);
            //selectStrBuilder.Append(nodeAlias + ".id");
            properties.Add("label");
            projectedColumnsType.Add(ColumnGraphType.Value);
            //selectStrBuilder.Append(nodeAlias + ".label");
            properties.Add("_edge");
            projectedColumnsType.Add(ColumnGraphType.OutAdjacencyList);
            //selectStrBuilder.Append(", ").Append(nodeAlias + "._edge");
            properties.Add("_reverse_edge");
            projectedColumnsType.Add(ColumnGraphType.InAdjacencyList);
            //selectStrBuilder.Append(", ").Append(nodeAlias + "._reverse_edge");
            // This takes care of the node.* property
            properties.Add("*");
            projectedColumnsType.Add(ColumnGraphType.VertexObject);
            //selectStrBuilder.Append(", ").Append(nodeAlias);

            selectStrBuilder.Append(nodeAlias);

            for (int i = GraphViewReservedProperties.ReservedNodeProperties.Count; i < node.Properties.Count; i++)
            {
                string selectName = nodeAlias + "." + node.Properties[i];
                properties.Add(node.Properties[i]);
                projectedColumnsType.Add(ColumnGraphType.Value);
                //selectStrBuilder.Append(", ").Append(selectName);
            }
                
            foreach (WBooleanExpression predicate in node.Predicates)
                searchCondition = WBooleanBinaryExpression.Conjunction(searchCondition, predicate);

            if (backwardMatchingEdges == null)
                backwardMatchingEdges = new List<MatchEdge>();

            //
            // Currently, no backwardMatchingEdges will be produced
            //
            foreach (MatchEdge edge in backwardMatchingEdges)
            {
                joinStrBuilder.Append(" Join ")
                    .Append(edge.EdgeAlias)
                    .Append(" in ")
                    .Append(node.NodeAlias)
                    .Append(IsTraversalThroughPhysicalReverseEdge(edge) ? "._reverse_edge" : "_edge");

                // TODO: Use the same offset in _edge and _reverse_edge
                selectStrBuilder.Append(ConstructMetaFieldSelectClauseOfEdge(edge));
                properties.Add(edge.EdgeAlias + "_source");
                projectedColumnsType.Add(ColumnGraphType.EdgeSource);
                properties.Add(edge.EdgeAlias + "_sink");
                projectedColumnsType.Add(ColumnGraphType.EdgeSink);
                properties.Add(edge.EdgeAlias + "_other");
                projectedColumnsType.Add(ColumnGraphType.Value);
                properties.Add(edge.EdgeAlias + "_ID");
                projectedColumnsType.Add(ColumnGraphType.EdgeOffset);
                properties.Add(edge.EdgeAlias + "_physical_ID");
                projectedColumnsType.Add(ColumnGraphType.EdgeOffset);
                // This adjType is used for notifying GetVertice that the following edgeField should be retrieved from _edge or _reverse_edge
                properties.Add("adjType");
                projectedColumnsType.Add(ColumnGraphType.Value);
                // This takes care of the edge.* property
                properties.Add(edge.EdgeAlias);
                projectedColumnsType.Add(ColumnGraphType.EdgeObject);

                for (int i = GraphViewReservedProperties.ReservedEdgeProperties.Count; i < edge.Properties.Count; i++)
                {
                    string property = edge.Properties[i];
                    //var selectName = edge.EdgeAlias + "." + property;
                    //var selectAlias = edge.EdgeAlias + "_" + property;

                    projectedColumnsType.Add(ColumnGraphType.Value);
                        
                    //selectStrBuilder.Append(", ").Append(string.Format("{0} AS {1}", selectName, selectAlias));
                    properties.Add(property);
                }   

                foreach (WBooleanExpression predicate in edge.Predicates)
                    searchCondition = WBooleanBinaryExpression.Conjunction(searchCondition, predicate);
            }

            BooleanWValueExpressionVisitor booleanWValueExpressionVisitor = new BooleanWValueExpressionVisitor();
            booleanWValueExpressionVisitor.Invoke(searchCondition);

            JsonQuery jsonQuery = new JsonQuery
            {
                Alias = nodeAlias,
                JoinClause = joinStrBuilder.ToString(),
                SelectClause = selectStrBuilder.ToString(),
                WhereSearchCondition = searchCondition != null ? searchCondition.ToString() : null,
                Properties = properties,
                ProjectedColumnsType = projectedColumnsType,
            };
            node.AttachedJsonQuery = jsonQuery;
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
                                SrcNode.Properties = new List<string>(GraphViewReservedProperties.ReservedNodeProperties);
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
                                    Properties = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties),
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
                                    Properties = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties),
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
                                        Properties = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties),
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
                            DestNode.Properties = new List<string>(GraphViewReservedProperties.ReservedNodeProperties);
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
                                    Properties = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties),
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
                    patternNode.Properties = new List<string>(GraphViewReservedProperties.ReservedNodeProperties);
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
                        edge.IsDanglingEdge = true;
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
                        edge.IsDanglingEdge = true;
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
                    childrenProcessor.Add(
                        new FilterOperator(
                            childrenProcessor.Count != 0 
                            ? childrenProcessor.Last() 
                            : context.OuterContextOp,
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

            foreach (var edge in edges)
            {
                var edgeIndexTuple = LocateAdjacencyListIndexes(context, edge);
                var localEdgeContext = GenerateLocalContextForAdjacentListDecoder(edge.EdgeAlias, edge.Properties);
                var edgePredicates = edge.RetrievePredicatesExpression();
                operatorChain.Add(new AdjacencyListDecoder2(
                    operatorChain.Last(),
                    context.LocateColumnReference(edge.SourceNode.NodeAlias, "id"),
                    context.LocateColumnReference(edge.SourceNode.NodeAlias, "label"),
                    edgeIndexTuple.Item1, edgeIndexTuple.Item2, !edge.IsReversed,
                    edgePredicates != null ? edgePredicates.CompileToFunction(localEdgeContext, connection) : null,
                    edge.Properties, connection));
                context.CurrentExecutionOperator = operatorChain.Last();

                // Update edge's context info
                tableReferences.Add(edge.EdgeAlias, TableGraphType.Edge);
                UpdateEdgeLayout(edge.EdgeAlias, edge.Properties, context);

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
                    context.CurrentExecutionOperator = operatorChain.Last();
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
            var node = backwardMatchingEdges[0].SourceNode;
            UpdateNodeLayout(node.NodeAlias, node.Properties, localContext);

            var matchingIndexes = new List<Tuple<int, int>>();
            foreach (var backwardMatchingEdge in backwardMatchingEdges)
            {
                // backwardEdges.SinkNode.id = backwardEdges.traversalColumn
                var sourceMatchIndex =
                    context.RawRecordLayout[new WColumnReferenceExpression(backwardMatchingEdge.SinkNode.NodeAlias, "id")];

                UpdateEdgeLayout(backwardMatchingEdge.EdgeAlias, backwardMatchingEdge.Properties, localContext);

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

        private void UpdateNodeLayout(string nodeAlias, List<string> properties, QueryCompilationContext context)
        {
            context.AddField(nodeAlias, "id", ColumnGraphType.VertexId);
            context.AddField(nodeAlias, "label", ColumnGraphType.Value);
            context.AddField(nodeAlias, "_edge", ColumnGraphType.OutAdjacencyList);
            context.AddField(nodeAlias, "_reverse_edge", ColumnGraphType.InAdjacencyList);
            context.AddField(nodeAlias, "*", ColumnGraphType.VertexObject);
            for (var i = GraphViewReservedProperties.ReservedNodeProperties.Count; i < properties.Count; i++)
                context.AddField(nodeAlias, properties[i], ColumnGraphType.Value);
        }

        private void UpdateEdgeLayout(string edgeAlias, List<string> properties, QueryCompilationContext context)
        {
            // Update context's record layout
            context.AddField(edgeAlias, "_source", ColumnGraphType.EdgeSource);
            context.AddField(edgeAlias, "_sink", ColumnGraphType.EdgeSink);
            context.AddField(edgeAlias, "_other", ColumnGraphType.Value);
            context.AddField(edgeAlias, "_offset", ColumnGraphType.EdgeOffset);
            context.AddField(edgeAlias, "*", ColumnGraphType.EdgeObject);
            for (var i = GraphViewReservedProperties.ReservedEdgeProperties.Count; i < properties.Count; i++)
            {
                context.AddField(edgeAlias, properties[i], ColumnGraphType.Value);
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
            {
                context.CurrentExecutionOperator = context.OuterContextOp;
                CheckRemainingPredicatesAndAppendFilterOp(context, connection,
                    new HashSet<string>(tableReferences.Keys), predicatesAccessedTableReferences,
                    operatorChain);
            }
                

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
                        UpdateNodeLayout(sourceNode.NodeAlias, sourceNode.Properties, context);
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
                            context.CurrentExecutionOperator = operatorChain.Last();

                            // Update sinkNode's context info
                            processedNodes.Add(sinkNode);
                            UpdateNodeLayout(sinkNode.NodeAlias, sinkNode.Properties, context);
                            tableReferences.Add(sinkNode.NodeAlias, TableGraphType.Vertex);

                            // Update backwardEdges' context info
                            foreach (var backwardMatchingEdge in backwardMatchingEdges)
                            {
                                tableReferences.Add(backwardMatchingEdge.EdgeAlias, TableGraphType.Edge);
                                UpdateEdgeLayout(backwardMatchingEdge.EdgeAlias, backwardMatchingEdge.Properties, context);
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
                    var derivedTableOp = tableReference.Compile(context, connection);
                    operatorChain.Add(derivedTableOp);
                    
                    // TODO: Change to correct ColumnGraphType
                    tableReferences.Add(tableReference.Alias.Value, TableGraphType.Vertex);
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
                // If operatorChain is empty and OuterContextOp is null, this is a SelectQueryBlock only selects WValueExpression
                // and a ConstantSource is faked as the input
                var projectOperator =
                    new ProjectOperator(operatorChain.Any()
                        ? operatorChain.Last()
                        : (context.OuterContextOp ?? new ConstantSourceOperator {ConstantSource = new RawRecord()}));

                // When CarryOn is set, in addition to the SELECT elements in the SELECT clause,
                // the query also projects fields from its parent context.
                if (context.CarryOn)
                {
                    foreach (var fieldPair in context.ParentContextRawRecordLayout.OrderBy(e => e.Value))
                    {
                        FieldValue fieldSelectFunc = new FieldValue(fieldPair.Value);
                        projectOperator.AddSelectScalarElement(fieldSelectFunc);
                    }
                }

                foreach (var expr in selectScalarExprList)
                {
                    ScalarFunction scalarFunction = expr.SelectExpr.CompileToFunction(context, connection);
                    projectOperator.AddSelectScalarElement(scalarFunction);
                }

                // Rebuilds the output layout of the context
                context.ClearField();
                int i = 0;
                if (context.CarryOn)
                {
                    foreach (var parentFieldPair in context.ParentContextRawRecordLayout)
                    {
                        context.RawRecordLayout.Add(parentFieldPair.Key, parentFieldPair.Value);
                    }
                    i = context.ParentContextRawRecordLayout.Count;
                }

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
                ProjectAggregation projectAggregationOp = new ProjectAggregation(operatorChain.Any()
                        ? operatorChain.Last()
                        : context.OuterContextOp);

                // When CarryOn is set, in addition to the SELECT elements in the SELECT clause,
                // the query is supposed to project fields from its parent context. 
                // But since this query contains aggregations, all the fields from the parent context
                // are set to null in the output. 
                if (context.CarryOn)
                {
                    foreach (var fieldPair in context.ParentContextRawRecordLayout.OrderBy(e => e.Value))
                    {
                        FieldValue fieldSelectFunc = new FieldValue(fieldPair.Value);
                        projectAggregationOp.AddAggregateSpec(null, null);
                    }
                }

                foreach (var selectScalar in selectScalarExprList)
                {
                    WFunctionCall fcall = selectScalar.SelectExpr as WFunctionCall;

                    if (fcall == null)
                    {
                        projectAggregationOp.AddAggregateSpec(null, null);
                    }

                    switch (fcall.FunctionName.Value.ToUpper())
                    {
                        case "COUNT":
                            projectAggregationOp.AddAggregateSpec(new CountFunction(), new List<ScalarFunction>());
                            break;
                        case "FOLD":
                            var foldedFunction = fcall.Parameters[0] as WFunctionCall;
                            if (foldedFunction == null)
                                throw new SyntaxErrorException("The parameter of a Fold function must be a Compose1 function.");
                            projectAggregationOp.AddAggregateSpec(new FoldFunction(), 
                                new List<ScalarFunction> { foldedFunction.CompileToFunction(context, connection), });
                            break;
                        case "TREE":
                            var pathField = fcall.Parameters[0] as WColumnReferenceExpression;
                            var pathFieldIndex = context.LocateColumnReference(pathField);
                            projectAggregationOp.AddAggregateSpec(
                                new TreeFunction(), 
                                new List<ScalarFunction>() { new FieldValue(pathFieldIndex) });
                            break;
                        case "CAP":
                            CapAggregate capAggregate = new CapAggregate();
                            for (int i = 0; i < fcall.Parameters.Count; i += 2)
                            {
                                WColumnNameList columnNameList = fcall.Parameters[i] as WColumnNameList;
                                WValueExpression capName = fcall.Parameters[i+1] as WValueExpression;

                                List<IAggregateFunction> sideEffectStateList;
                                if (!context.SideEffectStates.TryGetValue(capName.Value, out sideEffectStateList))
                                    throw new GraphViewException("SideEffect state " + capName + " doesn't exist in the context");
                                capAggregate.AddCapatureSideEffectState(capName.Value, sideEffectStateList);
                            }
                            projectAggregationOp.AddAggregateSpec(capAggregate, new List<ScalarFunction>());
                            break;
                        default:
                            projectAggregationOp.AddAggregateSpec(null, null);
                            break;
                    }
                }

                // Rebuilds the output layout of the context
                context.ClearField();

                if (context.CarryOn)
                {
                    foreach (var parentFieldPair in context.ParentContextRawRecordLayout)
                    {
                        context.RawRecordLayout.Add(parentFieldPair.Key, parentFieldPair.Value);
                    }
                }

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
    }

    partial class WWithPathClause
    {
        //internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        //{
        //    foreach (var path in Paths)
        //    {
        //        //path.Item2.SelectElements = new List<WSelectElement>();
        //        PathOperators.Add(new Tuple<string, GraphViewExecutionOperator, int>(path.Item1,
        //            path.Item2.Generate(dbConnection), path.Item3));
        //    }
        //    if (PathOperators.Count != 0) return PathOperators.First().Item2;
        //    else return null;
        //}
    }

    partial class WChoose
    {
        //internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        //{
        //    List<GraphViewExecutionOperator> Source = new List<GraphViewExecutionOperator>();
        //    foreach (var x in InputExpr)
        //    {
        //        Source.Add(x.Generate(dbConnection));
        //    }
        //    return new ConcatenateOperator(Source);
        //}
    }

    partial class WCoalesce
    {
        //internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        //{
        //    List<GraphViewExecutionOperator> Source = new List<GraphViewExecutionOperator>();
        //    foreach (var x in InputExpr)
        //    {
        //        Source.Add(x.Generate(dbConnection));
        //    }
        //    var op = new CoalesceOperator(Source, CoalesceNumber);
        //    return new OutputOperator(op, op.header, null);
        //}
    }

    partial class WSqlBatch
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            QueryCompilationContext priorContext = new QueryCompilationContext();
            GraphViewExecutionOperator op = null;
            foreach (WSqlStatement st in Statements)
            {
                QueryCompilationContext statementContext = new QueryCompilationContext(priorContext.TemporaryTableCollection, 
                    priorContext.SideEffectStates);
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
            ContainerOperator containerOp = new ContainerOperator(context.CurrentExecutionOperator);

            UnionOperator unionOp = new UnionOperator(context.CurrentExecutionOperator);

            bool isUnionWithoutAnyBranch = Parameters[0] is WValueExpression;

            WSelectQueryBlock firstSelectQuery = null;
            if (!isUnionWithoutAnyBranch)
            {
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
                    subcontext.CarryOn = true;
                    GraphViewExecutionOperator traversalOp = scalarSubquery.SubQueryExpr.Compile(subcontext, dbConnection);
                    subcontext.OuterContextOp.SourceEnumerator = containerOp.GetEnumerator();
                    unionOp.AddTraversal(subcontext.OuterContextOp, traversalOp);
                }
            }

            // Updates the raw record layout. The columns of this table-valued function 
            // are specified by the select elements of the input subqueries.
            if (!isUnionWithoutAnyBranch)
            {
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
                    if (columnRef.ColumnType == ColumnType.Wildcard)
                        continue;
                    string selectElementAlias = selectScalar.ColumnName;
                    context.AddField(Alias.Value, selectElementAlias ?? columnRef.ColumnName, columnRef.ColumnGraphType);
                }
            }
            else
            {
                foreach (WScalarExpression parameter in Parameters)
                {
                    WValueExpression columnName = parameter as WValueExpression;
                    context.AddField(Alias.Value, columnName.Value, ColumnGraphType.Value);
                }
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

                if (columnRef != null)
                {
                    int index;
                    if (!context.TryLocateColumnReference(columnRef, out index))
                        throw new SyntaxErrorException("Syntax Error!!!");
                    inputIndexes.Add(index);

                    columnList.Add(
                        new Tuple<WColumnReferenceExpression, string>(
                            new WColumnReferenceExpression(Alias.Value, selectScalar.ColumnName ?? columnRef.ColumnName,
                                columnRef.ColumnGraphType), selectScalar.ColumnName));
                }
                else
                {
                    WValueExpression nullExpression = selectScalar.SelectExpr as WValueExpression;
                    if (nullExpression == null)
                        throw new SyntaxErrorException("The SELECT elements of the sub-queries in a optional table reference must be column references or WValueExpression.");
                    if (nullExpression.ToString().Equals("null", StringComparison.OrdinalIgnoreCase))
                        inputIndexes.Add(-1);

                    columnList.Add(
                        new Tuple<WColumnReferenceExpression, string>(
                            new WColumnReferenceExpression(Alias.Value, selectScalar.ColumnName, ColumnGraphType.Value),
                            selectScalar.ColumnName));
                }
            }

            QueryCompilationContext subcontext = new QueryCompilationContext(context);
            bool isCarryOnMode = false;
            if (HasAggregateFunctionInTheOptionalSelectQuery(optionalSelect))
            {
                isCarryOnMode = true;
                ContainerOperator containerOp = new ContainerOperator(context.CurrentExecutionOperator);
                subcontext.CarryOn = true;
                subcontext.OuterContextOp.SourceEnumerator = containerOp.GetEnumerator();
            }

            GraphViewExecutionOperator optionalTraversalOp = optionalSelect.Compile(subcontext, dbConnection);

            //OptionalOperator optionalOp = new OptionalOperator(context.CurrentExecutionOperator, inputIndexes, optionalTraversalOp, subcontext.OuterContextOp);
            OptionalOperator optionalOp = new OptionalOperator(context.CurrentExecutionOperator, inputIndexes,
                optionalTraversalOp, subcontext.OuterContextOp, isCarryOnMode);
            context.CurrentExecutionOperator = optionalOp;

            // Updates the raw record layout. The columns of this table-valued function 
            // are specified by the select elements of the input subqueries.
            foreach (Tuple<WColumnReferenceExpression, string> tuple in columnList)
            {
                WColumnReferenceExpression columnRef = tuple.Item1;
                string selectElementAlias = tuple.Item2;
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
                Properties = new List<string>(GraphViewReservedProperties.ReservedNodeProperties),
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
                context.AddField(nodeAlias, "id", ColumnGraphType.VertexId);
                context.AddField(nodeAlias, "label", ColumnGraphType.Value);
                context.AddField(nodeAlias, "_edge", ColumnGraphType.OutAdjacencyList);
                context.AddField(nodeAlias, "_reverse_edge", ColumnGraphType.InAdjacencyList);
                context.AddField(nodeAlias, "*", ColumnGraphType.VertexObject);
                for (var i = GraphViewReservedProperties.ReservedNodeProperties.Count; i < matchNode.Properties.Count; i++)
                    context.AddField(nodeAlias, matchNode.Properties[i], ColumnGraphType.Value);
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
                Properties = new List<string>(GraphViewReservedProperties.ReservedNodeProperties),
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
                context.AddField(nodeAlias, "id", ColumnGraphType.VertexId);
                context.AddField(nodeAlias, "label", ColumnGraphType.Value);
                context.AddField(nodeAlias, "_edge", ColumnGraphType.OutAdjacencyList);
                context.AddField(nodeAlias, "_reverse_edge", ColumnGraphType.InAdjacencyList);
                context.AddField(nodeAlias, "*", ColumnGraphType.VertexObject);
                for (var i = GraphViewReservedProperties.ReservedNodeProperties.Count; i < matchNode.Properties.Count; i++)
                    context.AddField(nodeAlias, matchNode.Properties[i], ColumnGraphType.Value);
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
            var startVertexLabelParameter = Parameters[2] as WColumnReferenceExpression;
            
            var startVertexIndex = context.LocateColumnReference(startVertexIdParameter);
            var startVertexLabelIndex = context.LocateColumnReference(startVertexLabelParameter);
            var adjListIndex = context.LocateColumnReference(adjListParameter);

            var edgeAlias = Alias.Value;
            var projectFields = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties);

            for (int i = 3; i < Parameters.Count; i++)
            {
                var field = (Parameters[i] as WValueExpression).Value;
                if (!projectFields.Contains(field))
                    projectFields.Add(field);
            }

            var adjListDecoder = new AdjacencyListDecoder2(context.CurrentExecutionOperator, startVertexIndex, startVertexLabelIndex,
                adjListIndex, -1, true, null, projectFields, dbConnection);
            context.CurrentExecutionOperator = adjListDecoder;

            // Update context's record layout
            context.AddField(edgeAlias, "_source", ColumnGraphType.EdgeSource);
            context.AddField(edgeAlias, "_sink", ColumnGraphType.EdgeSink);
            context.AddField(edgeAlias, "_other", ColumnGraphType.Value);
            context.AddField(edgeAlias, "_offset", ColumnGraphType.EdgeOffset);
            context.AddField(edgeAlias, "*", ColumnGraphType.EdgeObject);
            for (var i = GraphViewReservedProperties.ReservedEdgeProperties.Count; i < projectFields.Count; i++)
            {
                context.AddField(edgeAlias, projectFields[i], ColumnGraphType.Value);
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
            var startVertexLabelParameter = Parameters[2] as WColumnReferenceExpression;

            var startVertexIndex = context.LocateColumnReference(startVertexIdParameter);
            var startVertexLabelIndex = context.LocateColumnReference(startVertexLabelParameter);
            var revAdjListIndex = context.LocateColumnReference(revAdjListParameter);

            var edgeAlias = Alias.Value;
            var projectFields = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties);

            for (int i = 3; i < Parameters.Count; i++)
            {
                var field = (Parameters[i] as WValueExpression).Value;
                if (!projectFields.Contains(field))
                    projectFields.Add(field);
            }

            var adjListDecoder = new AdjacencyListDecoder2(context.CurrentExecutionOperator, startVertexIndex, startVertexLabelIndex,
               - 1, revAdjListIndex, true, null, projectFields, dbConnection);
            context.CurrentExecutionOperator = adjListDecoder;

            // Update context's record layout
            context.AddField(edgeAlias, "_source", ColumnGraphType.EdgeSource);
            context.AddField(edgeAlias, "_sink", ColumnGraphType.EdgeSink);
            context.AddField(edgeAlias, "_other", ColumnGraphType.Value);
            context.AddField(edgeAlias, "_offset", ColumnGraphType.EdgeOffset);
            context.AddField(edgeAlias, "*", ColumnGraphType.EdgeObject);
            for (var i = GraphViewReservedProperties.ReservedEdgeProperties.Count; i < projectFields.Count; i++)
            {
                context.AddField(edgeAlias, projectFields[i], ColumnGraphType.Value);
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
            var startVertexLabelParameter = Parameters[3] as WColumnReferenceExpression;

            var startVertexIndex = context.LocateColumnReference(startVertexIdParameter);
            var startVertexLabelIndex = context.LocateColumnReference(startVertexLabelParameter);
            var adjListIndex = context.LocateColumnReference(adjListParameter);
            var revAdjListIndex = context.LocateColumnReference(revAdjListParameter);

            var edgeAlias = Alias.Value;
            var projectFields = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties);

            for (int i = 4; i < Parameters.Count; i++)
            {
                var field = (Parameters[i] as WValueExpression).Value;
                if (!projectFields.Contains(field))
                    projectFields.Add(field);
            }

            var adjListDecoder = new AdjacencyListDecoder2(context.CurrentExecutionOperator, startVertexIndex, startVertexLabelIndex,
                adjListIndex, revAdjListIndex, true, null, projectFields, dbConnection);
            context.CurrentExecutionOperator = adjListDecoder;

            // Update context's record layout
            context.AddField(edgeAlias, "_source", ColumnGraphType.EdgeSource);
            context.AddField(edgeAlias, "_sink", ColumnGraphType.EdgeSink);
            context.AddField(edgeAlias, "_other", ColumnGraphType.Value);
            context.AddField(edgeAlias, "_offset", ColumnGraphType.EdgeOffset);
            context.AddField(edgeAlias, "*", ColumnGraphType.EdgeObject);
            for (var i = GraphViewReservedProperties.ReservedEdgeProperties.Count; i < projectFields.Count; i++)
            {
                context.AddField(edgeAlias, projectFields[i], ColumnGraphType.Value);
            }

            return adjListDecoder;
        }
    }

    partial class WValuesTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            List<int> valuesIdxList = new List<int>();
            int allValuesIndex = -1;

            if (Parameters.Count == 1 &&
                (Parameters[0] as WColumnReferenceExpression).ColumnName.Equals("*", StringComparison.OrdinalIgnoreCase))
            {
                allValuesIndex = context.LocateColumnReference(Parameters[0] as WColumnReferenceExpression);
            }
            foreach (var expression in Parameters)
            {
                var columnReference = expression as WColumnReferenceExpression;
                if (columnReference == null)
                    throw new SyntaxErrorException("Parameters of Values function can only be WColumnReference.");
                valuesIdxList.Add(context.LocateColumnReference(columnReference));
            }

            GraphViewExecutionOperator valuesOperator = new ValuesOperator(context.CurrentExecutionOperator, valuesIdxList, allValuesIndex);
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
            int allPropertyIndex = -1;

            if (Parameters.Count == 1 &&
                (Parameters[0] as WColumnReferenceExpression).ColumnName.Equals("*", StringComparison.OrdinalIgnoreCase))
            {
                allPropertyIndex = context.LocateColumnReference(Parameters[0] as WColumnReferenceExpression);
            }
            else
            {
                foreach (var expression in Parameters)
                {
                    var columnReference = expression as WColumnReferenceExpression;
                    if (columnReference == null)
                        throw new SyntaxErrorException("Parameters of Properties function can only be WColumnReference.");

                    propertiesList.Add(new Tuple<string, int>(columnReference.ColumnName,
                        context.LocateColumnReference(columnReference)));
                }
            }

            GraphViewExecutionOperator propertiesOp = new PropertiesOperator(context.CurrentExecutionOperator, propertiesList, allPropertyIndex);
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
            var projectByOp = new ProjectByOperator(context.CurrentExecutionOperator);
            for (var i = 0; i < Parameters.Count; i += 2)
            {
                var scalarSubquery = Parameters[i] as WScalarSubquery;
                if (scalarSubquery == null)
                    throw new SyntaxErrorException("The parameter of ProjectTableReference at an odd position has to be a WScalarSubquery.");

                var projectName = Parameters[i + 1] as WValueExpression;
                if (projectName == null)
                    throw new SyntaxErrorException("The parameter of ProjectTableReference at an even position has to be a WValueExpression.");

                QueryCompilationContext subcontext = new QueryCompilationContext(context);
                GraphViewExecutionOperator projectOp = scalarSubquery.SubQueryExpr.Compile(subcontext, dbConnection);

                projectByOp.AddProjectBy(subcontext.OuterContextOp, projectOp, projectName.Value);
            }

            context.CurrentExecutionOperator = projectByOp;
            context.AddField(Alias.Value, "_value", ColumnGraphType.Value);

            for (var i = 0; i < Parameters.Count; i += 2)
            {
                var scalarSubquery = Parameters[i] as WScalarSubquery;
                if (scalarSubquery == null)
                    throw new SyntaxErrorException("The parameter of ProjectTableReference at an odd position has to be a WScalarSubquery.");
                var selectQuery = scalarSubquery.SubQueryExpr as WSelectQueryBlock;
                if (selectQuery == null)
                {
                    throw new SyntaxErrorException("The input of a project table reference must be one or more select query blocks.");
                }

                for (var j = 1; j < selectQuery.SelectElements.Count; j++)
                {
                    var scalarExpr = selectQuery.SelectElements[j] as WSelectScalarExpression;
                    var alias = scalarExpr.ColumnName;

                    // TODO: Change to correct ColumnGraphType
                    context.AddField(Alias.Value, alias, ColumnGraphType.Value);
                }
            }

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
                if (columnRef != null)
                {
                    int index;
                    if (!context.TryLocateColumnReference(columnRef, out index))
                        throw new SyntaxErrorException("Syntax Error!!!");
                    inputIndexes.Add(index);

                    string rColumnName = selectScalar.ColumnName ?? columnRef.ColumnName;
                    rTableContext.AddField("R", rColumnName, columnRef.ColumnGraphType);

                    columnList.Add(new WColumnReferenceExpression(Alias.Value, rColumnName, columnRef.ColumnGraphType));
                }
                else
                {
                    WValueExpression nullExpression = selectScalar.SelectExpr as WValueExpression;
                    if (nullExpression == null)
                        throw new SyntaxErrorException("The SELECT elements of the sub-queries in a repeat table reference must be column references or WValueExpression.");
                    if (nullExpression.ToString().Equals("null", StringComparison.OrdinalIgnoreCase))
                        inputIndexes.Add(-1);

                    string rColumnName = selectScalar.ColumnName ?? columnRef.ColumnName;
                    rTableContext.AddField("R", rColumnName, ColumnGraphType.Value);

                    columnList.Add(new WColumnReferenceExpression(Alias.Value, rColumnName, ColumnGraphType.Value));
                }
            }

            WRepeatConditionExpression repeatCondition = Parameters[1] as WRepeatConditionExpression;
            if (repeatCondition == null)
                throw new SyntaxErrorException("The second parameter of a repeat table reference must be WRepeatConditionExpression");

            int repeatTimes = repeatCondition.RepeatTimes;
            BooleanFunction terminationCondition = repeatCondition.TerminationCondition?.CompileToFunction(rTableContext, dbConnection);
            bool startFromContext = repeatCondition.StartFromContext;
            BooleanFunction emitCondition = repeatCondition.EmitCondition?.CompileToFunction(rTableContext, dbConnection);
            bool emitContext = repeatCondition.EmitContext;

            GraphViewExecutionOperator innerOp = repeatSelect.Compile(rTableContext, dbConnection);

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
            var unfoldColumns = new List<string>();
            for (var i = 1; i < Parameters.Count; i++)
            {
                var unfoldColumn = Parameters[i] as WValueExpression;
                unfoldColumns.Add(unfoldColumn.Value);
            }

            var unfoldOp = new UnfoldOperator(context.CurrentExecutionOperator,
                Parameters[0].CompileToFunction(context, dbConnection), unfoldColumns);
            context.CurrentExecutionOperator = unfoldOp;

            for (var i = 1; i < Parameters.Count; i++)
            {
                var columnName = (Parameters[i] as WValueExpression).Value;
                // TODO: Change to correct ColumnGraphType
                context.AddField(Alias.Value, columnName, ColumnGraphType.Value);
            }

            //context.AddField(Alias.Value, "_value", ColumnGraphType.Value);

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
                    subContext.OuterContextOp.ConstantSource = new RawRecord();
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
            WFunctionCall targetFieldParameter = Parameters[0] as WFunctionCall;
            if (targetFieldParameter == null)
                throw new SyntaxErrorException("The first parameter of a Store function must be a Compose1 function.");
            ScalarFunction getTargetFieldFunction = targetFieldParameter.CompileToFunction(context, dbConnection);

            string storedName = (Parameters[1] as WValueExpression).Value;
            StoreOperator storeOp = new StoreOperator(context.CurrentExecutionOperator, getTargetFieldFunction);
            context.CurrentExecutionOperator = storeOp;

            List<IAggregateFunction> sideEffectList;
            if (!context.SideEffectStates.TryGetValue(storedName, out sideEffectList))
            {
                sideEffectList = new List<IAggregateFunction>();
                context.SideEffectStates.Add(storedName, sideEffectList);
            }
            sideEffectList.Add(storeOp.StoreState);

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

    partial class WKeyTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression propertyField = Parameters[0] as WColumnReferenceExpression;

            PropertyKeyOperator keyOp = new PropertyKeyOperator(context.CurrentExecutionOperator,
                context.LocateColumnReference(propertyField));
            context.CurrentExecutionOperator = keyOp;
            context.AddField(Alias.Value, "_value", ColumnGraphType.Value);

            return keyOp;
        }
    }

    partial class WValueTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression propertyField = Parameters[0] as WColumnReferenceExpression;

            PropertyValueOperator valueOp = new PropertyValueOperator(context.CurrentExecutionOperator,
                context.LocateColumnReference(propertyField));
            context.CurrentExecutionOperator = valueOp;
            context.AddField(Alias.Value, "_value", ColumnGraphType.Value);

            return valueOp;
        }
    }

    partial class WGroupTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WScalarSubquery groupKeySubQuery = Parameters[1] as WScalarSubquery;
            WColumnReferenceExpression groupKeyColumnReference = Parameters[1] as WColumnReferenceExpression;
            WScalarSubquery aggregateSubQuery = Parameters[2] as WScalarSubquery;
            WColumnReferenceExpression elementPropertyProjection = Parameters[2] as WColumnReferenceExpression;

            if (groupKeySubQuery == null && groupKeyColumnReference == null)
                throw new SyntaxErrorException("The group key parameter of group table can only be WScalarSubquery or WColumnReferenceExpression.");
            if (aggregateSubQuery == null && elementPropertyProjection == null)
                throw new SyntaxErrorException("The group value parameter of group table can only be WScalarSubquery or WColumnReferenceExpression.");

            int groupKeyFieldIndex = groupKeyColumnReference == null
                                     ? -1
                                     : context.LocateColumnReference(groupKeyColumnReference);

            int elementPropertyProjectionIndex = elementPropertyProjection == null 
                                                 ? -1 
                                                 : context.LocateColumnReference(elementPropertyProjection);

            ScalarFunction groupKeyFunction = groupKeySubQuery?.CompileToFunction(context, dbConnection);

            QueryCompilationContext subcontext = new QueryCompilationContext(context);
            ConstantSourceOperator tempSourceOp = new ConstantSourceOperator();
            ContainerOperator aggregatedSourceOp = new ContainerOperator(tempSourceOp);
            GraphViewExecutionOperator aggregateOp = aggregateSubQuery?.SubQueryExpr.Compile(subcontext, dbConnection);
            subcontext.OuterContextOp.SourceEnumerator = aggregatedSourceOp.GetEnumerator();

            WValueExpression groupParameter = Parameters[0] as WValueExpression;
            if (!groupParameter.SingleQuoted && groupParameter.Value.Equals("null", StringComparison.OrdinalIgnoreCase))
            {
                GroupOperator groupOp = new GroupOperator(
                    context.CurrentExecutionOperator, 
                    groupKeyFunction, groupKeyFieldIndex,
                    tempSourceOp, aggregatedSourceOp, aggregateOp, 
                    elementPropertyProjectionIndex, 
                    context.CarryOn ? context.RawRecordLayout.Count : -1);

                context.CurrentExecutionOperator = groupOp;

                if (!context.CarryOn)
                    context.ClearField();
                // Change to correct ColumnGraphType
                context.AddField(Alias.Value, "_value", ColumnGraphType.Value);

                return groupOp;
            }
            else
            {
                GroupSideEffectOperator groupSideEffectOp = new GroupSideEffectOperator(
                    context.CurrentExecutionOperator, 
                    groupKeyFunction, groupKeyFieldIndex,
                    tempSourceOp, aggregatedSourceOp, aggregateOp,
                    elementPropertyProjectionIndex);

                context.CurrentExecutionOperator = groupSideEffectOp;

                List<IAggregateFunction> sideEffectList;
                if (!context.SideEffectStates.TryGetValue(groupParameter.Value, out sideEffectList))
                {
                    sideEffectList = new List<IAggregateFunction>();
                    context.SideEffectStates.Add(groupParameter.Value, sideEffectList);
                }
                sideEffectList.Add(groupSideEffectOp.GroupState);

                return groupSideEffectOp;
            }
        }
    }

    partial class WQueryDerivedTable
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WSelectQueryBlock derivedSelectQueryBlock = QueryExpr as WSelectQueryBlock;
            if (derivedSelectQueryBlock == null)
                throw new SyntaxErrorException("The QueryExpr of a WQueryDerviedTable must be one select query block.");

            QueryCompilationContext derivedTableContext = new QueryCompilationContext(context);

            // If QueryDerivedTable is the first table in the whole script
            if (context.CurrentExecutionOperator == null)
                derivedTableContext.OuterContextOp = null;
            else
            {
                derivedTableContext.CarryOn = true;

                // For Union and Optional's semantics, e.g. g.V().union(__.count())
                if (context.CarryOn)
                {
                    ContainerOperator containerOp = new ContainerOperator(context.CurrentExecutionOperator);
                    derivedTableContext.OuterContextOp.SourceEnumerator = containerOp.GetEnumerator();
                }
                // e.g. g.V().coalesce(__.count())
                else
                {
                    derivedTableContext.OuterContextOp = context.OuterContextOp;
                }
            }

            GraphViewExecutionOperator subQueryOp = derivedSelectQueryBlock.Compile(derivedTableContext, dbConnection);

            QueryDerivedTableOperator queryDerivedTableOp = new QueryDerivedTableOperator(subQueryOp);

            foreach (var selectElement in derivedSelectQueryBlock.SelectElements)
            {
                WSelectScalarExpression selectScalar = selectElement as WSelectScalarExpression;
                if (selectScalar == null)
                {
                    throw new SyntaxErrorException("The inner query of a WQueryDerivedTable can only select scalar elements.");
                }

                WColumnReferenceExpression columnRef = selectScalar.SelectExpr as WColumnReferenceExpression;
                if (columnRef != null && columnRef.ColumnType == ColumnType.Wildcard)
                    continue;

                string selectElementAlias = selectScalar.ColumnName;
                if (selectElementAlias == null)
                {
                    WValueExpression expr = selectScalar.SelectExpr as WValueExpression;;
                    if (expr == null)
                        throw new SyntaxErrorException(string.Format("The select element \"{0}\" doesn't have an alias.", selectScalar.ToString()));

                    selectElementAlias = expr.Value;
                }

                context.AddField(Alias.Value, selectElementAlias, columnRef != null ? columnRef.ColumnGraphType : ColumnGraphType.Value);
            }

            context.CurrentExecutionOperator = queryDerivedTableOp;

            return queryDerivedTableOp;
        }
    }
}

