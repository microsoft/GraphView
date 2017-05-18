using System;
using System.Collections;
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
            MatchGraph graphPattern = this.ConstructGraph2(out nonVertexTableReferences);

            // Vertex and edge aliases from the graph pattern, plus non-vertex table references.
            List<string> vertexAndEdgeAliases = new List<string>();

            foreach (ConnectedComponent subGraph in graphPattern.ConnectedSubGraphs) {
                vertexAndEdgeAliases.AddRange(subGraph.Nodes.Keys);
                vertexAndEdgeAliases.AddRange(subGraph.Edges.Keys);
            }
            foreach (WTableReferenceWithAlias nonVertexTableReference in nonVertexTableReferences) {
                vertexAndEdgeAliases.Add(nonVertexTableReference.Alias.Value);
            }

            // Normalizes the search condition into conjunctive predicates
            BooleanExpressionNormalizeVisitor booleanNormalize = new BooleanExpressionNormalizeVisitor();
            List<WBooleanExpression> conjunctivePredicates = 
                this.WhereClause != null && this.WhereClause.SearchCondition != null ?
                booleanNormalize.Invoke(this.WhereClause.SearchCondition) :
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
                    || !this.TryAttachPredicate(graphPattern, predicate, tableColumnReferences))
                {
                    // Attach cross-table predicate's referencing properties for later runtime evaluation
                    this.AttachProperties(graphPattern, tableColumnReferences);
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
                this.AttachProperties(graphPattern, tableColumnReferences);
            }

            foreach (WTableReferenceWithAlias nonVertexTableReference in nonVertexTableReferences)
            {
                bool isOnlyTargetTableReferenced;
                Dictionary<string, HashSet<string>> tableColumnReferences = columnVisitor.Invoke(
                    nonVertexTableReference, vertexAndEdgeAliases, out isOnlyTargetTableReferenced);
                // Attach referencing properties for later runtime evaluation
                this.AttachProperties(graphPattern, tableColumnReferences);
            }

            ConstructTraversalOrder(graphPattern);

            ConstructJsonQueries(dbConnection, graphPattern);

            return this.ConstructOperator2(dbConnection, graphPattern, context, nonVertexTableReferences,
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
                        node.Properties = new HashSet<string>();
                    foreach (var property in properties) {
                        node.Properties.Add(property);
                    }
                }
            }
        }

        internal static bool CanBePushedToServer(GraphViewConnection connection, MatchEdge matchEdge)
        {
            // For Compatible & Hybrid, we can't push edge predicates to server side
            if (connection.GraphType != GraphType.GraphAPIOnly) {
                Debug.Assert(connection.EdgeSpillThreshold == 1);
                return false;
            }

            if (IsTraversalThroughPhysicalReverseEdge(matchEdge) && !connection.UseReverseEdges) {
                return false;
            }

            return matchEdge != null && matchEdge.EdgeType != WEdgeType.BothEdge;
        }

        internal static MatchEdge GetPushedToServerEdge(GraphViewConnection connection,
            Tuple<MatchNode, MatchEdge, List<MatchEdge>, List<MatchEdge>, List<MatchEdge>> tuple)
        {
            MatchNode currentNode = tuple.Item1;
            MatchEdge traversalEdge = tuple.Item3.Count > 0 ? tuple.Item3[0] : null;
            bool hasNoBackwardingOrForwardingEdges = tuple.Item4.Count == 0 && tuple.Item5.Count == 0;

            MatchEdge pushedToServerEdge = null;
            if (hasNoBackwardingOrForwardingEdges)
            {
                if (traversalEdge != null)
                {
                    pushedToServerEdge = CanBePushedToServer(connection, traversalEdge)
                        ? traversalEdge
                        : null;
                }
                else if (currentNode.DanglingEdges.Count == 1)
                {
                    pushedToServerEdge = CanBePushedToServer(connection, currentNode.DanglingEdges[0])
                        ? currentNode.DanglingEdges[0]
                        : null;
                }
            }

            return pushedToServerEdge;
        }

        internal static void ConstructJsonQueries(GraphViewConnection connection, MatchGraph graphPattern)
        {
            foreach (ConnectedComponent subGraph in graphPattern.ConnectedSubGraphs)
            {
                HashSet<string> processedNodes = new HashSet<string>();
                List<Tuple<MatchNode, MatchEdge, List<MatchEdge>, List<MatchEdge>, List<MatchEdge>>> traversalOrder =
                    subGraph.TraversalOrder;

                bool isFirstNodeInTheComponent = true;
                foreach (Tuple<MatchNode, MatchEdge, List<MatchEdge>, List<MatchEdge>, List<MatchEdge>> tuple in traversalOrder)
                {
                    MatchNode currentNode = tuple.Item1;
                    MatchEdge traversalEdge = tuple.Item3.Count > 0 ? tuple.Item3[0] : null;
                    bool hasNoBackwardingOrForwardingEdges = tuple.Item4.Count == 0 && tuple.Item5.Count == 0;

                    if (!processedNodes.Contains(currentNode.NodeAlias))
                    {
                        MatchEdge pushedToServerEdge = GetPushedToServerEdge(connection, tuple);
                        //
                        // For the g.E() case
                        //
                        if (hasNoBackwardingOrForwardingEdges && traversalEdge == null && currentNode.DanglingEdges.Count == 1)
                        {
                            MatchEdge danglingEdge = currentNode.DanglingEdges[0];
                            if (isFirstNodeInTheComponent && danglingEdge.EdgeType == WEdgeType.OutEdge && 
                                (currentNode.Predicates == null || !currentNode.Predicates.Any()))
                            {
                                ConstructJsonQueryOnEdge(connection, currentNode, danglingEdge);
                                isFirstNodeInTheComponent = false;
                                currentNode.IsDummyNode = true;
                                processedNodes.Add(currentNode.NodeAlias);
                                continue;
                            }
                        }

                        string partitionKey = connection.RealPartitionKey;
                        ConstructJsonQueryOnNode(connection, currentNode, pushedToServerEdge, partitionKey);
                        //ConstructJsonQueryOnNodeViaExternalAPI(currentNode, null);
                        processedNodes.Add(currentNode.NodeAlias);
                        isFirstNodeInTheComponent = false;
                    }
                }
            }
        }
        

        internal static void ConstructJsonQueryOnNode(GraphViewConnection connection, MatchNode node, MatchEdge edge, string partitionKey)
        {
            string nodeAlias = node.NodeAlias;
            string edgeAlias = null;
            StringBuilder selectStrBuilder = new StringBuilder();
            StringBuilder joinStrBuilder = new StringBuilder();
            List<string> nodeProperties = new List<string> { nodeAlias };
            List<string> edgeProperties = new List<string>();
            bool isReverseAdj = edge != null ? IsTraversalThroughPhysicalReverseEdge(edge) : false;
            bool isStartVertexTheOriginVertex = edge != null ? !edge.IsReversed : false;


            foreach (string propertyName in node.Properties) {
                nodeProperties.Add(propertyName);
            }

            //
            // SELECT N_0 FROM Node N_0
            //
            selectStrBuilder.Append(nodeAlias);

            if (edge != null)
            {
                edgeAlias = edge.EdgeAlias;
                edgeProperties.Add(edge.EdgeAlias);
                edgeProperties.Add(isReverseAdj.ToString());
                edgeProperties.Add(isStartVertexTheOriginVertex.ToString());

                //
                // SELECT N_0, {"id": E_0.id} as E_0 FROM Node N_0 ...
                //
                selectStrBuilder.AppendFormat(", {{\"{0}\": {1}.{0}}} AS {1} ", GraphViewKeywords.KW_EDGE_ID, edgeAlias);

                foreach (string propertyName in edge.Properties) {
                    edgeProperties.Add(propertyName);
                }
            }

            WBooleanExpression tempNodeCondition = null;
            foreach (WBooleanExpression predicate in node.Predicates) {
                tempNodeCondition = WBooleanBinaryExpression.Conjunction(tempNodeCondition, predicate);
            }

            WBooleanExpression nodeCondition = tempNodeCondition.Copy();
            BooleanWValueExpressionVisitor booleanWValueExpressionVisitor = new BooleanWValueExpressionVisitor();
            booleanWValueExpressionVisitor.Invoke(nodeCondition);

            NormalizeNodePredicatesWColumnReferenceExpressionVisitor normalizeNodePredicatesColumnReferenceExpressionVisitor =
                new NormalizeNodePredicatesWColumnReferenceExpressionVisitor(partitionKey);
            Dictionary<string, string> referencedProperties =
                normalizeNodePredicatesColumnReferenceExpressionVisitor.Invoke(nodeCondition);

            foreach (KeyValuePair<string, string> referencedProperty in referencedProperties)
            {
                joinStrBuilder.AppendFormat(" JOIN {0} IN {1}['{2}'] ", referencedProperty.Key,
                    nodeAlias, referencedProperty.Value);
            }

            WBooleanExpression edgeCondition = null;
            
            //
            // Now we don't try to use a JOIN clause to fetch the edges along with the vertex unless in GraphAPI only graph
            // Thus, `edgeCondition` is always null
            //
            if (connection.GraphType != GraphType.GraphAPIOnly) {
                Debug.Assert(edge == null);
            }

            if (edge != null)
            {
                joinStrBuilder.AppendFormat(" JOIN {0} IN {1}.{2} ", edgeAlias, nodeAlias,
                    isReverseAdj ? GraphViewKeywords.KW_VERTEX_REV_EDGE : GraphViewKeywords.KW_VERTEX_EDGE);

                WBooleanExpression tempEdgeCondition = null;
                foreach (WBooleanExpression predicate in edge.Predicates) {
                    tempEdgeCondition = WBooleanBinaryExpression.Conjunction(tempEdgeCondition, predicate);
                }

                booleanWValueExpressionVisitor.Invoke(tempEdgeCondition);

                edgeCondition = tempEdgeCondition.Copy();
                DMultiPartIdentifierVisitor normalizeEdgePredicatesColumnReferenceExpressionVisitor = new DMultiPartIdentifierVisitor();
                normalizeEdgePredicatesColumnReferenceExpressionVisitor.Invoke(edgeCondition);
            }
            

            string edgeConditionString = edgeCondition?.ToString();
            if (!string.IsNullOrEmpty(edgeConditionString))
            {
                edgeConditionString = string.Format("({0}) OR {1}.{2} = true",
                    edgeConditionString,
                    nodeAlias,
                    isReverseAdj
                        ? GraphViewKeywords.KW_VERTEX_REVEDGE_SPILLED
                        : GraphViewKeywords.KW_VERTEX_EDGE_SPILLED);
            }

            bool hasNodePredicates = nodeCondition != null;
            bool hasEdgePredicates = edgeCondition != null;
            //
            // (IS_DEFINED(nodeAlias._viaGraphAPI) = true AND (nodeCondition)) AND (edgeCondition)
            //
            //string searchConditionString = string.Format(
            //    "(IS_DEFINED({0}.{1}) = true{2}){3}",
            //    nodeAlias, GraphViewKeywords.KW_VERTEX_VIAGRAPHAPI,
            //    hasNodePredicates ? $" AND ({nodeCondition.ToString()})" : "",
            //    hasEdgePredicates ? $" AND ({edgeConditionString})" : "");
            string searchConditionString = string.Format(
                "(IS_DEFINED({0}.{1}) = false {2}) {3}",
                nodeAlias, GraphViewKeywords.KW_EDGEDOC_IDENTIFIER,
                hasNodePredicates ? $" AND ({nodeCondition.ToString()})" : "",
                hasEdgePredicates ? $" AND ({edgeConditionString})" : "");

            JsonQuery jsonQuery = new JsonQuery
            {
                Alias = nodeAlias,
                JoinClause = joinStrBuilder.ToString(),
                SelectClause = selectStrBuilder.ToString(),
                WhereSearchCondition = searchConditionString,
                NodeProperties = nodeProperties,
                EdgeProperties = edgeProperties,
            };
            node.AttachedJsonQuery = jsonQuery;
        }

        internal static void ConstructJsonQueryOnEdge(GraphViewConnection connection, MatchNode node, MatchEdge edge)
        {
            string nodeAlias = node.NodeAlias;
            string edgeAlias = edge.EdgeAlias;
            StringBuilder selectStrBuilder = new StringBuilder();
            StringBuilder joinStrBuilder = new StringBuilder();
            List<string> nodeProperties = new List<string> { nodeAlias };
            List<string> edgeProperties = new List<string> { edgeAlias };
            nodeProperties.AddRange(node.Properties);
            edgeProperties.AddRange(edge.Properties);

            //
            // SELECT N_0, E_0 FROM Node N_0 Join E_0 IN N_0._edge
            //
            selectStrBuilder.AppendFormat("{0}, {1}", nodeAlias, edgeAlias);
            joinStrBuilder.AppendFormat(" JOIN {0} IN {1}.{2} ", edgeAlias, nodeAlias, GraphViewKeywords.KW_VERTEX_EDGE);

            WBooleanExpression tempEdgeCondition = null;
            foreach (WBooleanExpression predicate in edge.Predicates)
            {
                tempEdgeCondition = WBooleanBinaryExpression.Conjunction(tempEdgeCondition, predicate);
            }

            BooleanWValueExpressionVisitor booleanWValueExpressionVisitor = new BooleanWValueExpressionVisitor();
            booleanWValueExpressionVisitor.Invoke(tempEdgeCondition);

            WBooleanExpression edgeCondition = tempEdgeCondition.Copy();
            DMultiPartIdentifierVisitor normalizeEdgePredicatesColumnReferenceExpressionVisitor = new DMultiPartIdentifierVisitor();
            normalizeEdgePredicatesColumnReferenceExpressionVisitor.Invoke(edgeCondition);

            string edgeConditionString = edgeCondition?.ToString();

            //
            // WHERE ((N_0._isEdgeDoc = true AND N_0._is_reverse = false) OR N_0._edgeSpilled = false)
            // AND (edgeConditionString)
            //
            string searchConditionString = string.Format(
                "(({0}.{1} = true AND {0}.{2} = false) OR {0}.{3} = false){4}",
                nodeAlias, GraphViewKeywords.KW_EDGEDOC_IDENTIFIER, 
                GraphViewKeywords.KW_EDGEDOC_ISREVERSE, GraphViewKeywords.KW_VERTEX_EDGE_SPILLED,
                string.IsNullOrEmpty(edgeConditionString) ? "" : $" AND ({edgeConditionString})");

            JsonQuery jsonQuery = new JsonQuery
            {
                Alias = nodeAlias,
                JoinClause = joinStrBuilder.ToString(),
                SelectClause = selectStrBuilder.ToString(),
                WhereSearchCondition = searchConditionString,
                NodeProperties = nodeProperties,
                EdgeProperties = edgeProperties,
            };
            edge.AttachedJsonQuery = jsonQuery;
        }

        /*
        internal static void ConstructJsonQueryOnNodeViaExternalAPI(MatchNode node, MatchEdge edge)
        {
            string nodeAlias = node.NodeAlias;
            StringBuilder selectStrBuilder = new StringBuilder();
            StringBuilder joinStrBuilder = new StringBuilder();
            List<string> nodeProperties = new List<string> { nodeAlias };
            List<string> edgeProperties = new List<string>();

            foreach (string propertyName in node.Properties) {
                nodeProperties.Add(propertyName);
            }

            //
            // SELECT N_0 FROM Node N_0
            //
            selectStrBuilder.Append(nodeAlias);

            WBooleanExpression tempNodeCondition = null;
            foreach (WBooleanExpression predicate in node.Predicates) {
                tempNodeCondition = WBooleanBinaryExpression.Conjunction(tempNodeCondition, predicate);
            }

            WBooleanExpression nodeCondition = tempNodeCondition.Copy();
            BooleanWValueExpressionVisitor booleanWValueExpressionVisitor = new BooleanWValueExpressionVisitor();
            booleanWValueExpressionVisitor.Invoke(nodeCondition);

            DMultiPartIdentifierVisitor normalizeNodePredicatesColumnReferenceExpressionVisitor = new DMultiPartIdentifierVisitor();
            normalizeNodePredicatesColumnReferenceExpressionVisitor.Invoke(nodeCondition);

            bool hasNodePredicates = nodeCondition != null;

            //
            // (IS_DEFINED(nodeAlias._viaGraphAPI) = false AND IS_DEFINED(nodeAlias._vertex_id) = false AND (nodeCondition))
            //
            string searchConditionString = string.Format("(IS_DEFINED({0}.{1}) = false AND IS_DEFINED({0}.{2}) = false{3})",
                nodeAlias, 
                GraphViewKeywords.KW_VERTEX_VIAGRAPHAPI,
                GraphViewKeywords.KW_EDGEDOC_ISREVERSE,
                hasNodePredicates ? $" AND ({nodeCondition.ToString()})" : "");

            JsonQuery jsonQuery = new JsonQuery
            {
                Alias = nodeAlias,
                JoinClause = joinStrBuilder.ToString(),
                SelectClause = selectStrBuilder.ToString(),
                WhereSearchCondition = searchConditionString,
                NodeProperties = nodeProperties,
                EdgeProperties = edgeProperties,
            };
            node.AttachedJsonQueryOfNodesViaExternalAPI = jsonQuery;
        }
        */

        private MatchGraph ConstructGraph2(out List<WTableReferenceWithAlias> nonVertexTableReferences)
        {
            nonVertexTableReferences = new List<WTableReferenceWithAlias>();

            Dictionary<string, MatchPath> pathDictionary = new Dictionary<string, MatchPath>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, MatchEdge> reversedEdgeDict = new Dictionary<string, MatchEdge>();

            UnionFind unionFind = new UnionFind();
            Dictionary<string, MatchNode> vertexTableCollection = new Dictionary<string, MatchNode>(StringComparer.OrdinalIgnoreCase);
//            Dictionary<string, WNamedTableReference> vertexTableReferencesDict = new Dictionary<string, WNamedTableReference>();
            List<ConnectedComponent> connectedSubGraphs = new List<ConnectedComponent>();
            Dictionary<string, ConnectedComponent> subGraphMap = new Dictionary<string, ConnectedComponent>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, string> parent = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            unionFind.Parent = parent;

            // Goes through the FROM clause and extracts vertex table references and non-vertex table references
            if (this.FromClause != null)
            {
                List<WNamedTableReference> vertexTableList = new List<WNamedTableReference>();
                TableClassifyVisitor tcVisitor = new TableClassifyVisitor();
                tcVisitor.Invoke(FromClause, vertexTableList, nonVertexTableReferences);

                foreach (WNamedTableReference vertexTableRef in vertexTableList)
                {
                    vertexTableCollection.GetOrCreate(vertexTableRef.Alias.Value);
//                    vertexTableReferencesDict[vertexTableRef.Alias.Value] = vertexTableRef;
                    if (!parent.ContainsKey(vertexTableRef.Alias.Value))
                        parent[vertexTableRef.Alias.Value] = vertexTableRef.Alias.Value;
                }
            }

            // Consturct nodes and edges of a match graph defined by the SelectQueryBlock
            if (this.MatchClause != null)
            {
                if (this.MatchClause.Paths.Count > 0)
                {
                    foreach (WMatchPath path in this.MatchClause.Paths)
                    {
                        int index = 0;
                        MatchEdge edgeToSrcNode = null;

                        for (int count = path.PathEdgeList.Count; index < count; ++index)
                        {
                            WSchemaObjectName currentNodeTableRef = path.PathEdgeList[index].Item1;
                            WEdgeColumnReferenceExpression currentEdgeColumnRef = path.PathEdgeList[index].Item2;
                            string currentNodeExposedName = currentNodeTableRef.BaseIdentifier.Value;
                            WSchemaObjectName nextNodeTableRef = index != count - 1
                                ? path.PathEdgeList[index + 1].Item1
                                : path.Tail;

                            // Consturct the source node of a path in MatchClause.Paths
                            MatchNode srcNode = vertexTableCollection.GetOrCreate(currentNodeExposedName);
                            if (srcNode.NodeAlias == null)
                            {
                                srcNode.NodeAlias = currentNodeExposedName;
                                srcNode.Neighbors = new List<MatchEdge>();
                                srcNode.ReverseNeighbors = new List<MatchEdge>();
                                srcNode.DanglingEdges = new List<MatchEdge>();
                                srcNode.Predicates = new List<WBooleanExpression>();
                                srcNode.Properties = new HashSet<string>();
                            }

                            // Consturct the edge of a path in MatchClause.Paths
                            string edgeAlias = currentEdgeColumnRef.Alias;

                            MatchEdge edgeFromSrcNode;
                            if (currentEdgeColumnRef.MinLength == 1 && currentEdgeColumnRef.MaxLength == 1)
                            {
                                edgeFromSrcNode = new MatchEdge
                                {
                                    SourceNode = srcNode,
                                    EdgeColumn = currentEdgeColumnRef,
                                    EdgeAlias = edgeAlias,
                                    Predicates = new List<WBooleanExpression>(),
                                    BindNodeTableObjName =
                                        new WSchemaObjectName(
                                            ),
                                    IsReversed = false,
                                    EdgeType = currentEdgeColumnRef.EdgeType,
                                    Properties = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties),
                                };
                            }
                            else
                            {
                                MatchPath matchPath = new MatchPath
                                {
                                    SourceNode = srcNode,
                                    EdgeColumn = currentEdgeColumnRef,
                                    EdgeAlias = edgeAlias,
                                    Predicates = new List<WBooleanExpression>(),
                                    BindNodeTableObjName =
                                        new WSchemaObjectName(
                                            ),
                                    MinLength = currentEdgeColumnRef.MinLength,
                                    MaxLength = currentEdgeColumnRef.MaxLength,
                                    ReferencePathInfo = false,
                                    AttributeValueDict = currentEdgeColumnRef.AttributeValueDict,
                                    IsReversed = false,
                                    EdgeType = currentEdgeColumnRef.EdgeType,
                                    Properties = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties),
                                };
                                pathDictionary[edgeAlias] = matchPath;
                                edgeFromSrcNode = matchPath;
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
                                        Properties = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties),
                                    };
                                    srcNode.ReverseNeighbors.Add(reverseEdge);
                                    reversedEdgeDict[edgeToSrcNode.EdgeAlias] = reverseEdge;
                                }
                            }

                            edgeToSrcNode = edgeFromSrcNode;

                            if (!parent.ContainsKey(currentNodeExposedName))
                                parent[currentNodeExposedName] = currentNodeExposedName;

                            string nextNodeExposedName = nextNodeTableRef != null ? nextNodeTableRef.BaseIdentifier.Value : null;
                            if (nextNodeExposedName != null)
                            {
                                if (!parent.ContainsKey(nextNodeExposedName))
                                    parent[nextNodeExposedName] = nextNodeExposedName;

                                unionFind.Union(currentNodeExposedName, nextNodeExposedName);

                                srcNode.Neighbors.Add(edgeFromSrcNode);
                            }
                            // Dangling edge without SinkNode
                            else
                            {
                                srcNode.DanglingEdges.Add(edgeFromSrcNode);
                                srcNode.Properties.Add(GremlinKeyword.Star);
                                //if (edgeFromSrcNode.EdgeType == WEdgeType.BothEdge)
                                //{
                                //    srcNode.Properties.Add(GremlinKeyword.EdgeAdj);
                                //    srcNode.Properties.Add(GremlinKeyword.ReverseEdgeAdj);
                                //}
                                //else if (edgeFromSrcNode.EdgeType == WEdgeType.OutEdge) {
                                //    srcNode.Properties.Add(GremlinKeyword.EdgeAdj);
                                //}
                                //else {
                                //    srcNode.Properties.Add(GremlinKeyword.ReverseEdgeAdj);
                                //}
                            }
                        }
                        if (path.Tail == null) continue;

                        // Consturct destination node of a path in MatchClause.Paths
                        string tailExposedName = path.Tail.BaseIdentifier.Value;
                        MatchNode destNode = vertexTableCollection.GetOrCreate(tailExposedName);

                        if (destNode.NodeAlias == null)
                        {
                            destNode.NodeAlias = tailExposedName;
                            destNode.Neighbors = new List<MatchEdge>();
                            destNode.ReverseNeighbors = new List<MatchEdge>();
                            destNode.DanglingEdges = new List<MatchEdge>();
                            destNode.Predicates = new List<WBooleanExpression>();
                            destNode.Properties = new HashSet<string>();
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
                                    EdgeColumn = edgeToSrcNode.EdgeColumn,
                                    EdgeAlias = edgeToSrcNode.EdgeAlias,
                                    Predicates = edgeToSrcNode.Predicates,
                                    BindNodeTableObjName =
                                        new WSchemaObjectName(
                                        ),
                                    IsReversed = true,
                                    EdgeType = edgeToSrcNode.EdgeType,
                                    Properties = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties),
                                };
                                destNode.ReverseNeighbors.Add(reverseEdge);
                                reversedEdgeDict[edgeToSrcNode.EdgeAlias] = reverseEdge;
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

                MatchNode patternNode = node.Value;

                if (patternNode.NodeAlias == null)
                {
                    patternNode.NodeAlias = node.Key;
                    patternNode.Neighbors = new List<MatchEdge>();
                    patternNode.ReverseNeighbors = new List<MatchEdge>();
                    patternNode.DanglingEdges = new List<MatchEdge>();
                    patternNode.Predicates = new List<WBooleanExpression>();
                    patternNode.Properties = new HashSet<string>();
                }

                if (!subGraphMap.ContainsKey(root))
                {
                    ConnectedComponent subGraph = new ConnectedComponent();
                    subGraph.Nodes[node.Key] = node.Value;
                    foreach (MatchEdge edge in node.Value.Neighbors) {
                        subGraph.Edges[edge.EdgeAlias] = edge;
                    }
                    foreach (MatchEdge edge in node.Value.DanglingEdges) {
                        edge.IsDanglingEdge = true;
                        subGraph.Edges[edge.EdgeAlias] = edge;
                    }
                    subGraphMap[root] = subGraph;
                    connectedSubGraphs.Add(subGraph);
                    subGraph.IsTailNode[node.Value] = false;
                }
                else
                {
                    ConnectedComponent subGraph = subGraphMap[root];
                    subGraph.Nodes[node.Key] = node.Value;
                    foreach (MatchEdge edge in node.Value.Neighbors) {
                        subGraph.Edges[edge.EdgeAlias] = edge;
                    }
                    foreach (MatchEdge edge in node.Value.DanglingEdges) {
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

        private static void ConstructTraversalOrder(MatchGraph graphPattern)
        {
            DocDbGraphOptimizer graphOptimizer = new DocDbGraphOptimizer(graphPattern);
            foreach (ConnectedComponent subGraph in graphPattern.ConnectedSubGraphs) {
                subGraph.TraversalOrder = graphOptimizer.GetOptimizedTraversalOrder(subGraph);
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
        /// Return adjacency list's type as the parameter of adjacency list decoder
        /// Item1 indicates whether to cross apply forward adjacency list
        /// Item2 indicates whether to cross apply backward adjacency list
        /// </summary>
        /// <param name="context"></param>
        /// <param name="edge"></param>
        /// <returns></returns>
        private Tuple<bool, bool> GetAdjDecoderCrossApplyTypeParameter(MatchEdge edge)
        {
            if (edge.EdgeType == WEdgeType.BothEdge)
                return new Tuple<bool, bool>(true, true);

            if (IsTraversalThroughPhysicalReverseEdge(edge))
                return new Tuple<bool, bool>(false, true);
            else
                return new Tuple<bool, bool>(true, false);
        }

        /// <summary>
        /// Return the edge's traversal column reference
        /// </summary>
        /// <param name="edge"></param>
        /// <returns></returns>
        private static WColumnReferenceExpression GetAdjacencyListTraversalColumn(MatchEdge edge)
        {
            if (edge.EdgeType == WEdgeType.BothEdge)
                return new WColumnReferenceExpression(edge.EdgeAlias, GremlinKeyword.EdgeOtherV);
            return new WColumnReferenceExpression(edge.EdgeAlias,
                IsTraversalThroughPhysicalReverseEdge(edge) ? GremlinKeyword.EdgeSourceV : GremlinKeyword.EdgeSinkV);
        }

        /// <summary>
        /// Return traversal type
        /// </summary>
        /// <param name="context"></param>
        /// <param name="edge"></param>
        /// <returns></returns>
        private TraversalOperator2.TraversalTypeEnum GetTraversalType(MatchEdge edge)
        {
            if (edge.EdgeType == WEdgeType.BothEdge) {
                return TraversalOperator2.TraversalTypeEnum.Other;
            }

            return IsTraversalThroughPhysicalReverseEdge(edge)
                ? TraversalOperator2.TraversalTypeEnum.Source
                : TraversalOperator2.TraversalTypeEnum.Sink;
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
        private static void CheckRemainingPredicatesAndAppendFilterOp(QueryCompilationContext context, GraphViewConnection connection,
            HashSet<string> tableReferences,
            List<Tuple<WBooleanExpression, HashSet<string>>> remainingPredicatesAndTheirTableReferences,
            List<GraphViewExecutionOperator> childrenProcessor)
        {
            List<int> toBeRemovedIndexes = new List<int>();

            //
            // Predicates are appended in the order they are encountered in the WHERE clause
            //
            for (int i = 0; i < remainingPredicatesAndTheirTableReferences.Count; i++)
            {
                WBooleanExpression predicate = remainingPredicatesAndTheirTableReferences[i].Item1;
                HashSet<string> tableRefs = remainingPredicatesAndTheirTableReferences[i].Item2;

                if (tableReferences.IsSupersetOf(tableRefs))
                {
                    // Enable batch mode
                    childrenProcessor.Add(
                        new FilterInBatchOperator(
                            childrenProcessor.Count != 0
                            ? childrenProcessor.Last()
                            : context.OuterContextOp,
                            predicate.CompileToBatchFunction(context, connection)));

                    //childrenProcessor.Add(
                    //    new FilterOperator(
                    //        childrenProcessor.Count != 0
                    //        ? childrenProcessor.Last()
                    //        : context.OuterContextOp,
                    //        predicate.CompileToFunction(context, connection)));

                    toBeRemovedIndexes.Add(i);
                    context.CurrentExecutionOperator = childrenProcessor.Last();
                }
            }

            for (int i = toBeRemovedIndexes.Count - 1; i >= 0; i--)
            {
                int toBeRemovedIndex = toBeRemovedIndexes[i];
                remainingPredicatesAndTheirTableReferences.RemoveAt(toBeRemovedIndex);
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
        /// <param name="isMatchingEdges"></param>
        private void CrossApplyEdges(GraphViewConnection connection, QueryCompilationContext context, 
            List<GraphViewExecutionOperator> operatorChain, IList<MatchEdge> edges, 
            List<Tuple<WBooleanExpression, HashSet<string>>> predicatesAccessedTableReferences,
            bool isMatchingEdges = false)
        {
            Dictionary<string, TableGraphType> tableReferences = context.TableReferences;

            foreach (MatchEdge edge in edges)
            {
                Tuple<bool, bool> crossApplyTypeTuple = this.GetAdjDecoderCrossApplyTypeParameter(edge);
                QueryCompilationContext localEdgeContext = this.GenerateLocalContextForAdjacentListDecoder(edge.EdgeAlias, edge.Properties);
                WBooleanExpression edgePredicates = edge.RetrievePredicatesExpression();
                operatorChain.Add(new AdjacencyListDecoder(
                    operatorChain.Last(),
                    context.LocateColumnReference(edge.SourceNode.NodeAlias, GremlinKeyword.Star),
                    crossApplyTypeTuple.Item1, crossApplyTypeTuple.Item2, !edge.IsReversed,
                    edgePredicates != null ? edgePredicates.CompileToFunction(localEdgeContext, connection) : null,
                    edge.Properties, connection, context.RawRecordLayout.Count + edge.Properties.Count));
                context.CurrentExecutionOperator = operatorChain.Last();

                // Update edge's context info
                tableReferences.Add(edge.EdgeAlias, TableGraphType.Edge);
                this.UpdateEdgeLayout(edge.EdgeAlias, edge.Properties, context);

                if (isMatchingEdges)
                {
                    WColumnReferenceExpression sinkNodeIdColumnReference = new WColumnReferenceExpression(edge.SinkNode.NodeAlias, GremlinKeyword.NodeID);
                    //
                    // Add "edge.traversalColumn = sinkNode.id" filter
                    //
                    WColumnReferenceExpression edgeSinkColumnReference = GetAdjacencyListTraversalColumn(edge);
                    WBooleanComparisonExpression edgeJoinPredicate = new WBooleanComparisonExpression
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

        private void UpdateNodeLayout(string nodeAlias, HashSet<string> properties, QueryCompilationContext context)
        {
            foreach (string propertyName in properties)
            {
                ColumnGraphType columnGraphType = GraphViewReservedProperties.IsNodeReservedProperty(propertyName)
                    ? GraphViewReservedProperties.ReservedNodePropertiesColumnGraphTypes[propertyName]
                    : ColumnGraphType.Value;
                context.AddField(nodeAlias, propertyName, columnGraphType);
            }
        }

        private void UpdateEdgeLayout(string edgeAlias, List<string> properties, QueryCompilationContext context)
        {
            // Update context's record layout
            context.AddField(edgeAlias, GremlinKeyword.EdgeSourceV, ColumnGraphType.EdgeSource);
            context.AddField(edgeAlias, GremlinKeyword.EdgeSinkV, ColumnGraphType.EdgeSink);
            context.AddField(edgeAlias, GremlinKeyword.EdgeOtherV, ColumnGraphType.Value);
            context.AddField(edgeAlias, GremlinKeyword.EdgeID, ColumnGraphType.EdgeId);
            context.AddField(edgeAlias, GremlinKeyword.Star, ColumnGraphType.EdgeObject);
            for (var i = GraphViewReservedProperties.ReservedEdgeProperties.Count; i < properties.Count; i++)
            {
                context.AddField(edgeAlias, properties[i], ColumnGraphType.Value);
            }
        }

        private GraphViewExecutionOperator ConstructOperator2(GraphViewConnection connection, MatchGraph graphPattern,
            QueryCompilationContext context, List<WTableReferenceWithAlias> nonVertexTableReferences,
            List<Tuple<WBooleanExpression, HashSet<string>>> predicatesAccessedTableReferences)
        {
            List<GraphViewExecutionOperator> operatorChain = new List<GraphViewExecutionOperator>();
            Dictionary<string, TableGraphType> tableReferences = context.TableReferences;

            if (context.OuterContextOp != null)
            {
                context.CurrentExecutionOperator = context.OuterContextOp;
                CheckRemainingPredicatesAndAppendFilterOp(context, connection,
                    new HashSet<string>(tableReferences.Keys), predicatesAccessedTableReferences,
                    operatorChain);
            }
                
            foreach (ConnectedComponent subGraph in graphPattern.ConnectedSubGraphs)
            {
                List<Tuple<MatchNode, MatchEdge, List<MatchEdge>, List<MatchEdge>, List<MatchEdge>>> traversalOrder = 
                    subGraph.TraversalOrder;
                HashSet<string> processedNodes = new HashSet<string>();
                bool isFirstNodeInTheComponent = true;

                foreach (Tuple<MatchNode, MatchEdge, List<MatchEdge>, List<MatchEdge>, List<MatchEdge>> tuple in traversalOrder)
                {
                    MatchNode currentNode = tuple.Item1;
                    List<MatchEdge> traversalEdges = tuple.Item3;
                    List<MatchEdge> backwardMatchingEdges = tuple.Item4;
                    List<MatchEdge> forwardMatchingEdges = tuple.Item5;

                    if (isFirstNodeInTheComponent)
                    {
                        isFirstNodeInTheComponent = false;

                        GraphViewExecutionOperator startOp = currentNode.IsDummyNode
                            ? (GraphViewExecutionOperator)(new FetchEdgeOperator(connection,
                                currentNode.DanglingEdges[0].AttachedJsonQuery))
                            : new FetchNodeOperator2(
                                connection,
                                currentNode.AttachedJsonQuery
                                /*currentNode.AttachedJsonQueryOfNodesViaExternalAPI*/);

                        //
                        // The graph contains more than one component
                        //
                        if (operatorChain.Any())
                            operatorChain.Add(new CartesianProductOperator2(operatorChain.Last(), startOp));
                        //
                        // This WSelectQueryBlock is a sub query
                        //
                        else if (context.OuterContextOp != null)
                            operatorChain.Add(new CartesianProductOperator2(context.OuterContextOp, startOp));
                        else
                            operatorChain.Add(startOp);

                        context.CurrentExecutionOperator = operatorChain.Last();
                        //
                        // Update current node's context info
                        //
                        this.UpdateNodeLayout(currentNode.NodeAlias, currentNode.Properties, context);
                        tableReferences.Add(currentNode.NodeAlias, TableGraphType.Vertex);

                        if (currentNode.IsDummyNode)
                        {
                            Debug.Assert(currentNode.DanglingEdges.Count == 1);
                            MatchEdge danglingEdge = currentNode.DanglingEdges[0];

                            this.UpdateEdgeLayout(danglingEdge.EdgeAlias, danglingEdge.Properties, context);
                            tableReferences.Add(danglingEdge.EdgeAlias, TableGraphType.Edge);

                            CheckRemainingPredicatesAndAppendFilterOp(context, connection,
                                new HashSet<string>(tableReferences.Keys), predicatesAccessedTableReferences,
                                operatorChain);

                            continue;
                        }
                    }
                    else if (!processedNodes.Contains(currentNode.NodeAlias))
                    {
                        //
                        // This traversalEdge is the one whose sink is current node, and it has been pushed to server
                        //
                        MatchEdge traversalEdge = tuple.Item2;
                        operatorChain.Add(new TraversalOperator2(
                            operatorChain.Last(), 
                            connection,
                            context.LocateColumnReference(traversalEdge.EdgeAlias, GremlinKeyword.Star),
                            this.GetTraversalType(traversalEdge),
                            currentNode.AttachedJsonQuery,
                            //currentNode.AttachedJsonQueryOfNodesViaExternalAPI, 
                            null));
                        context.CurrentExecutionOperator = operatorChain.Last();
                        //
                        // Update current node's context info
                        //
                        this.UpdateNodeLayout(currentNode.NodeAlias, currentNode.Properties, context);
                        tableReferences.Add(currentNode.NodeAlias, TableGraphType.Vertex);
                    }

                    //
                    // Cross apply backwardMatchingEdges and update context info
                    //
                    this.CrossApplyEdges(connection, context, operatorChain, backwardMatchingEdges,
                        predicatesAccessedTableReferences, true);
                    //
                    // Cross apply forwardMatchingEdges and update context info
                    //
                    this.CrossApplyEdges(connection, context, operatorChain, forwardMatchingEdges,
                        predicatesAccessedTableReferences, true);
                    //
                    // Cross apply traversal edges whose source is current node and update context info
                    //
                    this.CrossApplyEdges(connection, context, operatorChain, traversalEdges,
                        predicatesAccessedTableReferences);
                    //
                    // Cross apply dangling edges and update context info
                    //
                    if (!processedNodes.Contains(currentNode.NodeAlias)) {
                        this.CrossApplyEdges(connection, context, operatorChain, currentNode.DanglingEdges,
                            predicatesAccessedTableReferences);
                    }

                    processedNodes.Add(currentNode.NodeAlias);
                    CheckRemainingPredicatesAndAppendFilterOp(context, connection,
                        new HashSet<string>(tableReferences.Keys), predicatesAccessedTableReferences,
                        operatorChain);
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
                        case "SUM":
                        case "MAX":
                        case "MIN":
                        case "MEAN":
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
                else if (context.InBatchMode)
                {
                    FieldValue indexValue = new FieldValue(0);
                    projectOperator.AddSelectScalarElement(indexValue);
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
                ProjectAggregation projectAggregationOp = context.InBatchMode ?
                    new ProjectAggregationInBatch(operatorChain.Any()
                        ? operatorChain.Last()
                        : context.OuterContextOp): 
                    new ProjectAggregation(operatorChain.Any()
                        ? operatorChain.Last()
                        : context.OuterContextOp);

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
                            WFunctionCall foldedFunction = fcall.Parameters[0] as WFunctionCall;
                            if (foldedFunction == null)
                                throw new SyntaxErrorException("The parameter of a Fold function must be a Compose1 function.");
                            projectAggregationOp.AddAggregateSpec(new FoldFunction(), 
                                new List<ScalarFunction> { foldedFunction.CompileToFunction(context, connection), });
                            break;
                        case "TREE":
                            WColumnReferenceExpression pathField = fcall.Parameters[0] as WColumnReferenceExpression;
                            projectAggregationOp.AddAggregateSpec(
                                new TreeFunction(), 
                                new List<ScalarFunction>() { pathField.CompileToFunction(context, connection) });
                            break;
                        case "CAP":
                            CapAggregate capAggregate = new CapAggregate();
                            for (int i = 0; i < fcall.Parameters.Count; i += 2)
                            {
                                WColumnNameList columnNameList = fcall.Parameters[i] as WColumnNameList;
                                WValueExpression capName = fcall.Parameters[i+1] as WValueExpression;

                                IAggregateFunction sideEffectState;
                                if (!context.SideEffectStates.TryGetValue(capName.Value, out sideEffectState))
                                    throw new GraphViewException("SideEffect state " + capName + " doesn't exist in the context");
                                capAggregate.AddCapatureSideEffectState(capName.Value, sideEffectState);
                            }
                            projectAggregationOp.AddAggregateSpec(capAggregate, new List<ScalarFunction>());
                            break;
                        case "SUM":
                            WColumnReferenceExpression targetField = fcall.Parameters[0] as WColumnReferenceExpression;
                            projectAggregationOp.AddAggregateSpec(
                                new SumFunction(), 
                                new List<ScalarFunction> { targetField.CompileToFunction(context, connection) });
                            break;
                        case "MAX":
                            targetField = fcall.Parameters[0] as WColumnReferenceExpression;
                            projectAggregationOp.AddAggregateSpec(
                                new MaxFunction(),
                                new List<ScalarFunction> { targetField.CompileToFunction(context, connection) });
                            break;
                        case "MIN":
                            targetField = fcall.Parameters[0] as WColumnReferenceExpression;
                            projectAggregationOp.AddAggregateSpec(
                                new MinFunction(), 
                                new List<ScalarFunction> { targetField.CompileToFunction(context, connection) });
                            break;
                        case "MEAN":
                            targetField = fcall.Parameters[0] as WColumnReferenceExpression;
                            projectAggregationOp.AddAggregateSpec(
                                new MeanFunction(),
                                new List<ScalarFunction> { targetField.CompileToFunction(context, connection) });
                            break;
                        default:
                            projectAggregationOp.AddAggregateSpec(null, null);
                            break;
                    }
                }

                // Rebuilds the output layout of the context
                context.ClearField();

                foreach (var expr in selectScalarExprList)
                {
                    var alias = expr.ColumnName;
                    // TODO: Change to Addfield with correct ColumnGraphType
                    context.AddField("", alias ?? GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);
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

    partial class WUnionTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            ContainerOperator containerOp = new ContainerOperator(context.CurrentExecutionOperator);

            UnionOperator unionOp = new UnionOperator(context.CurrentExecutionOperator, containerOp);

            bool isUnionWithoutAnyBranch = Parameters.Count == 0 || Parameters[0] is WValueExpression;

            WSelectQueryBlock firstSelectQuery = null;
            if (!isUnionWithoutAnyBranch)
            {
                foreach (WScalarExpression parameter in Parameters)
                {
                    WScalarSubquery scalarSubquery = parameter as WScalarSubquery;
                    if (scalarSubquery == null)
                    {
                        throw new SyntaxErrorException("The input of an union table reference must be one or more scalar subqueries.");
                    }

                    if (firstSelectQuery == null)
                    {
                        firstSelectQuery = scalarSubquery.SubQueryExpr as WSelectQueryBlock;
                        if (firstSelectQuery == null)
                        {
                            throw new SyntaxErrorException("The input of an union table reference must be one or more select query blocks.");
                        }
                    }

                    QueryCompilationContext subcontext = new QueryCompilationContext(context);
                    subcontext.CarryOn = true;
                    subcontext.InBatchMode = context.InBatchMode;
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
                    if (selectScalar == null) {
                        throw new SyntaxErrorException("The input subquery of an union table reference can only select scalar elements.");
                    }
                    Debug.Assert(selectScalar.ColumnName != null, "selectScalar.ColumnName != null");

                    WColumnReferenceExpression columnRef = selectScalar.SelectExpr as WColumnReferenceExpression;
                    //
                    // TODO: Remove this case
                    //
                    if (columnRef != null && columnRef.ColumnType == ColumnType.Wildcard) {
                        continue;
                    }
                    context.AddField(Alias.Value, selectScalar.ColumnName, columnRef?.ColumnGraphType ?? ColumnGraphType.Value);
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
                if (selectScalar == null) {
                    throw new SyntaxErrorException("The input subquery of a coalesce table reference can only select scalar elements.");
                }
                Debug.Assert(selectScalar.ColumnName != null, "selectScalar.ColumnName != null");

                WColumnReferenceExpression columnRef = selectScalar.SelectExpr as WColumnReferenceExpression;
                //
                // TODO: Remove this case
                //
                if (columnRef != null && columnRef.ColumnType == ColumnType.Wildcard) {
                    continue;
                }
                context.AddField(Alias.Value, selectScalar.ColumnName, columnRef?.ColumnGraphType ?? ColumnGraphType.Value);
            }

            context.CurrentExecutionOperator = coalesceOp;
            return coalesceOp;
        }
    }

    partial class WOptionalTableReference
    {
        //internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        //{
        //    WSelectQueryBlock contextSelect, optionalSelect;
        //    Split(out contextSelect, out optionalSelect);

        //    List<int> inputIndexes = new List<int>();
        //    List<Tuple<WColumnReferenceExpression, string>> columnList = new List<Tuple<WColumnReferenceExpression, string>>();

        //    foreach (WSelectElement selectElement in contextSelect.SelectElements)
        //    {
        //        WSelectScalarExpression selectScalar = selectElement as WSelectScalarExpression;
        //        if (selectScalar == null)
        //        {
        //            throw new SyntaxErrorException("The SELECT elements of the sub-queries in an optional table reference must be select scalar elements.");
        //        }
        //        WColumnReferenceExpression columnRef = selectScalar.SelectExpr as WColumnReferenceExpression;

        //        if (columnRef != null)
        //        {
        //            int index;
        //            if (!context.TryLocateColumnReference(columnRef, out index))
        //                throw new SyntaxErrorException("Syntax Error!!!");
        //            inputIndexes.Add(index);

        //            columnList.Add(
        //                new Tuple<WColumnReferenceExpression, string>(
        //                    new WColumnReferenceExpression(Alias.Value, selectScalar.ColumnName ?? columnRef.ColumnName,
        //                        columnRef.ColumnGraphType), selectScalar.ColumnName));
        //        }
        //        else
        //        {
        //            WValueExpression nullExpression = selectScalar.SelectExpr as WValueExpression;
        //            if (nullExpression == null)
        //                throw new SyntaxErrorException("The SELECT elements of the sub-queries in a optional table reference must be column references or WValueExpression.");
        //            if (nullExpression.ToString().Equals("null", StringComparison.OrdinalIgnoreCase))
        //                inputIndexes.Add(-1);

        //            columnList.Add(
        //                new Tuple<WColumnReferenceExpression, string>(
        //                    new WColumnReferenceExpression(Alias.Value, selectScalar.ColumnName, ColumnGraphType.Value),
        //                    selectScalar.ColumnName));
        //        }
        //    }

        //    QueryCompilationContext subcontext = new QueryCompilationContext(context);
        //    ContainerOperator containerOp = null;
        //    bool isCarryOnMode = false;
        //    if (this.HasAggregateFunctionAsChildren)
        //    {
        //        isCarryOnMode = true;
        //        subcontext.InBatchMode = context.InBatchMode;
        //        containerOp = new ContainerOperator(context.CurrentExecutionOperator);
        //        subcontext.CarryOn = true;
        //        subcontext.OuterContextOp.SourceEnumerator = containerOp.GetEnumerator();
        //    }

        //    GraphViewExecutionOperator optionalTraversalOp = optionalSelect.Compile(subcontext, dbConnection);

        //    //OptionalOperator optionalOp = new OptionalOperator(context.CurrentExecutionOperator, inputIndexes, optionalTraversalOp, subcontext.OuterContextOp);
        //    OptionalOperator optionalOp = new OptionalOperator(context.CurrentExecutionOperator, inputIndexes,
        //        optionalTraversalOp, subcontext.OuterContextOp, containerOp, isCarryOnMode);
        //    context.CurrentExecutionOperator = optionalOp;

        //    // Updates the raw record layout. The columns of this table-valued function 
        //    // are specified by the select elements of the input subqueries.
        //    foreach (Tuple<WColumnReferenceExpression, string> tuple in columnList)
        //    {
        //        WColumnReferenceExpression columnRef = tuple.Item1;
        //        string selectElementAlias = tuple.Item2;
        //        context.AddField(Alias.Value, selectElementAlias ?? columnRef.ColumnName, columnRef.ColumnGraphType);
        //    }

        //    return optionalOp;
        //}

        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WSelectQueryBlock contextSelect, optionalSelect;
            this.Split(out contextSelect, out optionalSelect);

            List<int> inputIndexes = new List<int>();
            List<Tuple<WColumnReferenceExpression, string>> columnList =
                new List<Tuple<WColumnReferenceExpression, string>>();

            foreach (WSelectElement selectElement in contextSelect.SelectElements)
            {
                WSelectScalarExpression selectScalar = selectElement as WSelectScalarExpression;
                if (selectScalar == null)
                {
                    throw new SyntaxErrorException(
                        "The SELECT elements of the sub-queries in an optional table reference must be select scalar elements.");
                }

                WColumnReferenceExpression columnRef = selectScalar.SelectExpr as WColumnReferenceExpression;

                if (columnRef != null)
                {
                    int index;
                    if (!context.TryLocateColumnReference(columnRef, out index))
                    {
                        throw new SyntaxErrorException("Syntax Error!!!");
                    }

                    inputIndexes.Add(index);

                    columnList.Add(
                        new Tuple<WColumnReferenceExpression, string>(
                            new WColumnReferenceExpression(
                                this.Alias.Value,
                                selectScalar.ColumnName ?? columnRef.ColumnName,
                                columnRef.ColumnGraphType),
                            selectScalar.ColumnName));
                }
                else
                {
                    WValueExpression nullExpression = selectScalar.SelectExpr as WValueExpression;
                    if (nullExpression == null)
                    {
                        throw new SyntaxErrorException(
                            "The SELECT elements of the sub-queries in a optional table reference must be column references or WValueExpression.");
                    }

                    if (nullExpression.ToString().Equals("null", StringComparison.OrdinalIgnoreCase))
                    {
                        inputIndexes.Add(-1);
                    }

                    columnList.Add(
                        new Tuple<WColumnReferenceExpression, string>(
                            new WColumnReferenceExpression(
                                this.Alias.Value,
                                selectScalar.ColumnName,
                                ColumnGraphType.Value),
                            selectScalar.ColumnName));
                }
            }

            QueryCompilationContext subcontext = new QueryCompilationContext(context);
            ContainerEnumerator sourceEnumerator = new ContainerEnumerator();
            subcontext.OuterContextOp.SourceEnumerator = sourceEnumerator;
            subcontext.InBatchMode = context.InBatchMode || !this.HasAggregateFunctionAsChildren;
            if (!this.HasAggregateFunctionAsChildren)
            {
                subcontext.AddField(
                    GremlinKeyword.IndexTableName, GremlinKeyword.IndexColumnName, ColumnGraphType.Value, true);
            }

            GraphViewExecutionOperator optionalTraversalOp = optionalSelect.Compile(subcontext, dbConnection);

            //OptionalOperator optionalOp = new OptionalOperator(
            //    context.CurrentExecutionOperator,
            //    inputIndexes,
            //    optionalTraversalOp,
            //    subcontext.OuterContextOp,
            //    containerOp,
            //    isCarryOnMode);

            OptionalInBatchOperator optionalOp = new OptionalInBatchOperator(
                context.CurrentExecutionOperator,
                inputIndexes,
                optionalTraversalOp,
                sourceEnumerator,
                this.HasAggregateFunctionAsChildren,
                context.InBatchMode,
                context.RawRecordLayout.Count);

            context.CurrentExecutionOperator = optionalOp;

            // Updates the raw record layout. The columns of this table-valued function 
            // are specified by the select elements of the input subqueries.
            foreach (Tuple<WColumnReferenceExpression, string> tuple in columnList)
            {
                WColumnReferenceExpression columnRef = tuple.Item1;
                string selectElementAlias = tuple.Item2;
                context.AddField(this.Alias.Value, selectElementAlias ?? columnRef.ColumnName, columnRef.ColumnGraphType);
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
                if (selectScalar == null) {
                    throw new SyntaxErrorException("The SELECT elements of the sub-query in a local table reference must be select scalar elements.");
                }
                Debug.Assert(selectScalar.ColumnName != null, "selectScalar.ColumnName != null");

                WColumnReferenceExpression columnRef = selectScalar.SelectExpr as WColumnReferenceExpression;
                //
                // TODO: Remove this case
                //
                if (columnRef != null && columnRef.ColumnType == ColumnType.Wildcard) {
                    continue;
                }
                context.AddField(Alias.Value, selectScalar.ColumnName, columnRef?.ColumnGraphType ?? ColumnGraphType.Value);
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

            ContainerEnumerator sourceEnumerator = new ContainerEnumerator();
            QueryCompilationContext subcontext = new QueryCompilationContext(context);
            subcontext.OuterContextOp.SourceEnumerator = sourceEnumerator;
            subcontext.AddField(GremlinKeyword.IndexTableName, GremlinKeyword.IndexColumnName, ColumnGraphType.Value, true);
            subcontext.InBatchMode = true;
            GraphViewExecutionOperator flatMapTraversalOp = flatMapSelect.Compile(subcontext, dbConnection);

            //FlatMapOperator flatMapOp = new FlatMapOperator(context.CurrentExecutionOperator, flatMapTraversalOp, subcontext.OuterContextOp);
            FlatMapInBatchOperator flatMapOp = new FlatMapInBatchOperator(context.CurrentExecutionOperator, flatMapTraversalOp, sourceEnumerator);
            context.CurrentExecutionOperator = flatMapOp;

            foreach (WSelectElement selectElement in flatMapSelect.SelectElements)
            {
                WSelectScalarExpression selectScalar = selectElement as WSelectScalarExpression;
                if (selectScalar == null)
                {
                    throw new SyntaxErrorException("The SELECT elements of the sub-query in a flatMap table reference must be select scalar elements.");
                }
                Debug.Assert(selectScalar.ColumnName != null, "selectScalar.ColumnName != null");

                WColumnReferenceExpression columnRef = selectScalar.SelectExpr as WColumnReferenceExpression;
                //
                // TODO: Remove this case
                //
                if (columnRef != null && columnRef.ColumnType == ColumnType.Wildcard)
                {
                    continue;
                }
                context.AddField(Alias.Value, selectScalar.ColumnName, columnRef?.ColumnGraphType ?? ColumnGraphType.Value);
            }

            return flatMapOp;
        }
    }

    partial class WBoundNodeTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            Debug.Assert(context.CurrentExecutionOperator != null, "context.CurrentExecutionOperator != null");

            string nodeAlias = Alias.Value;
            MatchNode matchNode = new MatchNode
            {
                AttachedJsonQuery = null,
                NodeAlias = nodeAlias,
                Predicates = new List<WBooleanExpression>(),
                Properties = new HashSet<string>(),
            };

            foreach (WScalarExpression expression in this.Parameters)
            {
                WValueExpression populateProperty = expression as WValueExpression;
                Debug.Assert(populateProperty != null, "populateProperty != null");

                matchNode.Properties.Add(populateProperty.Value);
            }

            WSelectQueryBlock.ConstructJsonQueryOnNode(dbConnection, matchNode, null, dbConnection.RealPartitionKey);
            //WSelectQueryBlock.ConstructJsonQueryOnNodeViaExternalAPI(matchNode, null);

            FetchNodeOperator2 fetchNodeOp = new FetchNodeOperator2(
                dbConnection, 
                matchNode.AttachedJsonQuery
                /*matchNode.AttachedJsonQueryOfNodesViaExternalAPI*/);

            foreach (string propertyName in matchNode.Properties) {
                ColumnGraphType columnGraphType = GraphViewReservedProperties.IsNodeReservedProperty(propertyName)
                    ? GraphViewReservedProperties.ReservedNodePropertiesColumnGraphTypes[propertyName]
                    : ColumnGraphType.Value;
                context.AddField(nodeAlias, propertyName, columnGraphType);
            }

            return new CartesianProductOperator2(context.CurrentExecutionOperator, fetchNodeOp);
        }
    }

    partial class WEdgeToVertexTableReference
    {
        private const int edgeFieldParameteIndex = 0;
        private const int populatePropertyParameterStartIndex = 1;

        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression edgeFieldParameter = this.Parameters[edgeFieldParameteIndex] as WColumnReferenceExpression;
            Debug.Assert(edgeFieldParameter != null, "edgeFieldParameter != null");
            int edgeFieldIndex = context.LocateColumnReference(edgeFieldParameter);

            string nodeAlias = this.Alias.Value;

            MatchNode matchNode = new MatchNode {
                AttachedJsonQuery = null,
                NodeAlias = nodeAlias,
                Predicates = new List<WBooleanExpression>(),
                Properties = new HashSet<string>(),
            };

            for (int i = populatePropertyParameterStartIndex; i < this.Parameters.Count; i++) {
                WValueExpression populateProperty = this.Parameters[i] as WValueExpression;
                Debug.Assert(populateProperty != null, "populateProperty != null");

                matchNode.Properties.Add(populateProperty.Value);
            }

            bool isSendQueryRequired = !(matchNode.Properties.Count == 1 &&
                                         matchNode.Properties.First().Equals(GraphViewKeywords.KW_DOC_ID));

            //
            // Construct JSON query
            //
            if (isSendQueryRequired) {
                WSelectQueryBlock.ConstructJsonQueryOnNode(dbConnection, matchNode, null, dbConnection.RealPartitionKey);
                //WSelectQueryBlock.ConstructJsonQueryOnNodeViaExternalAPI(matchNode, null);
            }

            TraversalOperator2 traversalOp = new TraversalOperator2(
                context.CurrentExecutionOperator, dbConnection, 
                edgeFieldIndex, this.GetTraversalTypeParameter(),
                matchNode.AttachedJsonQuery/*, matchNode.AttachedJsonQueryOfNodesViaExternalAPI*/, null);
            context.CurrentExecutionOperator = traversalOp;

            // Update context's record layout
            if (isSendQueryRequired) {
                foreach (string propertyName in matchNode.Properties) {
                    ColumnGraphType columnGraphType = GraphViewReservedProperties.IsNodeReservedProperty(propertyName)
                        ? GraphViewReservedProperties.ReservedNodePropertiesColumnGraphTypes[propertyName]
                        : ColumnGraphType.Value;
                    context.AddField(nodeAlias, propertyName, columnGraphType);
                }
            }
            else {
                context.AddField(nodeAlias, GremlinKeyword.NodeID, ColumnGraphType.VertexId);
            }

            return traversalOp;
        }
    }

    partial class WEdgeToSinkVertexTableReference
    {
        internal override TraversalOperator2.TraversalTypeEnum GetTraversalTypeParameter()
        {
            return TraversalOperator2.TraversalTypeEnum.Sink;
        }
    }

    partial class WEdgeToSourceVertexTableReference
    {
        internal override TraversalOperator2.TraversalTypeEnum GetTraversalTypeParameter()
        {
            return TraversalOperator2.TraversalTypeEnum.Source;
        }
    }

    partial class WEdgeToOtherVertexTableReference
    {
        internal override TraversalOperator2.TraversalTypeEnum GetTraversalTypeParameter()
        {
            return TraversalOperator2.TraversalTypeEnum.Other;
        }
    }

    partial class WEdgeToBothVertexTableReference
    {
        internal override TraversalOperator2.TraversalTypeEnum GetTraversalTypeParameter()
        {
            return TraversalOperator2.TraversalTypeEnum.Both;
        }
    }

    partial class WVertexToEdgeTableReference
    {
        private const int startVertexParameterIndex = 0;
        private const int populatePropertyParameterStartIndex = 1;

        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context,
            GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression startVertexParameter = this.Parameters[startVertexParameterIndex] as WColumnReferenceExpression;
            Debug.Assert(startVertexParameter != null, "startVertexParameter != null");
            int startVertexIndex = context.LocateColumnReference(startVertexParameter);

            string edgeAlias = this.Alias.Value;
            List<string> projectFields = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties);

            for (int i = populatePropertyParameterStartIndex; i < this.Parameters.Count; i++) {
                WValueExpression propertyParameter = this.Parameters[i] as WValueExpression;
                Debug.Assert(propertyParameter != null, "propertyParameter != null");
                string field = propertyParameter.Value;
                if (!projectFields.Contains(field))
                    projectFields.Add(field);
            }

            Tuple<bool, bool> crossApplyTypeParameter = this.GetAdjListDecoderCrossApplyTypeParameter();
            bool crossApplyForwardAdj = crossApplyTypeParameter.Item1;
            bool crossApplyBackwardAdj = crossApplyTypeParameter.Item2;

            AdjacencyListDecoder adjListDecoder = new AdjacencyListDecoder(
                context.CurrentExecutionOperator, startVertexIndex,
                crossApplyForwardAdj, crossApplyBackwardAdj, 
                true, null, 
                projectFields, dbConnection, 
                context.RawRecordLayout.Count + projectFields.Count);

            context.CurrentExecutionOperator = adjListDecoder;

            // Update context's record layout
            context.AddField(edgeAlias, GremlinKeyword.EdgeSourceV, ColumnGraphType.EdgeSource);
            context.AddField(edgeAlias, GremlinKeyword.EdgeSinkV, ColumnGraphType.EdgeSink);
            context.AddField(edgeAlias, GremlinKeyword.EdgeOtherV, ColumnGraphType.Value);
            context.AddField(edgeAlias, GremlinKeyword.EdgeID, ColumnGraphType.EdgeId);
            context.AddField(edgeAlias, GremlinKeyword.Star, ColumnGraphType.EdgeObject);

            for (int i = GraphViewReservedProperties.ReservedEdgeProperties.Count; i < projectFields.Count; i++) {
                context.AddField(edgeAlias, projectFields[i], ColumnGraphType.Value);
            }

            return adjListDecoder;
        }
    }

    partial class WVertexToForwardEdgeTableReference
    {
        internal override Tuple<bool, bool> GetAdjListDecoderCrossApplyTypeParameter()
        {
            return new Tuple<bool, bool>(true, false);
        }
    }

    partial class WVertexToBackwordEdgeTableReference
    {
        internal override Tuple<bool, bool> GetAdjListDecoderCrossApplyTypeParameter()
        {
            return new Tuple<bool, bool>(false, true);
        }
    }

    partial class WVertexToBothEdgeTableReference
    {
        internal override Tuple<bool, bool> GetAdjListDecoderCrossApplyTypeParameter()
        {
            return new Tuple<bool, bool>(true, true);
        }
    }

    partial class WValuesTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            List<int> propertiesIndex = new List<int>();

            foreach (WScalarExpression expression in Parameters)
            {
                WColumnReferenceExpression targetParameter = expression as WColumnReferenceExpression;
                if (targetParameter != null)
                {
                    propertiesIndex.Add(context.LocateColumnReference(targetParameter));
                    continue;
                }

                throw new QueryCompilationException(
                    "Parameters of Properties table can only be WColumnReferenceExpression.");
            }

            GraphViewExecutionOperator valuesOp = new ValuesOperator(context.CurrentExecutionOperator, propertiesIndex);
            context.CurrentExecutionOperator = valuesOp;
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return valuesOp;
        }
    }


    partial class WLabelTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            Debug.Assert(this.Parameters.Count == 1);

            WColumnReferenceExpression targetVertexOrEdge = this.Parameters[0] as WColumnReferenceExpression;
            Debug.Assert(targetVertexOrEdge != null);

            int targetVertexOrEdgeIndex = context.LocateColumnReference(targetVertexOrEdge);

            GraphViewExecutionOperator labelOp = new LabelOperator(context.CurrentExecutionOperator, targetVertexOrEdgeIndex);
            context.CurrentExecutionOperator = labelOp;
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return labelOp;
        }
    }


    partial class WIdTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            Debug.Assert(this.Parameters.Count == 1);

            // Can be VertexField, EdgeField, or VertexSinglePropertyField
            WColumnReferenceExpression target = this.Parameters[0] as WColumnReferenceExpression;
            Debug.Assert(target != null);

            int targetIndex = context.LocateColumnReference(target);

            GraphViewExecutionOperator IdOp = new IdOperator(context.CurrentExecutionOperator, targetIndex);
            context.CurrentExecutionOperator = IdOp;
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return IdOp;
        }
    }


    partial class WAllPropertiesTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression inputParameter = Parameters[0] as WColumnReferenceExpression;
            Debug.Assert(inputParameter != null, "inputParameter != null");

            int inputTargetIndex = context.LocateColumnReference(inputParameter);

            List<string> populatedMetaproperties = new List<string>();
            for (int metaPropertiesIndex = 1; metaPropertiesIndex < Parameters.Count; metaPropertiesIndex++)
            {
                WValueExpression metaPropertyExpression = Parameters[metaPropertiesIndex] as WValueExpression;
                Debug.Assert(metaPropertyExpression != null, "metaPropertyExpression != null");
                
                populatedMetaproperties.Add(metaPropertyExpression.Value);
            }

            
            AllPropertiesOperator allPropertiesOp = new AllPropertiesOperator(context.CurrentExecutionOperator,
                inputTargetIndex, populatedMetaproperties);
            context.CurrentExecutionOperator = allPropertiesOp;

            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);
            foreach (string metapropertyName in populatedMetaproperties) {
                context.AddField(Alias.Value, metapropertyName, ColumnGraphType.Value);
            }

            return allPropertiesOp;
        }
    }

    partial class WAllValuesTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression inputParameter = Parameters[0] as WColumnReferenceExpression;
            Debug.Assert(inputParameter != null, "inputParameter != null");

            int inputTargetIndex = context.LocateColumnReference(inputParameter);

            AllValuesOperator allValuesOp = new AllValuesOperator(context.CurrentExecutionOperator, inputTargetIndex);
            context.CurrentExecutionOperator = allValuesOp;
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return allValuesOp;
        }
    }

    partial class WPropertiesTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            List<int> propertiesIndex = new List<int>();
            List<string> populateMetaproperties = new List<string>();

            foreach (WScalarExpression expression in Parameters)
            {
                WColumnReferenceExpression targetParameter = expression as WColumnReferenceExpression;
                if (targetParameter != null)
                {
                    propertiesIndex.Add(context.LocateColumnReference(targetParameter));
                    continue;
                }

                WValueExpression populateMetapropertyNameParameter = expression as WValueExpression;
                if (populateMetapropertyNameParameter != null)
                {
                    populateMetaproperties.Add(populateMetapropertyNameParameter.Value);
                    continue;
                }

                throw new QueryCompilationException(
                    "Parameters of Properties table can only be WColumnReferenceExpression or WValueExpression.");

            }

            GraphViewExecutionOperator propertiesOp = new PropertiesOperator(context.CurrentExecutionOperator,
                propertiesIndex, populateMetaproperties);
            context.CurrentExecutionOperator = propertiesOp;

            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);
            foreach (string metapropertyName in populateMetaproperties) {
                context.AddField(Alias.Value, metapropertyName, ColumnGraphType.Value);
            }
        
            return propertiesOp;
        }
    }

    partial class WDedupGlobalTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            List<ScalarFunction> targetValueFunctionList =
                this.Parameters.Select(expression => expression.CompileToFunction(context, dbConnection)).ToList();

            DeduplicateOperator dedupOp = context.InBatchMode
                ? new DeduplicateInBatchOperator(context.CurrentExecutionOperator, targetValueFunctionList)
                : new DeduplicateOperator(context.CurrentExecutionOperator, targetValueFunctionList);
            context.CurrentExecutionOperator = dedupOp;

            return dedupOp;
        }
    }

    partial class WDedupLocalTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            DeduplicateLocalOperator dedupLocalOp = new DeduplicateLocalOperator(context.CurrentExecutionOperator,
                Parameters[0].CompileToFunction(context, dbConnection));
            context.CurrentExecutionOperator = dedupLocalOp;
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return dedupLocalOp;
        }
    }

    partial class WConstantReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            List<ScalarFunction> constantValues = new List<ScalarFunction>();

            foreach (WScalarExpression expression in this.Parameters)
            {
                WValueExpression constantValue = expression as WValueExpression;
                Debug.Assert(constantValue != null, "constantValue != null");
                constantValues.Add(constantValue.CompileToFunction(context, dbConnection));
            }

            ConstantOperator constantOp = new ConstantOperator(context.CurrentExecutionOperator, constantValues,
                this.IsList, GremlinKeyword.TableDefaultColumnName);
            context.CurrentExecutionOperator = constantOp;
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return constantOp;
        }
    }

    partial class WProjectTableReference
    {
        private const int StartParameterIndex = 0;
        private const int ParameterStep = 2;

        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            ProjectByOperator projectByOp = new ProjectByOperator(context.CurrentExecutionOperator);

            for (int i = StartParameterIndex; i < this.Parameters.Count; i += ParameterStep)
            {
                WScalarSubquery scalarSubquery = this.Parameters[i] as WScalarSubquery;
                if (scalarSubquery == null)
                    throw new SyntaxErrorException("The parameter of ProjectTableReference at an odd position has to be a WScalarSubquery.");

                WValueExpression projectName = this.Parameters[i + 1] as WValueExpression;
                if (projectName == null)
                    throw new SyntaxErrorException("The parameter of ProjectTableReference at an even position has to be a WValueExpression.");

                ScalarFunction byFunction = scalarSubquery.CompileToFunction(context, dbConnection);

                projectByOp.AddProjectBy(projectName.Value, byFunction);
            }

            context.CurrentExecutionOperator = projectByOp;
            context.AddField(this.Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return projectByOp;
        }
    }

    partial class WRepeatTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WSelectQueryBlock contextSelect, repeatSelect;
            Split(out contextSelect, out repeatSelect);

            QueryCompilationContext rTableContext = new QueryCompilationContext(context);
            rTableContext.InBatchMode = context.InBatchMode;
            rTableContext.CarryOn = true;

            QueryCompilationContext initalContext = new QueryCompilationContext(context);
            initalContext.InBatchMode = context.InBatchMode;
            initalContext.CarryOn = true;
            GraphViewExecutionOperator getInitialRecordOp = contextSelect.Compile(initalContext, dbConnection);

            foreach (WSelectElement selectElement in contextSelect.SelectElements)
            {
                WSelectScalarExpression selectScalar = selectElement as WSelectScalarExpression;
                Debug.Assert(selectScalar != null,
                    "The SELECT elements of the sub-queries in a repeat table reference must be select scalar elements.");
                Debug.Assert(selectScalar.ColumnName != null, "selectScalar.ColumnName != null");

                rTableContext.AddField("R", selectScalar.ColumnName, ColumnGraphType.Value);
            }

            WRepeatConditionExpression repeatCondition = Parameters[1] as WRepeatConditionExpression;
            if (repeatCondition == null)
                throw new SyntaxErrorException("The second parameter of a repeat table reference must be WRepeatConditionExpression");

            int repeatTimes = repeatCondition.RepeatTimes;
            BooleanFunction terminationCondition = repeatCondition.TerminationCondition?.CompileToFunction(rTableContext, dbConnection);
            bool startFromContext = repeatCondition.StartFromContext;
            BooleanFunction emitCondition = repeatCondition.EmitCondition?.CompileToFunction(rTableContext, dbConnection);
            bool emitContext = repeatCondition.EmitContext;

            ConstantSourceOperator tempSourceOp = new ConstantSourceOperator();
            ContainerOperator innerSourceOp = new ContainerOperator(tempSourceOp);
            GraphViewExecutionOperator innerOp = repeatSelect.Compile(rTableContext, dbConnection);
            rTableContext.OuterContextOp.SourceEnumerator = innerSourceOp.GetEnumerator();

            RepeatOperator repeatOp;
            if (repeatTimes == -1)
                repeatOp = new RepeatOperator(context.CurrentExecutionOperator, initalContext.OuterContextOp, getInitialRecordOp, 
                    tempSourceOp, innerSourceOp, innerOp, terminationCondition, startFromContext, emitCondition, emitContext);
            else
                repeatOp = new RepeatOperator(context.CurrentExecutionOperator, initalContext.OuterContextOp, getInitialRecordOp, 
                    tempSourceOp, innerSourceOp, innerOp, repeatTimes, emitCondition, emitContext);

            context.CurrentExecutionOperator = repeatOp;

            //
            // Updates the raw record layout
            //
            foreach (WSelectElement selectElement in contextSelect.SelectElements)
            {
                WSelectScalarExpression selectScalar = selectElement as WSelectScalarExpression;
                context.AddField(Alias.Value, selectScalar.ColumnName, ColumnGraphType.Value);
            }

            return repeatOp;
        }
    }

    partial class WUnfoldTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            List<string> unfoldColumns = new List<string>();
            for (int i = 1; i < this.Parameters.Count; i++)
            {
                WValueExpression unfoldColumn = this.Parameters[i] as WValueExpression;
                Debug.Assert(unfoldColumn != null, "unfoldColumn != null");
                unfoldColumns.Add(unfoldColumn.Value);
            }

            UnfoldOperator unfoldOp = new UnfoldOperator(
                context.CurrentExecutionOperator,
                Parameters[0].CompileToFunction(context, dbConnection), 
                unfoldColumns);
            context.CurrentExecutionOperator = unfoldOp;

            foreach (string columnName in unfoldColumns) {
                context.AddField(Alias.Value, columnName, ColumnGraphType.Value);
            }

            return unfoldOp;
        }
    }

    partial class WPathTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            List<Tuple<ScalarFunction, bool, HashSet<string>>> pathStepList;
            List<ScalarFunction> byFuncList;
            WPathTableReference.GetPathStepListAndByFuncList(context, dbConnection, this.Parameters, 
                out pathStepList, out byFuncList);

            PathOperator pathOp = new PathOperator(context.CurrentExecutionOperator, pathStepList, byFuncList);
            context.CurrentExecutionOperator = pathOp;
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return pathOp;
        }
    }

    partial class WPath2TableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbcConnection)
        {
            //
            // If the boolean value is true, then it's a subPath to be unfolded
            //
            List<Tuple<ScalarFunction, bool>> pathStepList = new List<Tuple<ScalarFunction, bool>>();
            List<ScalarFunction> byFuncList = new List<ScalarFunction>();
            QueryCompilationContext byInitContext = new QueryCompilationContext(context);
            byInitContext.ClearField();
            byInitContext.AddField(GremlinKeyword.Compose1TableDefaultName, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            foreach (WScalarExpression expression in Parameters)
            {
                WFunctionCall basicStep = expression as WFunctionCall;
                WColumnReferenceExpression subPath = expression as WColumnReferenceExpression;
                WScalarSubquery byFunc = expression as WScalarSubquery;

                if (basicStep != null)
                {
                    pathStepList.Add(new Tuple<ScalarFunction, bool>(basicStep.CompileToFunction(context, dbcConnection), false));
                }
                else if (subPath != null)
                {
                    pathStepList.Add(new Tuple<ScalarFunction, bool>(subPath.CompileToFunction(context, dbcConnection), true));
                }
                else if (byFunc != null)
                {
                    byFuncList.Add(byFunc.CompileToFunction(byInitContext, dbcConnection));
                }
                else {
                    throw new QueryCompilationException(
                        "The parameter of WPathTableReference can only be a WFunctionCall/WColumnReferenceExpression/WScalarSubquery.");
                }
            }

            PathOperator2 pathOp = new PathOperator2(context.CurrentExecutionOperator, pathStepList, byFuncList);
            context.CurrentExecutionOperator = pathOp;
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return pathOp;
        }
    }

    partial class WInjectTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression injectColumn = this.Parameters[0] as WColumnReferenceExpression;
            //
            // In g.Inject() case, this injectColumnIndex parameter is useless
            //
            int injectColumnIndex = injectColumn == null ? 0 : context.LocateColumnReference(injectColumn);

            List<ScalarFunction> injectValues = new List<ScalarFunction>();

            for (int i = 1; i < this.Parameters.Count; i++)
            {
                WValueExpression injectValue = this.Parameters[i] as WValueExpression;
                Debug.Assert(injectValue != null, "injectValue != null");
                injectValues.Add(injectValue.CompileToFunction(context, dbConnection));

            }

            InjectOperator injectOp = new InjectOperator(context.CurrentExecutionOperator, context.RawRecordLayout.Count, injectColumnIndex,
                injectValues, this.IsList, GremlinKeyword.TableDefaultColumnName);
            context.CurrentExecutionOperator = injectOp;

            //
            // In g.Inject() case, the inject() step creates a new column in RawRecord
            //
            if (context.RawRecordLayout.Count == 0)
                context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return injectOp;
        }
    }

    partial class WAggregateTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WScalarSubquery getAggregateObjectSubqueryParameter = Parameters[0] as WScalarSubquery;
            if (getAggregateObjectSubqueryParameter == null)
                throw new SyntaxErrorException("The first parameter of an Aggregate function must be a WScalarSubquery.");
            ScalarFunction getAggregateObjectFunction = getAggregateObjectSubqueryParameter.CompileToFunction(context, dbConnection);

            string storedName = (Parameters[1] as WValueExpression).Value;

            IAggregateFunction sideEffectState;
            if (!context.SideEffectStates.TryGetValue(storedName, out sideEffectState))
            {
                sideEffectState = new CollectionFunction();
                context.SideEffectStates.Add(storedName, sideEffectState);
            }
            else if (!(sideEffectState is CollectionFunction)) {
                if (sideEffectState is GroupFunction) {
                    throw new QueryCompilationException("It's illegal to use the same sideEffect key of a group(string) step and an aggregate(string) step!");
                }
                else {
                    throw new QueryCompilationException("It's illegal to use the same sideEffect key of a tree(string) step and an aggregate(string) step!");
                }
            }


            AggregateOperator aggregateOp = new AggregateOperator(context.CurrentExecutionOperator, getAggregateObjectFunction,
                (CollectionFunction)sideEffectState);
            context.CurrentExecutionOperator = aggregateOp;
            // TODO: Change to correct ColumnGraphType
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return aggregateOp;
        }
    }

    partial class WStoreTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WScalarSubquery getStoreObjectSubqueryParameter = Parameters[0] as WScalarSubquery;
            if (getStoreObjectSubqueryParameter == null)
                throw new SyntaxErrorException("The first parameter of a Store function must be a WScalarSubquery.");
            ScalarFunction getStoreObjectFunction = getStoreObjectSubqueryParameter.CompileToFunction(context, dbConnection);

            string storedName = (Parameters[1] as WValueExpression).Value;
            
            IAggregateFunction sideEffectState;
            if (!context.SideEffectStates.TryGetValue(storedName, out sideEffectState))
            {
                sideEffectState = new CollectionFunction();
                context.SideEffectStates.Add(storedName, sideEffectState);
            }
            else if (!(sideEffectState is CollectionFunction)) {
                if (sideEffectState is GroupFunction) {
                    throw new QueryCompilationException("It's illegal to use the same sideEffect key of a group(string) step and an aggregate(string) step!");
                }
                else {
                    throw new QueryCompilationException("It's illegal to use the same sideEffect key of a tree(string) step and an aggregate(string) step!");
                }
            }

            StoreOperator storeOp = new StoreOperator(context.CurrentExecutionOperator, getStoreObjectFunction,
                (CollectionFunction) sideEffectState);
            context.CurrentExecutionOperator = storeOp;
            // TODO: Change to correct ColumnGraphType
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);


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
                Debug.Assert(selectScalar.ColumnName != null, "selectScalar.ColumnName != null");

                WColumnReferenceExpression columnRef = selectScalar.SelectExpr as WColumnReferenceExpression;
                //
                // TODO: Remove this case
                //
                if (columnRef != null && columnRef.ColumnType == ColumnType.Wildcard) {
                    continue;
                }
                context.AddField(Alias.Value, selectScalar.ColumnName, columnRef?.ColumnGraphType ?? ColumnGraphType.Value);
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
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

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
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return valueOp;
        }
    }

    partial class WTreeTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WValueExpression sideEffectKey = Parameters[0] as WValueExpression;
            WColumnReferenceExpression pathColumn = Parameters[1] as WColumnReferenceExpression;
            Debug.Assert(sideEffectKey != null, "sideEffectKey != null");
            Debug.Assert(pathColumn != null, "pathColumn != null");
            int pathIndex = context.LocateColumnReference(pathColumn);

            IAggregateFunction sideEffectState;
            if (!context.SideEffectStates.TryGetValue(sideEffectKey.Value, out sideEffectState))
            {
                sideEffectState = new TreeFunction();
                context.SideEffectStates.Add(sideEffectKey.Value, sideEffectState);
            }
            else if (sideEffectState is GroupFunction) {
                throw new QueryCompilationException("It's illegal to use the same sideEffect key of a group(string) step and a tree(string) step!");
            }
            else {
                throw new QueryCompilationException("It's illegal to use the same sideEffect key of a store/aggregate(string) step and a tree(string) step!");
            }

            TreeSideEffectOperator treeSideEffectOp = new TreeSideEffectOperator(
                context.CurrentExecutionOperator,
                (TreeFunction)sideEffectState,
                pathIndex);

            context.CurrentExecutionOperator = treeSideEffectOp;

            return treeSideEffectOp;
        }
    }

    partial class WGroupTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WScalarExpression groupKeySubQuery = Parameters[1];
            WScalarSubquery aggregateSubQuery = Parameters[2] as WScalarSubquery;
            Debug.Assert(aggregateSubQuery != null, "aggregateSubQuery != null");

            ScalarFunction groupKeyFunction = groupKeySubQuery.CompileToFunction(context, dbConnection);

            QueryCompilationContext subcontext = new QueryCompilationContext(context);
            ConstantSourceOperator tempSourceOp = new ConstantSourceOperator();
            ContainerOperator aggregatedSourceOp = new ContainerOperator(tempSourceOp);
            GraphViewExecutionOperator aggregateOp = aggregateSubQuery.SubQueryExpr.Compile(subcontext, dbConnection);
            subcontext.OuterContextOp.SourceEnumerator = aggregatedSourceOp.GetEnumerator();

            WValueExpression groupParameter = Parameters[0] as WValueExpression;
            if (!groupParameter.SingleQuoted && groupParameter.Value.Equals("null", StringComparison.OrdinalIgnoreCase))
            {
                GroupOperator groupOp = context.InBatchMode
                    ? new GroupInBatchOperator(
                        context.CurrentExecutionOperator,
                        groupKeyFunction,
                        tempSourceOp, aggregatedSourceOp, aggregateOp,
                        this.IsProjectingACollection,
                        context.RawRecordLayout.Count)
                    : new GroupOperator(
                        context.CurrentExecutionOperator,
                        groupKeyFunction,
                        tempSourceOp, aggregatedSourceOp, aggregateOp,
                        this.IsProjectingACollection,
                        context.RawRecordLayout.Count);

                context.CurrentExecutionOperator = groupOp;

                //if (!context.CarryOn)
                //    context.ClearField();
                // Change to correct ColumnGraphType
                context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

                return groupOp;
            }
            else
            {
                IAggregateFunction sideEffectState;
                if (!context.SideEffectStates.TryGetValue(groupParameter.Value, out sideEffectState))
                {
                    sideEffectState = new GroupFunction(tempSourceOp, aggregatedSourceOp, aggregateOp,
                        this.IsProjectingACollection);
                    context.SideEffectStates.Add(groupParameter.Value, sideEffectState);
                }
                else if (sideEffectState is GroupFunction) {
                    throw new QueryCompilationException("Multi group with a same sideEffect key is an undefined behavior in Gremlin and hence not supported.");
                }
                else if (sideEffectState is TreeFunction) {
                    throw new QueryCompilationException("It's illegal to use the same sideEffect key of a group(string) step and a tree(string) step!");
                }
                else {
                    throw new QueryCompilationException("It's illegal to use the same sideEffect key of a group(string) step and a store/aggregate(string) step!");
                }

                GroupSideEffectOperator groupSideEffectOp = new GroupSideEffectOperator(
                    context.CurrentExecutionOperator,
                    (GroupFunction)sideEffectState,
                    groupKeyFunction);

                context.CurrentExecutionOperator = groupSideEffectOp;

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
            ContainerEnumerator sourceEnumerator = new ContainerEnumerator();

            // If QueryDerivedTable is the first table in the whole script
            if (context.CurrentExecutionOperator == null)
                derivedTableContext.OuterContextOp = null;
            else
            {
                derivedTableContext.InBatchMode = context.InBatchMode;
                derivedTableContext.OuterContextOp.SourceEnumerator = sourceEnumerator;
            }
            
            GraphViewExecutionOperator subQueryOp = derivedSelectQueryBlock.Compile(derivedTableContext, dbConnection);

            QueryDerivedTableOperator queryDerivedTableOp =
                context.InBatchMode
                    ? new QueryDerivedInBatchOperator(context.CurrentExecutionOperator, subQueryOp, sourceEnumerator,
                        context.RawRecordLayout.Count)
                    : new QueryDerivedTableOperator(context.CurrentExecutionOperator, subQueryOp, sourceEnumerator,
                        context.RawRecordLayout.Count);

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

    partial class WSumLocalTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression targetField = Parameters[0] as WColumnReferenceExpression;

            SumLocalOperator sumLocalOp = new SumLocalOperator(context.CurrentExecutionOperator,
                context.LocateColumnReference(targetField));
            context.CurrentExecutionOperator = sumLocalOp;
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return sumLocalOp;
        }
    }

    partial class WMaxLocalTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression targetField = Parameters[0] as WColumnReferenceExpression;

            MaxLocalOperator maxLocalOp = new MaxLocalOperator(context.CurrentExecutionOperator,
                context.LocateColumnReference(targetField));
            context.CurrentExecutionOperator = maxLocalOp;
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return maxLocalOp;
        }
    }

    partial class WMinLocalTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression targetField = Parameters[0] as WColumnReferenceExpression;

            MinLocalOperator minLocalOp = new MinLocalOperator(context.CurrentExecutionOperator,
                context.LocateColumnReference(targetField));
            context.CurrentExecutionOperator = minLocalOp;
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return minLocalOp;
        }
    }

    partial class WMeanLocalTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression targetField = Parameters[0] as WColumnReferenceExpression;

            MeanLocalOperator meanLocalOp = new MeanLocalOperator(context.CurrentExecutionOperator,
                context.LocateColumnReference(targetField));
            context.CurrentExecutionOperator = meanLocalOp;
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return meanLocalOp;
        }
    }

    partial class WCoinTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            GraphViewExecutionOperator inputOp = context.CurrentExecutionOperator;

            double probability = double.Parse(((WValueExpression)this.Parameters[0]).Value);

            GraphViewExecutionOperator coinOp = new CoinOperator(inputOp, probability);
            context.CurrentExecutionOperator = coinOp;
            return coinOp;
        }
    }

    partial class WSampleGlobalTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            GraphViewExecutionOperator inputOp = context.CurrentExecutionOperator;
            long amountToSample = long.Parse(((WValueExpression)this.Parameters[0]).Value);
            ScalarFunction byFunction = this.Parameters.Count > 1 
                ? this.Parameters[1].CompileToFunction(context, dbConnection) 
                : null;  // Can be null if no "by" step

            GraphViewExecutionOperator sampleOp = new SampleOperator(inputOp, amountToSample, byFunction);
            context.CurrentExecutionOperator = sampleOp;
            return sampleOp;
        }
    }

    partial class WOrderGlobalTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            List<Tuple<ScalarFunction, IComparer>> orderByElements = new List<Tuple<ScalarFunction, IComparer>>();

            foreach (Tuple<WScalarExpression, IComparer> tuple in OrderParameters)
            {
                WScalarExpression byParameter = tuple.Item1;

                ScalarFunction byFunction = byParameter.CompileToFunction(context, dbConnection);
                IComparer comparer = tuple.Item2;

                orderByElements.Add(new Tuple<ScalarFunction, IComparer>(byFunction, comparer));
            }

            OrderOperator orderOp = context.InBatchMode
                ? new OrderInBatchOperator(context.CurrentExecutionOperator, orderByElements) 
                : new OrderOperator(context.CurrentExecutionOperator, orderByElements);
            context.CurrentExecutionOperator = orderOp;

            return orderOp;
        }
    }

    partial class WOrderLocalTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression inputObject = this.Parameters[0] as WColumnReferenceExpression;
            Debug.Assert(inputObject != null, "inputObject != null");
            int inputObjectIndex = context.LocateColumnReference(inputObject);

            QueryCompilationContext byInitContext = new QueryCompilationContext(context);
            byInitContext.ClearField();
            byInitContext.AddField(GremlinKeyword.Compose1TableDefaultName, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            List<Tuple<ScalarFunction, IComparer>> orderByElements = new List<Tuple<ScalarFunction, IComparer>>();

            foreach (Tuple<WScalarExpression, IComparer> tuple in OrderParameters)
            {
                WScalarExpression byParameter = tuple.Item1;

                ScalarFunction byFunction = byParameter.CompileToFunction(byInitContext, dbConnection);
                IComparer comparer = tuple.Item2;

                orderByElements.Add(new Tuple<ScalarFunction, IComparer>(byFunction, comparer));
            }

            List<string> populateColumns = new List<string> { GraphViewKeywords.KW_TABLE_DEFAULT_COLUMN_NAME };
            for (int i = this.OrderParameters.Count + 1; i < this.Parameters.Count; i++)
            {
                WValueExpression populateColumn = this.Parameters[i] as WValueExpression;
                Debug.Assert(populateColumn != null, "populateColumn != null");

                if (populateColumn.Value.Equals(GraphViewKeywords.KW_TABLE_DEFAULT_COLUMN_NAME)) {
                    continue;
                }
                populateColumns.Add(populateColumn.Value);
            }

            OrderLocalOperator orderLocalOp = new OrderLocalOperator(context.CurrentExecutionOperator, inputObjectIndex,
                orderByElements, populateColumns);
            context.CurrentExecutionOperator = orderLocalOp;
            foreach (string columnName in populateColumns) {
                context.AddField(Alias.Value, columnName, ColumnGraphType.Value);
            }

            return orderLocalOp;
        }
    }

    partial class WRangeTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            //
            // The first parameter is used only when isLocal = true
            //
            WColumnReferenceExpression inputCollection = Parameters[0] as WColumnReferenceExpression;
            int lowEnd = int.Parse((Parameters[1] as WValueExpression).Value);
            int highEnd = int.Parse((Parameters[2] as WValueExpression).Value);
            int localFlag = int.Parse((Parameters[3] as WValueExpression).Value);
            int tailFlag = int.Parse((Parameters[4] as WValueExpression).Value);
            bool isLocal = localFlag > 0;
            bool isTail = tailFlag > 0;

            List<string> populateColumns = new List<string> { GraphViewKeywords.KW_TABLE_DEFAULT_COLUMN_NAME };
            for (int i = 5; i < this.Parameters.Count; i++)
            {
                WValueExpression populateColumn = this.Parameters[i] as WValueExpression;
                Debug.Assert(populateColumn != null, "populateColumn != null");

                if (populateColumn.Value.Equals(GraphViewKeywords.KW_TABLE_DEFAULT_COLUMN_NAME)) {
                    continue;
                }
                populateColumns.Add(populateColumn.Value);
            }

            //
            // Compilation of Tail op, which returns lastN elements
            //
            if (isTail)
            {
                int lastN = highEnd < 0 ? 1 : highEnd;

                if (isLocal)
                {
                    TailLocalOperator tailLocalOp = new TailLocalOperator(context.CurrentExecutionOperator,
                        context.LocateColumnReference(inputCollection), lastN, populateColumns);
                    context.CurrentExecutionOperator = tailLocalOp;
                    foreach (string columnName in populateColumns) {
                        context.AddField(Alias.Value, columnName, ColumnGraphType.Value);
                    }

                    return tailLocalOp;
                }
                else
                {
                    TailOperator tailOp = context.InBatchMode
                        ? new TailInBatchOperator(context.CurrentExecutionOperator, lastN)
                        : new TailOperator(context.CurrentExecutionOperator, lastN);
                    context.CurrentExecutionOperator = tailOp;

                    return tailOp;
                }
            }
            //
            // Compilation of Range op, which return elements from [startIndex, startIndex + count)
            // If count == -1, return all elements starting from startIndex 
            //
            else
            {
                if ((lowEnd > highEnd && highEnd >= 0) || (lowEnd >= 0 && highEnd < -1)) {
                    throw new QueryCompilationException(string.Format("Not a legal range: [{0}, {1}]", lowEnd, highEnd));
                }

                int startIndex = lowEnd < 0 ? 0 : lowEnd;
                int count;
                if (highEnd == -1) {
                    count = -1;
                }
                else if ((count = highEnd - startIndex) < 0) {
                    count = 0;
                }

                if (isLocal)
                {
                    RangeLocalOperator rangeLocalOp = new RangeLocalOperator(context.CurrentExecutionOperator,
                        context.LocateColumnReference(inputCollection), startIndex, count, populateColumns);
                    context.CurrentExecutionOperator = rangeLocalOp;
                    foreach (string columnName in populateColumns) {
                        context.AddField(Alias.Value, columnName, ColumnGraphType.Value);
                    }

                    return rangeLocalOp;;
                }
                else
                {
                    RangeOperator rangeOp = context.InBatchMode
                        ? new RangeInBatchOperator(context.CurrentExecutionOperator, startIndex, count)
                        : new RangeOperator(context.CurrentExecutionOperator, startIndex, count);
                    context.CurrentExecutionOperator = rangeOp;

                    return rangeOp;
                }
            }
        }
    }

    /// <summary>
    /// The table-valued function that takes as input a CompositeField field in an input record, 
    /// and outputs a new record in which members in the CompositeField field populated as separate
    /// fields/columns. 
    /// </summary>
    partial class WDecomposeTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression decomposeTargetParameter = this.Parameters[0] as WColumnReferenceExpression;
            Debug.Assert(decomposeTargetParameter != null, "decomposeTargetParameter != null");

            int decomposeTargetIndex = context.LocateColumnReference(decomposeTargetParameter);
            List<string> populateColumns = new List<string>();

            for (int i = 1; i < this.Parameters.Count; i++)
            {
                WValueExpression populateColumn = this.Parameters[i] as WValueExpression;
                Debug.Assert(populateColumn != null, "populateColumn != null");

                populateColumns.Add(populateColumn.Value);
            }

            Decompose1Operator decompose1Op = new Decompose1Operator(context.CurrentExecutionOperator,
                decomposeTargetIndex, populateColumns, GremlinKeyword.TableDefaultColumnName);
            context.CurrentExecutionOperator = decompose1Op;

            foreach (string populateColumn in populateColumns) {
                context.AddField(Alias.Value, populateColumn, ColumnGraphType.Value);
            }

            return decompose1Op;
        }
    }

    partial class WSimplePathTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression pathColumn = Parameters[0] as WColumnReferenceExpression;
            Debug.Assert(pathColumn != null, "pathColumn != null");
            int pathIndex = context.LocateColumnReference(pathColumn);

            SimplePathOperator simplePathOp = new SimplePathOperator(context.CurrentExecutionOperator, pathIndex);
            context.CurrentExecutionOperator = simplePathOp;

            return simplePathOp;
        }
    }

    partial class WCyclicPathTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression pathColumn = Parameters[0] as WColumnReferenceExpression;
            Debug.Assert(pathColumn != null, "pathColumn != null");
            int pathIndex = context.LocateColumnReference(pathColumn);

            CyclicPathOperator cyclicPathOp = new CyclicPathOperator(context.CurrentExecutionOperator, pathIndex);
            context.CurrentExecutionOperator = cyclicPathOp;

            return cyclicPathOp;
        }
    }

    partial class WValueMapTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression inputTarget = this.Parameters[0] as WColumnReferenceExpression;
            Debug.Assert(inputTarget != null, "inputTarget != null");
            int inputTargetIndex = context.LocateColumnReference(inputTarget);

            WValueExpression includingMetaParameter = this.Parameters[1] as WValueExpression;
            Debug.Assert(includingMetaParameter != null, "includingMetaParameter != null");
            bool includingMetaValue = int.Parse(includingMetaParameter.Value) > 0;

            List<string> propertyNameList = new List<string>();
            for (int i = 2; i < this.Parameters.Count; i++)
            {
                WValueExpression propertyName = this.Parameters[i] as WValueExpression;
                Debug.Assert(propertyName != null, "propertyName != null");

                propertyNameList.Add(propertyName.Value);
            }

            ValueMapOperator valueMapOp = new ValueMapOperator(context.CurrentExecutionOperator, inputTargetIndex,
                includingMetaValue, propertyNameList);
            context.CurrentExecutionOperator = valueMapOp;
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return valueMapOp;
        }
    }

    partial class WPropertyMapTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression inputTarget = this.Parameters[0] as WColumnReferenceExpression;
            Debug.Assert(inputTarget != null, "inputTarget != null");
            int inputTargetIndex = context.LocateColumnReference(inputTarget);

            List<string> propertyNameList = new List<string>();
            for (int i = 1; i < this.Parameters.Count; i++)
            {
                WValueExpression propertyName = this.Parameters[i] as WValueExpression;
                Debug.Assert(propertyName != null, "propertyName != null");

                propertyNameList.Add(propertyName.Value);
            }

            PropertyMapOperator propertyMapOp = new PropertyMapOperator(context.CurrentExecutionOperator,
                inputTargetIndex, propertyNameList);
            context.CurrentExecutionOperator = propertyMapOp;
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return propertyMapOp;
        }
    }

    partial class WChooseTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WScalarSubquery targetSubquery = this.Parameters[0] as WScalarSubquery;
            Debug.Assert(targetSubquery != null, "targetSubquery != null");

            WScalarSubquery trueTraversalParameter = this.Parameters[1] as WScalarSubquery;
            Debug.Assert(trueTraversalParameter != null, "trueTraversalParameter != null");
            WSelectQueryBlock selectQueryBlock = trueTraversalParameter.SubQueryExpr as WSelectQueryBlock;
            Debug.Assert(selectQueryBlock != null, "selectQueryBlock != null");

            WScalarSubquery falseTraversalParameter = this.Parameters[2] as WScalarSubquery;
            Debug.Assert(falseTraversalParameter != null, "falseTraversalParameter != null");


            ScalarFunction targetSubqueryFunc = targetSubquery.CompileToFunction(context, dbConnection);

            ConstantSourceOperator tempSourceOp = new ConstantSourceOperator();

            QueryCompilationContext trueBranchSubContext = new QueryCompilationContext(context);
            trueBranchSubContext.CarryOn = true;
            trueBranchSubContext.InBatchMode = context.InBatchMode;
            ContainerOperator trueBranchSourceOp = new ContainerOperator(tempSourceOp);
            GraphViewExecutionOperator trueBranchTraversalOp = trueTraversalParameter.SubQueryExpr.Compile(trueBranchSubContext, dbConnection);
            trueBranchSubContext.OuterContextOp.SourceEnumerator = trueBranchSourceOp.GetEnumerator();

            QueryCompilationContext falseBranchSubContext = new QueryCompilationContext(context);
            falseBranchSubContext.CarryOn = true;
            falseBranchSubContext.InBatchMode = context.InBatchMode;
            ContainerOperator falseBranchSourceOp = new ContainerOperator(tempSourceOp);
            GraphViewExecutionOperator falseBranchTraversalOp = falseTraversalParameter.SubQueryExpr.Compile(falseBranchSubContext, dbConnection);
            falseBranchSubContext.OuterContextOp.SourceEnumerator = falseBranchSourceOp.GetEnumerator();

            ChooseOperator chooseOp = new ChooseOperator(
                context.CurrentExecutionOperator,
                targetSubqueryFunc,
                tempSourceOp, 
                trueBranchSourceOp, trueBranchTraversalOp, 
                falseBranchSourceOp, falseBranchTraversalOp);
            context.CurrentExecutionOperator = chooseOp;

            foreach (WSelectElement selectElement in selectQueryBlock.SelectElements)
            {
                WSelectScalarExpression selectScalar = selectElement as WSelectScalarExpression;
                Debug.Assert(selectScalar != null, "selectScalar != null");
                Debug.Assert(selectScalar.ColumnName != null, "selectScalar.ColumnName != null");

                WColumnReferenceExpression columnRef = selectScalar.SelectExpr as WColumnReferenceExpression;
                //
                // TODO: Remove this case
                //
                if (columnRef != null && columnRef.ColumnType == ColumnType.Wildcard) {
                    continue;
                }
                context.AddField(Alias.Value, selectScalar.ColumnName, columnRef?.ColumnGraphType ?? ColumnGraphType.Value);
            }

            return chooseOp;
        }
    }

    partial class WChooseWithOptionsTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WScalarSubquery targetSubquery = this.Parameters[0] as WScalarSubquery;
            Debug.Assert(targetSubquery != null, "targetSubquery != null");

            ScalarFunction targetSubqueryFunc = targetSubquery.CompileToFunction(context, dbConnection);
            ConstantSourceOperator tempSourceOp = new ConstantSourceOperator();
            ContainerOperator optionSourceOp = new ContainerOperator(tempSourceOp);

            ChooseWithOptionsOperator chooseWithOptionsOp =
                new ChooseWithOptionsOperator(context.CurrentExecutionOperator, targetSubqueryFunc, tempSourceOp,
                    optionSourceOp);

            WSelectQueryBlock firstSelectQuery = null;
            for (int i = 1; i < this.Parameters.Count; i += 2)
            {
                WValueExpression value = this.Parameters[i] as WValueExpression;
                Debug.Assert(value != null, "value != null");
                if (this.IsOptionNone(value)) {
                    value = null;
                }

                WScalarSubquery scalarSubquery = this.Parameters[i + 1] as WScalarSubquery;
                Debug.Assert(scalarSubquery != null, "scalarSubquery != null");

                if (firstSelectQuery == null)
                {
                    firstSelectQuery = scalarSubquery.SubQueryExpr as WSelectQueryBlock;
                    Debug.Assert(firstSelectQuery != null, "firstSelectQuery != null");
                }

                QueryCompilationContext subcontext = new QueryCompilationContext(context);
                subcontext.CarryOn = true;
                subcontext.InBatchMode = context.InBatchMode;
                GraphViewExecutionOperator optionTraversalOp = scalarSubquery.SubQueryExpr.Compile(subcontext, dbConnection);
                subcontext.OuterContextOp.SourceEnumerator = optionSourceOp.GetEnumerator();
                chooseWithOptionsOp.AddOptionTraversal(value?.CompileToFunction(context, dbConnection), optionTraversalOp);
            }

            foreach (WSelectElement selectElement in firstSelectQuery.SelectElements)
            {
                WSelectScalarExpression selectScalar = selectElement as WSelectScalarExpression;
                Debug.Assert(selectScalar != null, "selectScalar != null");
                Debug.Assert(selectScalar.ColumnName != null, "selectScalar.ColumnName != null");

                WColumnReferenceExpression columnRef = selectScalar.SelectExpr as WColumnReferenceExpression;
                //
                // TODO: Remove this case
                //
                if (columnRef != null && columnRef.ColumnType == ColumnType.Wildcard) {
                    continue;
                }
                context.AddField(Alias.Value, selectScalar.ColumnName, columnRef?.ColumnGraphType ?? ColumnGraphType.Value);
            }

            context.CurrentExecutionOperator = chooseWithOptionsOp;
            return chooseWithOptionsOp;
        }
    }

    /// <summary>
    /// This table-valued function is for Map.select(keys) or Map.select(values)
    /// </summary>
    partial class WSelectColumnTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            // inputTargetParameter always points to MapField
            WColumnReferenceExpression inputTargetParameter = this.Parameters[0] as WColumnReferenceExpression;
            Debug.Assert(inputTargetParameter != null, "inputTargetParameter != null");
            int inputTargetIndex = context.LocateColumnReference(inputTargetParameter);

            // Whether extracts keys or values from MapField
            WValueExpression selectParameter = this.Parameters[1] as WValueExpression;
            Debug.Assert(selectParameter != null, "selectParameter != null");
            bool isSelectKeys = selectParameter.Value.Equals("keys", StringComparison.OrdinalIgnoreCase);
            List<string> populateColumns = new List<string>();
            for (int i = 2; i < this.Parameters.Count; i++)
            {
                WValueExpression populateParameter = this.Parameters[i] as WValueExpression;
                Debug.Assert(populateParameter != null, "populateParameter != null");
                populateColumns.Add(populateParameter.Value);
            }

            SelectColumnOperator selectColumnOp = new SelectColumnOperator(context.CurrentExecutionOperator,
                inputTargetIndex, isSelectKeys, populateColumns);
            context.CurrentExecutionOperator = selectColumnOp;
            foreach (string populateColumnName in populateColumns)
            {
                context.AddField(Alias.Value, populateColumnName, ColumnGraphType.Value);
            }

            return selectColumnOp;
        }
    }

    /// <summary>
    /// This TVF is for select() of more than one key. The result of this function is MapField.
    /// </summary>
    partial class WSelectTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbcConnection)
        {
            WColumnReferenceExpression inputObjectParameter = this.Parameters[0] as WColumnReferenceExpression;
            Debug.Assert(inputObjectParameter != null, "inputObjectParameter != null");
            int inputObjectIndex = context.LocateColumnReference(inputObjectParameter);

            WColumnReferenceExpression pathParameter = this.Parameters[1] as WColumnReferenceExpression;
            Debug.Assert(pathParameter != null, "pathParameter != null");
            int pathIndex = context.LocateColumnReference(pathParameter);

            WValueExpression popParameter = this.Parameters[2] as WValueExpression;
            Debug.Assert(popParameter != null, "popParameter != null");
            GraphViewKeywords.Pop popType;
            if (!Enum.TryParse(popParameter.Value, true, out popType))
                throw new QueryCompilationException("Unsupported pop type.");

            List<ScalarFunction> byFuncList = new List<ScalarFunction>();
            QueryCompilationContext byInitContext = new QueryCompilationContext(context);
            byInitContext.ClearField();
            byInitContext.AddField(GremlinKeyword.Compose1TableDefaultName, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            List<string> selectLabels = new List<string>();

            for (int i = 3; i < this.Parameters.Count; i++)
            {
                WValueExpression label = this.Parameters[i] as WValueExpression;
                WScalarSubquery byFunc = this.Parameters[i] as WScalarSubquery;

                if (label != null) {
                    selectLabels.Add(label.Value);
                }
                else if (byFunc != null) {
                    byFuncList.Add(byFunc.CompileToFunction(byInitContext, dbcConnection));
                }
                else {
                    throw new QueryCompilationException(
                        "The parameter of WSelectTableReference can only be a WValueExpression or WScalarSubquery.");
                }
            }
            
            SelectOperator selectOp = new SelectOperator(
                context.CurrentExecutionOperator,
                context.SideEffectStates,
                inputObjectIndex,
                pathIndex,
                popType,
                selectLabels,
                byFuncList,
                GremlinKeyword.TableDefaultColumnName);

            context.CurrentExecutionOperator = selectOp;
            context.AddField(Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return selectOp;
        }
    }

    /// <summary>
    /// The TVF is for select() of a single key. The result of the function follows the following precedence:
    /// 
    /// 1) if there is a global table tagged with key through store()/aggregate()/group()/groupCount()/tree(),
    /// return for each record a new record with an additional field containing the content of the table.
    /// 2) if the prior step returns MapField, select the key from the map.   
    /// 3) Otherwise, 
    ///     when the key appears only once, the result is 
    ///     a record in which the tagged (composite) field's elements are populated as individual fields. 
    ///     When the key appears more than once, the result is an array (CollectionField) with each array element
    ///     being tagged CompositeField. 
    /// </summary>
    partial class WSelectOneTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbcConnection)
        {
            // inputObjectParameter points to the table reference right before WPathTableReference
            WColumnReferenceExpression inputObjectParameter = this.Parameters[0] as WColumnReferenceExpression;
            Debug.Assert(inputObjectParameter != null, "inputObjectParameter != null");
            int inputObjectIndex = context.LocateColumnReference(inputObjectParameter);

            // WPathTableReference always proceeds WSelectOneTableReference in the FROM clause
            WColumnReferenceExpression pathParameter = this.Parameters[1] as WColumnReferenceExpression;
            Debug.Assert(pathParameter != null, "pathParameter != null");
            int pathIndex = context.LocateColumnReference(pathParameter);

            WValueExpression popParameter = this.Parameters[2] as WValueExpression;
            Debug.Assert(popParameter != null, "popParameter != null");
            GraphViewKeywords.Pop popType;
            if (!Enum.TryParse(popParameter.Value, true, out popType))
                throw new QueryCompilationException("Unsupported pop type.");


            WValueExpression labelParameter = this.Parameters[3] as WValueExpression;
            Debug.Assert(labelParameter != null, "labelParameter != null");
            string selectLabel = labelParameter.Value;

            WScalarSubquery byParameter = this.Parameters[4] as WScalarSubquery;
            Debug.Assert(byParameter != null, "byParameter != null");

            QueryCompilationContext byInitContext = new QueryCompilationContext(context);
            byInitContext.ClearField();
            byInitContext.AddField(GremlinKeyword.Compose1TableDefaultName, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);
            ScalarFunction byFunc = byParameter.CompileToFunction(byInitContext, dbcConnection);
            
            List<string> populateColumns = new List<string>();
            for (int i = 5; i < this.Parameters.Count; i++)
            {
                WValueExpression populateColumnParameter = this.Parameters[i] as WValueExpression;
                Debug.Assert(populateColumnParameter != null, "populateColumnParameter != null");
                
                populateColumns.Add(populateColumnParameter.Value);
            }

            SelectOneOperator selectOneOp = new SelectOneOperator(
                context.CurrentExecutionOperator,
                context.SideEffectStates,
                inputObjectIndex,
                pathIndex,
                popType,
                selectLabel,
                byFunc,
                populateColumns,
                GremlinKeyword.TableDefaultColumnName
                );

            context.CurrentExecutionOperator = selectOneOp;
            foreach (string columnName in populateColumns) {
                context.AddField(Alias.Value, columnName, ColumnGraphType.Value);
            }

            return selectOneOp;
        }
    }

    partial class WCountLocalTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewConnection dbConnection)
        {
            WColumnReferenceExpression inputObjectParameter = this.Parameters[0] as WColumnReferenceExpression;
            Debug.Assert(inputObjectParameter != null, "inputObjectParameter != null");
            int inputObjectIndex = context.LocateColumnReference(inputObjectParameter);

            CountLocalOperator countLocalOp = new CountLocalOperator(context.CurrentExecutionOperator, inputObjectIndex);
            context.CurrentExecutionOperator = countLocalOp;
            context.AddField(this.Alias.Value, GremlinKeyword.TableDefaultColumnName, ColumnGraphType.Value);

            return countLocalOp;
        }
    }
}

