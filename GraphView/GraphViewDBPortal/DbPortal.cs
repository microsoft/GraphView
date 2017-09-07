using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace GraphView.GraphViewDBPortal
{
    internal enum DatabaseType
    {
        DocumentDB,
        JsonServer
    }

    internal abstract class DbPortal : IDisposable
    {
        public GraphViewConnection Connection { get; protected set; }

        public void Dispose() { }

        public IEnumerator<Tuple<VertexField, RawRecord>> GetVerticesAndEdgesViaVertices(JsonQuery vertexQuery,
            GraphViewCommand command)
        {
            IEnumerable<JObject> items = this.ExecuteQueryScript(vertexQuery);
            List<string> nodeProperties = new List<string>(vertexQuery.NodeProperties);
            List<string> edgeProperties = new List<string>(vertexQuery.EdgeProperties);

            string nodeAlias = nodeProperties[0];
            // Skip i = 0, which is the (node.* as nodeAlias) field
            nodeProperties.RemoveAt(0);

            //
            // TODO: Refactor
            //
            string edgeAlias = null;
            bool isReverseAdj = false;
            bool isStartVertexTheOriginVertex = false;
            bool crossApplyEdgeOnServer = edgeProperties.Any();
            if (crossApplyEdgeOnServer)
            {
                edgeAlias = edgeProperties[0];
                isReverseAdj = bool.Parse(edgeProperties[1]);
                isStartVertexTheOriginVertex = bool.Parse(edgeProperties[2]);
                edgeProperties.RemoveAt(0);
                edgeProperties.RemoveAt(0);
                edgeProperties.RemoveAt(0);
            }

            //
            // Batch strategy:
            //  - For "small" vertexes, they have been cross applied on the server side
            //  - For "large" vertexes, just return the VertexField, the adjacency list decoder will
            //    construct spilled adjacency lists in batch mode and cross apply edges after that 
            //
            Func<VertexField, string, Tuple<VertexField, RawRecord>> makeCrossAppliedRecord = (vertexField, edgeId) => {
                Debug.Assert(vertexField != null);

                RawRecord nodeRecord = new RawRecord();
                //
                // Fill node property field
                //
                foreach (string propertyName in nodeProperties)
                {
                    FieldObject propertyValue = vertexField[propertyName];
                    nodeRecord.Append(propertyValue);
                }

                RawRecord edgeRecord = new RawRecord(edgeProperties.Count);

                EdgeField edgeField =
                    ((AdjacencyListField)vertexField[isReverseAdj ? DocumentDBKeywords.KW_VERTEX_REV_EDGE : DocumentDBKeywords.KW_VERTEX_EDGE])
                    .GetEdgeField(edgeId, true);

                string startVertexId = vertexField.VertexId;
                AdjacencyListDecoder.FillMetaField(edgeRecord, edgeField, startVertexId, vertexField.Partition, isStartVertexTheOriginVertex, isReverseAdj);
                AdjacencyListDecoder.FillPropertyField(edgeRecord, edgeField, edgeProperties);

                nodeRecord.Append(edgeRecord);
                return new Tuple<VertexField, RawRecord>(vertexField, nodeRecord);
            };

            Func<VertexField, Tuple<VertexField, RawRecord>> makeRawRecord = (vertexField) => {
                Debug.Assert(vertexField != null);

                RawRecord rawRecord = new RawRecord();
                //
                // Fill node property field
                //
                foreach (string propertyName in nodeProperties)
                {
                    FieldObject propertyValue = vertexField[propertyName];
                    rawRecord.Append(propertyValue);
                }
                return new Tuple<VertexField, RawRecord>(vertexField, rawRecord);
            };

            HashSet<string> uniqueVertexIds = new HashSet<string>();
            HashSet<string> uniqueEdgeIds = new HashSet<string>();
            foreach (JObject dynamicItem in items)
            {
                JObject tmpVertexObject = (JObject)((JObject)dynamicItem)[nodeAlias];
                string vertexId = (string)tmpVertexObject[DocumentDBKeywords.KW_DOC_ID];

                if (crossApplyEdgeOnServer)
                {
                    // Note: since vertex properties can be multi-valued, 
                    // a DocumentDB query needs a join clause in the FROM clause
                    // to retrieve vertex property values, which may result in 
                    // the same vertex being returned multiple times. 
                    // We use the hash set uniqueVertexIds to ensure one vertex is 
                    // produced only once. 
                    if (EdgeDocumentHelper.IsBuildingTheAdjacencyListLazily(
                            tmpVertexObject,
                            isReverseAdj,
                            this.Connection.UseReverseEdges) &&
                        uniqueVertexIds.Add(vertexId))
                    {
                        VertexField vertexField = command.VertexCache.AddOrUpdateVertexField(vertexId, tmpVertexObject);
                        yield return makeRawRecord(vertexField);
                    }
                    else // When the DocumentDB query crosses apply edges 
                    {
                        JObject edgeObjct = (JObject)((JObject)dynamicItem)[edgeAlias];
                        string edgeId = (string)edgeObjct[DocumentDBKeywords.KW_EDGE_ID];

                        if (uniqueEdgeIds.Add(edgeId))
                        {
                            VertexField vertexField = command.VertexCache.AddOrUpdateVertexField(vertexId, tmpVertexObject);
                            yield return makeCrossAppliedRecord(vertexField, edgeId);
                        }
                    }
                }
                else
                {
                    if (!uniqueVertexIds.Add(vertexId))
                    {
                        continue;
                    }
                    VertexField vertexField = command.VertexCache.AddOrUpdateVertexField(vertexId, tmpVertexObject);
                    yield return makeRawRecord(vertexField);
                }
            }
        }

        public IEnumerator<RawRecord> GetVerticesAndEdgesViaEdges(JsonQuery edgeQuery, GraphViewCommand command)
        {
            IEnumerable<JObject> items = this.ExecuteQueryScript(edgeQuery);
            List<string> nodeProperties = new List<string>(edgeQuery.NodeProperties);
            List<string> edgeProperties = new List<string>(edgeQuery.EdgeProperties);

            string nodeAlias = nodeProperties[0];
            nodeProperties.RemoveAt(0);

            string edgeAlias = edgeProperties[0];
            edgeProperties.RemoveAt(0);

            HashSet<string> spilledVertexIdSet = new HashSet<string>();
            HashSet<string> spilledVertexPartitionSet = new HashSet<string>();
            //
            // <vertex id, edge id>
            //
            Dictionary<string, List<string>> vertexIdAndEdgeIdsDict = new Dictionary<string, List<string>>();
            //
            // <vertex id, <edgeDocumentId, edgeObject>>
            //
            Dictionary<string, List<Tuple<string, JObject>>> vertexIdAndEdgeObjectsDict =
                new Dictionary<string, List<Tuple<string, JObject>>>();

            foreach (JObject dynamicItem in items)
            {
                JObject tmpObject = (JObject)dynamicItem[nodeAlias];
                JObject edgeObject = (JObject)dynamicItem[edgeAlias];
                //
                // This is a spilled edge document
                //
                if (tmpObject[DocumentDBKeywords.KW_EDGEDOC_VERTEXID] != null)
                {
                    string vertexId = tmpObject[DocumentDBKeywords.KW_EDGEDOC_VERTEXID].ToString();
                    spilledVertexIdSet.Add(vertexId);
                    string partition = this.Connection.GetDocumentPartition(tmpObject);
                    if (partition != null)
                    {
                        spilledVertexPartitionSet.Add(partition);
                    }

                    List<Tuple<string, JObject>> edgeObjects;
                    if (!vertexIdAndEdgeObjectsDict.TryGetValue(vertexId, out edgeObjects))
                    {
                        edgeObjects = new List<Tuple<string, JObject>>();
                        vertexIdAndEdgeObjectsDict.Add(vertexId, edgeObjects);
                    }
                    edgeObjects.Add(new Tuple<string, JObject>((string)tmpObject[DocumentDBKeywords.KW_DOC_ID], edgeObject));

                    List<string> edgeIds;
                    if (!vertexIdAndEdgeIdsDict.TryGetValue(vertexId, out edgeIds))
                    {
                        edgeIds = new List<string>();
                        vertexIdAndEdgeIdsDict.Add(vertexId, edgeIds);
                    }
                    edgeIds.Add((string)edgeObject[DocumentDBKeywords.KW_DOC_ID]);
                }
                else
                {
                    string vertexId = (string)tmpObject[DocumentDBKeywords.KW_DOC_ID];
                    command.VertexCache.AddOrUpdateVertexField(vertexId, tmpObject);
                    List<string> edgeIds;
                    if (!vertexIdAndEdgeIdsDict.TryGetValue(vertexId, out edgeIds))
                    {
                        edgeIds = new List<string>();
                        vertexIdAndEdgeIdsDict.Add(vertexId, edgeIds);
                    }
                    edgeIds.Add((string)edgeObject[DocumentDBKeywords.KW_DOC_ID]);
                }
            }

            if (spilledVertexIdSet.Any())
            {
                const string NODE_ALISE = "Node";
                JsonQuery query = new JsonQuery
                {
                    NodeAlias = NODE_ALISE
                };
                query.AddSelectElement("*");
                query.RawWhereClause = new WInPredicate(new WColumnReferenceExpression(NODE_ALISE, "id"), spilledVertexIdSet.ToList());
                if (spilledVertexPartitionSet.Any())
                {
                    query.WhereConjunction(
                        new WInPredicate(
                            new WValueExpression($"{NODE_ALISE}{this.Connection.GetPartitionPathIndexer()}"),
                            spilledVertexPartitionSet.ToList()), BooleanBinaryExpressionType.And);
                }

                // TODO: remove this code when you understand it.
                //                string idInClause = string.Join(", ", spilledVertexIdSet.Select(id => $"'{id}'"));
                //                string partitionInClause = string.Join(", ", spilledVertexPartitionSet.Select(partition => $"'{partition}'"));
                //                string queryScript = $"SELECT * FROM Node WHERE Node.id IN ({idInClause})" +
                //                                     (string.IsNullOrEmpty(partitionInClause)
                //                                         ? ""
                //                                         : $" AND Node{this.Connection.GetPartitionPathIndexer()} IN ({partitionInClause})");
                //                IEnumerable<dynamic> spilledVertices = this.Connection.ExecuteDocDbQuery(queryScript);
                IEnumerable<JObject> spilledVertices = this.ExecuteQueryScript(query);
                foreach (JObject vertex in spilledVertices)
                {
                    JObject vertexObject = vertex;
                    string vertexId = (string)vertexObject[DocumentDBKeywords.KW_DOC_ID];
                    VertexField vertexField = command.VertexCache.AddOrUpdateVertexField(vertexId, vertexObject);
                    vertexField.ConstructPartialLazyAdjacencyList(vertexIdAndEdgeObjectsDict[vertexId], false);
                }
            }

            foreach (KeyValuePair<string, List<string>> pair in vertexIdAndEdgeIdsDict)
            {
                string vertexId = pair.Key;
                List<string> edgeIds = pair.Value;
                VertexField vertexField = command.VertexCache.GetVertexField(vertexId);

                foreach (string edgeId in edgeIds)
                {
                    RawRecord nodeRecord = new RawRecord();
                    //
                    // Fill node property field
                    //
                    foreach (string propertyName in nodeProperties)
                    {
                        FieldObject propertyValue = vertexField[propertyName];
                        nodeRecord.Append(propertyValue);
                    }

                    RawRecord edgeRecord = new RawRecord(edgeProperties.Count);

                    EdgeField edgeField = vertexField.AdjacencyList.GetEdgeField(edgeId, false);
                    Debug.Assert(edgeField != null, "edgeField != null");

                    string startVertexId = vertexField.VertexId;
                    AdjacencyListDecoder.FillMetaField(edgeRecord, edgeField, startVertexId, vertexField.Partition, true, false);
                    AdjacencyListDecoder.FillPropertyField(edgeRecord, edgeField, edgeProperties);

                    nodeRecord.Append(edgeRecord);
                    yield return nodeRecord;
                }
            }
        }

        public List<VertexField> GetVerticesByIds(HashSet<string> vertexId, GraphViewCommand command, string partition,
            bool constructEdges = false)
        {

            const string NODE_ALIAS = "node";
            var jsonQuery = new JsonQuery
            {
                NodeAlias = NODE_ALIAS,
                RawWhereClause = new WBooleanComparisonExpression
                {
                    ComparisonType = BooleanComparisonType.Equals,
                    FirstExpr = new WColumnReferenceExpression(NODE_ALIAS, DocumentDBKeywords.KW_EDGEDOC_IDENTIFIER),
                    SecondExpr = new WValueExpression("null")
                }
            };
            // SELECT node
            jsonQuery.AddSelectElement(NODE_ALIAS);
            jsonQuery.FlatProperties.Add(DocumentDBKeywords.KW_EDGEDOC_IDENTIFIER);

            jsonQuery.WhereConjunction(new WInPredicate(
                new WColumnReferenceExpression(NODE_ALIAS, GremlinKeyword.NodeID),
                vertexId.ToList()), BooleanBinaryExpressionType.And);

            if (partition != null)
            {
                jsonQuery.WhereConjunction(new WBooleanComparisonExpression
                {
                    ComparisonType = BooleanComparisonType.Equals,
                    FirstExpr = new WValueExpression($"{NODE_ALIAS}{command.Connection.GetPartitionPathIndexer()}", false),
                    SecondExpr = new WValueExpression(partition, true)
                }, BooleanBinaryExpressionType.And);
            }

            jsonQuery.NodeProperties = new List<string> { "node", "*" };
            jsonQuery.EdgeProperties = new List<string>();

            IEnumerator<Tuple<VertexField, RawRecord>> queryResult = this.GetVerticesAndEdgesViaVertices(jsonQuery, command);

            List<VertexField> result = new List<VertexField>();
            while (queryResult.MoveNext())
            {
                VertexField vertex = queryResult.Current.Item1;
                result.Add(vertex);
            }

            if (constructEdges)
            {
                // TODO: need double check on JsonServer
                EdgeDocumentHelper.ConstructLazyAdjacencyList(command, EdgeType.Both, vertexId, new HashSet<string>());
            }

            return result;
        }
        
        public List<JObject> GetEdgeDocuments(JsonQuery query)
        {
            return this.ExecuteQueryScript(query).ToList();
        }

        public JObject GetEdgeDocument(JsonQuery query)
        {
            return this.ExecuteQueryScript(query).First();
        }

        public JObject GetVertexDocument(JsonQuery query)
        {
            return this.ExecuteQueryScript(query).First();
        }
//
//        public abstract Task ReplaceOrDeleteDocumentAsync(string docId, JObject docObject, GraphViewCommand command, string partition = null);

        internal abstract IEnumerable<JObject> ExecuteQueryScript(JsonQuery jsonQuery);
    }

}
