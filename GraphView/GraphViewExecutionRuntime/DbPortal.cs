using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Linq;
using static GraphView.GraphViewKeywords;

namespace GraphView
{
    internal enum DatabaseType
    {
        DocumentDB,
        JsonServer
    }

    internal class JsonQuery
    {
        public string SelectClause { get; set; }
        public string JoinClause { get; set; }
        public string WhereSearchCondition { get; set; }
        public string Alias { get; set; }

        public List<string> NodeProperties { get; set; } 

        public List<string> EdgeProperties { get; set; }

        public JsonQuery() { }

        public JsonQuery(JsonQuery rhs)
        {
            this.SelectClause = rhs.SelectClause;
            this.JoinClause = rhs.JoinClause;
            this.WhereSearchCondition = rhs.WhereSearchCondition;
            this.Alias = rhs.Alias;
            this.NodeProperties = rhs.NodeProperties;
            this.EdgeProperties = rhs.EdgeProperties;
        }

        public string ToString(DatabaseType dbType)
        {
            switch (dbType) {
            case DatabaseType.DocumentDB:
                return $"SELECT {this.SelectClause} " +
                       $"FROM Node {this.Alias} " +
                       $"{this.JoinClause} " +
                       $"{(string.IsNullOrEmpty(this.WhereSearchCondition) ? "" : $"WHERE {this.WhereSearchCondition}")}";
            case DatabaseType.JsonServer:
                return $"FOR {this.Alias} IN ('Node') " +
                       $"{(string.IsNullOrEmpty(this.WhereSearchCondition) ? "" : $"WHERE {this.WhereSearchCondition}")}" +
                       $"{this.SelectClause}";
            default:
                throw new NotImplementedException();
            }
        }
    }

    internal abstract class DbPortal : IDisposable
    {
        public GraphViewConnection Connection { get; protected set; }

        public void Dispose() { }

        public abstract IEnumerator<RawRecord> GetVerticesAndEdgesViaVertices(JsonQuery vertexQuery);
        //public abstract IEnumerator<RawRecord> GetVerticesViaExternalAPI(JsonQuery vertexQuery);

        public abstract IEnumerator<RawRecord> GetVerticesAndEdgesViaEdges(JsonQuery edgeQuery);
    }

    internal class DocumentDbPortal : DbPortal
    {
        public DocumentDbPortal(GraphViewConnection connection)
        {
            Connection = connection;
        }

        public override IEnumerator<RawRecord> GetVerticesAndEdgesViaVertices(JsonQuery vertexQuery)
        {
            string queryScript = vertexQuery.ToString(DatabaseType.DocumentDB);
            IEnumerable<dynamic> items = this.Connection.ExecuteQuery(queryScript);
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
            if (crossApplyEdgeOnServer) {
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
            Func<VertexField, string, RawRecord> makeCrossAppliedRecord = (vertexField, edgeId) => {
                Debug.Assert(vertexField != null);

                RawRecord nodeRecord = new RawRecord();
                //
                // Fill node property field
                //
                foreach (string propertyName in nodeProperties) {
                    FieldObject propertyValue = vertexField[propertyName];
                    nodeRecord.Append(propertyValue);
                }

                RawRecord edgeRecord = new RawRecord(edgeProperties.Count);

                EdgeField edgeField =
                    ((AdjacencyListField) vertexField[isReverseAdj ? KW_VERTEX_REV_EDGE : KW_VERTEX_EDGE])
                    .GetEdgeField(edgeId, true);

                string startVertexId = vertexField.VertexId;
                AdjacencyListDecoder.FillMetaField(edgeRecord, edgeField, startVertexId, vertexField.Partition, isStartVertexTheOriginVertex, isReverseAdj);
                AdjacencyListDecoder.FillPropertyField(edgeRecord, edgeField, edgeProperties);

                nodeRecord.Append(edgeRecord);
                return nodeRecord;
            };

            Func<VertexField, RawRecord> makeRawRecord = (vertexField) => {
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
                return rawRecord;
            };

            HashSet<string> uniqueVertexIds = new HashSet<string>();
            HashSet<string> uniqueEdgeIds = new HashSet<string>();
            foreach (dynamic dynamicItem in items) {
                JObject tmpVertexObject = (JObject)((JObject)dynamicItem)[nodeAlias];
                string vertexId = (string)tmpVertexObject[KW_DOC_ID];

                if (crossApplyEdgeOnServer) {
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
                        VertexField vertexField = this.Connection.VertexCache.AddOrUpdateVertexField(vertexId, tmpVertexObject);
                        yield return makeRawRecord(vertexField);
                    }
                    else // When the DocumentDB query crosses apply edges 
                    {
                        JObject edgeObjct = (JObject)((JObject)dynamicItem)[edgeAlias];
                        string edgeId = (string)edgeObjct[KW_EDGE_ID];

                        if (uniqueEdgeIds.Add(edgeId)) {
                            VertexField vertexField = this.Connection.VertexCache.AddOrUpdateVertexField(vertexId, tmpVertexObject);
                            yield return makeCrossAppliedRecord(vertexField, edgeId);
                        }
                    }
                }
                else
                {
                    if (!uniqueVertexIds.Add(vertexId)) {
                        continue;
                    }
                    VertexField vertexField = this.Connection.VertexCache.AddOrUpdateVertexField(vertexId, tmpVertexObject);
                    yield return makeRawRecord(vertexField);
                }
            }
        }

        public override IEnumerator<RawRecord> GetVerticesAndEdgesViaEdges(JsonQuery edgeQuery)
        {
            string queryScript = edgeQuery.ToString(DatabaseType.DocumentDB);
            IEnumerable<dynamic> items = this.Connection.ExecuteQuery(queryScript);
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

            foreach (dynamic dynamicItem in items)
            {
                JObject tmpObject = (JObject)((JObject)dynamicItem)[nodeAlias];
                JObject edgeObject = (JObject)((JObject)dynamicItem)[edgeAlias];
                //
                // This is a spilled edge document
                //
                if (tmpObject[KW_EDGEDOC_VERTEXID] != null)
                {
                    string vertexId = tmpObject[KW_EDGEDOC_VERTEXID].ToString();
                    spilledVertexIdSet.Add(vertexId);
                    string partition = this.Connection.GetDocumentPartition(tmpObject);
                    if (partition != null) {
                        spilledVertexPartitionSet.Add(partition);
                    }

                    List<Tuple<string, JObject>> edgeObjects;
                    if (!vertexIdAndEdgeObjectsDict.TryGetValue(vertexId, out edgeObjects))
                    {
                        edgeObjects = new List<Tuple<string, JObject>>();
                        vertexIdAndEdgeObjectsDict.Add(vertexId, edgeObjects);
                    }
                    edgeObjects.Add(new Tuple<string, JObject>((string)tmpObject[KW_DOC_ID], edgeObject));

                    List<string> edgeIds;
                    if (!vertexIdAndEdgeIdsDict.TryGetValue(vertexId, out edgeIds))
                    {
                        edgeIds = new List<string>();
                        vertexIdAndEdgeIdsDict.Add(vertexId, edgeIds);
                    }
                    edgeIds.Add((string)edgeObject[KW_DOC_ID]);
                }
                else
                {
                    string vertexId = (string)tmpObject[KW_DOC_ID];
                    this.Connection.VertexCache.AddOrUpdateVertexField(vertexId, tmpObject);
                    List<string> edgeIds;
                    if (!vertexIdAndEdgeIdsDict.TryGetValue(vertexId, out edgeIds))
                    {
                        edgeIds = new List<string>();
                        vertexIdAndEdgeIdsDict.Add(vertexId, edgeIds);
                    }
                    edgeIds.Add((string)edgeObject[KW_DOC_ID]);
                }
            }

            if (spilledVertexIdSet.Any())
            {
                string idInClause = string.Join(", ", spilledVertexIdSet.Select(id => $"'{id}'"));
                string partitionInClause = string.Join(", ", spilledVertexPartitionSet.Select(partition => $"'{partition}'"));
                queryScript = $"SELECT * FROM Node WHERE Node.id IN ({idInClause})" +
                              (string.IsNullOrEmpty(partitionInClause)
                                  ? ""
                                  : $" AND Node{this.Connection.GetPartitionPathIndexer()} IN ({partitionInClause})");
                IEnumerable<dynamic> spilledVertices = this.Connection.ExecuteQuery(queryScript);
                foreach (dynamic vertex in spilledVertices)
                {
                    JObject vertexObject = (JObject)vertex;
                    string vertexId = (string)vertexObject[KW_DOC_ID];
                    VertexField vertexField = this.Connection.VertexCache.AddOrUpdateVertexField(vertexId, vertexObject);
                    vertexField.ConstructPartialLazyAdjacencyList(vertexIdAndEdgeObjectsDict[vertexId], false);
                }
            }

            foreach (KeyValuePair<string, List<string>> pair in vertexIdAndEdgeIdsDict)
            {
                string vertexId = pair.Key;
                List<string> edgeIds = pair.Value;
                VertexField vertexField = this.Connection.VertexCache.GetVertexField(vertexId);

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

        /*
        /// <summary>
        /// Retrieve vertices in conventional JSON documents. The JSON query to send
        /// returns both vertex documents and edge documents, from which 
        /// vertex fields are constructed and returned. 
        /// </summary>
        /// <param name="vertexQuery"></param>
        /// <returns></returns>
        public override IEnumerator<RawRecord> GetVerticesViaExternalAPI(JsonQuery vertexQuery)
        {
            string queryScript = vertexQuery.ToString(DatabaseType.DocumentDB);
            IEnumerable<dynamic> items = this.Connection.ExecuteQuery(queryScript);
            List<string> nodeProperties = new List<string>(vertexQuery.NodeProperties);

            string nodeAlias = nodeProperties[0];
            // Skip i = 0, which is the (node.* as nodeAlias) field
            nodeProperties.RemoveAt(0);

            Func<VertexField, RawRecord> makeRawRecord = (vertexField) => {
                Debug.Assert(vertexField != null);

                RawRecord rawRecord = new RawRecord();
                //
                // Fill node property field
                //
                foreach (string propertyName in nodeProperties) {
                    FieldObject propertyValue = vertexField[propertyName];
                    rawRecord.Append(propertyValue);
                }
                return rawRecord;
            };

            List<RawRecord> results = new List<RawRecord>();
            List<dynamic> edgeDocuments = new List<dynamic>();
            HashSet<string> gotVertexIds = new HashSet<string>();

            foreach (dynamic dynamicItem in items) {
                JObject tmpObject = (JObject)((JObject)dynamicItem)[nodeAlias];
                //
                // This is a spilled edge document
                //
                if (tmpObject[GraphViewKeywords.KW_EDGEDOC_VERTEXID] != null) {
                    edgeDocuments.Add(tmpObject);
                }
                else {
                    string vertexId = (string)tmpObject[KW_DOC_ID];
                    gotVertexIds.Add(vertexId);
                    VertexField vertexField = this.Connection.VertexCache.AddOrUpdateVertexField(vertexId, tmpObject);
                    results.Add(makeRawRecord(vertexField));
                }
            }

            //
            // Construct vertice's forward/backward adjacency lists
            //
            if (edgeDocuments.Count > 0)
            {
                // Dictionary<vertexId, Dictionary<edgeDocumentId, edgeDocument>>
                Dictionary<string, Dictionary<string, JObject>> edgeDict =
                    new Dictionary<string, Dictionary<string, JObject>>();

                foreach (JObject edgeDocument in edgeDocuments) {
                    // Save edgeDocument's etag if necessary
                    this.Connection.VertexCache.SaveCurrentEtagNoOverride(edgeDocument);
                }

                EdgeDocumentHelper.FillEdgeDict(edgeDict, edgeDocuments);

                foreach (KeyValuePair<string, Dictionary<string, JObject>> pair in edgeDict) {
                    string vertexId = pair.Key;
                    Dictionary<string, JObject> edgeDocDict = pair.Value; // contains both in & out edges
                    VertexField vertexField;
                    vertexField = this.Connection.VertexCache.GetVertexField(vertexId);

                    if (this.Connection.UseReverseEdges) {
                        vertexField.ConstructSpilledOrVirtualAdjacencyListField(edgeDocDict);
                    }
                    else {
                        string vertexLabel = vertexField[GraphViewKeywords.KW_VERTEX_LABEL].ToValue;
                        if (!vertexField.AdjacencyList.HasBeenFetched) {
                            vertexField.ConstructSpilledOrVirtualAdjacencyListField(vertexId, vertexLabel, vertexField.Partition, false, edgeDocDict);
                        }
                    }
                    gotVertexIds.Remove(vertexId);
                }

                foreach (string vertexId in gotVertexIds) {
                    VertexField vertexField;
                    this.Connection.VertexCache.TryGetVertexField(vertexId, out vertexField);
                    if (this.Connection.UseReverseEdges) {
                        vertexField.ConstructSpilledOrVirtualAdjacencyListField(new Dictionary<string, JObject>());
                    }
                    else {
                        string vertexLabel = vertexField[GraphViewKeywords.KW_VERTEX_LABEL].ToValue;
                        if (!vertexField.AdjacencyList.HasBeenFetched) {
                            vertexField.ConstructSpilledOrVirtualAdjacencyListField(vertexId, vertexLabel, vertexField.Partition, false, new Dictionary<string, JObject>());
                        }
                    }
                }
            }

            foreach (RawRecord record in results) {
                yield return record;
            }
        }
        */
    }
}
