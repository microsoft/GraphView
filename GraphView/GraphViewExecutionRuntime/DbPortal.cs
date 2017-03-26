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

        public abstract IEnumerator<RawRecord> GetVertices(JsonQuery vertexQuery);
    }

    internal class DocumentDbPortal : DbPortal
    {
        public DocumentDbPortal(GraphViewConnection connection)
        {
            Connection = connection;
        }

        public override IEnumerator<RawRecord> GetVertices(JsonQuery vertexQuery)
        {
            if (string.IsNullOrEmpty(vertexQuery.WhereSearchCondition)) {
                vertexQuery.WhereSearchCondition = $"{vertexQuery.Alias}.{KW_DOC_ID} = {vertexQuery.Alias}.{KW_DOC_PARTITION}";
            }
            else {
                vertexQuery.WhereSearchCondition = $"({vertexQuery.WhereSearchCondition}) AND ({vertexQuery.Alias}.{KW_DOC_ID} = {vertexQuery.Alias}.{KW_DOC_PARTITION})";
            }

            string queryScript = vertexQuery.ToString(DatabaseType.DocumentDB);
            IQueryable<dynamic> items = this.Connection.ExecuteQuery(queryScript);
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
                    (vertexField[isReverseAdj ? KW_VERTEX_REV_EDGE : KW_VERTEX_EDGE] as AdjacencyListField)
                    .GetEdgeField(edgeId);

                string startVertexId = vertexField[KW_DOC_ID].ToValue;
                AdjacencyListDecoder.FillMetaField(edgeRecord, edgeField, startVertexId, isStartVertexTheOriginVertex, isReverseAdj);
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

            HashSet<string> gotVertexIds = new HashSet<string>();
            HashSet<string> gotEdgeIds = new HashSet<string>();
            foreach (dynamic dynamicItem in items) {
                JObject vertexObject = (JObject)((JObject)dynamicItem)[nodeAlias];
                string vertexId = (string)vertexObject[KW_DOC_ID];

                if (crossApplyEdgeOnServer) {
                    //
                    // Note: checking gotVertexIds.Add(vertexId) is for the correctness of cardinality
                    //
                    if (EdgeDocumentHelper.IsSpilledVertex(vertexObject, isReverseAdj) && gotVertexIds.Add(vertexId)) {
                        VertexField vertexField = this.Connection.VertexCache.GetVertexField(vertexId, vertexObject);
                        yield return makeRawRecord(vertexField);
                    }
                    else
                    {
                        JObject edgeObjct = (JObject)((JObject)dynamicItem)[edgeAlias];
                        string edgeId = (string)edgeObjct[KW_EDGE_ID];
                        //
                        // Note: checking gotEdgeIds.Add(edgeId) is for the correctness of cardinality
                        //
                        if (gotEdgeIds.Add(edgeId)) {
                            VertexField vertexField = this.Connection.VertexCache.GetVertexField(vertexId, vertexObject);
                            yield return makeCrossAppliedRecord(vertexField, edgeId);
                        }
                    }
                }
                else
                {
                    if (!gotVertexIds.Add(vertexId)) {
                        continue;
                    }
                    VertexField vertexField = this.Connection.VertexCache.GetVertexField(vertexId, vertexObject);
                    yield return makeRawRecord(vertexField);
                }
            }

            //// 
            //// In case the spilled edge-document's amount is too much (and exceeds DocDB InClauseLimit),
            //// Split the dictionary into multiple parts
            ////
            //// List<Dictionary<vertexId, vertexObject>>
            //List<Dictionary<string, JObject>> vertexDicts = new List<Dictionary<string, JObject>>(
            //    largeVertexes.Count / GraphViewConnection.InClauseLimit + 1);
            //{
            //    int index = 0;
            //    Dictionary<string, JObject> current = null;
            //    foreach (KeyValuePair<string, JObject> pair in largeVertexes) {
            //        if (index == 0) {
            //            current = new Dictionary<string, JObject>();
            //            vertexDicts.Add(current);
            //        }
            //        current.Add(pair.Key, pair.Value);

            //        if (index == GraphViewConnection.InClauseLimit - 1) {
            //            index = 0;
            //            current = null;
            //        }
            //    }
            //}

            //// Process each vertexDict in vertexDicts
            //// Elements in a vertexDict are limited to "InClauseLimit" (thus can be batched)
            //foreach (Dictionary<string, JObject> vertexDict in vertexDicts)
            //{
            //    string inClause = string.Join(", ", vertexDict.Keys.Select(vertexId => $"'{vertexId}'"));
            //    string edgeDocumentsQuery =
            //        $"SELECT *\n" +
            //        $"FROM edgeDoc\n" +
            //        $"WHERE edgeDoc.{KW_EDGEDOC_VERTEXID} IN ({inClause})";
            //    IQueryable<dynamic> edgeDocuments = Connection.ExecuteQuery(edgeDocumentsQuery);

            //    // Dictionary<vertexId, Dictionary<edgeDocumentId, edgeDocument>>
            //    Dictionary<string, Dictionary<string, JObject>> edgeDict = new Dictionary<string, Dictionary<string, JObject>>();
            //    foreach (JObject edgeDocument in edgeDocuments)
            //    {
            //        string vertexId = (string)edgeDocument[KW_EDGEDOC_VERTEXID];
            //        Dictionary<string, JObject> edgeDocSet;
            //        edgeDict.TryGetValue(vertexId, out edgeDocSet);
            //        if (edgeDocSet == null)
            //        {
            //            edgeDocSet = new Dictionary<string, JObject>();
            //            edgeDict.Add(vertexId, edgeDocSet);
            //        }

            //        edgeDocSet.Add((string)edgeDocument[KW_EDGE_ID], edgeDocument);
            //    }

            //    foreach (KeyValuePair<string, Dictionary<string, JObject>> pair in edgeDict)
            //    {
            //        string vertexId = pair.Key;
            //        Dictionary<string, JObject> edgeDocDict = pair.Value;  // contains both in & out edges
            //        VertexField vertexField = this.Connection.VertexCache.GetVertexField(vertexId, vertexDict[vertexId], edgeDocDict);
            //        yield return makeRawRecord(vertexField);
            //    }
            //}
        }
    }
}
