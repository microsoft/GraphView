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
                    ((AdjacencyListField) vertexField[isReverseAdj ? KW_VERTEX_REV_EDGE : KW_VERTEX_EDGE])
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
                JObject tmpVertexObject = (JObject)((JObject)dynamicItem)[nodeAlias];
                string vertexId = (string)tmpVertexObject[KW_DOC_ID];

                if (crossApplyEdgeOnServer) {
                    //
                    // Note: checking gotVertexIds.Add(vertexId) is for the correctness of cardinality
                    //
                    if (EdgeDocumentHelper.IsBuildingTheAdjacencyListLazily(
                            tmpVertexObject, isReverseAdj, this.Connection.UseReverseEdges) 
                        && gotVertexIds.Add(vertexId)) {
                        VertexField vertexField = this.Connection.VertexCache.AddOrUpdateVertexField(vertexId, tmpVertexObject);
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
                            VertexField vertexField = this.Connection.VertexCache.AddOrUpdateVertexField(vertexId, tmpVertexObject);
                            yield return makeCrossAppliedRecord(vertexField, edgeId);
                        }
                    }
                }
                else
                {
                    if (!gotVertexIds.Add(vertexId)) {
                        continue;
                    }
                    VertexField vertexField = this.Connection.VertexCache.AddOrUpdateVertexField(vertexId, tmpVertexObject);
                    yield return makeRawRecord(vertexField);
                }
            }
        }
    }
}
