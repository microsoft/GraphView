using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Linq;

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
        public List<string> Properties { get; set; } 

        public List<ColumnGraphType> ProjectedColumnsType { get; set; }

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
            if (string.IsNullOrEmpty(vertexQuery.WhereSearchCondition))
            {
                vertexQuery.WhereSearchCondition = $"{vertexQuery.Alias}.id = {vertexQuery.Alias}._partition";
            }
            else {
                vertexQuery.WhereSearchCondition = $"({vertexQuery.WhereSearchCondition}) AND ({vertexQuery.Alias}.id = {vertexQuery.Alias}._partition)";
            }

            string queryScript = vertexQuery.ToString(DatabaseType.DocumentDB);
            IQueryable<dynamic> items = this.Connection.ExecuteQuery(queryScript);

            List<string> properties = new List<string>(vertexQuery.Properties);
            List<ColumnGraphType> projectedColumnsType = vertexQuery.ProjectedColumnsType;

            string nodeAlias = properties[0];
            // Skip i = 0, which is the (node.* as nodeAlias) field
            properties.RemoveAt(0);


            //
            // Batch strategy:
            //  - For "small" vertexes, just parse JObject & return the VertexField
            //  - For "large" vertexes, store them in a list, send query to get their edge-documents later
            //
            // Dictionary<vertexId, vertexObject>
            Dictionary<string, JObject> largeVertexes = new Dictionary<string, JObject>();

            Func<VertexField, RawRecord> makeRawRecord = (vertexField) => {
                Debug.Assert(vertexField != null);

                RawRecord rawRecord = new RawRecord();
                int endOfNodePropertyIndex = projectedColumnsType.FindIndex(e => e == ColumnGraphType.EdgeSource);
                if (endOfNodePropertyIndex == -1) endOfNodePropertyIndex = properties.Count;
                // Fill node property field
                for (int i = 0; i < endOfNodePropertyIndex; i++) {
                    FieldObject propertyValue = vertexField[properties[i]];
                    rawRecord.Append(propertyValue);
                }
                return rawRecord;
            };


            HashSet<string> gotVertexIds = new HashSet<string>();

            foreach (dynamic dynamicItem in items) {
                JObject vertexObject = (JObject)((JObject)dynamicItem)[nodeAlias];
                string vertexId = (string)vertexObject["id"];
                if (!gotVertexIds.Add(vertexId)) {
                    continue;
                }

                if (EdgeDocumentHelper.IsSpilledVertex(vertexObject, true) ||
                    EdgeDocumentHelper.IsSpilledVertex(vertexObject, false)) {

                    // If either incoming or outgoing edges are spilled, retrieve them in a batch.
                    largeVertexes.Add(vertexId, vertexObject);
                }
                else {
                    // If no edge spilling, return them first
                    VertexField vertexField = this.Connection.VertexCache.GetVertexField((string)vertexObject["id"], vertexObject);
                    yield return makeRawRecord(vertexField);
                }

                //
                // Commented by Wenbin Hou
                // These codes does not work for DocDB (we never get into the for-loop)
                // CROSS APPLY over spilled edge-document is impossible in DocDB
                //
                #region ================== COMMENT BEGIN ==================
                //
                // Fill all the backward matching edges' fields
                //int startOfEdgeIndex = endOfNodePropertyIndex;
                //int endOfEdgeIndex = projectedColumnsType.FindIndex(startOfEdgeIndex,
                //    e => e == ColumnGraphType.EdgeSource);
                //if (endOfEdgeIndex == -1) endOfEdgeIndex = properties.Count;
                //for (int i = startOfEdgeIndex; i < properties.Count;)
                //{
                //    // These are corresponding meta fields generated in the ConstructMetaFieldSelectClauseOfEdge()
                //    string source = item[properties[i++]].ToString();
                //    string sink = item[properties[i++]].ToString();
                //    string other = item[properties[i++]].ToString();
                //    string edgeOffset = item[properties[i++]].ToString();
                //    long physicalOffset = (long)item[properties[i++]];
                //    string adjType = item[properties[i++]].ToString();
                //    //var isReversedAdjList = adjType.Equals("_reverse_edge", StringComparison.OrdinalIgnoreCase);

                //    EdgeField edgeField = (vertexObject[adjType] as AdjacencyListField).GetEdgeField(source, physicalOffset);

                //    rawRecord.Append(new StringField(source));
                //    rawRecord.Append(new StringField(sink));
                //    rawRecord.Append(new StringField(other));
                //    rawRecord.Append(new StringField(edgeOffset));

                //    // Fill edge property field
                //    for (; i < endOfEdgeIndex; i++)
                //        rawRecord.Append(edgeField[properties[i]]);

                //    //edgeField.Label = edgeField["label"]?.ToValue;
                //    //edgeField.InV = source;
                //    //edgeField.OutV = sink;
                //    //edgeField.InVLabel = isReversedAdjList
                //    //    ? edgeField["_sinkLabel"]?.ToValue
                //    //    : vertexObject["label"]?.ToValue;
                //    //edgeField.OutVLabel = isReversedAdjList
                //    //    ? vertexObject["label"]?.ToValue
                //    //    : edgeField["_sinkLabel"]?.ToValue;

                //    endOfEdgeIndex = projectedColumnsType.FindIndex(i,
                //        e => e == ColumnGraphType.EdgeSource);
                //}
                //
                #endregion ================== COMMENT END ==================

            }

            // 
            // In case the spilled edge-document's amount is too much (and exceeds DocDB InClauseLimit),
            // Split the dictionary into multiple parts
            //
            // List<Dictionary<vertexId, vertexObject>>
            List<Dictionary<string, JObject>> vertexDicts = new List<Dictionary<string, JObject>>(
                largeVertexes.Count / GraphViewConnection.InClauseLimit + 1);
            {
                int index = 0;
                Dictionary<string, JObject> current = null;
                foreach (KeyValuePair<string, JObject> pair in largeVertexes) {
                    if (index == 0) {
                        current = new Dictionary<string, JObject>();
                        vertexDicts.Add(current);
                    }
                    current.Add(pair.Key, pair.Value);

                    if (index == GraphViewConnection.InClauseLimit - 1) {
                        index = 0;
                        current = null;
                    }
                }
            }

            // Process each vertexDict in vertexDicts
            // Elements in a vertexDict are limited to "InClauseLimit" (thus can be batched)
            foreach (Dictionary<string, JObject> vertexDict in vertexDicts) {
                string inClause = string.Join(", ", vertexDict.Keys.Select(vertexId => $"'{vertexId}'"));
                string edgeDocumentsQuery =
                    $"SELECT *\n" +
                    $"FROM edgeDoc\n" +
                    $"WHERE edgeDoc._vertex_id IN ({inClause})";
                IQueryable<dynamic> edgeDocuments = Connection.ExecuteQuery(edgeDocumentsQuery);

                // Dictionary<vertexId, Dictionary<edgeDocumentId, edgeDocument>>
                Dictionary<string, Dictionary<string, JObject>> edgeDict = new Dictionary<string, Dictionary<string, JObject>>();
                foreach (JObject edgeDocument in edgeDocuments) {
                    string vertexId = (string)edgeDocument["_vertex_id"];
                    Dictionary<string, JObject> edgeDocSet;
                    edgeDict.TryGetValue(vertexId, out edgeDocSet);
                    if (edgeDocSet == null) {
                        edgeDocSet = new Dictionary<string, JObject>();
                        edgeDict.Add(vertexId, edgeDocSet);
                    }

                    edgeDocSet.Add((string)edgeDocument["id"], edgeDocument);
                }

                foreach (KeyValuePair<string, Dictionary<string, JObject>> pair in edgeDict) {
                    string vertexId = pair.Key;
                    Dictionary<string, JObject> edgeDocDict = pair.Value;  // contains both in & out edges
                    VertexField vertexField = this.Connection.VertexCache.GetVertexField(vertexId, vertexDict[vertexId], edgeDocDict);
                    yield return makeRawRecord(vertexField);
                }
            }
        }
    }
}
