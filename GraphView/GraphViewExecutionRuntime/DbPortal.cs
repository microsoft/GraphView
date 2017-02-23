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

            foreach (dynamic dynamicItem in items)
            {
                JObject item = (JObject)dynamicItem;
                JToken vertexJson = item[nodeAlias];
                RawRecord rawRecord = new RawRecord();
                //VertexField vertexObject = Connection.VertexCache.GetVertexField(vertexJson["id"].ToString(), vertexJson.ToString());
                VertexField vertexObject = this.Connection.VertexCache.GetVertexField((string)vertexJson["id"], (JObject)vertexJson);
                Debug.Assert(vertexObject != null);

                int endOfNodePropertyIndex = projectedColumnsType.FindIndex(e => e == ColumnGraphType.EdgeSource);
                if (endOfNodePropertyIndex == -1) endOfNodePropertyIndex = properties.Count;
                // Fill node property field
                for (int i = 0; i < endOfNodePropertyIndex; i++)
                {
                    FieldObject propertyValue = vertexObject[properties[i]];
                    //var propertyType = projectedColumnsType[i];

                    rawRecord.Append(propertyValue);
                }

                // TODO: No more backward edges processing when GetVertices()
                // Fill all the backward matching edges' fields
                int startOfEdgeIndex = endOfNodePropertyIndex;
                int endOfEdgeIndex = projectedColumnsType.FindIndex(startOfEdgeIndex,
                    e => e == ColumnGraphType.EdgeSource);
                if (endOfEdgeIndex == -1) endOfEdgeIndex = properties.Count;
                for (int i = startOfEdgeIndex; i < properties.Count;)  // TODO: ASK: i < endOfEdgeIndex?
                {
                    // These are corresponding meta fields generated in the ConstructMetaFieldSelectClauseOfEdge()
                    string source = item[properties[i++]].ToString();
                    string sink = item[properties[i++]].ToString();
                    string other = item[properties[i++]].ToString();
                    string edgeOffset = item[properties[i++]].ToString();
                    long physicalOffset = (long)item[properties[i++]];
                    string adjType = item[properties[i++]].ToString();
                    //var isReversedAdjList = adjType.Equals("_reverse_edge", StringComparison.OrdinalIgnoreCase);

                    // TODO: What does physicalOffset mean?
                    EdgeField edgeField = (vertexObject[adjType] as AdjacencyListField).GetEdgeField(source, physicalOffset);

                    rawRecord.Append(new StringField(source));
                    rawRecord.Append(new StringField(sink));
                    rawRecord.Append(new StringField(other));
                    rawRecord.Append(new StringField(edgeOffset));

                    // Fill edge property field
                    for (; i < endOfEdgeIndex; i++)
                        rawRecord.Append(edgeField[properties[i]]);

                    //edgeField.Label = edgeField["label"]?.ToValue;
                    //edgeField.InV = source;
                    //edgeField.OutV = sink;
                    //edgeField.InVLabel = isReversedAdjList
                    //    ? edgeField["_sinkLabel"]?.ToValue
                    //    : vertexObject["label"]?.ToValue;
                    //edgeField.OutVLabel = isReversedAdjList
                    //    ? vertexObject["label"]?.ToValue
                    //    : edgeField["_sinkLabel"]?.ToValue;

                    endOfEdgeIndex = projectedColumnsType.FindIndex(i,
                        e => e == ColumnGraphType.EdgeSource);
                }

                yield return rawRecord;
            }
        }
    }
}
