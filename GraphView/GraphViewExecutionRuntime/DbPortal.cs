using System;
using System.Collections.Generic;
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

        public List<ColumnGraphType> ProjectedColumns { get; set; }

        public string ToString(DatabaseType dbType)
        {
            switch (dbType)
            {
                case DatabaseType.DocumentDB:
                    return string.Format("{0} FROM Node {1} {2} {3} {4}", SelectClause, Alias, JoinClause,
                        string.IsNullOrEmpty(WhereSearchCondition) ? "" : "WHERE", WhereSearchCondition);
                case DatabaseType.JsonServer:
                    return string.Format("FOR {0} IN ('Node') {1} {2} {3}", Alias,
                        string.IsNullOrEmpty(WhereSearchCondition) ? "" : "WHERE", WhereSearchCondition, SelectClause);
                default:
                    throw new NotImplementedException();
            }
        }
    }

    internal abstract class DbPortal : IDisposable
    {
        public GraphViewConnection Connection { get; protected set; }

        public void Dispose() { }

        public abstract List<RawRecord> GetVertices(JsonQuery vertexQuery);
    }

    internal class DocumentDbPortal : DbPortal
    {
        public DocumentDbPortal(GraphViewConnection connection)
        {
            Connection = connection;
        }

        public override List<RawRecord> GetVertices(JsonQuery vertexQuery)
        {
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
            IQueryable<dynamic> items = Connection.DocDBclient.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(Connection.DocDB_DatabaseId, Connection.DocDB_CollectionId),
                vertexQuery.ToString(DatabaseType.DocumentDB), queryOptions);

            var properties = vertexQuery.Properties;
            var newRecordLength = properties.Count;
            var results = new List<RawRecord>();

            foreach (var dynamicItem in items)
            {
                var rawRecord = new RawRecord(newRecordLength);
                var item = (JObject)dynamicItem;
                var index = 0;

                foreach (var property in properties)
                {
                    var propertyValue = item[property];
                    if (propertyValue != null)
                        rawRecord.fieldValues[index] = propertyValue.ToString();
                    ++index;
                }

                results.Add(rawRecord);
            }

            return results;
        }
    }
}
