using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;

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
        public abstract List<dynamic> GetRawVertices(JsonQuery vertexQuery);
    }

    internal class DocumentDbPortal : DbPortal
    {
        public DocumentDbPortal(GraphViewConnection connection)
        {
            Connection = connection;
        }

        public override List<RawRecord> GetVertices(JsonQuery vertexQuery)
        {
            throw new NotImplementedException();
        }

        public override List<dynamic> GetRawVertices(JsonQuery vertexQuery)
        {
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
            IQueryable<dynamic> results = Connection.DocDBclient.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(Connection.DocDB_DatabaseId, Connection.DocDB_CollectionId),
                vertexQuery.ToString(DatabaseType.DocumentDB), queryOptions);
            return results.ToList();
        }
    }
}
