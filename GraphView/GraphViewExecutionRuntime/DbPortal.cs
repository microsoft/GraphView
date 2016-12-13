using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        public string WhereSearchCondition { get; set; }
        public string Alias { get; set; }

        public List<ColumnGraphType> ProjectedColumns { get; set; }

        public string ToString(DatabaseType dbType)
        {
            switch (dbType)
            {
                case DatabaseType.DocumentDB:
                    return string.Format("{0} FROM Node {1} WHERE {2}", SelectClause, Alias, WhereSearchCondition);
                case DatabaseType.JsonServer:
                    return string.Format("FOR {0} IN ('Node') WHERE {1} {2}", Alias, WhereSearchCondition, SelectClause);
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
            throw new NotImplementedException();
        }
    }
}
