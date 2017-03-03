using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace GraphView
{
    public class GraphViewDebugHelper
    {
        private readonly GraphViewConnection _connection;

        public GraphViewDebugHelper(GraphViewConnection connection)
        {
            this._connection = connection;
        }

        public List<JObject> GetAllEdges(bool isReverse, string vertexId = null)
        {
            if (isReverse) {
                throw new NotImplementedException("Can't get reverse edges (TODO)");
            }

            string query;
            if (vertexId == null) {  // All vertexes
                query = $"SELECT edge\n" +
                        $"FROM doc\n" +
                        $"JOIN edge IN doc._edge\n";
            }
            else {
                query = $"SELECT edge\n" +
                        $"FROM doc\n" +
                        $"JOIN edge IN doc._edge\n" +
                        $"WHERE doc._partition = '{vertexId}'\n";
            }

            List<JObject> ret = new List<JObject>();
            IQueryable<dynamic> results = this._connection.ExecuteQuery(query);
            foreach (JObject result in results) {
                JObject edgeObject = (JObject)result["edge"];

                if (isReverse) {
                    if (edgeObject["_srcV"] != null) {
                        // This is an incoming edge
                        ret.Add(edgeObject);
                    }
                    else if (edgeObject["_sinkV"] != null) {
                    }
                    else {
                        throw new Exception($"Invalid edgeObject! {result}");
                    }
                }
                else {
                    if (edgeObject["_sinkV"] != null) {
                        // This is an outgoing edge
                        ret.Add(edgeObject);
                    }
                    else if (edgeObject["_srcV"] != null) {
                    }
                    else {
                        throw new Exception($"Invalid edgeObject! {result}");
                    }
                }
            }

            return ret;
        }
    }
}
