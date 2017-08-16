using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Linq;

namespace GraphView.GraphViewDBPortal
{
    internal class JsonServerDbPortal : DbPortal
    {
        public JsonServerDbPortal(GraphViewConnection connection)
        {
            this.Connection = connection;
        }

        public override IEnumerator<Tuple<VertexField, RawRecord>> GetVerticesAndEdgesViaVertices(JsonQuery vertexQuery, GraphViewCommand command)
        {
            throw new NotImplementedException();
        }

        public override IEnumerator<RawRecord> GetVerticesAndEdgesViaEdges(JsonQuery edgeQuery, GraphViewCommand command)
        {
            throw new NotImplementedException();
        }

        public override List<JObject> GetEdgeDocuments(JsonQuery query)
        {
            throw new NotImplementedException();
        }

        public override JObject GetEdgeDocument(JsonQuery query)
        {
            throw new NotImplementedException();
        }

        public override JObject GetVertexDocument(JsonQuery query)
        {
            throw new NotImplementedException();
        }

        public override List<VertexField> GetVerticesByIds(HashSet<string> vertexId, GraphViewCommand command, string partition, bool constructEdges = false)
        {
            throw new NotImplementedException();
        }

        public override async Task<ResourceResponse<Document>> CreateDocumentAsync(JObject docObject)
        {
            throw new NotImplementedException();
        }

        public override async Task ReplaceOrDeleteDocumentAsync(string docId, JObject docObject, GraphViewCommand command, string partition = null)
        {
            throw new NotImplementedException();
        }
    }
}
