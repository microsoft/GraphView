using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Linq;
using static GraphView.DocumentDBKeywords;

namespace GraphView.GraphViewDBPortal
{

    internal class DocumentDbPortal : DbPortal
    {
        public DocumentDbPortal(GraphViewConnection connection)
        {
            this.Connection = connection;
        }

        public async Task<ResourceResponse<Document>> CreateDocumentAsync(JObject docObject)
        {
            return await this.Connection.DocDBClient.CreateDocumentAsync(this.Connection._docDBCollectionUri, docObject);
        }

        public async Task ReplaceOrDeleteDocumentAsync(string docId, JObject docObject, GraphViewCommand command, string partition = null)
        {
            RequestOptions option = new RequestOptions();
            if (this.Connection.CollectionType == CollectionType.PARTITIONED)
            {
                option.PartitionKey = new PartitionKey(partition);
            }

            option.AccessCondition = new AccessCondition
            {
                Type = AccessConditionType.IfMatch,
                Condition = command.VertexCache.GetCurrentEtag(docId),
            };

            Uri documentUri = UriFactory.CreateDocumentUri(this.Connection.DocDBDatabaseId, this.Connection.DocDBCollectionId, docId);
            if (docObject == null)
            {
                this.Connection.DocDBClient.DeleteDocumentAsync(documentUri, option).Wait();

                // Remove the document's etag from saved
                command.VertexCache.RemoveEtag(docId);
            }
            else
            {
                Debug.Assert(docObject[KW_DOC_ID] is JValue);
                Debug.Assert((string)docObject[KW_DOC_ID] == docId, "The replaced document should match ID in the parameter");
                //Debug.Assert(partition != null && partition == (string)docObject[KW_DOC_PARTITION]);

                Document document = await this.Connection.DocDBClient.ReplaceDocumentAsync(documentUri, docObject, option);

                // Update the document's etag
                docObject[KW_DOC_ETAG] = document.ETag;
                command.VertexCache.UpdateCurrentEtag(document);
            }
        }

        internal override IEnumerable<JObject> ExecuteQueryScript(JsonQuery jsonQuery)
        {
            string queryScript = jsonQuery.ToString(DatabaseType.DocumentDB);
            IEnumerable<dynamic> items = this.Connection.ExecuteDocDbQuery(queryScript);
            foreach (dynamic item in items)
            {
                yield return (JObject)item;
            }
        }
    }
}
