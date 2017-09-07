using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphView.GraphViewDBPortal
{
    internal class JsonServerDbPortal : DbPortal
    {
        public JsonServerDbPortal(GraphViewConnection connection)
        {
            this.Connection = connection;
        }

        internal override IEnumerable<JObject> ExecuteQueryScript(JsonQuery jsonQuery)
        {
            jsonQuery.JsonServerCollectionName = this.Connection.jsonServerCollectionName;
            string script = jsonQuery.ToString(DatabaseType.JsonServer);
            List<JObject> results = new List<JObject>();
            IDataReader reader = this.Connection.JsonServerClient.ExecuteReader(script);
            StringBuilder jsonStringBuilder = new StringBuilder();
            while (reader.Read())
            {
                jsonStringBuilder.Clear();
                JObject job = new JObject();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    if (reader.GetName(i) == "")
                    {
                        // case: SELECT * ==> SELECT Doc(xxx), without `AS`.
                        Debug.Assert(reader.FieldCount == 1, "More than one unnamed column is unparseable");
                        job = JObject.Parse(reader.GetString(i)); // must be an object? Needs double check.
                    }
                    else
                    {
//                        string qqq = reader.GetName(i);
//                        string ppp = reader.GetString(i);
                        job.Add(new JProperty(reader.GetName(i), JToken.Parse(reader.GetString(i))));
                    }
                }
                results.Add(job);
            }
            reader.Close();
            return results;
        }

        public JObject CreateDocument(JObject docObject)
        {
            // Add or update etag
            docObject[DocumentDBKeywords.KW_DOC_ETAG] = DateTimeOffset.Now.ToUniversalTime().ToString();
            string doc = docObject.ToString(Formatting.None);
            this.Connection.JsonServerClient.InsertJson(doc, this.Connection.jsonServerCollectionName);
            return docObject;
        }

        private void DeleteDocument(string docId, string partition = null)
        {
            Debug.Assert(this.Connection.jsonServerCollectionName != null, "Delete document from JsonServer needs collection name.");
            string partitionIndexer = this.Connection.GetPartitionPathIndexer().Replace("[", ".[");
            string deleteString = $"FOR md IN ('{this.Connection.jsonServerCollectionName}')\n" +
                                  $"WHERE md.{DocumentDBKeywords.KW_DOC_ID} = '{docId}' " +
                                  (partition == null ? "" : $"AND md{partitionIndexer} = '{partition}'") +
                                  $"\nDELETE md";
            this.Connection.JsonServerClient.ExecuteNonQuery(deleteString);
        }

        public void ReplaceOrDeleteDocument(string docId, JObject docObject, GraphViewCommand command, string partition = null)
        {
            this.DeleteDocument(docId, partition);
            if (docObject != null)
            {
                // Replace
                Debug.Assert(docObject[DocumentDBKeywords.KW_DOC_ID] is JValue);
                Debug.Assert((string)docObject[DocumentDBKeywords.KW_DOC_ID] == docId);
                JObject document = this.CreateDocument(docObject);

                // Update the document's etag
                command.VertexCache.UpdateCurrentEtag(document);
            }
            else
            {
                // Delete only
                command.VertexCache.RemoveEtag(docId);
            }
        }
    }
}
