using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Newtonsoft.Json.Linq;
using static GraphView.DocumentDBKeywords;

namespace GraphView
{

    /// <summary>
    /// The vertex object cache is per-connection.
    /// NOTE: The vertex object cache is cleared after every GraphViewCommand.
    /// NOTE: A VertexObjectCache is accessed by only one thread. lock is not necessary.
    /// </summary>
    internal sealed class VertexObjectCache
    {
        public DocumentDBConnection Connection { get; }

        private readonly Dictionary<string, string> _currentEtags = new Dictionary<string, string>();

        public string GetCurrentEtag(string docId)
        {
            Debug.Assert(docId != null);
            return this._currentEtags[docId];
        }

        public void RemoveEtag(string docId)
        {
            Debug.Assert(docId != null);
            Debug.Assert(this._currentEtags.ContainsKey(docId));

            this._currentEtags.Remove(docId);
        }

        public void SaveCurrentEtagNoOverride(JObject docObject)
        {
            string docId = (string)docObject[KW_DOC_ID];
            string etag = (string)docObject[KW_DOC_ETAG];
            Debug.Assert(docId != null);
            Debug.Assert(etag != null);

            if (!this._currentEtags.ContainsKey(docId))
            {
                this._currentEtags.Add(docId, etag);
            }
        }

        public void UpdateCurrentEtag(Document document)
        {
            string docId = document.Id;
            string etag = document.ETag;
            Debug.Assert(docId != null);
            Debug.Assert(etag != null);

            this._currentEtags[docId] = etag;
        }


        //
        // NOTE: _cachedVertex is ALWAYS up-to-date with DocDB!
        // Every query operation could be directly done with the cache
        // Every vertex/edge modification MUST be synchonized with the cache
        //
        private readonly Dictionary<string, VertexField> _cachedVertexField = new Dictionary<string, VertexField>();

        public VertexObjectCache(DocumentDBConnection dbConnection)
        {
            this.Connection = dbConnection;
        }


        public VertexField GetVertexField(string vertexId, string partition = null)
        {
            VertexField result;
            if (!this._cachedVertexField.TryGetValue(vertexId, out result)) {
                JObject vertexObject = this.Connection.RetrieveDocumentById(vertexId, partition);
                result = new VertexField(this.Connection, vertexObject);
                this._cachedVertexField.Add(vertexId, result);
            }
            return result;
        }

        public VertexField AddOrUpdateVertexField(string vertexId, JObject vertexObject)
        {
            VertexField vertexField;
            if (this._cachedVertexField.TryGetValue(vertexId, out vertexField)) {
                // TODO: Update?
            }
            else {
                //
                // Update saved etags when Constructing a vertex field
                // NOTE: For each vertex document, only ONE VertexField will be constructed ever.
                //
                this.SaveCurrentEtagNoOverride(vertexObject);

                vertexField = new VertexField(this.Connection, vertexObject);
                this._cachedVertexField.Add(vertexId, vertexField);
            }
            return vertexField;
        }

        public bool TryGetVertexField(string vertexId, out VertexField vertexField)
        {
            return this._cachedVertexField.TryGetValue(vertexId, out vertexField);
        }

        public bool TryRemoveVertexField(string vertexId)
        {
#if DEBUG
            VertexField vertexField;
            bool found = this._cachedVertexField.TryGetValue(vertexId, out vertexField);
            if (found) {
                Debug.Assert(!vertexField.AdjacencyList.AllEdges.Any(), "The deleted edge's should contain no outgoing edges");
                Debug.Assert(!vertexField.RevAdjacencyList.AllEdges.Any(), "The deleted edge's should contain no incoming edges");
            }
            return found;
#else
            return this._cachedVertexField.Remove(vertexId);
#endif
        }
    }
}
