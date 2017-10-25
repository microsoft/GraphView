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
    public sealed class VertexObjectCache
    {
        public GraphViewCommand Command { get; }

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

        // DocDB
        public void UpdateCurrentEtag(Document document)
        {
            string docId = document.Id;
            string etag = document.ETag;
            Debug.Assert(docId != null);
            Debug.Assert(etag != null);

            this._currentEtags[docId] = etag;
        }

        // JsonServer
        public void UpdateCurrentEtag(JObject document)
        {
            string docId = document.GetValue(KW_DOC_ID).ToString(); // check this
            string etag = document.GetValue(KW_DOC_ETAG).ToString();
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

        public VertexObjectCache(GraphViewCommand command)
        {
            this.Command = command;
        }


        public VertexField GetVertexField(string vertexId, string partition = null)
        {
            VertexField result;
            if (!this._cachedVertexField.TryGetValue(vertexId, out result)) {
                JObject vertexObject = this.Command.Connection.RetrieveDocumentById(vertexId, partition, this.Command);
                result = new VertexField(this.Command, vertexObject);
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

                vertexField = new VertexField(this.Command, vertexObject);
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

        private readonly Dictionary<string, DeltaVertexField> vertexDelta = new Dictionary<string, DeltaVertexField>();
        private readonly Dictionary<string, DeltaEdgeField> edgeDelta = new Dictionary<string, DeltaEdgeField>();

        internal void AddOrUpdateVertexDelta(VertexField vertexField, DeltaLog log)
        {
            string vertexId = vertexField.VertexId;
            DeltaVertexField delta;
            if (vertexDelta.ContainsKey(vertexId))
            {
                delta = vertexDelta[vertexId];
            }
            else
            {
                delta = new DeltaVertexField(vertexField);
                vertexDelta.Add(vertexId, delta);
            }
            delta.AddDeltaLog(log);
        }

        internal void AddOrUpdateEdgeDelta(EdgeField outEdgeField, VertexField srcVertexField, 
            EdgeField inEdgeField, VertexField sinkVertexField, DeltaLog log, bool useReverseEdges)
        {
            string edgeId = outEdgeField.EdgeId;

            DeltaEdgeField delta;
            if (edgeDelta.ContainsKey(edgeId))
            {
                delta = edgeDelta[edgeId];
            }
            else
            {
                delta = new DeltaEdgeField(outEdgeField, srcVertexField, inEdgeField, sinkVertexField, useReverseEdges);
                edgeDelta[edgeId] = delta;
            }
            delta.AddDeltaLog(log);
        }

        internal void UploadDelta()
        {
            foreach (KeyValuePair<string, DeltaVertexField> pair in vertexDelta)
            {
                pair.Value.Upload(this.Command);
            }
            this.vertexDelta.Clear();

            foreach (KeyValuePair<string, DeltaEdgeField> pair in edgeDelta)
            {
                pair.Value.Upload(this.Command);
            }
            this.edgeDelta.Clear();
        }
    }
}
