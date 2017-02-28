using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace GraphView
{

    internal sealed class VertexObjectCache
    {
        public GraphViewConnection Connection { get; }

        //
        // NOTE: _cachedVertex is ALWAYS up-to-date with DocDB!
        // Every query operation could be directly done with the cache
        // Every vertex/edge modification MUST be synchonized with the cache
        //
        private readonly Dictionary<string, VertexField> _cachedVertexField = new Dictionary<string, VertexField>();

        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

        
        private static readonly ConcurrentDictionary<string, Dictionary<string, VertexField>> __caches =
            new ConcurrentDictionary<string, Dictionary<string, VertexField>>();


        private VertexObjectCache(GraphViewConnection dbConnection, Dictionary<string, VertexField> cachedVertexField)
        {
            this.Connection = dbConnection;
        }

        /// <summary>
        /// Construct a VertexCache from a connection.
        /// NOTE: VertexCache is per-collection, but for the same colloction, it's cross-connection
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public static VertexObjectCache FromConnection(GraphViewConnection connection)
        {
            Debug.Assert(connection.Identifier != null, "The connection's identifier must have been initialized");

            // MUST NOT use __caches.GetOrAdd() here!
            var cachedVertexField = new Dictionary<string, VertexField>();
            if (!__caches.TryAdd(connection.Identifier, cachedVertexField)) {
                cachedVertexField = __caches[connection.Identifier];
            }

            return new VertexObjectCache(connection, cachedVertexField);
        }

        
        public VertexField GetVertexField(string vertexId, JObject currVertexObject = null, Dictionary<string, JObject> edgeDocSet = null)
        {
            try {
                this._lock.EnterUpgradeableReadLock();

                try {
                    this._lock.EnterWriteLock();

                    VertexField result;
                    if (!_cachedVertexField.TryGetValue(vertexId, out result)) {
                        JObject vertexObject = currVertexObject ?? this.Connection.RetrieveDocumentById(vertexId);
                        result = FieldObject.ConstructVertexField(this.Connection, vertexObject, edgeDocSet);
                        _cachedVertexField.Add(vertexId, result);
                    }
                    return result;
                }
                finally {
                    if (this._lock.IsWriteLockHeld) {
                        this._lock.ExitWriteLock();
                    }
                }
            }
            finally {
                if (this._lock.IsUpgradeableReadLockHeld) {
                    this._lock.ExitUpgradeableReadLock();
                }
            }
        }

        public bool TryRemoveVertexField(string vertexId)
        {
            try {
                this._lock.EnterWriteLock();
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
            finally {
                if (this._lock.IsWriteLockHeld) {
                    this._lock.ExitWriteLock();
                }
            }
        }
    }


    //internal sealed class VertexObjectCache
    //{
    //    public GraphViewConnection Connection { get; }

    //    /// <summary>
    //    /// NOTE: VertexCache is per-connection! (cross-connection may lead to unpredictable errors)
    //    /// </summary>
    //    /// <param name="dbConnection"></param>
    //    public VertexObjectCache(GraphViewConnection dbConnection)
    //    {
    //        this.Connection = dbConnection;
    //    }

    //    //
    //    // Can we use ConcurrentDictionary<string, VertexField> here?
    //    // Yes, I reckon...
    //    //
    //    private readonly Dictionary<string, VertexField> _cachedVertexCollection = new Dictionary<string, VertexField>();
    //    private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

    //    public VertexField GetVertexField(string vertexId, string vertexJson)
    //    {
    //        try {
    //            this._lock.EnterUpgradeableReadLock();

    //            // Try to retrieve vertexObject from the cache
    //            VertexField vertexField;
    //            if (this._cachedVertexCollection.TryGetValue(vertexId, out vertexField)) {
    //                return vertexField;
    //            }

    //            // Cache miss: parse vertexJson, and add the result to cache
    //            try {
    //                this._lock.EnterWriteLock();

    //                JObject vertexObject = JObject.Parse(vertexJson);
    //                vertexField = FieldObject.ConstructVertexField(this.Connection, vertexObject);
    //                this._cachedVertexCollection.Add(vertexId, vertexField);
    //            }
    //            finally {
    //                if (this._lock.IsWriteLockHeld) {
    //                    this._lock.ExitWriteLock();
    //                }
    //            }
    //            return vertexField;
    //        }
    //        finally {
    //            if (this._lock.IsUpgradeableReadLockHeld) {
    //                this._lock.ExitUpgradeableReadLock();
    //            }
    //        }
    //    }
    //}
}
