using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Linq;
using static GraphView.GraphViewKeywords;

namespace GraphView
{
    [Flags]
    internal enum EdgeType : int
    {
        Outgoing = 1,
        Incoming = 2,
        Both = Outgoing | Incoming,
    }

    internal class Wrap<T>
    {
        public T Value { get; set; }

        public Wrap(T value = default(T))
        {
            this.Value = value;
        }

        public static implicit operator T(Wrap<T> wrap)
        {
            Debug.Assert(wrap != null);
            return wrap.Value;
        }
    }

    internal static class EdgeDocumentHelper
    {
        /// <summary>
        /// To check whether the vertex has spilled edge-document
        /// </summary>
        /// <param name="vertexObject"></param>
        /// <param name="checkReverse">true if check the incoming edges, false if check the outgoing edges</param>
        /// <returns></returns>
        public static bool IsSpilledVertex(JObject vertexObject, bool checkReverse)
        {
            Debug.Assert(vertexObject != null);

            JToken isSpilled = vertexObject[checkReverse ? KW_VERTEX_REVEDGE_SPILLED : KW_VERTEX_EDGE_SPILLED];

            return isSpilled == null || (bool)isSpilled;

            //if (edgeContainer is JObject) {
            //    Debug.Assert(edgeContainer["_edges"] is JArray);
            //    return true;
            //}
            //else if (edgeContainer is JArray) {
            //    return false;
            //}
            //else {
            //    throw new Exception("Should not get here!");
            //}
        }

        /// <summary>
        /// If a vertex got spilled adjacency/reverse adjacency list, lazy the construction of list.
        /// If a vertex got a normal reverse adjacency list but useReverseEdges is false, the construction will be lazied, too.
        /// </summary>
        /// <param name="vertexObject"></param>
        /// <param name="checkReverse">true if check the incoming edges, false if check the outgoing edges</param>
        /// <param name="useReverseEdges"></param>
        /// <returns></returns>
        public static bool IsBuildingTheAdjacencyListLazily(
            JObject vertexObject, 
            bool checkReverse, 
            bool useReverseEdges)
        {
            Debug.Assert(vertexObject != null);

            if (checkReverse && !useReverseEdges) {
                return true;
            }

            JToken isSpilled = vertexObject[checkReverse ? KW_VERTEX_REVEDGE_SPILLED : KW_VERTEX_EDGE_SPILLED];

            return isSpilled == null || (bool)isSpilled;
        }


        /// <summary>
        /// Try to upload one document. 
        /// If the operation fails because document is too large, nothing is changed and "tooLarge" is set true.
        /// If the operation fails due to other reasons, nothing is changed and an exception is thrown
        /// If the operation succeeds, docObject[KW_DOC_ID] is set if it doesn't have one
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="docId"></param>
        /// <param name="docObject"></param>
        /// <param name="tooLarge"></param>
        private static void UploadOne(GraphViewConnection connection, string docId, JObject docObject, bool isCreate, out bool tooLarge)
        {
            tooLarge = false;
            try {
                Debug.Assert(docObject != null);
                if (isCreate) {
                    connection.CreateDocumentAsync(docObject).Wait();
                }
                else {
                    connection.ReplaceOrDeleteDocumentAsync(docId, docObject, connection.GetDocumentPartition(docObject)).Wait();
                }
            }
            catch (AggregateException ex)
                when ((ex.InnerException as DocumentClientException)?.Error.Code == "RequestEntityTooLarge") {
                tooLarge = true;
            }
        }


        /// <summary>
        /// Add an edge from one vertex (source) to another (sink)
        /// NOTE: Both the source and sink vertex are modified.
        /// NOTE: This function may upload the edge-document.
        /// NOTE: srcVertex and sinkVertex are updated and uploaded.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="srcId"></param>
        /// <param name="sinkId"></param>
        /// <param name="srcVertexField"></param>
        /// <param name="sinkVertexField"></param>
        /// <param name="edgeJsonObject"></param>
        /// <param name="srcVertexObject"></param>
        /// <param name="sinkVertexObject"></param>
        /// <param name="outEdgeObject"></param>
        /// <param name="outEdgeDocID"></param>
        /// <param name="inEdgeObject"></param>
        /// <param name="inEdgeDocID"></param>
        public static void InsertEdgeAndUpload(
            GraphViewConnection connection,
            string srcId, string sinkId,
            VertexField srcVertexField, VertexField sinkVertexField,
            JObject edgeJsonObject,
            JObject srcVertexObject, JObject sinkVertexObject,
            out JObject outEdgeObject, out string outEdgeDocID,
            out JObject inEdgeObject, out string inEdgeDocID)
        {
            //long edgeOffset = (long)srcVertexObject[KW_VERTEX_NEXTOFFSET];
            //srcVertexObject[KW_VERTEX_NEXTOFFSET] = edgeOffset + 1;

            outEdgeObject = (JObject)edgeJsonObject.DeepClone();
            inEdgeObject = (JObject)edgeJsonObject.DeepClone();

            // Add "id" property to edgeObject
            string edgeId = GraphViewConnection.GenerateDocumentId();

            string srcLabel = srcVertexObject[KW_VERTEX_LABEL]?.ToString();
            string sinkLabel = sinkVertexObject[KW_VERTEX_LABEL]?.ToString();
            GraphViewJsonCommand.UpdateEdgeMetaProperty(outEdgeObject, edgeId, false, sinkId, sinkLabel, sinkVertexField.Partition);
            GraphViewJsonCommand.UpdateEdgeMetaProperty(inEdgeObject, edgeId, true, srcId, srcLabel, srcVertexField.Partition);

            InsertEdgeObjectInternal(connection, srcVertexObject, srcVertexField, outEdgeObject, false, out outEdgeDocID); // srcVertex uploaded

            if (connection.UseReverseEdges) {
                InsertEdgeObjectInternal(connection, sinkVertexObject, sinkVertexField, inEdgeObject, true, out inEdgeDocID); // sinkVertex uploaded
            }
            else {
                inEdgeDocID = EdgeDocumentHelper.VirtualReverseEdgeDocId;
            }
        }


        /// <summary>
        /// Insert edgeObject to one a vertex.
        /// NOTE: vertex-document and edge-document(s) are uploaded.
        /// NOTE: If changing _edge/_reverse_edge field from JArray to JObject, the "EdgeDocId" of existing 
        ///       edges in VertexCache are updated (from null to the newly created edge-document's id)
        /// NOTE: Adding the newly created edge into VertexCache is not operated by this function. Actually, 
        ///       if called by <see cref="UpdateEdgeProperty"/>, VertexCache should be updated by setting an
        ///       edge's property, but not adding a new edge.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="vertexObject"></param>
        /// <param name="vertexField">Can be null if we already know edgeContainer is JObject</param>
        /// <param name="edgeObject"></param>
        /// <param name="isReverse"></param>
        /// <param name="newEdgeDocId"></param>
        private static void InsertEdgeObjectInternal(
            GraphViewConnection connection,
            JObject vertexObject,
            VertexField vertexField,
            JObject edgeObject,
            bool isReverse,
            out string newEdgeDocId)
        {
            bool tooLarge;
            JArray edgeContainer = (JArray)vertexObject[isReverse ? KW_VERTEX_REV_EDGE : KW_VERTEX_EDGE]; // JArray or JObject
            bool isSpilled = IsSpilledVertex(vertexObject, isReverse);

            //
            // This graph is compatible only, thus add an edge-document directly
            //
            if (connection.GraphType != GraphType.GraphAPIOnly || connection.EdgeSpillThreshold == 1) {
                Debug.Assert(connection.EdgeSpillThreshold == 1);

                // Create a new edge-document to store the edge.
                JObject edgeDocObject = new JObject {
                    [KW_DOC_ID] = GraphViewConnection.GenerateDocumentId(),
                    [KW_EDGEDOC_ISREVERSE] = isReverse,
                    [KW_EDGEDOC_VERTEXID] = (string)vertexObject[KW_DOC_ID],
                    [KW_EDGEDOC_VERTEX_LABEL] = (string)vertexObject[KW_VERTEX_LABEL],
                    [KW_EDGEDOC_EDGE] = new JArray(edgeObject),
                    [KW_EDGEDOC_IDENTIFIER] = (JValue)true,
                };
                if (connection.PartitionPathTopLevel != null) {
                    // This may be KW_DOC_PARTITION, maybe not
                    edgeDocObject[connection.PartitionPathTopLevel] = vertexObject[connection.PartitionPathTopLevel];
                }

                // Upload the edge-document
                bool dummyTooLarge;
                UploadOne(connection, (string)edgeDocObject[KW_DOC_ID], edgeDocObject, true, out dummyTooLarge);
                Debug.Assert(!dummyTooLarge);


                newEdgeDocId = (string)edgeDocObject[KW_DOC_ID];
                return;
            }


            if (isSpilled) {
                // Now it is a large-degree vertex, and contains at least 1 edge-document
                JArray edgeDocumentsArray = edgeContainer;
                Debug.Assert(edgeDocumentsArray != null, "edgeDocumentsArray != null");
                Debug.Assert(edgeDocumentsArray.Count == 1, "edgeDocumentsArray.Count == 1");

                string lastEdgeDocId = (string)edgeDocumentsArray.Last[KW_DOC_ID];
                JObject edgeDocument = connection.RetrieveDocumentById(lastEdgeDocId, vertexField.Partition);
                Debug.Assert(((string)edgeDocument[KW_DOC_ID]).Equals(lastEdgeDocId), $"((string)edgeDocument[{KW_DOC_ID}]).Equals(lastEdgeDocId)");
                Debug.Assert((bool)edgeDocument[KW_EDGEDOC_ISREVERSE] == isReverse, $"(bool)edgeDocument['{KW_EDGEDOC_ISREVERSE}'] == isReverse");
                Debug.Assert((string)edgeDocument[KW_EDGEDOC_VERTEXID] == (string)vertexObject[KW_DOC_ID], $"(string)edgeDocument['{KW_EDGEDOC_VERTEXID}'] == (string)vertexObject['{KW_DOC_ID}']");

                JArray edgesArray = (JArray)edgeDocument[KW_EDGEDOC_EDGE];
                Debug.Assert(edgesArray != null, "edgesArray != null");
                Debug.Assert(edgesArray.Count > 0, "edgesArray.Count > 0");

                if (connection.EdgeSpillThreshold == 0) {
                    // Don't spill an edge-document until it is too large
                    edgesArray.Add(edgeObject);
                    tooLarge = false;
                }
                else {
                    // Explicitly specified a threshold
                    Debug.Assert(connection.EdgeSpillThreshold > 0, "connection.EdgeSpillThreshold > 0");
                    if (edgesArray.Count >= connection.EdgeSpillThreshold) {
                        // The threshold is reached!
                        tooLarge = true;
                    }
                    else {
                        // The threshold is not reached
                        edgesArray.Add(edgeObject);
                        tooLarge = false;
                    }
                }

                // If the edge-document is not too large (reach the threshold), try to
                //   upload the edge into the document
                if (!tooLarge) {
                    UploadOne(connection, lastEdgeDocId, edgeDocument, false, out tooLarge);
                }
                if (tooLarge) {
                    // The edge is too large to be filled into the last edge-document
                    // or the threashold is reached:
                    // Create a new edge-document to store the edge.
                    JObject edgeDocObject = new JObject {
                        [KW_DOC_ID] = GraphViewConnection.GenerateDocumentId(),
                        [KW_EDGEDOC_ISREVERSE] = isReverse,
                        [KW_EDGEDOC_VERTEXID] = (string)vertexObject[KW_DOC_ID],
                        [KW_EDGEDOC_VERTEX_LABEL] = (string)vertexObject[KW_VERTEX_LABEL],
                        [KW_EDGEDOC_EDGE] = new JArray(edgeObject),
                        [KW_EDGEDOC_IDENTIFIER] = (JValue)true,
                    };
                    if (connection.PartitionPathTopLevel != null) {
                        // This may be KW_DOC_PARTITION, maybe not
                        edgeDocObject[connection.PartitionPathTopLevel] = vertexObject[connection.PartitionPathTopLevel];
                    }
                    lastEdgeDocId = connection.CreateDocumentAsync(edgeDocObject).Result;

                    //// Add the newly create edge-document to vertexObject & upload the vertexObject
                    //edgeDocumentsArray.Add(new JObject {
                    //    [KW_DOC_ID] = lastEdgeDocId
                    //});

                    // Replace the newly created edge-document to vertexObject
                    Debug.Assert(edgeDocumentsArray.Count == 1);
                    edgeDocumentsArray[0][KW_DOC_ID] = lastEdgeDocId;
                }
                newEdgeDocId = lastEdgeDocId;

                // Upload the vertex documention (at least, its _nextXxx is changed)
                bool dummyTooLarge;
                UploadOne(connection, (string)vertexObject[KW_DOC_ID], vertexObject, false, out dummyTooLarge);
                Debug.Assert(!dummyTooLarge);
            }
            else {
                // This vertex is not spilled
                bool? spillReverse;
                ((JArray)edgeContainer).Add(edgeObject);
                if (connection.EdgeSpillThreshold == 0) {
                    // Don't spill an edge-document until it is too large
                    tooLarge = false;
                    spillReverse = null;
                }
                else {
                    // Explicitly specified a threshold
                    Debug.Assert(connection.EdgeSpillThreshold > 0, "connection.EdgeSpillThreshold > 0");
                    tooLarge = (((JArray)edgeContainer).Count > connection.EdgeSpillThreshold);
                    spillReverse = isReverse;
                }

                if (!tooLarge) {
                    UploadOne(connection, (string)vertexObject[KW_DOC_ID], vertexObject, false, out tooLarge);
                }
                if (tooLarge) {
                    string existEdgeDocId;
                    // The vertex object is uploaded in SpillVertexEdgesToDocument
                    SpillVertexEdgesToDocument(connection, vertexObject, ref spillReverse, out existEdgeDocId, out newEdgeDocId);

                    // Update the in & out edges in vertex field
                    Debug.Assert(spillReverse != null);
                    Debug.Assert(vertexField != null);
                    if (spillReverse.Value) {
                        foreach (EdgeField edge in vertexField.RevAdjacencyList.AllEdges) {
                            Debug.Assert(edge.EdgeDocID == null);
                            edge.EdgeDocID = existEdgeDocId;
                        }
                    }
                    else {
                        foreach (EdgeField edge in vertexField.AdjacencyList.AllEdges) {
                            Debug.Assert(edge.EdgeDocID == null);
                            edge.EdgeDocID = existEdgeDocId;
                        }
                    }
                }
                else {
                    newEdgeDocId = null;
                }
            }
        }

        /// <summary>
        /// This function spills a small-degree vertex, stores its edges into seperate documents
        /// Either its incoming or outgoing edges are moved to a new document, decided by which is larger in size
        /// NOTE: This function will upload the vertex document
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="vertexObject"></param>
        /// <param name="spillReverse">
        /// Whether to spill the outgoing edges or incoming edges.
        /// If it's null, let this function decide. 
        /// (This happens when no spilling threshold is set but the document size limit is reached)
        /// </param>
        /// <param name="existEdgeDocId">This is the first edge-document (to store the existing edges)</param>
        /// <param name="newEdgeDocId">This is the second edge-document (to store the currently creating edge)</param>
        private static void SpillVertexEdgesToDocument(GraphViewConnection connection, JObject vertexObject, ref bool? spillReverse, out string existEdgeDocId, out string newEdgeDocId)
        {
            //Debug.Assert(vertexObject[KW_DOC_PARTITION] != null);
            //Debug.Assert((string)vertexObject[KW_DOC_ID] == (string)vertexObject[KW_DOC_PARTITION]);
            if (spillReverse == null) {  
                // Let this function decide whether incoming/outgoing edges to spill
                Debug.Assert(!IsSpilledVertex(vertexObject, true) || !IsSpilledVertex(vertexObject, false));
            }
            else {
                Debug.Assert(!IsSpilledVertex(vertexObject, spillReverse.Value));
            }

            // NOTE: The VertexCache is not updated here
            bool outEdgeSeperated = IsSpilledVertex(vertexObject, false);
            bool inEdgeSeperated = IsSpilledVertex(vertexObject, true);
            if (inEdgeSeperated && outEdgeSeperated) {
                throw new Exception("BUG: Should not get here! Either incoming or outgoing edegs should not have been seperated");
            }

            JArray targetEdgeArray;
            if (inEdgeSeperated) {
                targetEdgeArray = (JArray)vertexObject[KW_VERTEX_EDGE];
                spillReverse = false;
            }
            else if (outEdgeSeperated) {
                targetEdgeArray = (JArray)vertexObject[KW_VERTEX_REV_EDGE];
                spillReverse = true;
            }
            else {
                JArray outEdgeArray = (JArray)vertexObject[KW_VERTEX_EDGE];
                JArray inEdgeArray = (JArray)vertexObject[KW_VERTEX_REV_EDGE];
                spillReverse = (outEdgeArray.ToString().Length < inEdgeArray.ToString().Length);
                targetEdgeArray = spillReverse.Value ? inEdgeArray : outEdgeArray;
            }

            // Create a new edge-document to store the currently creating edge
            JObject newEdgeDocObject = new JObject {
                [KW_DOC_ID] = GraphViewConnection.GenerateDocumentId(),
                [KW_EDGEDOC_ISREVERSE] = spillReverse.Value,
                [KW_EDGEDOC_VERTEXID] = (string)vertexObject[KW_DOC_ID],
                [KW_EDGEDOC_VERTEX_LABEL] = (string)vertexObject[KW_VERTEX_LABEL],
                [KW_EDGEDOC_EDGE] = new JArray(targetEdgeArray.Last),
                [KW_EDGEDOC_IDENTIFIER] = (JValue)true,
            };
            if (connection.PartitionPathTopLevel != null) {
                newEdgeDocObject[connection.PartitionPathTopLevel] = vertexObject[connection.PartitionPathTopLevel];
            }

            newEdgeDocId = connection.CreateDocumentAsync(newEdgeDocObject).Result;
            targetEdgeArray.Last.Remove();  // Remove the currently create edge appended just now

            // Create another new edge-document to store the existing edges.
            JObject existEdgeDocObject = new JObject {
                [KW_DOC_ID] = GraphViewConnection.GenerateDocumentId(),
                [KW_EDGEDOC_ISREVERSE] = spillReverse.Value,
                [KW_EDGEDOC_VERTEXID] = (string)vertexObject[KW_DOC_ID],
                [KW_EDGEDOC_VERTEX_LABEL] = (string)vertexObject[KW_VERTEX_LABEL],
                [KW_EDGEDOC_EDGE] = targetEdgeArray,
                [KW_EDGEDOC_IDENTIFIER] = (JValue)true,
            };
            if (connection.PartitionPathTopLevel != null) {
                existEdgeDocObject[connection.PartitionPathTopLevel] = vertexObject[connection.PartitionPathTopLevel];
            }
            existEdgeDocId = connection.CreateDocumentAsync(existEdgeDocObject).Result;

            // Update vertexObject to store the newly create edge-document & upload the vertexObject
            vertexObject[spillReverse.Value ? KW_VERTEX_REV_EDGE : KW_VERTEX_EDGE] = new JArray {
                // Store the last spilled edge document only.
                //new JObject {
                //    [KW_DOC_ID] = existEdgeDocId,
                //},
                new JObject {
                    [KW_DOC_ID] = newEdgeDocId,
                },
            };

            // Update the vertex document to indicate whether it's spilled
            if (spillReverse.Value) {
                Debug.Assert((bool)vertexObject[KW_VERTEX_REVEDGE_SPILLED] == false);
                vertexObject[KW_VERTEX_REVEDGE_SPILLED] = true;
            }
            else {
                Debug.Assert((bool)vertexObject[KW_VERTEX_EDGE_SPILLED] == false);
                vertexObject[KW_VERTEX_EDGE_SPILLED] = true;
            }

            bool dummyTooLarge;
            UploadOne(connection, (string)vertexObject[KW_DOC_ID], vertexObject, false, out dummyTooLarge);
            Debug.Assert(!dummyTooLarge);
        }


        /// <summary>
        /// Find incoming or outgoing edge by "srcId and edgeId"
        /// Output the edgeObject, as well as the edgeDocId (null for small-degree edges)
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="vertexObject"></param>
        /// <param name="srcVertexId"></param>
        /// <param name="edgeId"></param>
        /// <param name="isReverseEdge"></param>
        /// <param name="edgeObject"></param>
        /// <param name="edgeDocId"></param>
        public static void FindEdgeBySourceAndEdgeId(
            GraphViewConnection connection,
            JObject vertexObject, string srcVertexId, string edgeId, bool isReverseEdge,
            out JObject edgeObject, out string edgeDocId)
        {
            if (!isReverseEdge) {
                Debug.Assert((string)vertexObject[KW_DOC_ID] == srcVertexId);
            }
            JToken edgeContainer = vertexObject[isReverseEdge ? KW_VERTEX_REV_EDGE : KW_VERTEX_EDGE];

            if (!IsSpilledVertex(vertexObject, isReverseEdge)) {
                // for small-degree vertexes
                if (isReverseEdge) {
                    edgeObject = (from edgeObj in edgeContainer.Children<JObject>()
                        where (string) edgeObj[KW_EDGE_SRCV] == srcVertexId
                        where (string) edgeObj[KW_EDGE_ID] == edgeId
                        select edgeObj
                    ).FirstOrDefault();
                }
                else {
                    edgeObject = (from edgeObj in edgeContainer.Children<JObject>()
                        where (string) edgeObj[KW_EDGE_ID] == edgeId
                        select edgeObj
                    ).FirstOrDefault();
                }
                edgeDocId = null;
            }
            else {  // For large-degree vertices
                // Now the vertex document stores the last(latest) spilled edge document only.
                //string edgeDocIdList = string.Join(", ", edgeContainer.Children<JObject>().Select(e => $"'{e[KW_DOC_ID]}'"));

                const string EDGE_SELECT_TAG = "edge";
                string partition = connection.GetDocumentPartition(vertexObject);
                string query = $"SELECT doc.{KW_DOC_ID}, {EDGE_SELECT_TAG}\n" +
                               $"FROM doc\n" +
                               $"JOIN {EDGE_SELECT_TAG} IN doc.{KW_EDGEDOC_EDGE}\n" +
                               $"WHERE (doc.{KW_EDGEDOC_ISREVERSE} = {isReverseEdge.ToString().ToLowerInvariant()})\n" +
                               $"  AND ({EDGE_SELECT_TAG}.{KW_EDGE_ID} = '{edgeId}')\n" + 
                               (partition != null ? $" AND (doc{connection.GetPartitionPathIndexer()} = '{partition}')" : "");

                JObject result = connection.ExecuteQueryUnique(query);
                edgeDocId = (string) result?[KW_DOC_ID];
                edgeObject = (JObject) result?[EDGE_SELECT_TAG];
            }
        }


        public static void RemoveEdge(
            Dictionary<string, Tuple<JObject, string>> documentMap,
            GraphViewConnection connection,
            string edgeDocId,
            VertexField vertexField,
            bool isReverse,
            string srcVertexId, string edgeId)
        {
            JObject vertexObject = vertexField.VertexJObject;
            JArray edgeContainer = (JArray)vertexObject[isReverse ? KW_VERTEX_REV_EDGE : KW_VERTEX_EDGE];

            // Check is this vertex an external vertex?
            if (!vertexField.ViaGraphAPI) {
#if DEBUG
                JObject edgeDocument = connection.RetrieveDocumentById(edgeDocId, connection.GetDocumentPartition(vertexObject));
                Debug.Assert(connection.GetDocumentPartition(edgeDocument) == connection.GetDocumentPartition(vertexObject));
#endif
                documentMap[edgeDocId] = new Tuple<JObject, string>(null, connection.GetDocumentPartition(vertexObject));
                return;
            }


            if (IsSpilledVertex(vertexObject, isReverse)) {
                // Now it is a large-degree vertex, and contains at least 1 edge-document
                Debug.Assert(!string.IsNullOrEmpty(edgeDocId), "!string.IsNullOrEmpty(edgeDocId)");

                JArray edgeDocumentsArray = edgeContainer;
                Debug.Assert(edgeDocumentsArray != null, "edgeDocuments != null");
                Debug.Assert(edgeDocumentsArray.Count == 1, "edgeDocuments.Count == 1");

                JObject edgeDocument = connection.RetrieveDocumentById(edgeDocId, connection.GetDocumentPartition(vertexObject));
                //Debug.Assert(edgeDocument[KW_DOC_PARTITION] != null);
                //Debug.Assert(vertexObject[KW_DOC_PARTITION] != null);
                Debug.Assert(((string)edgeDocument[KW_DOC_ID]).Equals(edgeDocId), $"((string)edgeDocument['{KW_DOC_ID}']).Equals(edgeDocId)");
                Debug.Assert((bool)edgeDocument[KW_EDGEDOC_ISREVERSE] == isReverse, $"(bool)edgeDocument['{KW_EDGEDOC_ISREVERSE}'] == isReverse");
                Debug.Assert((string)edgeDocument[KW_EDGEDOC_VERTEXID] == (string)vertexObject[KW_DOC_ID], $"(string)edgeDocument['{KW_EDGEDOC_VERTEXID}'] == (string)vertexObject['id']");

                // The edge to be removed must exist! (garanteed by caller)
                JArray edgesArray = (JArray)edgeDocument[KW_EDGEDOC_EDGE];
                Debug.Assert(edgesArray != null, "edgesArray != null");
                Debug.Assert(edgesArray.Count > 0, "edgesArray.Count > 0");
                if (isReverse) {
                    edgesArray.First(e => (string)e[KW_EDGE_SRCV] == srcVertexId && (string)e[KW_EDGE_ID] == edgeId).Remove();
                }
                else {
                    edgesArray.First(e => (string)e[KW_EDGE_ID] == edgeId).Remove();
                }

                // 
                // If the edge-document contains no edge after the removal, delete this edge-document.
                // Don't forget to update the vertex-document at the same time.
                //
                if (edgesArray.Count == 0) {

                    //
                    // Currently, 
                    // If the modified edge document contains no edges, just do nothing, leave it here.
                    // This means the lastest edge document is empty
                    //
                    // If the empty document is not the latest document, delete the edge-document, 
                    // and add the vertex-document to the upload list
                    if (connection.EdgeSpillThreshold == 1 ||
                        (string)edgeDocumentsArray[0][KW_DOC_ID] != edgeDocId) {
                        documentMap[edgeDocId] = new Tuple<JObject, string>(null, connection.GetDocumentPartition(edgeDocument));
                    }
                    else {
                        documentMap[edgeDocId] = new Tuple<JObject, string>(edgeDocument, connection.GetDocumentPartition(edgeDocument));
                    }
                    // The vertex object needn't change
                    //documentMap[(string)vertexObject[KW_DOC_ID]] = new Tuple<JObject, string>(vertexObject, connection.GetDocumentPartition(vertexObject));
                }
                else {
                    documentMap[edgeDocId] = new Tuple<JObject, string>(edgeDocument, connection.GetDocumentPartition(edgeDocument));
                }
            }
            else {
                // This vertex is not spilled
                Debug.Assert(edgeDocId == null, "edgeDocId == null");

                if (isReverse) {
                    ((JArray)edgeContainer).First(e => (string)e[KW_EDGE_SRCV] == srcVertexId && (string)e[KW_EDGE_ID] == edgeId).Remove();
                }
                else {
                    ((JArray)edgeContainer).First(e => (string)e[KW_EDGE_ID] == edgeId).Remove();
                }
                documentMap[(string)vertexObject[KW_DOC_ID]] = new Tuple<JObject, string>(vertexObject, connection.GetDocumentPartition(vertexObject));
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="vertexObject"></param>
        /// <param name="edgeDocId"></param>
        /// <param name="isReverse"></param>
        /// <param name="newEdgeObject"></param>
        public static void UpdateEdgeProperty(
            GraphViewConnection connection,
            JObject vertexObject,
            string edgeDocId,  // Can be null
            bool isReverse,
            JObject newEdgeObject  // With all metadata (including id, partition, srcV/sinkV, edgeId)
        )
        {
            bool tooLarge;
            string srcOrSinkVInEdgeObject = isReverse ? KW_EDGE_SRCV : KW_EDGE_SINKV;

            if (edgeDocId == null) {
                JArray edgeContainer = (JArray)vertexObject[isReverse ? KW_VERTEX_REV_EDGE : KW_VERTEX_EDGE];

                // Don't use JToken.Replace() here.
                // Make sure the currently modified edge is the last child of edgeContainer, which 
                // garantees the newly created edge-document won't be too large.
                //
                // NOTE: The following line applies for both incomming and outgoing edge.
                edgeContainer.Children<JObject>().First(
                    e => (string)e[KW_EDGE_ID] == (string)newEdgeObject[KW_EDGE_ID] &&
                         (string)e[srcOrSinkVInEdgeObject] == (string)newEdgeObject[srcOrSinkVInEdgeObject]
                ).Remove();
                edgeContainer.Add(newEdgeObject);

                UploadOne(connection, (string)vertexObject[KW_DOC_ID], vertexObject, false, out tooLarge);
                if (tooLarge) {
                    // Handle this situation: The updated edge is too large to be filled into the vertex-document
                    string existEdgeDocId, newEdgeDocId;
                    bool? spillReverse = null;
                    SpillVertexEdgesToDocument(connection, vertexObject, ref spillReverse, out existEdgeDocId, out newEdgeDocId);
                }
            }
            else {
                // Large vertex

                JObject edgeDocObject = connection.RetrieveDocumentById(edgeDocId, connection.GetDocumentPartition(vertexObject));
                edgeDocObject[KW_EDGEDOC_EDGE].Children<JObject>().First(
                    e => (string)e[KW_EDGE_ID] == (string)newEdgeObject[KW_EDGE_ID] &&
                         (string)e[srcOrSinkVInEdgeObject] == (string)newEdgeObject[srcOrSinkVInEdgeObject]
                ).Remove();
                ((JArray)edgeDocObject[KW_EDGEDOC_EDGE]).Add(newEdgeObject);
                UploadOne(connection, edgeDocId, edgeDocObject, false, out tooLarge);
                if (tooLarge) {

                    if (connection.EdgeSpillThreshold == 1) {
                        throw new GraphViewException("The edge is too large to be stored in one document!");
                    }

                    // Handle this situation: The modified edge is too large to be filled into the original edge-document
                    // Remove the edgeObject added just now, and upload the original edge-document
                    ((JArray)edgeDocObject[KW_EDGEDOC_EDGE]).Last.Remove();
                    UploadOne(connection, edgeDocId, edgeDocObject, false, out tooLarge);
                    Debug.Assert(!tooLarge);

                    // Insert the edgeObject to one of the vertex's edge-documents
                    InsertEdgeObjectInternal(connection, vertexObject, null, newEdgeObject, isReverse, out edgeDocId);
                }
            }
        }

        internal static string VirtualReverseEdge = "virtualReverseEdge";
        internal static string VirtualReverseEdgeObject = "virtualReverseEdgeObject";
        internal static string VirtualReverseEdgeDocId = "$VIRTUAL$";

        /// <summary>
        /// edgeDict: [vertexId, [spilled edge document id, spilled edge document]]
        /// </summary>
        /// <param name="edgeDict"></param>
        /// <param name="edgeDocuments"></param>
        internal static void FillEdgeDict(Dictionary<string, Dictionary<string, JObject>> edgeDict,
            List<dynamic> edgeDocuments)
        {
            foreach (JObject edgeDocument in edgeDocuments) {

                string vertexId = (string)edgeDocument[KW_EDGEDOC_VERTEXID];
                Dictionary<string, JObject> edgeDocSet;
                edgeDict.TryGetValue(vertexId, out edgeDocSet);
                if (edgeDocSet == null) {
                    edgeDocSet = new Dictionary<string, JObject>();
                    edgeDict.Add(vertexId, edgeDocSet);
                }

                edgeDocSet.Add((string)edgeDocument[KW_DOC_ID], edgeDocument);
            }
        }

        /// <summary>
        /// Use forward edge objects to construct a virtual reverse edge document, whose schema is the same
        /// as a real spilled reverse edge document.
        /// </summary>
        /// <param name="virtualReverseEdges"></param>
        /// <returns></returns>
        private static List<JObject> ConstructVirtualReverseEdgeDocuments(List<dynamic> virtualReverseEdges)
        {
            Dictionary<string, JObject> virtualReverseEdgeDocumentsDict = new Dictionary<string, JObject>();
            foreach (JObject virtualReverseEdge in virtualReverseEdges)
            {
                JObject virtualReverseEdgeObject = (JObject)virtualReverseEdge[EdgeDocumentHelper.VirtualReverseEdge];

                string srcV, srcVLabel;

                // This is a spilled edge
                if (virtualReverseEdgeObject[KW_EDGEDOC_VERTEXID] != null) {
                    srcV = virtualReverseEdgeObject[KW_EDGEDOC_VERTEXID].ToString();
//                    srcVLabel = labelOfSrcVertexOfSpilledEdge[srcV];
                    srcVLabel = virtualReverseEdgeObject[KW_EDGEDOC_VERTEX_LABEL].ToString();
                } else {
                    srcV = virtualReverseEdgeObject[KW_EDGE_SRCV].ToString();
                    srcVLabel = virtualReverseEdgeObject[KW_EDGE_SRCV_LABEL]?.ToString();
                }
                string srcVPartition = virtualReverseEdgeObject[KW_EDGE_SRCV_PARTITION]?.ToString();

                JObject edgeObject = (JObject)virtualReverseEdgeObject[EdgeDocumentHelper.VirtualReverseEdgeObject];
                string vertexId = (string)edgeObject[KW_EDGE_SINKV];
                string vertexLabel = (string) edgeObject[KW_EDGE_SINKV_LABEL];

                JObject virtualReverseEdgeDocument;
                if (!virtualReverseEdgeDocumentsDict.TryGetValue(vertexId, out virtualReverseEdgeDocument)) {
                    virtualReverseEdgeDocument = new JObject {
                        { KW_DOC_ID, EdgeDocumentHelper.VirtualReverseEdgeDocId },
                        { KW_EDGEDOC_VERTEXID, vertexId },
                        { KW_EDGEDOC_VERTEX_LABEL, vertexLabel },
                        { KW_EDGEDOC_ISREVERSE, true },
                        { KW_EDGEDOC_EDGE, new JArray() }
                    };
                    virtualReverseEdgeDocumentsDict.Add(vertexId, virtualReverseEdgeDocument);
                }

                JArray reverseEdgeDocumentEdges = (JArray)virtualReverseEdgeDocument[KW_EDGEDOC_EDGE];
                JObject virtualRevEdgeObject = edgeObject;
                virtualRevEdgeObject.Add(KW_EDGE_SRCV, srcV);
                if (srcVLabel != null) {
                    virtualRevEdgeObject.Add(KW_EDGE_SRCV_LABEL, srcVLabel);
                }
                if (srcVPartition != null) {
                    virtualRevEdgeObject.Add(KW_EDGE_SRCV_PARTITION, srcVPartition);
                }
                reverseEdgeDocumentEdges.Add(virtualRevEdgeObject);
            }

            return virtualReverseEdgeDocumentsDict.Values.ToList();
        }

        /// <summary>
        /// For every vertex in the vertexIdSet, retrieve their spilled edge documents to construct their forward or backward
        /// adjacency list.
        /// When connection.useReverseEdge is false, this method will retrieve all the forward edge documents whose sink
        /// is the target vertex to build a virtual reverse adjacency list.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="edgeType"></param>
        /// <param name="vertexIdSet"></param>
        /// <param name="vertexPartitionKeySet"></param>
        public static void ConstructLazyAdjacencyList(
            GraphViewConnection connection,
            EdgeType edgeType,
            HashSet<string> vertexIdSet, 
            HashSet<string> vertexPartitionKeySet)
        {
            if (!vertexIdSet.Any()) return;

            string inClause = string.Join(", ", vertexIdSet.Select(vertexId => $"'{vertexId}'"));
            string partitionInClause = string.Join(", ", vertexPartitionKeySet.Select(partitionKey => $"'{partitionKey}'"));
            string edgeDocumentsQuery =
                $"SELECT * " +
                $"FROM edgeDoc " +
                $"WHERE edgeDoc.{KW_EDGEDOC_VERTEXID} IN ({inClause}) " +
                (string.IsNullOrEmpty(partitionInClause)
                     ? ""
                     : $"AND edgeDoc{connection.GetPartitionPathIndexer()} IN ({partitionInClause}) ") +
                (edgeType == EdgeType.Outgoing
                     ? $"AND edgeDoc.{KW_EDGEDOC_ISREVERSE} = false "
                     : edgeType == EdgeType.Incoming
                         ? $"AND edgeDoc.{KW_EDGEDOC_ISREVERSE} = true "
                         : "");
            List<dynamic> edgeDocuments = connection.ExecuteQuery(edgeDocumentsQuery).ToList();

            // Dictionary<vertexId, Dictionary<edgeDocumentId, edgeDocument>>
            Dictionary<string, Dictionary<string, JObject>> edgeDict =
                new Dictionary<string, Dictionary<string, JObject>>();

            foreach (JObject edgeDocument in edgeDocuments) {
                // Save edgeDocument's etag if necessary
                connection.VertexCache.SaveCurrentEtagNoOverride(edgeDocument);
            }

            EdgeDocumentHelper.FillEdgeDict(edgeDict, edgeDocuments);

            //
            // Use all edges whose sink is vertexId to construct a virtual reverse adjacency list of this vertex
            //
            if (!connection.UseReverseEdges && edgeType.HasFlag(EdgeType.Incoming))
            {
                edgeDocumentsQuery =
                    $"SELECT {{" +
                    $"  \"{EdgeDocumentHelper.VirtualReverseEdgeObject}\": edge, " +
                    $"  \"{KW_EDGE_SRCV}\": doc.{KW_DOC_ID}, " +
                    $"  \"{KW_EDGE_SRCV_LABEL}\": doc.{KW_VERTEX_LABEL}," +
                    (connection.PartitionPath != null 
                        ? $"  \"{KW_EDGE_SRCV_PARTITION}\": doc{connection.GetPartitionPathIndexer()}," 
                        : "") + 
                    $"  \"{KW_EDGEDOC_VERTEXID}\": doc.{KW_EDGEDOC_VERTEXID}," +
                    $"  \"{KW_EDGEDOC_VERTEX_LABEL}\": doc.{KW_EDGEDOC_VERTEX_LABEL}" +
                    $"}} AS {EdgeDocumentHelper.VirtualReverseEdge}\n" +
                    $"FROM doc\n" +
                    $"JOIN edge IN doc.{GraphViewKeywords.KW_VERTEX_EDGE}\n" +
                    $"WHERE edge.{KW_EDGE_SINKV} IN ({inClause})";

                edgeDocuments = connection.ExecuteQuery(edgeDocumentsQuery).ToList();

                List<JObject> virtualReverseEdgeDocuments = EdgeDocumentHelper.ConstructVirtualReverseEdgeDocuments(edgeDocuments);

                EdgeDocumentHelper.FillEdgeDict(edgeDict, virtualReverseEdgeDocuments.Cast<dynamic>().ToList());
            }

            foreach (KeyValuePair<string, Dictionary<string, JObject>> pair in edgeDict)
            {
                string vertexId = pair.Key;
                Dictionary<string, JObject> edgeDocDict = pair.Value; // contains both in & out edges
                VertexField vertexField;
                connection.VertexCache.TryGetVertexField(vertexId, out vertexField);
                vertexField.ConstructSpilledOrVirtualAdjacencyListField(edgeType, edgeDocDict);
                vertexIdSet.Remove(vertexId);
            }

            foreach (string vertexId in vertexIdSet)
            {
                VertexField vertexField;
                connection.VertexCache.TryGetVertexField(vertexId, out vertexField);
                vertexField.ConstructSpilledOrVirtualAdjacencyListField(edgeType, new Dictionary<string, JObject>());
            }
        }
    }
}
