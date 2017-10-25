using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.GraphViewDBPortal;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Linq;
using static GraphView.DocumentDBKeywords;

namespace GraphView
{
    [Flags]
    public enum EdgeType : int
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
        /// <param name="command"></param>
        /// <param name="docId"></param>
        /// <param name="docObject"></param>
        /// <param name="tooLarge"></param>
        private static void UploadOne(GraphViewCommand command, string docId, JObject docObject, bool isCreate, out bool tooLarge)
        {
            tooLarge = false;
            try {
                Debug.Assert(docObject != null);
                if (isCreate) {
                    command.Connection.CreateDocumentAsync(docObject, command).Wait();
                }
                else {
                    command.Connection.ReplaceOrDeleteDocumentAsync(docId, docObject, command.Connection.GetDocumentPartition(docObject), command).Wait();
                }
            }
            catch (AggregateException ex)
                when ((ex.InnerException as DocumentClientException)?.Error.Code == "RequestEntityTooLarge") {
                tooLarge = true;
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
        /// <param name="command"></param>
        /// <param name="vertexObject"></param>
        /// <param name="vertexField">Can be null if we already know edgeContainer is JObject</param>
        /// <param name="edgeObject"></param>
        /// <param name="isReverse"></param>
        /// <param name="newEdgeDocId"></param>
        internal static void InsertEdgeObjectInternal(
            GraphViewCommand command,
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
            if (command.Connection.GraphType != GraphType.GraphAPIOnly || command.Connection.EdgeSpillThreshold == 1) {
                Debug.Assert(command.Connection.EdgeSpillThreshold == 1);

                // Create a new edge-document to store the edge.
                JObject edgeDocObject = new JObject {
                    [KW_DOC_ID] = GraphViewConnection.GenerateDocumentId(),
                    [KW_EDGEDOC_ISREVERSE] = isReverse,
                    [KW_EDGEDOC_VERTEXID] = (string)vertexObject[KW_DOC_ID],
                    [KW_EDGEDOC_VERTEX_LABEL] = (string)vertexObject[KW_VERTEX_LABEL],
                    [KW_EDGEDOC_EDGE] = new JArray(edgeObject),
                    [KW_EDGEDOC_IDENTIFIER] = (JValue)true,
                };
                if (command.Connection.PartitionPathTopLevel != null) {
                    // This may be KW_DOC_PARTITION, maybe not
                    edgeDocObject[command.Connection.PartitionPathTopLevel] = vertexObject[command.Connection.PartitionPathTopLevel];
                }

                // Upload the edge-document
                bool dummyTooLarge;
                UploadOne(command, (string)edgeDocObject[KW_DOC_ID], edgeDocObject, true, out dummyTooLarge);
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
                JObject edgeDocument = command.Connection.RetrieveDocumentById(lastEdgeDocId, vertexField.Partition, command);
                Debug.Assert(((string)edgeDocument[KW_DOC_ID]).Equals(lastEdgeDocId), $"((string)edgeDocument[{KW_DOC_ID}]).Equals(lastEdgeDocId)");
                Debug.Assert((bool)edgeDocument[KW_EDGEDOC_ISREVERSE] == isReverse, $"(bool)edgeDocument['{KW_EDGEDOC_ISREVERSE}'] == isReverse");
                Debug.Assert((string)edgeDocument[KW_EDGEDOC_VERTEXID] == (string)vertexObject[KW_DOC_ID], $"(string)edgeDocument['{KW_EDGEDOC_VERTEXID}'] == (string)vertexObject['{KW_DOC_ID}']");

                JArray edgesArray = (JArray)edgeDocument[KW_EDGEDOC_EDGE];
                Debug.Assert(edgesArray != null, "edgesArray != null");
                Debug.Assert(edgesArray.Count > 0, "edgesArray.Count > 0");

                if (command.Connection.EdgeSpillThreshold == 0) {
                    // Don't spill an edge-document until it is too large
                    edgesArray.Add(edgeObject);
                    tooLarge = false;
                }
                else {
                    // Explicitly specified a threshold
                    Debug.Assert(command.Connection.EdgeSpillThreshold > 0, "connection.EdgeSpillThreshold > 0");
                    if (edgesArray.Count >= command.Connection.EdgeSpillThreshold) {
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
                    UploadOne(command, lastEdgeDocId, edgeDocument, false, out tooLarge);
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
                    if (command.Connection.PartitionPathTopLevel != null) {
                        // This may be KW_DOC_PARTITION, maybe not
                        edgeDocObject[command.Connection.PartitionPathTopLevel] = vertexObject[command.Connection.PartitionPathTopLevel];
                    }
                    lastEdgeDocId = command.Connection.CreateDocumentAsync(edgeDocObject, command).Result;

                    // Replace the newly created edge-document to vertexObject
                    Debug.Assert(edgeDocumentsArray.Count == 1);
                    edgeDocumentsArray[0][KW_DOC_ID] = lastEdgeDocId;
                }
                newEdgeDocId = lastEdgeDocId;

                // Upload the vertex documention (at least, its _nextXxx is changed)
                bool dummyTooLarge;
                UploadOne(command, (string)vertexObject[KW_DOC_ID], vertexObject, false, out dummyTooLarge);
                Debug.Assert(!dummyTooLarge);
            }
            else {
                // This vertex is not spilled
                bool? spillReverse;
                ((JArray)edgeContainer).Add(edgeObject);
                if (command.Connection.EdgeSpillThreshold == 0) {
                    // Don't spill an edge-document until it is too large
                    tooLarge = false;
                    spillReverse = null;
                }
                else {
                    // Explicitly specified a threshold
                    Debug.Assert(command.Connection.EdgeSpillThreshold > 0, "connection.EdgeSpillThreshold > 0");
                    tooLarge = (((JArray)edgeContainer).Count > command.Connection.EdgeSpillThreshold);
                    spillReverse = isReverse;
                }

                if (!tooLarge) {
                    UploadOne(command, (string)vertexObject[KW_DOC_ID], vertexObject, false, out tooLarge);
                }
                if (tooLarge) {
                    string existEdgeDocId;
                    // The vertex object is uploaded in SpillVertexEdgesToDocument
                    EdgeDocumentHelper.SpillVertexEdgesToDocument(command, vertexObject, ref spillReverse, out existEdgeDocId, out newEdgeDocId);

                    // the edges are spilled into two ducuments. 
                    // one stores old edges(docId = existEdgeDocId), the other one stores the new edge.
                    // Because the new edge is not in the vertexCache, hence we can set all edges' docId as existEdgeDocId
                    Debug.Assert(spillReverse != null);
                    Debug.Assert(vertexField != null);
                    if (spillReverse.Value) {
                        vertexField.RevAdjacencyList.ResetFetchedEdgesDocId(existEdgeDocId);
                    }
                    else {
                        vertexField.AdjacencyList.ResetFetchedEdgesDocId(existEdgeDocId);
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
        /// <param name="command"></param>
        /// <param name="vertexObject"></param>
        /// <param name="spillReverse">
        /// Whether to spill the outgoing edges or incoming edges.
        /// If it's null, let this function decide. 
        /// (This happens when no spilling threshold is set but the document size limit is reached)
        /// </param>
        /// <param name="existEdgeDocId">This is the first edge-document (to store the existing edges)</param>
        /// <param name="newEdgeDocId">This is the second edge-document (to store the currently creating edge)</param>
        private static void SpillVertexEdgesToDocument(GraphViewCommand command, JObject vertexObject, ref bool? spillReverse, out string existEdgeDocId, out string newEdgeDocId)
        {
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
            if (command.Connection.PartitionPathTopLevel != null) {
                newEdgeDocObject[command.Connection.PartitionPathTopLevel] = vertexObject[command.Connection.PartitionPathTopLevel];
            }

            newEdgeDocId = command.Connection.CreateDocumentAsync(newEdgeDocObject, command).Result;
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
            if (command.Connection.PartitionPathTopLevel != null) {
                existEdgeDocObject[command.Connection.PartitionPathTopLevel] = vertexObject[command.Connection.PartitionPathTopLevel];
            }
            existEdgeDocId = command.Connection.CreateDocumentAsync(existEdgeDocObject, command).Result;

            // Update vertexObject to store the newly create edge-document & upload the vertexObject
            vertexObject[spillReverse.Value ? KW_VERTEX_REV_EDGE : KW_VERTEX_EDGE] = new JArray {
                // Store the last spilled edge document only.
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
            UploadOne(command, (string)vertexObject[KW_DOC_ID], vertexObject, false, out dummyTooLarge);
            Debug.Assert(!dummyTooLarge);
        }


        /// <summary>
        /// Find incoming or outgoing edge by "srcId and edgeId"
        /// Output the edgeObject, as well as the edgeDocId (null for small-degree edges)
        /// </summary>
        /// <param name="command"></param>
        /// <param name="vertexObject"></param>
        /// <param name="srcVertexId"></param>
        /// <param name="edgeId"></param>
        /// <param name="isReverseEdge"></param>
        /// <param name="edgeObject"></param>
        /// <param name="edgeDocId"></param>
        public static void FindEdgeBySourceAndEdgeId(
            GraphViewCommand command,
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
            else {  
                // For large-degree vertices
                // Now the vertex document stores the last(latest) spilled edge document only.

                const string VERTEX_ALIAS = "doc";
                const string EDGE_SELECT_TAG = "edge";
                var jsonQuery = new JsonQuery
                {
                    NodeAlias = VERTEX_ALIAS,
                    EdgeAlias = EDGE_SELECT_TAG
                };
                // SELECT doc.id, edge
                jsonQuery.AddSelectElement(VERTEX_ALIAS);
                jsonQuery.AddSelectElement(EDGE_SELECT_TAG);

                jsonQuery.JoinDictionary.Add(EDGE_SELECT_TAG, $"{VERTEX_ALIAS}.{KW_EDGEDOC_EDGE}");
                jsonQuery.EdgeProperties = new List<string>();
                jsonQuery.NodeProperties = new List<string>();
                jsonQuery.RawWhereClause = new WBooleanComparisonExpression
                {
                    ComparisonType = BooleanComparisonType.Equals,
                    FirstExpr = new WColumnReferenceExpression(VERTEX_ALIAS, KW_EDGEDOC_ISREVERSE),
                    SecondExpr = new WValueExpression(isReverseEdge.ToString().ToLowerInvariant(), false)
                };
                jsonQuery.FlatProperties.Add(KW_EDGEDOC_ISREVERSE);

                jsonQuery.WhereConjunction(new WBooleanComparisonExpression
                {
                    ComparisonType = BooleanComparisonType.Equals,
                    FirstExpr = new WColumnReferenceExpression(EDGE_SELECT_TAG, KW_EDGE_ID),
                    SecondExpr = new WValueExpression(edgeId, true)
                }, BooleanBinaryExpressionType.And);

                string partition = command.Connection.GetDocumentPartition(vertexObject);
                if (partition != null)
                {
                    jsonQuery.FlatProperties.Add(partition);
                    jsonQuery.WhereConjunction(new WBooleanComparisonExpression
                    {
                        ComparisonType = BooleanComparisonType.Equals,
                        // TODO: new type to represent this??
                        FirstExpr = new WValueExpression($"{VERTEX_ALIAS}{command.Connection.GetPartitionPathIndexer()}", false),
                        SecondExpr = new WValueExpression(partition, true)
                    }, BooleanBinaryExpressionType.And);
                }
                
                
                JObject result = command.Connection.CreateDatabasePortal().GetEdgeDocument(jsonQuery);
                edgeDocId = (string) result?[VERTEX_ALIAS]?[KW_DOC_ID];
                edgeObject = (JObject) result?[EDGE_SELECT_TAG];
            }
        }


        public static void RemoveEdge(
            Dictionary<string, Tuple<JObject, string>> documentMap,
            GraphViewCommand command,
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
                JObject edgeDocument = command.Connection.RetrieveDocumentById(edgeDocId, command.Connection.GetDocumentPartition(vertexObject), command);
                Debug.Assert(command.Connection.GetDocumentPartition(edgeDocument) == command.Connection.GetDocumentPartition(vertexObject));
#endif
                documentMap[edgeDocId] = new Tuple<JObject, string>(null, command.Connection.GetDocumentPartition(vertexObject));
                return;
            }


            if (IsSpilledVertex(vertexObject, isReverse)) {
                // Now it is a large-degree vertex, and contains at least 1 edge-document
                Debug.Assert(!string.IsNullOrEmpty(edgeDocId), "!string.IsNullOrEmpty(edgeDocId)");

                JArray edgeDocumentsArray = edgeContainer;
                Debug.Assert(edgeDocumentsArray != null, "edgeDocuments != null");
                Debug.Assert(edgeDocumentsArray.Count == 1, "edgeDocuments.Count == 1");

                JObject edgeDocument = command.Connection.RetrieveDocumentById(edgeDocId, command.Connection.GetDocumentPartition(vertexObject), command);

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
                    if (command.Connection.EdgeSpillThreshold == 1 ||
                        (string)edgeDocumentsArray[0][KW_DOC_ID] != edgeDocId) {
                        documentMap[edgeDocId] = new Tuple<JObject, string>(null, command.Connection.GetDocumentPartition(edgeDocument));
                    }
                    else {
                        documentMap[edgeDocId] = new Tuple<JObject, string>(edgeDocument, command.Connection.GetDocumentPartition(edgeDocument));
                    }
                }
                else {
                    documentMap[edgeDocId] = new Tuple<JObject, string>(edgeDocument, command.Connection.GetDocumentPartition(edgeDocument));
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
                documentMap[(string)vertexObject[KW_DOC_ID]] = new Tuple<JObject, string>(vertexObject, command.Connection.GetDocumentPartition(vertexObject));
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
            GraphViewCommand command,
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

                UploadOne(command, (string)vertexObject[KW_DOC_ID], vertexObject, false, out tooLarge);
                if (tooLarge) {
                    // Handle this situation: The updated edge is too large to be filled into the vertex-document
                    string existEdgeDocId, newEdgeDocId;
                    bool? spillReverse = null;
                    EdgeDocumentHelper.SpillVertexEdgesToDocument(command, vertexObject, ref spillReverse, out existEdgeDocId, out newEdgeDocId);
                }
            }
            else {
                // Large vertex

                JObject edgeDocObject = command.Connection.RetrieveDocumentById(edgeDocId, command.Connection.GetDocumentPartition(vertexObject), command);
                edgeDocObject[KW_EDGEDOC_EDGE].Children<JObject>().First(
                    e => (string)e[KW_EDGE_ID] == (string)newEdgeObject[KW_EDGE_ID] &&
                         (string)e[srcOrSinkVInEdgeObject] == (string)newEdgeObject[srcOrSinkVInEdgeObject]
                ).Remove();
                ((JArray)edgeDocObject[KW_EDGEDOC_EDGE]).Add(newEdgeObject);
                UploadOne(command, edgeDocId, edgeDocObject, false, out tooLarge);
                if (tooLarge) {

                    if (command.Connection.EdgeSpillThreshold == 1) {
                        throw new GraphViewException("The edge is too large to be stored in one document!");
                    }

                    // Handle this situation: The modified edge is too large to be filled into the original edge-document
                    // Remove the edgeObject added just now, and upload the original edge-document
                    ((JArray)edgeDocObject[KW_EDGEDOC_EDGE]).Last.Remove();
                    UploadOne(command, edgeDocId, edgeDocObject, false, out tooLarge);
                    Debug.Assert(!tooLarge);

                    // Insert the edgeObject to one of the vertex's edge-documents
                    InsertEdgeObjectInternal(command, vertexObject, null, newEdgeObject, isReverse, out edgeDocId);
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
            List<JObject> edgeDocuments)
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
        private static List<JObject> ConstructVirtualReverseEdgeDocuments(List<JObject> virtualReverseEdges)
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
        /// <param name="command"></param>
        /// <param name="edgeType"></param>
        /// <param name="vertexIdSet"></param>
        /// <param name="vertexPartitionKeySet"></param>
        public static void ConstructLazyAdjacencyList(
            GraphViewCommand command,
            EdgeType edgeType,
            HashSet<string> vertexIdSet, 
            HashSet<string> vertexPartitionKeySet)
        {
            if (!vertexIdSet.Any()) return;
            
            const string ALIAS = "edgeDoc";
            
            var queryBoth = new JsonQuery
            {
                EdgeAlias = ALIAS
            };
            queryBoth.AddSelectElement("*");
            queryBoth.RawWhereClause = new WInPredicate(new WColumnReferenceExpression(ALIAS, KW_EDGEDOC_VERTEXID), vertexIdSet.ToList());
            queryBoth.FlatProperties.Add(KW_EDGEDOC_VERTEXID);

            if (vertexPartitionKeySet.Any())
            {
                // TODO: Refactor this.
                queryBoth.WhereConjunction(new WInPredicate(new WValueExpression($"{ALIAS}{command.Connection.GetPartitionPathIndexer()}"), vertexPartitionKeySet.ToList()),
                    BooleanBinaryExpressionType.And);
            }
            if (edgeType == EdgeType.Outgoing)
            {
                queryBoth.WhereConjunction(new WBooleanComparisonExpression
                {
                    ComparisonType = BooleanComparisonType.Equals,
                    FirstExpr = new WColumnReferenceExpression(ALIAS, KW_EDGEDOC_ISREVERSE),
                    SecondExpr = new WValueExpression("false")
                }, BooleanBinaryExpressionType.And);
                queryBoth.FlatProperties.Add(KW_EDGEDOC_ISREVERSE);
            }
            else if (edgeType == EdgeType.Incoming)
            {
                queryBoth.WhereConjunction(new WBooleanComparisonExpression
                {
                    ComparisonType = BooleanComparisonType.Equals,
                    FirstExpr = new WColumnReferenceExpression(ALIAS, KW_EDGEDOC_ISREVERSE),
                    SecondExpr = new WValueExpression("true")
                }, BooleanBinaryExpressionType.And);
                queryBoth.FlatProperties.Add(KW_EDGEDOC_ISREVERSE);
            }

//            var qqq = queryBoth.ToJsonServerString();

            List<JObject> edgeDocuments = command.Connection.CreateDatabasePortal().GetEdgeDocuments(queryBoth);

            // Dictionary<vertexId, Dictionary<edgeDocumentId, edgeDocument>>
            var edgeDict = new Dictionary<string, Dictionary<string, JObject>>();

            foreach (JObject edgeDocument in edgeDocuments) {
                // Save edgeDocument's etag if necessary
                command.VertexCache.SaveCurrentEtagNoOverride(edgeDocument);
            }

            EdgeDocumentHelper.FillEdgeDict(edgeDict, edgeDocuments);

            //
            // Use all edges whose sink is vertexId to construct a virtual reverse adjacency list of this vertex
            //
            if (!command.Connection.UseReverseEdges && edgeType.HasFlag(EdgeType.Incoming))
            {
                // TODO: old Version JsonQuery, delete it when you understand this query.
//                string selectClause = $"{{" +
//                               $"  \"{EdgeDocumentHelper.VirtualReverseEdgeObject}\": edge, " +
//                               $"  \"{KW_EDGE_SRCV}\": doc.{KW_DOC_ID}, " +
//                               $"  \"{KW_EDGE_SRCV_LABEL}\": doc.{KW_VERTEX_LABEL}," +
//                               (command.Connection.PartitionPath != null
//                                   ? $"  \"{KW_EDGE_SRCV_PARTITION}\": doc{command.Connection.GetPartitionPathIndexer()},"
//                                   : "") +
//                               $"  \"{KW_EDGEDOC_VERTEXID}\": doc.{KW_EDGEDOC_VERTEXID}," +
//                               $"  \"{KW_EDGEDOC_VERTEX_LABEL}\": doc.{KW_EDGEDOC_VERTEX_LABEL}" +
//                               $"}} AS {EdgeDocumentHelper.VirtualReverseEdge}";
//                string alise = "doc";
//                string joinClause = $"JOIN edge IN doc.{DocumentDBKeywords.KW_VERTEX_EDGE}";
//                string inClause = string.Join(", ", vertexIdSet.Select(vertexId => $"'{vertexId}'"));
//                string whereSearchCondition = $"edge.{KW_EDGE_SINKV} IN ({inClause})";

                const string NODE_ALISE_S = "node";
                const string EDGE_ALISE_S = "edge";
                var queryReversed = new JsonQuery
                {
                    NodeAlias = NODE_ALISE_S,
                    EdgeAlias = EDGE_ALISE_S
                };
                // Construct select clause.
                List<WPrimaryExpression> selectList = new List<WPrimaryExpression>
                {
                    new WValueExpression($"{{  \"{EdgeDocumentHelper.VirtualReverseEdgeObject}\": "),
                    new WColumnReferenceExpression(EDGE_ALISE_S, "*"),
                    new WValueExpression($",  \"{KW_EDGE_SRCV}\": "),
                    new WColumnReferenceExpression(NODE_ALISE_S, KW_DOC_ID),
                    new WValueExpression($",  \"{KW_EDGE_SRCV_LABEL}\": "),
                    new WColumnReferenceExpression(NODE_ALISE_S, KW_VERTEX_LABEL),
                    new WValueExpression($",  \"{KW_EDGEDOC_VERTEXID}\": "),
                    new WColumnReferenceExpression(NODE_ALISE_S, KW_EDGEDOC_VERTEXID),
                    new WValueExpression($",  \"{KW_EDGEDOC_VERTEX_LABEL}\": "),
                    new WColumnReferenceExpression(NODE_ALISE_S, KW_EDGEDOC_VERTEX_LABEL),
                };
                if (command.Connection.PartitionPath != null)
                {
                    selectList.Add(new WValueExpression($",  \"{KW_EDGE_SRCV_PARTITION}\": "));
                    // TODO: hack operation, when meet columnName[0] = '[', the toDocDbString function will do something special.
                    selectList.Add(new WColumnReferenceExpression(NODE_ALISE_S, command.Connection.GetPartitionPathIndexer()));
                }
                selectList.Add(new WValueExpression("}"));
                queryReversed.AddSelectElement(EdgeDocumentHelper.VirtualReverseEdge, selectList);

                // Construct join clause
                queryReversed.JoinDictionary.Add(EDGE_ALISE_S, $"{NODE_ALISE_S}.{DocumentDBKeywords.KW_VERTEX_EDGE}");

                // construct where clause
                queryReversed.RawWhereClause = new WInPredicate(
                    new WColumnReferenceExpression(EDGE_ALISE_S, KW_EDGE_SINKV),
                    new List<string>(vertexIdSet));

                
                edgeDocuments = command.Connection.CreateDatabasePortal().GetEdgeDocuments(queryReversed);

                List<JObject> virtualReverseEdgeDocuments = EdgeDocumentHelper.ConstructVirtualReverseEdgeDocuments(edgeDocuments);

                EdgeDocumentHelper.FillEdgeDict(edgeDict, virtualReverseEdgeDocuments);
            }

            foreach (KeyValuePair<string, Dictionary<string, JObject>> pair in edgeDict)
            {
                string vertexId = pair.Key;
                Dictionary<string, JObject> edgeDocDict = pair.Value; // contains both in & out edges
                VertexField vertexField;
                command.VertexCache.TryGetVertexField(vertexId, out vertexField);
                vertexField.ConstructSpilledOrVirtualAdjacencyListField(edgeType, edgeDocDict);
                vertexIdSet.Remove(vertexId);
            }

            foreach (string vertexId in vertexIdSet)
            {
                VertexField vertexField;
                command.VertexCache.TryGetVertexField(vertexId, out vertexField);
                vertexField.ConstructSpilledOrVirtualAdjacencyListField(edgeType, new Dictionary<string, JObject>());
            }
        }
    }
}
