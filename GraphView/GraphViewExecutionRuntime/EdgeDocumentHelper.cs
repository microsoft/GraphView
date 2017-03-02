using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Linq;

namespace GraphView
{
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

            JToken edgeContainer = vertexObject[checkReverse ? "_reverse_edge" : "_edge"];
            if (edgeContainer is JObject) {
                Debug.Assert(edgeContainer["_edges"] is JArray);
                return true;
            }
            else if (edgeContainer is JArray) {
                return false;
            }
            else {
                throw new Exception("Should not get here!");
            }
        }


        /// <summary>
        /// Try to upload one document. 
        /// If the operation fails because document is too large, nothing is changed and "tooLarge" is set true.
        /// If the operation fails due to other reasons, nothing is changed and an exception is thrown
        /// If the operation succeeds, docObject["id"] is set if it doesn't have one
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="docId"></param>
        /// <param name="docObject"></param>
        /// <param name="tooLarge"></param>
        private static void UploadOne(GraphViewConnection connection, string docId, JObject docObject, out bool tooLarge)
        {
            tooLarge = false;
            try {
                connection.ReplaceOrDeleteDocumentAsync(docId, docObject).Wait();
            }
            catch (AggregateException ex) when (ex.InnerException?.GetType().Name.Equals("RequestEntityTooLargeException") ?? false) {
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
            long edgeOffset = (long)srcVertexObject["_nextEdgeOffset"];
            srcVertexObject["_nextEdgeOffset"] = edgeOffset + 1;

            outEdgeObject = (JObject)edgeJsonObject.DeepClone();
            inEdgeObject = (JObject)edgeJsonObject.DeepClone();

            // Add "id" property to edgeObject if desired
            if (connection.GenerateEdgeId) {
                string guid = GraphViewConnection.GenerateDocumentId();
                outEdgeObject["_edgeId"] = guid;
                inEdgeObject["_edgeId"] = guid;
            }

            string srcLabel = srcVertexObject["label"]?.ToString();
            string sinkLabel = sinkVertexObject["label"]?.ToString();
            GraphViewJsonCommand.UpdateEdgeMetaProperty(outEdgeObject, edgeOffset, false, sinkId, sinkLabel);
            GraphViewJsonCommand.UpdateEdgeMetaProperty(inEdgeObject, edgeOffset, true, srcId, srcLabel);

            InsertEdgeObjectInternal(connection, srcVertexObject, srcVertexField, outEdgeObject, false, out outEdgeDocID); // srcVertex uploaded
            InsertEdgeObjectInternal(connection, sinkVertexObject, sinkVertexField, inEdgeObject, true, out inEdgeDocID); // sinkVertex uploaded
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
            JToken edgeContainer = vertexObject[isReverse ? "_reverse_edge" : "_edge"]; // JArray or JObject
            if (edgeContainer is JObject) {
                // Now it is a large-degree vertex, and contains at least 1 edge-document
                JArray edgeDocumentsArray = (JArray)edgeContainer["_edges"];
                Debug.Assert(edgeDocumentsArray != null, "edgeDocuments != null");
                Debug.Assert(edgeDocumentsArray.Count > 0, "edgeDocuments.Count > 0");

                string lastEdgeDocId = (string)edgeDocumentsArray.Last["id"];
                JObject edgeDocument = connection.RetrieveDocumentById(lastEdgeDocId);
                Debug.Assert(((string)edgeDocument["id"]).Equals(lastEdgeDocId), "((string)edgeDocument['id']).Equals(lastEdgeDocId)");
                Debug.Assert((bool)edgeDocument["_is_reverse"] == isReverse, "(bool)edgeDocument['_is_reverse'] == isReverse");
                Debug.Assert((string)edgeDocument["_vertex_id"] == (string)vertexObject["id"], "(string)edgeDocument['_vertex_id'] == (string)vertexObject['id']");

                JArray edgesArray = (JArray)edgeDocument["_edge"];
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
                    UploadOne(connection, lastEdgeDocId, edgeDocument, out tooLarge);
                }
                if (tooLarge) {
                    // The edge is too large to be filled into the last edge-document
                    // or the threashold is reached:
                    // Create a new edge-document to store the edge.
                    JObject edgeDocObject = new JObject {
                        ["id"] = GraphViewConnection.GenerateDocumentId(),
                        ["_partition"] = vertexObject["_partition"],
                        ["_is_reverse"] = isReverse,
                        ["_vertex_id"] = (string)vertexObject["id"],
                        ["_edge"] = new JArray(edgeObject)
                    };
                    lastEdgeDocId = connection.CreateDocumentAsync(edgeDocObject).Result;

                    // Add the newly create edge-document to vertexObject & upload the vertexObject
                    edgeDocumentsArray.Add(new JObject {
                        ["id"] = lastEdgeDocId
                    });
                }
                newEdgeDocId = lastEdgeDocId;

                // Upload the vertex documention (at least, its _nextXxx is changed)
                bool dummyTooLarge;
                UploadOne(connection, (string)vertexObject["id"], vertexObject, out dummyTooLarge);
                Debug.Assert(!dummyTooLarge);
            }
            else if (edgeContainer is JArray) {

                ((JArray)edgeContainer).Add(edgeObject);
                if (connection.EdgeSpillThreshold == 0) {
                    // Don't spill an edge-document until it is too large
                    tooLarge = false;
                }
                else {
                    // Explicitly specified a threshold
                    Debug.Assert(connection.EdgeSpillThreshold > 0, "connection.EdgeSpillThreshold > 0");
                    tooLarge = (((JArray)edgeContainer).Count >= connection.EdgeSpillThreshold);
                }

                if (!tooLarge) {
                    UploadOne(connection, (string)vertexObject["id"], vertexObject, out tooLarge);
                }
                if (tooLarge) {
                    string existEdgeDocId;
                    SpillVertexEdgesToDocument(connection, vertexObject, out existEdgeDocId, out newEdgeDocId);

                    // Update the in & out edges in vertex field
                    if (isReverse) {
                        Debug.Assert(vertexField.RevAdjacencyList.AllEdges.All(edge => edge.EdgeDocID == null));
                        foreach (EdgeField edge in vertexField.RevAdjacencyList.AllEdges) {
                            edge.EdgeDocID = existEdgeDocId;
                        }
                    }
                    else {
                        Debug.Assert(vertexField.AdjacencyList.AllEdges.All(edge => edge.EdgeDocID == null));
                        foreach (EdgeField edge in vertexField.AdjacencyList.AllEdges) {
                            edge.EdgeDocID = existEdgeDocId;
                        }
                    }
                }
                else {
                    newEdgeDocId = null;
                }
            }
            else {
                throw new Exception($"BUG: edgeContainer should either be JObject or JArray, but now: {edgeContainer?.GetType()}");
            }
        }

        /// <summary>
        /// This function spills a small-degree vertex, stores its edges into seperate documents
        /// Either its incoming or outgoing edges are moved to a new document, decided by which is larger in size
        /// NOTE: This function will upload the vertex document
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="vertexObject"></param>
        /// <param name="existEdgeDocId">This is the first edge-document (to store the existing edges)</param>
        /// <param name="newEdgeDocId">This is the second edge-document (to store the currently creating edge)</param>
        private static void SpillVertexEdgesToDocument(GraphViewConnection connection, JObject vertexObject, out string existEdgeDocId, out string newEdgeDocId)
        {
            Debug.Assert(vertexObject["_partition"] != null);
            Debug.Assert((string)vertexObject["id"] == (string)vertexObject["_partition"]);

            // NOTE: The VertexCache is not updated here
            bool outEdgeSeperated = vertexObject["_edge"] is JObject;
            bool inEdgeSeperated = vertexObject["_reverse_edge"] is JObject;
            if (inEdgeSeperated && outEdgeSeperated) {
                throw new Exception("BUG: Should not get here! Either incoming or outgoing edegs should not have been seperated");
            }

            JArray targetEdgeArray;
            bool targetEdgeIsReverse;
            if (inEdgeSeperated) {
                targetEdgeArray = (JArray)vertexObject["_edge"];
                targetEdgeIsReverse = false;
            }
            else if (outEdgeSeperated) {
                targetEdgeArray = (JArray)vertexObject["_reverse_edge"];
                targetEdgeIsReverse = true;
            }
            else {
                JArray outEdgeArray = (JArray)vertexObject["_edge"];
                JArray inEdgeArray = (JArray)vertexObject["_reverse_edge"];
                targetEdgeIsReverse = (outEdgeArray.ToString().Length < inEdgeArray.ToString().Length);
                targetEdgeArray = targetEdgeIsReverse ? inEdgeArray : outEdgeArray;
            }

            // Create a new edge-document to store the currently creating edge
            JObject newEdgeDocObject = new JObject {
                ["id"] = GraphViewConnection.GenerateDocumentId(),
                ["_partition"] = vertexObject["_partition"],
                ["_is_reverse"] = targetEdgeIsReverse,
                ["_is_reverse"] = targetEdgeIsReverse,
                ["_vertex_id"] = (string)vertexObject["id"],
                ["_edge"] = new JArray(targetEdgeArray.Last),
            };
            newEdgeDocId = connection.CreateDocumentAsync(newEdgeDocObject).Result;
            targetEdgeArray.Last.Remove();  // Remove the currently create edge appended just now

            // Create another new edge-document to store the existing edges.
            JObject existEdgeDocObject = new JObject {
                ["id"] = GraphViewConnection.GenerateDocumentId(),
                ["_partition"] = vertexObject["_partition"],
                ["_is_reverse"] = targetEdgeIsReverse,
                ["_vertex_id"] = (string)vertexObject["id"],
                ["_edge"] = targetEdgeArray,
            };
            existEdgeDocId = connection.CreateDocumentAsync(existEdgeDocObject).Result;

            // Update vertexObject to store the newly create edge-document & upload the vertexObject
            vertexObject[targetEdgeIsReverse ? "_reverse_edge" : "_edge"] = new JObject {
                ["_edges"] = new JArray {
                    new JObject {
                        ["id"] = existEdgeDocId,
                    },
                    new JObject {
                        ["id"] = newEdgeDocId,
                    },
                },
            };            
            bool dummyTooLarge;
            UploadOne(connection, (string)vertexObject["id"], vertexObject, out dummyTooLarge);
        }


        /// <summary>
        /// Find incoming or outgoing edge by "srcId and _offset"
        /// Output the edgeObject, as well as the edgeDocId (null for small-degree edges)
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="vertexObject"></param>
        /// <param name="srcVertexId"></param>
        /// <param name="edgeOffset"></param>
        /// <param name="isReverseEdge"></param>
        /// <param name="edgeObject"></param>
        /// <param name="edgeDocId"></param>
        public static void FindEdgeBySourceAndOffset(
            GraphViewConnection connection,
            JObject vertexObject, string srcVertexId, long edgeOffset, bool isReverseEdge,
            out JObject edgeObject, out string edgeDocId)
        {
            if (!isReverseEdge) {
                Debug.Assert((string)vertexObject["id"] == srcVertexId);
            }
            JToken edgeContainer = vertexObject[isReverseEdge ? "_reverse_edge" : "_edge"];

            if (edgeContainer is JArray) {  // for small-degree vertexes
                if (isReverseEdge) {
                    edgeObject = (from edgeObj in edgeContainer.Children<JObject>()
                                  where (string)edgeObj["_srcV"] == srcVertexId
                                  where (long)edgeObj["_offset"] == edgeOffset
                                  select edgeObj
                                 ).FirstOrDefault();
                }
                else {
                    edgeObject = (from edgeObj in edgeContainer.Children<JObject>()
                                  where (long)edgeObj["_offset"] == edgeOffset
                                  select edgeObj
                                 ).FirstOrDefault();
                }
                edgeDocId = null;
            }
            else if (edgeContainer is JObject) { // for large-degree vertexes
                string edgeIdList = string.Join(", ", edgeContainer["_edges"].Children<JObject>().Select(e => $"'{e["id"]}'"));
                string query;
                if (isReverseEdge) {
                    query = $"SELECT doc.id, edge\n" +
                            $"FROM doc\n" +
                            $"JOIN edge IN doc._edge\n" +
                            $"WHERE doc.id IN ({edgeIdList})\n" +
                            $"  AND (edge._srcV = '{srcVertexId}')\n" +
                            $"  AND (edge._offset = {edgeOffset})\n";
                }
                else {
                    query = $"SELECT doc.id, edge\n" +
                            $"FROM doc\n" +
                            $"JOIN edge IN doc._edge\n" +
                            $"WHERE doc.id IN ({edgeIdList})\n" +
                            $"  AND (edge._offset = {edgeOffset})\n";
                }
                JObject result = connection.ExecuteQueryUnique(query);
                edgeDocId = (string)result?["id"];
                edgeObject = (JObject)result?["edge"];
            }
            else {
                throw new Exception($"BUG: edgeContainer should either be JObject or JArray, but now: {edgeContainer?.GetType()}");
            }
        }


        public static void RemoveEdge(
            Dictionary<string, JObject> documentMap,
            GraphViewConnection connection,
            string edgeDocId,
            JObject vertexObject,
            bool isReverse,
            string srcVertexId, long edgeOffset)
        {
            JToken edgeContainer = vertexObject[isReverse ? "_reverse_edge" : "_edge"];
            if (edgeContainer is JObject) {
                // Now it is a large-degree vertex, and contains at least 1 edge-document
                Debug.Assert(!string.IsNullOrEmpty(edgeDocId), "!string.IsNullOrEmpty(edgeDocId)");

                JArray edgeDocumentsArray = (JArray)edgeContainer["_edges"];
                Debug.Assert(edgeDocumentsArray != null, "edgeDocuments != null");
                Debug.Assert(edgeDocumentsArray.Count > 0, "edgeDocuments.Count > 0");

                JObject edgeDocument = connection.RetrieveDocumentById(edgeDocId);
                Debug.Assert(((string)edgeDocument["id"]).Equals(edgeDocId), "((string)edgeDocument['id']).Equals(edgeDocId)");
                Debug.Assert((bool)edgeDocument["_is_reverse"] == isReverse, "(bool)edgeDocument['_is_reverse'] == isReverse");
                Debug.Assert((string)edgeDocument["_vertex_id"] == (string)vertexObject["id"], "(string)edgeDocument['_vertex_id'] == (string)vertexObject['id']");

                // The edge to be removed must exist! (garanteed by caller)
                JArray edgesArray = (JArray)edgeDocument["_edge"];
                Debug.Assert(edgesArray != null, "edgesArray != null");
                Debug.Assert(edgesArray.Count > 0, "edgesArray.Count > 0");
                if (isReverse) {
                    edgesArray.First(e => (string)e["_srcV"] == srcVertexId && (long)e["_offset"] == edgeOffset).Remove();
                }
                else {
                    edgesArray.First(e => (long)e["_offset"] == edgeOffset).Remove();
                }

                // 
                // If the edge-document contains no edge after the removal, delete this edge-document.
                // Don't forget to update the vertex-document at the same time.
                //
                if (edgesArray.Count == 0) {
                    
                    edgeDocumentsArray.First(edoc => ((string)edoc["id"]).Equals(edgeDocId)).Remove();

                    //
                    // If the vertex-document contains no (incoming or outgoing) edges now, set edgeContainer 
                    // ("_edge" or "_reverse_edge") to an empry JArray! Anyway, we ensure that if edgeContainer 
                    // is JObject, it contains at least one edge-document
                    //
                    if (edgeDocumentsArray.Count == 0) {
                        vertexObject[isReverse ? "_reverse_edge" : "_edge"] = new JArray();
                    }

                    // Delete the edge-document, and add the vertex-document to the upload list
                    documentMap[edgeDocId] = null;
                    documentMap[(string)vertexObject["id"]] = vertexObject;
                }
                else {
                    documentMap[edgeDocId] = edgeDocument;
                }
            }
            else if (edgeContainer is JArray) {
                Debug.Assert(edgeDocId == null, "edgeDocId == null");

                if (isReverse) {
                    ((JArray)edgeContainer).First(e => (string)e["_srcV"] == srcVertexId && (long)e["_offset"] == edgeOffset).Remove();
                }
                else {
                    ((JArray)edgeContainer).First(e => (long)e["_offset"] == edgeOffset).Remove();
                }
                documentMap[(string)vertexObject["id"]] = vertexObject;
            }
            else {
                throw new Exception($"BUG: edgeContainer should either be JObject or JArray, but now: {edgeContainer?.GetType()}");
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
            JObject newEdgeObject  // With all metadata (including id, _partition, _srcV/_sinkV, _offset)
        )
        {
            bool tooLarge;
            string srcOrSinkVInEdgeObject = isReverse ? "_srcV" : "_sinkV";

            if (edgeDocId == null) {
                JArray edgeContainer = (JArray)vertexObject[isReverse ? "_reverse_edge" : "_edge"];

                // Don't use JToken.Replace() here.
                // Make sure the currently modified edge is the last child of edgeContainer, which 
                // garantees the newly created edge-document won't be too large.
                //
                // NOTE: The following line applies for both incomming and outgoing edge.
                edgeContainer.Children<JObject>().First(
                    e => (long)e["_offset"] == (long)newEdgeObject["_offset"] &&
                         (string)e[srcOrSinkVInEdgeObject] == (string)newEdgeObject[srcOrSinkVInEdgeObject]
                ).Remove();
                edgeContainer.Add(newEdgeObject);

                UploadOne(connection, (string)vertexObject["id"], vertexObject, out tooLarge);
                if (tooLarge) {
                    // Handle this situation: The updated edge is too large to be filled into the vertex-document
                    string existEdgeDocId, newEdgeDocId;
                    SpillVertexEdgesToDocument(connection, vertexObject, out existEdgeDocId, out newEdgeDocId);
                }
            }
            else {
                JObject edgeDocObject = connection.RetrieveDocumentById(edgeDocId);
                edgeDocObject["_edge"].Children<JObject>().First(
                    e => (long)e["_offset"] == (long)newEdgeObject["_offset"] &&
                         (string)e[srcOrSinkVInEdgeObject] == (string)newEdgeObject[srcOrSinkVInEdgeObject]
                ).Remove();
                ((JArray)edgeDocObject["_edge"]).Add(newEdgeObject);
                UploadOne(connection, edgeDocId, edgeDocObject, out tooLarge);
                if (tooLarge) {
                    // Handle this situation: The modified edge is too large to be filled into the original edge-document
                    // Remove the edgeObject added just now, and upload the original edge-document
                    ((JArray)edgeDocObject["_edge"]).Last.Remove();
                    UploadOne(connection, edgeDocId, edgeDocObject, out tooLarge);
                    Debug.Assert(!tooLarge);

                    // Insert the edgeObject to one of the vertex's edge-documents
                    InsertEdgeObjectInternal(connection, vertexObject, null, newEdgeObject, isReverse, out edgeDocId);
                }
            }
        }

        public static AdjacencyListField GetReverseAdjacencyListOfVertex(GraphViewConnection connection, string vertexId)
        {
            AdjacencyListField result = new AdjacencyListField();

            string query = $"SELECT {{\"edge\": edge, " +
                           $"\"_srcV\": doc.id, " +
                           $"\"_srcVLabel\": doc.label}} AS incomingEdgeMetadata\n" +
                           $"FROM doc\n" +
                           $"JOIN edge IN doc._edge\n" +
                           $"WHERE edge._sinkV = '{vertexId}'\n";

            foreach (JObject edgeDocument in connection.ExecuteQuery(query))
            {
                JObject edgeMetadata = (JObject)edgeDocument["incomingEdgeMetadata"];
                JObject edgeObject = (JObject)edgeMetadata["edge"];
                string srcV = edgeMetadata["_srcV"].ToString();
                string srcVLabel = edgeMetadata["_srcVLabel"]?.ToString();

                EdgeField edgeField = EdgeField.ConstructForwardEdgeField(srcV, srcVLabel, null, edgeObject);
                edgeField.EdgeProperties.Add("_srcV", new EdgePropertyField("_srcV", srcV, JsonDataType.String));
                if (srcVLabel != null) {
                    edgeField.EdgeProperties.Add("_srcVLabel",
                        new EdgePropertyField("_srcVLabel", srcVLabel, JsonDataType.String));
                }

                result.AddEdgeField(srcV, (long)edgeObject["_offset"], edgeField);
            }

            return result;
        }

        public static Dictionary<string, AdjacencyListField> GetReverseAdjacencyListsOfVertexCollection(GraphViewConnection connection, HashSet<string> vertexIdSet)
        {
            Dictionary<string, AdjacencyListField> revAdjacencyListCollection = new Dictionary<string, AdjacencyListField>();

            StringBuilder vertexIdList = new StringBuilder();

            foreach (string vertexId in vertexIdSet)
            {
                if (vertexIdList.Length > 0) {
                    vertexIdList.Append(", ");
                }
                vertexIdList.AppendFormat("'{0}'", vertexId);

                revAdjacencyListCollection[vertexId] = new AdjacencyListField();
            }

            string query = $"SELECT {{\"edge\": edge, " +
                           $"\"vertexId\": edge._sinkV, "+
                           $"\"_srcV\": doc.id, " +
                           $"\"_srcVLabel\": doc.label}} AS incomingEdgeMetadata\n" +
                           $"FROM doc\n" +
                           $"JOIN edge IN doc._edge\n" +
                           $"WHERE edge._sinkV IN ({vertexIdList.ToString()})\n";

            foreach (JObject edgeDocument in connection.ExecuteQuery(query))
            {
                JObject edgeMetadata = (JObject)edgeDocument["incomingEdgeMetadata"];
                JObject edgeObject = (JObject)edgeMetadata["edge"];
                string vertexId = edgeMetadata["vertexId"].ToString();
                string srcV = edgeMetadata["_srcV"].ToString();
                string srcVLabel = edgeMetadata["_srcVLabel"]?.ToString();

                EdgeField edgeField = EdgeField.ConstructForwardEdgeField(srcV, srcVLabel, null, edgeObject);
                edgeField.EdgeProperties.Add("_srcV", new EdgePropertyField("_srcV", srcV, JsonDataType.String));
                if (srcVLabel != null) {
                    edgeField.EdgeProperties.Add("_srcVLabel",
                        new EdgePropertyField("_srcVLabel", srcVLabel, JsonDataType.String));
                }

                AdjacencyListField revAdjList = revAdjacencyListCollection[vertexId];
                revAdjList.AddEdgeField(srcV, (long)edgeObject["_offset"], edgeField);
            }

            return revAdjacencyListCollection;
        }
    }
}
