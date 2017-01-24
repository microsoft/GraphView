using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Linq;

namespace GraphView
{
    internal abstract class ModificationBaseOpertaor2 : GraphViewExecutionOperator
    {
        protected GraphViewConnection Connection;
        protected GraphViewExecutionOperator InputOperator;

        protected ModificationBaseOpertaor2(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection)
        {
            InputOperator = pInputOp;
            Connection = pConnection;
            Open();
        }

        internal abstract RawRecord DataModify(RawRecord record);

        public override RawRecord Next()
        {
            RawRecord srcRecord = null;

            while (InputOperator.State() && (srcRecord = InputOperator.Next()) != null)
            {
                var result = DataModify(srcRecord);
                if (result == null) continue;;

                var resultRecord = new RawRecord(srcRecord);
                resultRecord.Append(result);

                return resultRecord;
            }

            Close();
            return null;
        }

        public override void ResetState()
        {
            InputOperator.ResetState();
            Open();
        }

        protected string RetrieveDocumentById(string id)
        {
            var script = string.Format("SELECT * FROM Node WHERE Node.id = '{0}'", id);
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
            List<dynamic> result = Connection.DocDBclient.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(Connection.DocDB_DatabaseId, Connection.DocDB_CollectionId),
                script, queryOptions).ToList();

            if (result.Count == 0)
                return null;
            return ((JObject)result[0]).ToString();
        }

        protected void Upload(Dictionary<string, string> documentsMap)
        {
            ReplaceDocument(documentsMap).Wait();
        }

        protected async Task ReplaceDocument(Dictionary<string, string> documentsMap)
        {
            foreach (var pair in documentsMap)
                await DataModificationUtils.ReplaceDocument(Connection, pair.Key, pair.Value)
                    .ConfigureAwait(continueOnCapturedContext: false);
        }

        /// <summary>
        /// Given a edge's source node document and its offset, return the edge's sink id and reverse edge's offset
        /// </summary>
        /// <param name="jsonString"></param>
        /// <param name="edgeOffset"></param>
        /// <returns></returns>
        protected List<string> GetSinkIdAndReverseEdgeOffset(string jsonString, string edgeOffset)
        {
            var document = JObject.Parse(jsonString);
            var adjList = (JArray)document["_edge"];

            foreach (var edge in adjList.Children<JObject>())
            {
                if (edge["_ID"].ToString().Equals(edgeOffset))
                    return new List<string> { edge["_sink"].ToString(), edge["_reverse_ID"].ToString() };
            }

            return null;
        }
    }

    internal class AddVOperator : ModificationBaseOpertaor2
    {
        private string _jsonDocument;
        private Document _createdDocument;
        private List<string> _projectedFieldList; 

        public AddVOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection, string pJsonDocument, List<string> pProjectedFieldList)
            : base(pInputOp, pConnection)
        {
            _jsonDocument = pJsonDocument;
            _projectedFieldList = pProjectedFieldList;
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            var obj = JObject.Parse(_jsonDocument);

            Upload(obj);

            var vertexField = Connection.VertexCache.GetVertexField(_createdDocument.Id, _createdDocument.ToString());

            var result = new RawRecord();

            foreach (var fieldName in _projectedFieldList)
            {
                var fieldValue = vertexField[fieldName];

                result.Append(fieldValue);
            }

            return result;
        }

        private void Upload(JObject obj)
        {
            CreateDocument(obj).Wait();
        }

        private async Task CreateDocument(JObject obj)
        {
            _createdDocument = await Connection.DocDBclient.CreateDocumentAsync("dbs/" + Connection.DocDB_DatabaseId + "/colls/" + Connection.DocDB_CollectionId, obj)
                .ConfigureAwait(continueOnCapturedContext: false);
        }
    }

    internal class DropNodeOperator : ModificationBaseOpertaor2
    {
        private int _nodeIdIndex;

        public DropNodeOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection, int pNodeIdIndex)
            : base(pInputOp, pConnection)
        {
            _nodeIdIndex = pNodeIdIndex;
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            var targetId = record[_nodeIdIndex].ToValue;

            var targetJson = RetrieveDocumentById(targetId);

            var targetJObject = JObject.Parse(targetJson);

            // Temporarily change
            DropEdgeOperator dropEdgeOp = new DropEdgeOperator(null, Connection, 0, 1);
            RawRecord temp = new RawRecord(2);

            var adj = (JArray)targetJObject["_edge"];

            foreach (var edge in adj.Children<JObject>())
            {
                temp.fieldValues[0] = new StringField(targetId);
                temp.fieldValues[1] = new StringField(edge["_ID"].ToString());
                dropEdgeOp.DataModify(temp);
            }

            var revAdj = (JArray)targetJObject["_reverse_edge"];

            foreach (var revEdge in revAdj.Children<JObject>())
            {
                temp.fieldValues[0] = new StringField(revEdge["_sink"].ToString());
                temp.fieldValues[1] = new StringField(revEdge["_reverse_ID"].ToString());
                dropEdgeOp.DataModify(temp);
            }

            DeleteNode(targetId);

            return null;
        }

        /// <summary>
        /// Check whether the target node is isolated first. If true, then delete it.
        /// </summary>
        private void DeleteNode(string targetId)
        {
            var collectionLink = "dbs/" + Connection.DocDB_DatabaseId + "/colls/" + Connection.DocDB_CollectionId;
            var checkIsolatedScript =
                string.Format(
                    "SELECT * FROM Node WHERE Node.id = '{0}' AND ARRAY_LENGTH(Node._edge) = 0 AND ARRAY_LENGTH(Node._reverse_edge) = 0",
                    targetId);
            var toBeDeletedNodes = SendQuery(checkIsolatedScript, Connection);

            foreach (var node in toBeDeletedNodes)
            {
                var docLink = collectionLink + "/docs/" + node.id;
                DeleteDocument(docLink).Wait();
            }
        }

        private async Task DeleteDocument(string docLink)
        {
            await Connection.DocDBclient.DeleteDocumentAsync(docLink).ConfigureAwait(continueOnCapturedContext: false);
        }
    }

    internal class AddEOperator : ModificationBaseOpertaor2
    {
        private int _otherVTag;
        // The scalar subquery function select the vertex ID of source and sink of the edge to be added or deleted
        private ScalarFunction _srcFunction;
        private ScalarFunction _sinkFunction;
        // The initial json object string of to-be-inserted edge, waiting to update the edgeOffset field
        private string _edgeJsonDocument;
        private List<string> _edgeProperties;

        public AddEOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection, 
            ScalarFunction pSrcFunction, ScalarFunction pSinkFunction, 
            int otherVTag, string pEdgeJsonDocument, List<string> pProjectedFieldList)
            : base(pInputOp, pConnection)
        {
            _srcFunction = pSrcFunction;
            _sinkFunction = pSinkFunction;
            _otherVTag = otherVTag;
            _edgeJsonDocument = pEdgeJsonDocument;
            _edgeProperties = pProjectedFieldList;
        }

        // TODO: If the scalarSubquery yields a vertex field, we could skip the RetrieveDocument from server
        // TODO: and that means we need a function which can translate a VertexField back to a Json string for uploading to the server
        internal override RawRecord DataModify(RawRecord record)
        {
            var srcFieldObject = _srcFunction.Evaluate(record);
            var sinkFieldObject = _sinkFunction.Evaluate(record);

            if (srcFieldObject == null || sinkFieldObject == null) return null;

            string srcId;
            string sinkId;

            // TODO: Just a hack, need to modify the translation code
            if (srcFieldObject is StringField) srcId = (srcFieldObject as StringField).Value;
            else if (srcFieldObject is PropertyField) srcId = (srcFieldObject as PropertyField).PropertyValue;
            else if (srcFieldObject is VertexField) srcId = (srcFieldObject as VertexField)["id"].ToValue;
            else srcId = srcFieldObject.ToString();
            // TODO: Just a hack, need to modify the translation code
            if (sinkFieldObject is StringField) sinkId = (sinkFieldObject as StringField).Value;
            else if (sinkFieldObject is PropertyField) sinkId = (sinkFieldObject as PropertyField).PropertyValue;
            else if (sinkFieldObject is VertexField) sinkId = (sinkFieldObject as VertexField)["id"].ToValue;
            else sinkId = sinkFieldObject.ToString();

            var srcJsonDocument = RetrieveDocumentById(srcId);
            var sinkJsonDocument = srcId.Equals(sinkId) ? srcJsonDocument : RetrieveDocumentById(sinkId);

            var srcVertexField = (srcFieldObject is VertexField) 
                                    ? srcFieldObject as VertexField 
                                    : Connection.VertexCache.GetVertexField(srcId, srcJsonDocument);
            var sinkVertexField = (sinkFieldObject is VertexField) 
                                    ? sinkFieldObject as VertexField 
                                    : Connection.VertexCache.GetVertexField(sinkId, sinkJsonDocument);

            JObject edgeObject, revEdgeObject;
            var results = InsertEdge(srcJsonDocument, sinkJsonDocument, _edgeJsonDocument, srcId, sinkId, out edgeObject, out revEdgeObject);

            Upload(results);

            // Update vertex's adjacency list and reverse adjacency list
            var edgeField = FieldObject.GetEdgeField(edgeObject);
            var revEdgeField = FieldObject.GetEdgeField(revEdgeObject);

            srcVertexField.AdjacencyList.Edges.Add(edgeField["_ID"].ToValue, edgeField);
            sinkVertexField.RevAdjacencyList.Edges.Add(revEdgeField["_ID"].ToValue, revEdgeField);

            var result = new RawRecord();

            // source, sink, other, offset, *
            result.Append(new StringField(srcId));
            result.Append(new StringField(sinkId));
            result.Append(new StringField(_otherVTag == 0 ? srcId : sinkId));
            result.Append(new StringField(edgeObject["_ID"].ToString()));
            result.Append(edgeField);

            edgeField.Label = edgeField["label"]?.ToValue;
            edgeField.InV = srcId;
            edgeField.OutV = sinkId;
            edgeField.InVLabel = srcVertexField["label"]?.ToValue;
            edgeField.OutVLabel = sinkVertexField["label"]?.ToValue;

            for (var i = GraphViewReservedProperties.ReservedEdgeProperties.Count; i < _edgeProperties.Count; i++)
            {
                var fieldValue = edgeField[_edgeProperties[i]];
                result.Append(fieldValue);
            }

            return result;
        }

        private Dictionary<string, string> InsertEdge(string srcJsonDocumentString, string sinkJsonDocumentString, string edgeJsonDocumentString, 
            string srcId, string sinkId, out JObject edgeObject, out JObject revEdgeObject)
        {
            var documentsMap = new Dictionary<string, string>();

            var srcJsonDocument = JObject.Parse(srcJsonDocumentString);
            var sinkJsonDocument = JObject.Parse(sinkJsonDocumentString);
            var edgeJsonDocument = JObject.Parse(edgeJsonDocumentString);
            var edgeOffset = srcJsonDocument["_nextEdgeOffset"].ToObject<long>();
            var reverseEdgeOffset = sinkJsonDocument["_nextReverseEdgeOffset"].ToObject<long>();

            // Construct the edge object for srcNode._edge
            var sinkNodeLabel = sinkJsonDocument["label"]?.ToString();
            GraphViewJsonCommand.UpdateEdgeMetaProperty(edgeJsonDocument, edgeOffset, reverseEdgeOffset, sinkId, sinkNodeLabel);

            // Insert the edge object in the srcNode._edge and update the _nextEdgeOffset
            var srcJsonEdgeArray = (JArray)srcJsonDocument["_edge"];
            srcJsonEdgeArray.Add(edgeJsonDocument);
            srcJsonDocument["_nextEdgeOffset"] = edgeOffset + 1;
            documentsMap[srcId] = srcJsonDocument.ToString();

            edgeObject = JObject.FromObject(edgeJsonDocument);

            // Construct the edge object for sinkNode._reverse_edge
            edgeJsonDocument = JObject.FromObject(edgeJsonDocument);
            var srcNodeLabel = srcJsonDocument["label"]?.ToString();
            GraphViewJsonCommand.UpdateEdgeMetaProperty(edgeJsonDocument, reverseEdgeOffset, edgeOffset, srcId, srcNodeLabel);

            // Insert the edge object in the sinkNode._reverse_edge and update the _nextReverseEdgeOffset
            if (srcId.Equals(sinkId)) sinkJsonDocument = srcJsonDocument;
            var sinkJsonReverseEdgeArray = (JArray) sinkJsonDocument["_reverse_edge"];
            sinkJsonReverseEdgeArray.Add(edgeJsonDocument);
            sinkJsonDocument["_nextReverseEdgeOffset"] = reverseEdgeOffset + 1;
            documentsMap[sinkId] = sinkJsonDocument.ToString();

            revEdgeObject = JObject.FromObject(edgeJsonDocument);

            //var edgeOffset = GraphViewJsonCommand.get_edge_num(srcJsonDocumentString);
            //var reverseEdgeOffset = GraphViewJsonCommand.get_reverse_edge_num(sinkJsonDocumentString);

            //edgeJsonDocumentString = GraphViewJsonCommand.insert_property(edgeJsonDocumentString, edgeOffset.ToString(), "_ID").ToString();
            //edgeJsonDocumentString = GraphViewJsonCommand.insert_property(edgeJsonDocumentString, reverseEdgeOffset.ToString(), "_reverse_ID").ToString();
            //edgeJsonDocumentString = GraphViewJsonCommand.insert_property(edgeJsonDocumentString, '\"' + sinkId + '\"', "_sink").ToString();
            //edgeObjectString = edgeJsonDocumentString;
            //documentsMap[srcId] = GraphViewJsonCommand.insert_edge(srcJsonDocumentString, edgeJsonDocumentString, edgeOffset).ToString();

            //edgeJsonDocumentString = GraphViewJsonCommand.insert_property(edgeJsonDocumentString, reverseEdgeOffset.ToString(), "_ID").ToString();
            //edgeJsonDocumentString = GraphViewJsonCommand.insert_property(edgeJsonDocumentString, edgeOffset.ToString(), "_reverse_ID").ToString();
            //edgeJsonDocumentString = GraphViewJsonCommand.insert_property(edgeJsonDocumentString, '\"' + srcId + '\"', "_sink").ToString();
            //documentsMap[sinkId] = GraphViewJsonCommand.insert_reverse_edge(documentsMap[sinkId], edgeJsonDocumentString, reverseEdgeOffset).ToString();

            return documentsMap;
        }
    }

    internal class DropEdgeOperator : ModificationBaseOpertaor2
    {
        private int _srcIdIndex;
        private int _edgeOffsetIndex;

        public DropEdgeOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection, int pSrcIdIndex, int pEdgeOffsetIndex)
            : base(pInputOp, pConnection)
        {
            _srcIdIndex = pSrcIdIndex;
            _edgeOffsetIndex = pEdgeOffsetIndex;
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            var srcId = record[_srcIdIndex].ToValue;
            var edgeOffset = record[_edgeOffsetIndex].ToValue;

            var srcJsonDocument = RetrieveDocumentById(srcId);
            var sinkIdAndReverseEdgeOffset = GetSinkIdAndReverseEdgeOffset(srcJsonDocument, edgeOffset);

            if (sinkIdAndReverseEdgeOffset == null) return null;

            var sinkId = sinkIdAndReverseEdgeOffset[0];
            var revEdgeOffset = sinkIdAndReverseEdgeOffset[1];

            var sinkJsonDocument = srcId.Equals(sinkId) ? srcJsonDocument : RetrieveDocumentById(sinkId);

            var srcVertexField = Connection.VertexCache.GetVertexField(srcId, srcJsonDocument);
            var sinkVertexField = Connection.VertexCache.GetVertexField(sinkId, sinkJsonDocument);

            var results = DeleteEdge(srcId, sinkId, edgeOffset, revEdgeOffset, srcJsonDocument, sinkJsonDocument);

            Upload(results);

            srcVertexField.AdjacencyList.Edges.Remove(edgeOffset);
            sinkVertexField.RevAdjacencyList.Edges.Remove(revEdgeOffset);

            return null;
        }

        private Dictionary<string, string> DeleteEdge(string srcId, string sinkId, string edgeOffset, string reverseEdgeOffset,
            string srcJsonDocumentString, string sinkJsonDocumentString)
        {
            var documentsMap = new Dictionary<string, string>();

            // Delete the edge object in the srcNode._edge
            var srcJsonDocument = JObject.Parse(srcJsonDocumentString);
            var edgeArray = (JArray)srcJsonDocument["_edge"];
            foreach (var edgeObject in edgeArray.Children<JObject>())
            {
                if (edgeObject["_ID"].ToString().Equals(edgeOffset, StringComparison.OrdinalIgnoreCase))
                {
                    edgeObject.Remove();
                    break;
                }
            }
            documentsMap[srcId] = srcJsonDocument.ToString();

            // Delete the edge object in the sinkNode._reverse_edge
            var sinkJsonDocument = srcId.Equals(sinkId, StringComparison.OrdinalIgnoreCase) ? srcJsonDocument : JObject.Parse(sinkJsonDocumentString);
            var reverseEdgeArry = (JArray) sinkJsonDocument["_reverse_edge"];
            foreach (var edgeObject in reverseEdgeArry.Children<JObject>())
            {
                if (edgeObject["_ID"].ToString().Equals(reverseEdgeOffset, StringComparison.OrdinalIgnoreCase))
                {
                    edgeObject.Remove();
                    break;
                }
            }
            documentsMap[sinkId] = sinkJsonDocument.ToString();

            //documentsMap[srcId] = GraphViewJsonCommand.Delete_edge(documentsMap[srcId], int.Parse(edgeOffset));
            //documentsMap[sinkId] = GraphViewJsonCommand.Delete_reverse_edge(documentsMap[sinkId], int.Parse(reverseEdgeOffset));

            return documentsMap;
        }
    }

    internal abstract class UpdatePropertiesBaseOperator : ModificationBaseOpertaor2
    {
        internal enum UpdatePropertyMode
        {
            Set,
            Append
        };

        protected UpdatePropertyMode Mode;
        /// <summary>
        /// Item1 is property key.
        /// Item2 is property value. If it is null, then delete the property
        /// Item3 is property's index in the input record. If it is -1, then the input record doesn't contain this property.
        /// </summary>
        // TODO: Now the item3 is useless
        // TODO: Both the translation code and the physical operator haven't handled the g.V().properties().drop() case.
        // TODO: Handle <*, null>
        protected List<Tuple<WValueExpression, WValueExpression, int>> PropertiesToBeUpdated;

        protected UpdatePropertiesBaseOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection,
            List<Tuple<WValueExpression, WValueExpression, int>> pPropertiesToBeUpdated, UpdatePropertyMode pMode = UpdatePropertyMode.Set)
            : base(pInputOp, pConnection)
        {
            PropertiesToBeUpdated = pPropertiesToBeUpdated;
            Mode = pMode;
        }

        public override RawRecord Next()
        {
            RawRecord srcRecord = null;

            while (InputOperator.State() && (srcRecord = InputOperator.Next()) != null)
            {
                var result = DataModify(srcRecord);
                if (result == null) continue;

                //foreach (var tuple in PropertiesToBeUpdated)
                //{
                //    var propertyIndex = tuple.Item3;
                //    var propertyNewValue = tuple.Item2;
                //    if (propertyIndex == -1) continue;

                //    srcRecord.fieldValues[propertyIndex] = new StringField(propertyNewValue.Value);
                //}

                return srcRecord;
            }

            Close();
            return null;
        }
    }

    internal class UpdateNodePropertiesOperator : UpdatePropertiesBaseOperator
    {
        private int _nodeIdIndex;

        public UpdateNodePropertiesOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection,
            int pNodeIndex, List<Tuple<WValueExpression, WValueExpression, int>> pPropertiesList, UpdatePropertyMode pMode = UpdatePropertyMode.Set)
            : base(pInputOp, pConnection, pPropertiesList, pMode)
        {
            _nodeIdIndex = pNodeIndex;
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            string targetId = record[_nodeIdIndex].ToValue;

            var targetJsonDocument = RetrieveDocumentById(targetId);

            var targetVertexField = Connection.VertexCache.GetVertexField(targetId, targetJsonDocument);

            var results = UpdateNodeProperties(targetId, targetJsonDocument, targetVertexField, PropertiesToBeUpdated, Mode);

            Upload(results);

            // Drop step, return null
            if (PropertiesToBeUpdated.Any(t => t.Item2 == null)) return null;
            return record;
        }

        private Dictionary<string, string> UpdateNodeProperties(string targetId, string documentString, VertexField targetVertexField,
            List<Tuple<WValueExpression, WValueExpression, int>> propList, UpdatePropertyMode mode)
        {
            var document = JObject.Parse(documentString);

            foreach (var t in propList)
            {
                var keyExpression = t.Item1;
                var valueExpression = t.Item2;

                if (mode == UpdatePropertyMode.Set)
                {
                    JProperty updatedProperty = GraphViewJsonCommand.UpdateProperty(document, keyExpression, valueExpression);
                    if (updatedProperty == null)
                        targetVertexField.VertexProperties.Remove(keyExpression.Value);
                    else
                        targetVertexField.UpdateVertexProperty(updatedProperty.Name, updatedProperty.Value.ToString());
                }
                else
                {
                    throw new NotImplementedException();
                }
            }

            return new Dictionary<string, string> { {targetId, document.ToString()} };
        }
    }

    internal class UpdateEdgePropertiesOperator : UpdatePropertiesBaseOperator
    {
        private int _srcIdIndex;
        private int _edgeOffsetIndex;

        public UpdateEdgePropertiesOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection, 
            int pSrcIdIndex, int pEdgeOffsetIndex, 
            List<Tuple<WValueExpression, WValueExpression, int>> propertiesList, UpdatePropertyMode pMode = UpdatePropertyMode.Set)
            : base(pInputOp, pConnection, propertiesList, pMode)
        {
            _srcIdIndex = pSrcIdIndex;
            _edgeOffsetIndex = pEdgeOffsetIndex;
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            string srcId = record[_srcIdIndex].ToValue;
            string edgeOffset = record[_edgeOffsetIndex].ToValue;

            var srcJsonDocument = RetrieveDocumentById(srcId);

            var sinkIdAndReverseEdgeOffset = GetSinkIdAndReverseEdgeOffset(srcJsonDocument, edgeOffset);
            if (sinkIdAndReverseEdgeOffset == null) return record;

            var sinkId = sinkIdAndReverseEdgeOffset[0];
            var reverseEdgeOffset = sinkIdAndReverseEdgeOffset[1];

            var sinkJsonDocument = srcId.Equals(sinkId) ? srcJsonDocument : RetrieveDocumentById(sinkId);

            var srcVertexField = Connection.VertexCache.GetVertexField(srcId, srcJsonDocument);
            var sinkVertexField = Connection.VertexCache.GetVertexField(sinkId, sinkJsonDocument);
            var edgeField = srcVertexField.AdjacencyList.Edges[edgeOffset];
            var revEdgeField = sinkVertexField.RevAdjacencyList.Edges[reverseEdgeOffset];

            var results = UpdateEdgeAndReverseEdgeProperties(srcId, edgeOffset, sinkId, reverseEdgeOffset,
                srcJsonDocument, sinkJsonDocument, edgeField, revEdgeField, Mode);

            Upload(results);

            // Drop step, return null
            if (PropertiesToBeUpdated.Any(t => t.Item2 == null)) return null;
            return record;
        }

        private Dictionary<string, string> UpdateEdgeAndReverseEdgeProperties(string srcId, string edgeOffset,
            string sinkId, string revEdgeOffset, string srcJsonDocument, string sinkJsonDocument,
            EdgeField edgeField, EdgeField revEdgeField, UpdatePropertyMode mode)
        {
            var documentsMap = new Dictionary<string, string>();
            documentsMap.Add(srcId, srcJsonDocument);
            if (!documentsMap.ContainsKey(sinkId)) documentsMap.Add(sinkId, sinkJsonDocument);

            UpdateEdgeProperties(documentsMap, PropertiesToBeUpdated, srcId, edgeOffset, false, edgeField, mode);
            UpdateEdgeProperties(documentsMap, PropertiesToBeUpdated, sinkId, revEdgeOffset, true, revEdgeField, mode);

            return documentsMap;
        }

        private void UpdateEdgeProperties(Dictionary<string, string> documentsMap, List<Tuple<WValueExpression, WValueExpression, int>> propList, 
            string id, string edgeOffset, bool isReverseEdge, EdgeField edgeField, UpdatePropertyMode mode)
        {
            var document = JObject.Parse(documentsMap[id]);
            var adj = isReverseEdge ? (JArray)document["_reverse_edge"] : (JArray)document["_edge"];
            var edgeId = int.Parse(edgeOffset);

            foreach (var edge in adj.Children<JObject>())
            {
                if (int.Parse(edge["_ID"].ToString()) != edgeId) continue;

                foreach (var t in propList)
                {
                    var keyExpression = t.Item1;
                    var valueExpression = t.Item2;

                    if (mode == UpdatePropertyMode.Set)
                    {
                        var updatedProperty = GraphViewJsonCommand.UpdateProperty(edge, keyExpression, valueExpression);
                        if (updatedProperty == null)
                            edgeField.EdgeProperties.Remove(keyExpression.Value);
                        else
                            edgeField.UpdateEdgeProperty(updatedProperty.Name, updatedProperty.Value.ToString());
                    }
                    else
                    {
                        throw new NotImplementedException();
                    }
                }
                break;
            }

            documentsMap[id] = document.ToString();
        }
    }
}
