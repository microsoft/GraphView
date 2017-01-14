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

            var document = JObject.Parse(_createdDocument.ToString());
            var result = new RawRecord { fieldValues = new List<FieldObject>() };

            foreach (var fieldName in _projectedFieldList)
            {
                var fieldValue = document[fieldName];

                result.Append(fieldValue != null ? new StringField(fieldValue.ToString()) : null);
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

    internal class DropVOperator : ModificationBaseOpertaor2
    {
        private int _nodeIdIndex;

        public DropVOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection, int pNodeIdIndex)
            : base(pInputOp, pConnection)
        {
            _nodeIdIndex = pNodeIdIndex;
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            var targetId = record[_nodeIdIndex];

            DeleteNode(targetId.ToString());

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

        private const int ReservedMetaFieldCount = 4;

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

        internal override RawRecord DataModify(RawRecord record)
        {
            var srcId = _srcFunction.Evaluate(record).ToString();
            var sinkId = _sinkFunction.Evaluate(record).ToString();

            if (srcId == null || sinkId == null) return null;

            var srcJsonDocument = RetrieveDocumentById(srcId);
            var sinkJsonDocument = srcId.Equals(sinkId) ? srcJsonDocument : RetrieveDocumentById(sinkId);

            string edgeObjectString;
            var results = InsertEdge(srcJsonDocument, sinkJsonDocument, _edgeJsonDocument, srcId, sinkId, out edgeObjectString);

            Upload(results);

            var edgeJObject = JObject.Parse(edgeObjectString);
            var result = new RawRecord { fieldValues = new List<FieldObject>() };

            record.Append(new StringField(srcId)); 
            record.Append(new StringField(sinkId));
            record.Append(new StringField(_otherVTag == 0 ? srcId : sinkId));
            record.Append(new StringField(edgeJObject["_ID"].ToString()));

            for (var i = ReservedMetaFieldCount; i < _edgeProperties.Count; i++)
            {

                var fieldValue = edgeJObject[_edgeProperties[i]];
                result.Append(fieldValue != null ? new StringField(fieldValue.ToString()) : null);
            }

            return result;
        }

        private Dictionary<string, string> InsertEdge(string srcJsonDocument, string sinkJsonDocument, string edgeJsonDocument, 
            string srcId, string sinkId, out string edgeObjectString)
        {
            var documentsMap = new Dictionary<string, string>();
            documentsMap.Add(srcId, srcJsonDocument);
            if (!documentsMap.ContainsKey(sinkId)) documentsMap.Add(sinkId, sinkJsonDocument);

            var edgeOffset = GraphViewJsonCommand.get_edge_num(srcJsonDocument);
            var reverseEdgeOffset = GraphViewJsonCommand.get_reverse_edge_num(sinkJsonDocument);

            edgeJsonDocument = GraphViewJsonCommand.insert_property(edgeJsonDocument, edgeOffset.ToString(), "_ID").ToString();
            edgeJsonDocument = GraphViewJsonCommand.insert_property(edgeJsonDocument, reverseEdgeOffset.ToString(), "_reverse_ID").ToString();
            edgeJsonDocument = GraphViewJsonCommand.insert_property(edgeJsonDocument, '\"' + sinkId + '\"', "_sink").ToString();
            edgeObjectString = edgeJsonDocument;
            documentsMap[srcId] = GraphViewJsonCommand.insert_edge(srcJsonDocument, edgeJsonDocument, edgeOffset).ToString();

            edgeJsonDocument = GraphViewJsonCommand.insert_property(edgeJsonDocument, reverseEdgeOffset.ToString(), "_ID").ToString();
            edgeJsonDocument = GraphViewJsonCommand.insert_property(edgeJsonDocument, edgeOffset.ToString(), "_reverse_ID").ToString();
            edgeJsonDocument = GraphViewJsonCommand.insert_property(edgeJsonDocument, '\"' + srcId + '\"', "_sink").ToString();
            documentsMap[sinkId] = GraphViewJsonCommand.insert_reverse_edge(documentsMap[sinkId], edgeJsonDocument, reverseEdgeOffset).ToString();

            return documentsMap;
        }
    }

    internal class DropEOperator : ModificationBaseOpertaor2
    {
        private int _srcIdIndex;
        private int _edgeOffsetIndex;

        public DropEOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection, int pSrcIdIndex, int pEdgeOffsetIndex)
            : base(pInputOp, pConnection)
        {
            _srcIdIndex = pSrcIdIndex;
            _edgeOffsetIndex = pEdgeOffsetIndex;
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            var srcId = record[_srcIdIndex];
            var edgeOffset = record[_edgeOffsetIndex];

            var srcJsonDocument = RetrieveDocumentById(srcId.ToString());
            var sinkIdAndReverseEdgeOffset = GetSinkIdAndReverseEdgeOffset(srcJsonDocument, edgeOffset.ToString());

            if (sinkIdAndReverseEdgeOffset == null) return null;

            var sinkId = sinkIdAndReverseEdgeOffset[0];
            var revEdgeOffset = sinkIdAndReverseEdgeOffset[1];

            var sinkJsonDocument = srcId.Equals(sinkId) ? srcJsonDocument : RetrieveDocumentById(sinkId);

            var results = DeleteEdge(srcId.ToString(), sinkId, edgeOffset.ToString(), revEdgeOffset, srcJsonDocument, sinkJsonDocument);

            Upload(results);

            return null;
        }

        private Dictionary<string, string> DeleteEdge(string srcId, string sinkId, string edgeOffset, string reverseEdgeOffset,
            string srcJsonDocument, string sinkJsonDocument)
        {
            var documentsMap = new Dictionary<string, string>();
            documentsMap.Add(srcId, srcJsonDocument);
            if (!documentsMap.ContainsKey(sinkId)) documentsMap.Add(sinkId, sinkJsonDocument);

            documentsMap[srcId] = GraphViewJsonCommand.Delete_edge(documentsMap[srcId], int.Parse(edgeOffset));
            documentsMap[sinkId] = GraphViewJsonCommand.Delete_reverse_edge(documentsMap[sinkId], int.Parse(reverseEdgeOffset));

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
        protected List<Tuple<string, string, int>> PropertiesToBeUpdated;

        protected UpdatePropertiesBaseOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection,
            List<Tuple<string, string, int>> pPropertiesToBeUpdated, UpdatePropertyMode pMode = UpdatePropertyMode.Set)
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

                foreach (var tuple in PropertiesToBeUpdated)
                {
                    var propertyIndex = tuple.Item3;
                    var propertyNewValue = tuple.Item2;
                    if (propertyIndex == -1) continue;

                    result.fieldValues[propertyIndex] = new StringField(propertyNewValue);
                }

                return result;
            }

            Close();
            return null;
        }
    }

    internal class UpdateNodePropertiesOperator : UpdatePropertiesBaseOperator
    {
        private int _nodeIdIndex;

        public UpdateNodePropertiesOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection,
            int pNodeIndex, List<Tuple<string, string, int>> pPropertiesList, UpdatePropertyMode pMode = UpdatePropertyMode.Set)
            : base(pInputOp, pConnection, pPropertiesList, pMode)
        {
            _nodeIdIndex = pNodeIndex;
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            var targetId = record[_nodeIdIndex];

            var targetJsonDocument = RetrieveDocumentById(targetId.ToString());
            var results = UpdateNodeProperties(targetId.ToString(), targetJsonDocument, PropertiesToBeUpdated, Mode);

            Upload(results);

            // Drop step, return null
            if (PropertiesToBeUpdated.Any(t => t.Item2 == null)) return null;
            return record;
        }

        private Dictionary<string, string> UpdateNodeProperties(string targetId, string documentString, 
            List<Tuple<string, string, int>> propList, UpdatePropertyMode mode)
        {
            var document = JObject.Parse(documentString);

            foreach (var t in propList)
            {
                var key = t.Item1;
                var value = t.Item2;

                var property = document.Property(key);
                // Delete property
                if (value == null)
                    property?.Remove();
                // Insert property
                else if (property == null)
                    document.Add(key, value);
                // Update property
                else
                {
                    if (mode == UpdatePropertyMode.Set)
                        document[key] = value;
                    else
                        document[key] = document[key].ToString() + ", " + value;
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
            List<Tuple<string, string, int>> propertiesList, UpdatePropertyMode pMode = UpdatePropertyMode.Set)
            : base(pInputOp, pConnection, propertiesList, pMode)
        {
            _srcIdIndex = pSrcIdIndex;
            _edgeOffsetIndex = pEdgeOffsetIndex;
        }

        internal override RawRecord DataModify(RawRecord record)
        {
            var srcId = record[_srcIdIndex];
            var edgeOffset = record[_edgeOffsetIndex];

            var srcJsonDocument = RetrieveDocumentById(srcId.ToString());

            var sinkIdAndReverseEdgeOffset = GetSinkIdAndReverseEdgeOffset(srcJsonDocument, edgeOffset.ToString());
            if (sinkIdAndReverseEdgeOffset == null) return record;

            var sinkId = sinkIdAndReverseEdgeOffset[0];
            var reverseEdgeOffset = sinkIdAndReverseEdgeOffset[1];

            var sinkJsonDocument = srcId.Equals(sinkId) ? srcJsonDocument : RetrieveDocumentById(sinkId);

            var results = UpdateEdgeAndReverseEdgeProperties(srcId.ToString(), edgeOffset.ToString(), sinkId, reverseEdgeOffset,
                srcJsonDocument, sinkJsonDocument, Mode);

            Upload(results);

            // Drop step, return null
            if (PropertiesToBeUpdated.Any(t => t.Item2 == null)) return null;
            return record;
        }

        private Dictionary<string, string> UpdateEdgeAndReverseEdgeProperties(string srcId, string edgeOffset,
            string sinkId, string revEdgeOffset, string srcJsonDocument, string sinkJsonDocument, UpdatePropertyMode mode)
        {
            var documentsMap = new Dictionary<string, string>();
            documentsMap.Add(srcId, srcJsonDocument);
            if (!documentsMap.ContainsKey(sinkId)) documentsMap.Add(sinkId, sinkJsonDocument);

            UpdateEdgeProperties(documentsMap, PropertiesToBeUpdated, srcId, edgeOffset, false, mode);
            UpdateEdgeProperties(documentsMap, PropertiesToBeUpdated, sinkId, revEdgeOffset, true, mode);

            return documentsMap;
        }

        private void UpdateEdgeProperties(Dictionary<string, string> documentsMap, List<Tuple<string, string, int>> propList, 
            string id, string edgeOffset, bool isReverseEdge, UpdatePropertyMode mode)
        {
            var document = JObject.Parse(documentsMap[id]);
            var adj = isReverseEdge ? (JArray)document["_reverse_edge"] : (JArray)document["_edge"];
            var edgeId = int.Parse(edgeOffset);

            foreach (var edge in adj.Children<JObject>())
            {
                if (int.Parse(edge["_ID"].ToString()) != edgeId) continue;

                foreach (var t in propList)
                {
                    var key = t.Item1;
                    var value = t.Item2;

                    var property = edge.Property(key);
                    // Delete property
                    if (value == null)
                        property?.Remove();
                    // Insert property
                    else if (property == null)
                        edge.Add(key, value);
                    // Update property
                    else
                    {
                        if (mode == UpdatePropertyMode.Set)
                            edge[key] = value;
                        else
                            edge[key] = edge[key].ToString() + ", " + value;
                    }
                }
                break;
            }

            documentsMap[id] = document.ToString();
        }
    }
}
