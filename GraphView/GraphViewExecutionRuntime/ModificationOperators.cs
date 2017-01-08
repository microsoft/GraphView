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
    internal abstract class ModificationBaseOpertaor2 : TableValuedFunction
    {
        protected GraphViewConnection Connection;

        protected ModificationBaseOpertaor2(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection)
            : base(pInputOp)
        {
            Connection = pConnection;
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

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var obj = JObject.Parse(_jsonDocument);

            Upload(obj);

            var document = JObject.Parse(_createdDocument.ToString());
            var result = new RawRecord { fieldValues = new List<string>() };

            foreach (var fieldName in _projectedFieldList)
            {
                var fieldValue = document[fieldName];
                result.Append(fieldValue?.ToString() ?? "");
            }

            return new List<RawRecord> { result };
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
        // The scalar subquery function select the vertex ID of deleted node
        private ScalarFunction _nodeFunction;

        public DropVOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection, ScalarFunction pNodeFunction)
            : base(pInputOp, pConnection)
        {
            _nodeFunction = pNodeFunction;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var targetId = _nodeFunction.Evaluate(record);

            if (targetId == null) return new List<RawRecord>();

            DeleteNode(targetId);

            return new List<RawRecord>();
        }

        /// <summary>
        /// Check whether the target node is isolated first. If true, then delete it.
        /// </summary>
        private void DeleteNode(string targetId)
        {
            var collectionLink = "dbs/" + Connection.DocDB_DatabaseId + "/colls/" + Connection.DocDB_CollectionId;
            var checkIsolatedScript =
                string.Format(
                    "SELECT * FROM Node WHERE Node.id = {0} AND ARRAY_LENGTH(Node._edge) = 0 AND ARRAY_LENGTH(Node._reverse_edge) = 0",
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

    internal abstract class EdgeModificationBaseOperator : ModificationBaseOpertaor2
    {
        // The scalar subquery function select the vertex ID of source and sink of the edge to be added or deleted
        protected ScalarFunction SrcFunction;
        protected ScalarFunction SinkFunction;

        public EdgeModificationBaseOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection,
            ScalarFunction pSrcFunction, ScalarFunction pSinkFunction)
            : base(pInputOp, pConnection)
        {
            SrcFunction = pSrcFunction;
            SinkFunction = pSinkFunction;
        }
    }

    internal class AddEOperator : EdgeModificationBaseOperator
    {
        // The initial json object string of to-be-inserted edge, waiting to update the edgeOffset field
        private string _edgeJsonDocument;
        private List<string> _edgeProperties;

        public AddEOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection, 
            ScalarFunction pSrcFunction, ScalarFunction pSinkFunction, 
            string pEdgeJsonDocument, List<string> pProjectedFieldList)
            : base(pInputOp, pConnection, pSrcFunction, pSinkFunction)
        {
            _edgeJsonDocument = pEdgeJsonDocument;
            _edgeProperties = pProjectedFieldList;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var srcId = SrcFunction.Evaluate(record);
            var sinkId = SinkFunction.Evaluate(record);

            if (srcId == null || sinkId == null) return new List<RawRecord>();

            var srcJsonDocument = RetrieveDocumentById(srcId);
            var sinkJsonDocument = srcId.Equals(sinkId) ? srcJsonDocument : RetrieveDocumentById(sinkId);

            string edgeObjectString;
            var results = InsertEdge(srcJsonDocument, sinkJsonDocument, _edgeJsonDocument, srcId, sinkId, out edgeObjectString);

            Upload(results);

            var edgeJObject = JObject.Parse(edgeObjectString);
            var result = new RawRecord { fieldValues = new List<string>() };

            foreach (var fieldName in _edgeProperties)
            {
                var fieldValue = edgeJObject[fieldName];
                result.Append(fieldValue?.ToString() ?? "");
            }

            return new List<RawRecord> { result };
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

    internal class DropEOperator : EdgeModificationBaseOperator
    {
        // The scalar subquery function select the to-be-deleted edge's offset
        private ScalarFunction _edgeOffsetFunction;

        public DropEOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection,
            ScalarFunction pSrcFunction, ScalarFunction pSinkFunction, ScalarFunction pEdgeOffsetFunction)
            : base(pInputOp, pConnection, pSrcFunction, pSinkFunction)
        {
            _edgeOffsetFunction = pEdgeOffsetFunction;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var srcId = SrcFunction.Evaluate(record);
            var sinkId = SinkFunction.Evaluate(record);
            var edgeOffset = _edgeOffsetFunction.Evaluate(record);

            if (srcId == null || sinkId == null || edgeOffset == null) return new List<RawRecord>();

            var srcJsonDocument = RetrieveDocumentById(srcId);
            var sinkJsonDocument = srcId.Equals(sinkId) ? srcJsonDocument : RetrieveDocumentById(sinkId);

            var revEdgeOffset = GetReverseEdgeOffset(srcJsonDocument, edgeOffset);
            if (revEdgeOffset == null) return new List<RawRecord>();

            var results = DeleteEdge(srcId, sinkId, edgeOffset, revEdgeOffset, srcJsonDocument, sinkJsonDocument);

            Upload(results);

            return new List<RawRecord>();
        }

        private string GetReverseEdgeOffset(string jsonString, string edgeOffset)
        {
            var document = JObject.Parse(jsonString);
            var adjList = (JArray)document["_edge"];

            foreach (var edge in adjList.Children<JObject>())
            {
                if (edge["_ID"].ToString().Equals(edgeOffset))
                    return edge["_reverse_ID"].ToString();
            }

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
        protected List<Tuple<string, string>> propertiesToBeUpdated;

        protected UpdatePropertiesBaseOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection,
            List<Tuple<string, string>> pPropertiesToBeUpdated, UpdatePropertyMode pMode = UpdatePropertyMode.Set)
            : base(pInputOp, pConnection)
        {
            propertiesToBeUpdated = pPropertiesToBeUpdated;
            Mode = pMode;
        }
    }

    internal class UpdateNodePropertiesOperator : UpdatePropertiesBaseOperator
    {
        // The scalar subquery function select the vertex ID of target node
        private ScalarFunction _nodeFunction;

        public UpdateNodePropertiesOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection,
            ScalarFunction pNodeFunction, List<Tuple<string, string>> pPropertiesList, UpdatePropertyMode pMode = UpdatePropertyMode.Set)
            : base(pInputOp, pConnection, pPropertiesList, pMode)
        {
            _nodeFunction = pNodeFunction;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var targetId = _nodeFunction.Evaluate(record);

            if (targetId == null) return new List<RawRecord>();

            var targetJsonDocument = RetrieveDocumentById(targetId);
            var results = UpdateNodeProperties(targetId, targetJsonDocument, propertiesToBeUpdated, Mode);

            Upload(results);

            // TODO: Update property value in the original record
            return new List<RawRecord> {record};
        }

        private Dictionary<string, string> UpdateNodeProperties(string targetId, string documentString, List<Tuple<string, string>> propList, UpdatePropertyMode mode)
        {
            var document = JObject.FromObject(documentString);

            foreach (var t in propList)
            {
                var key = t.Item1;
                var value = t.Item2;

                var property = document.Property(key);
                // Delete property
                if (value == null && property != null)
                    property.Remove();
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
        private ScalarFunction _srcFunction;
        private ScalarFunction _edgeOffsetFunction;

        public UpdateEdgePropertiesOperator(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection, 
            ScalarFunction pSrcFunction, ScalarFunction pEdgeOffsetFunction, 
            List<Tuple<string, string>> propertiesList, UpdatePropertyMode pMode = UpdatePropertyMode.Set)
            : base(pInputOp, pConnection, propertiesList, pMode)
        {
            _srcFunction = pSrcFunction;
            _edgeOffsetFunction = pEdgeOffsetFunction;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var srcId = _srcFunction.Evaluate(record);
            var edgeOffset = _edgeOffsetFunction.Evaluate(record);

            if (srcId == null || edgeOffset == null) return new List<RawRecord>();

            var srcJsonDocument = RetrieveDocumentById(srcId);

            var sinkIdAndReverseEdgeOffset = GetSinkIdAndReverseEdgeOffset(srcJsonDocument, edgeOffset);
            if (sinkIdAndReverseEdgeOffset == null) return new List<RawRecord>();

            var sinkId = sinkIdAndReverseEdgeOffset[0];
            var reverseEdgeOffset = sinkIdAndReverseEdgeOffset[1];

            var sinkJsonDocument = srcId.Equals(sinkId) ? srcJsonDocument : RetrieveDocumentById(sinkId);

            var results = UpdateEdgeAndReverseEdgeProperties(srcId, edgeOffset, sinkId, reverseEdgeOffset,
                srcJsonDocument, sinkJsonDocument, Mode);

            Upload(results);

            // TODO: Update property value in the original record
            return new List<RawRecord> {record};
        }

        private List<string> GetSinkIdAndReverseEdgeOffset(string jsonString, string edgeOffset)
        {
            var document = JObject.Parse(jsonString);
            var adjList = (JArray)document["_edge"];

            foreach (var edge in adjList.Children<JObject>())
            {
                if (edge["_ID"].ToString().Equals(edgeOffset))
                    return new List<string> {edge["_sink"].ToString(), edge["_reverse_ID"].ToString()};
            }

            return null;
        }

        private Dictionary<string, string> UpdateEdgeAndReverseEdgeProperties(string srcId, string edgeOffset,
            string sinkId, string revEdgeOffset, string srcJsonDocument, string sinkJsonDocument, UpdatePropertyMode mode)
        {
            var documentsMap = new Dictionary<string, string>();
            documentsMap.Add(srcId, srcJsonDocument);
            if (!documentsMap.ContainsKey(sinkId)) documentsMap.Add(sinkId, sinkJsonDocument);

            UpdateEdgeProperties(documentsMap, propertiesToBeUpdated, srcId, edgeOffset, false, mode);
            UpdateEdgeProperties(documentsMap, propertiesToBeUpdated, sinkId, revEdgeOffset, true, mode);

            return documentsMap;
        }

        private void UpdateEdgeProperties(Dictionary<string, string> documentsMap, List<Tuple<string, string>> propList, string id, string edgeOffset, 
            bool isReverseEdge, UpdatePropertyMode mode)
        {
            var document = JObject.FromObject(documentsMap[id]);
            var adj = isReverseEdge ? (JArray)document["_reverse_edge"] : (JArray)document["_edge"];
            var offsetFieldName = isReverseEdge ? "_reverse_ID" : "_ID";
            var edgeId = int.Parse(edgeOffset);

            foreach (var edge in adj.Children<JObject>())
            {
                if (int.Parse(edge[offsetFieldName].ToString()) != edgeId) continue;

                foreach (var t in propList)
                {
                    var key = t.Item1;
                    var value = t.Item2;

                    var property = edge.Property(key);
                    // Delete property
                    if (value == null && property != null)
                        property.Remove();
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
