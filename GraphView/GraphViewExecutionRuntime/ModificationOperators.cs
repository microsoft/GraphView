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
    }

    internal class InsertNodeOperator2 : ModificationBaseOpertaor2
    {
        private string _jsonDocument;
        private Document _createdDocument;
        private List<string> _projectedFieldList; 

        public InsertNodeOperator2(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection, string pJsonDocument, List<string> pProjectedFieldList)
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

    internal class InsertEdgeOperator2 : ModificationBaseOpertaor2
    {
        // The scalar subquery function select the vertex ID of source and sink
        private ScalarFunction _srcFunction;
        private ScalarFunction _sinkFunction;
        // The initial json object string of inserted edge, waited to update the edgeOffset field
        private string _edgeJsonDocument;
        private List<string> _edgeProperties;

        public InsertEdgeOperator2(GraphViewExecutionOperator pInputOp, GraphViewConnection pConnection, 
            ScalarFunction pSrcFunction, ScalarFunction pSinkFunction, 
            string pEdgeJsonDocument, List<string> pProjectedFieldList)
            : base(pInputOp, pConnection)
        {
            _srcFunction = pSrcFunction;
            _sinkFunction = pSinkFunction;
            _edgeJsonDocument = pEdgeJsonDocument;
            _edgeProperties = pProjectedFieldList;
        }

        internal override IEnumerable<RawRecord> CrossApply(RawRecord record)
        {
            var srcId = _srcFunction.Evaluate(record);
            var sinkId = _sinkFunction.Evaluate(record);

            if (srcId == null || sinkId == null) return new List<RawRecord>();

            var sourceJsonStr = RetrieveDocumentById(srcId);
            var sinkJsonStr = RetrieveDocumentById(sinkId);

            string edgeObjectString;
            var results = InsertEdge(sourceJsonStr, sinkJsonStr, _edgeJsonDocument, srcId, sinkId, out edgeObjectString);

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

        private void Upload(Dictionary<string, string> documentsMap)
        {
            ReplaceDocument(documentsMap).Wait();
        }

        private async Task ReplaceDocument(Dictionary<string, string> documentsMap)
        {
            foreach (var pair in documentsMap)
                await DataModificationUtils.ReplaceDocument(Connection, pair.Key, pair.Value)
                    .ConfigureAwait(continueOnCapturedContext: false);
        }

        private string RetrieveDocumentById(string id)
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
    }
}
