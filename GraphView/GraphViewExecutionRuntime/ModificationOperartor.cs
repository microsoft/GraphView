using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Linq;

namespace GraphView
{
    //internal abstract class ModificationBaseOpertaor : GraphViewExecutionOperator
    //{
    //    public GraphViewConnection dbConnection;
    //    public string source, sink;
    //    internal Dictionary<string, string> map;

    //    internal void Upload()
    //    {
    //        ReplaceDocument().Wait();
    //    }

    //    public async Task ReplaceDocument()
    //    {
    //        foreach (var cnt in map)
    //            await DataModificationUtils.ReplaceDocument(dbConnection, cnt.Key, cnt.Value)
    //                .ConfigureAwait(continueOnCapturedContext: false);
    //    }
    //}

    //internal class InsertEdgeOperator : ModificationBaseOpertaor
    //{
    //    public GraphViewExecutionOperator SelectInput;
    //    public string edge;
    //    public Queue<RawRecord> OutputBuffer;

    //    public InsertEdgeOperator(GraphViewConnection dbConnection, GraphViewExecutionOperator SelectInput, string edge, string source, string sink)
    //    {
    //        this.dbConnection = dbConnection;
    //        this.SelectInput = SelectInput;
    //        this.edge = edge;
    //        this.source = source;
    //        this.sink = sink;
    //        Open();
    //    }

    //    #region Unfinish GroupBy-method
    //    /*public override Record Next()
    //    {
    //        Dictionary<string, List<string>> groupBySource = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.List<string>>();
    //        Dictionary<string, List<string>> groupBySink = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.List<string>>();

    //        SelectInput.Open();
    //        while (SelectInput.Status())
    //        {
    //            Record rec = (Record)SelectInput.Next();
    //            string source = rec.RetriveData(null, 0);
    //            string sink = rec.RetriveData(null, 1);

    //            if (!groupBySource.ContainsKey(source))
    //            {
    //                groupBySource[source] = new System.Collections.Generic.List<string>();
    //            }
    //            groupBySource[source].Add(sink);

    //            if (!groupBySink.ContainsKey(sink))
    //            {
    //                groupBySink[sink] = new System.Collections.Generic.List<string>();
    //            }
    //            groupBySink[sink].Add(source);
    //        }
    //        SelectInput.Close();

    //        foreach (string source in groupBySource.Keys)
    //        {
    //            // Insert edges into the source doc
    //        }

    //        foreach (string sink in groupBySink.Keys)
    //        {
    //            // Insert reverse edges into the sink doc
    //        }

    //        return null;
    //    }*/
    //    #endregion

    //    public override RawRecord Next()
    //    {
    //        if (OutputBuffer == null)
    //            OutputBuffer = new Queue<RawRecord>();
    //        if (OutputBuffer.Count != 0)
    //        {
    //            if (OutputBuffer.Count == 1)
    //                this.Close();
    //            return OutputBuffer.Dequeue();
    //        }

    //        map = new Dictionary<string, string>();

    //        while (SelectInput.State())
    //        {
    //            //get source and sink's id from SelectQueryBlock's TraversalProcessor 
    //            RawRecord rec = SelectInput.Next();
    //            if (rec == null) break;
    //            List<string> header = (SelectInput as OutputOperator).SelectedElement;
    //            string sourceid = rec.RetriveData(header, source).ToString();
    //            string sinkid = rec.RetriveData(header, sink).ToString();
    //            string sourceDoc = source.Substring(0,source.Length-3) + ".doc";
    //            string sinkDoc = sink.Substring(0, source.Length - 3) + ".doc";

    //            string sourceJsonStr = rec.RetriveData(header, sourceDoc).ToString();
    //            string sinkJsonStr = rec.RetriveData(header, sinkDoc).ToString();
                
    //            int edgeId = InsertEdgeInMap(sourceid, sinkid, sourceJsonStr, sinkJsonStr);

    //            var record = new RawRecord(3)
    //            {
    //                fieldValues =
    //                {
    //                    [0] = new StringField(sourceid),
    //                    [1] = new StringField(sinkid),
    //                    [2] = new StringField(edgeId.ToString())
    //                }
    //            };

    //            OutputBuffer.Enqueue(record);
    //        }

    //        Upload();

    //        Close();

    //        if (OutputBuffer.Count <= 1) Close();
    //        if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
    //        return null;
    //    }

    //    internal int InsertEdgeInMap(string sourceid, string sinkid, string source_doc, string sink_doc)
    //    {
    //        if (!map.ContainsKey(sourceid))
    //            map[sourceid] = source_doc;
            
    //        if (!map.ContainsKey(sinkid))
    //            map[sinkid] = sink_doc;
            
    //        return DataModificationUtils.InsertEdge(map, edge, sourceid, sinkid);
    //    }
    //}

    //internal class InsertEdgeFromTwoSourceOperator : ModificationBaseOpertaor
    //{
    //    public GraphViewExecutionOperator SrcSelectInput;
    //    public GraphViewExecutionOperator DestSelectInput;
    //    public string edge;
    //    public Queue<RawRecord> OutputBuffer;

    //    public InsertEdgeFromTwoSourceOperator(GraphViewConnection dbConnection, GraphViewExecutionOperator pSrcSelectInput, GraphViewExecutionOperator pDestSelectInput, string edge, string source, string sink)
    //    {
    //        this.dbConnection = dbConnection;
    //        this.SrcSelectInput = pSrcSelectInput;
    //        this.DestSelectInput = pDestSelectInput;
    //        this.edge = edge;
    //        this.source = source;
    //        this.sink = sink;
    //        Open();
    //    }

    //    public override RawRecord Next()
    //    {
    //        if (OutputBuffer == null)
    //            OutputBuffer = new Queue<RawRecord>();
    //        if (OutputBuffer.Count != 0)
    //        {
    //            if (OutputBuffer.Count == 1)
    //                this.Close();
    //            return OutputBuffer.Dequeue();
    //        }

    //        map = new Dictionary<string, string>();

    //        List<RawRecord> SrcNode = new List<RawRecord>();
    //        List<RawRecord> DestNode = new List<RawRecord>();

    //        while(SrcSelectInput.State()) SrcNode.Add(SrcSelectInput.Next());
    //        while(DestSelectInput.State()) DestNode.Add(DestSelectInput.Next());

    //        foreach (var x in SrcNode)
    //        {
    //            foreach (var y in DestNode)
    //            {
    //                //get source and sink's id from SelectQueryBlock's TraversalProcessor 
    //                if (x == null || y == null) break;
    //                List<string> headerx = (SrcSelectInput as OutputOperator).SelectedElement;
    //                List<string> headery = (DestSelectInput as OutputOperator).SelectedElement;
    //                string sourceid = x.RetriveData(headerx, source).ToString();
    //                string sinkid = y.RetriveData(headery, sink).ToString();
    //                string sourceDoc = source.Substring(0, source.Length - 3) + ".doc";
    //                string sinkDoc = sink.Substring(0, source.Length - 3) + ".doc";

    //                string sourceJsonStr = x.RetriveData(headerx, sourceDoc).ToString();
    //                string sinkJsonStr = y.RetriveData(headery, sinkDoc).ToString();

    //                int edgeId = InsertEdgeInMap(sourceid, sinkid, sourceJsonStr, sinkJsonStr);

    //                var record = new RawRecord(4)
    //                {
    //                    fieldValues =
    //                    {
    //                        [0] = new StringField(sourceid),
    //                        [1] = new StringField(sinkid),
    //                        [2] = new StringField(edgeId.ToString())
    //                    }
    //                };
    //                OutputBuffer.Enqueue(record);
    //            }
    //        }

    //        Upload();

    //        Close();

    //        if (OutputBuffer.Count <= 1) Close();
    //        if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
    //        return null;
    //    }

    //    internal int InsertEdgeInMap(string sourceid, string sinkid, string source_doc, string sink_doc)
    //    {
    //        if (!map.ContainsKey(sourceid))
    //            map[sourceid] = source_doc;

    //        if (!map.ContainsKey(sinkid))
    //            map[sinkid] = sink_doc;

    //        return DataModificationUtils.InsertEdge(map, edge, sourceid, sinkid);
    //    }
    //}

    //internal class DeleteEdgeOperator : ModificationBaseOpertaor
    //{
    //    // The SELECT query that returns the edges to be deleted.
    //    // The query returns three columns: edge's source, edge's sink and edge's offset in the adjacency list
    //    GraphViewExecutionOperator edgeInput;

    //    string edgeSourceId;
    //    string edgeSinkId;

    //    public DeleteEdgeOperator(GraphViewConnection dbConnection, GraphViewExecutionOperator SelectInput,  string source, string sink, string EdgeID_str, string EdgeReverseID_str)
    //    {
    //        this.dbConnection = dbConnection;
    //        this.edgeInput = SelectInput;
    //        this.source = source;
    //        this.sink = sink;
    //        this.edgeSourceId = EdgeID_str;
    //        this.edgeSinkId = EdgeReverseID_str;
    //        Open();
    //    }

    //    public override RawRecord Next()
    //    {
    //        if (!State()) return null;
    //        map = new Dictionary<string, string>();

    //        while (edgeInput.State())
    //        {
    //            //get source and sink's id from SelectQueryBlock's TraversalProcessor 
    //            RawRecord rec = edgeInput.Next();
    //            if (rec == null) break;
    //            List<string> header = (edgeInput as OutputOperator).SelectedElement;
    //            string sourceid = rec.RetriveData(header, source).ToString();
    //            string sinkid = rec.RetriveData(header, sink).ToString();
    //            //The "e" in the Record is "Reverse_e" in fact
    //            string EdgeReverseID = rec.RetriveData(header, edgeSourceId).ToString();
    //            string EdgeID = rec.RetriveData(header, edgeSinkId).ToString();

    //            //get source.doc and sink.doc
    //            string sourceDoc = source.Substring(0, source.Length - 3) + ".doc";
    //            string sinkDoc = sink.Substring(0, source.Length - 3) + ".doc";
    //            string sourceJsonStr = rec.RetriveData(header, sourceDoc).ToString();
    //            string sinkJsonStr = rec.RetriveData(header, sinkDoc).ToString();

    //            int ID, reverse_ID;
    //            int.TryParse(EdgeID, out ID);
    //            int.TryParse(EdgeReverseID, out reverse_ID);

    //            DeleteEdgeInMap(sourceid, sinkid, ID, reverse_ID, sourceJsonStr, sinkJsonStr);
    //        }

    //        Upload();

    //        Close();

    //        return null;
    //    }

    //    internal void DeleteEdgeInMap(string sourceid, string sinkid, int ID, int reverse_ID, string source_json_str, string sink_json_str)
    //    {
    //        //Create one if a document not exist locally.
    //        if (!map.ContainsKey(sourceid))
    //            map[sourceid] = source_json_str;
    //        if (!map.ContainsKey(sinkid))
    //            map[sinkid] = sink_json_str;

    //        map[sourceid] = GraphViewJsonCommand.Delete_edge(map[sourceid], ID);
    //        map[sinkid] = GraphViewJsonCommand.Delete_reverse_edge(map[sinkid], reverse_ID);
    //    }
    //}

    //internal class InsertNodeOperator : ModificationBaseOpertaor
    //{
    //    string Json_str;
    //    Document _createdDocument;

    //    public InsertNodeOperator(GraphViewConnection dbConnection, string Json_str)
    //    {
    //        this.dbConnection = dbConnection;
    //        this.Json_str = Json_str;
    //        Open();
    //    }
    //    public override RawRecord Next()
    //    {
    //        if (!State()) return null;

    //        var obj = JObject.Parse(Json_str);

    //        Upload(obj);

    //        Close();
            
    //        var result = new RawRecord(1);
    //        result.fieldValues[0] = new StringField(_createdDocument.Id);

    //        return result;
    //    }

    //    internal void Upload(JObject obj)
    //    {
    //        CreateDocument(obj).Wait();
    //    }

    //    public async Task CreateDocument(JObject obj)
    //    {
    //        _createdDocument = await dbConnection.DocDBClient.CreateDocumentAsync("dbs/" + dbConnection.DocDBDatabaseId + "/colls/" + dbConnection.DocDBCollectionId, obj)
    //            .ConfigureAwait(continueOnCapturedContext: false);
    //    }
    //}

    //internal class DeleteNodeOperator : ModificationBaseOpertaor
    //{
    //    public string Selectstr;

    //    // The SELECT query that returns the IDs of the vertices to be deleted.
    //    GraphViewExecutionOperator selectVertexOp;

    //    public DeleteNodeOperator(GraphViewConnection dbConnection, string Selectstr)
    //    {
    //        this.dbConnection = dbConnection;
    //        this.Selectstr = Selectstr;
    //        Open();
    //    }

    //    /// <summary>
    //    /// Get isolated nodes that satisifies the search condition and then delete them.
    //    /// </summary>
    //    internal void DeleteNodes()
    //    {
    //        var collectionLink = "dbs/" + dbConnection.DocDBDatabaseId + "/colls/" + dbConnection.DocDBCollectionId;
    //        var toBeDeletedNodes = SendQuery(Selectstr, dbConnection);

    //        foreach (var node in toBeDeletedNodes)
    //        {
    //            var docLink = collectionLink + "/docs/" + node.id;
    //            DeleteDocument(docLink).Wait();
    //        }
    //    }

    //    /// <summary>
    //    /// First check if there are some edges still connect to these nodes.
    //    /// If not, delete them.
    //    /// </summary>
    //    /// <returns></returns>
    //    public override RawRecord Next()
    //    {
    //        if (!State())
    //            return null;
            
    //        DeleteNodes();
            
    //        Close();
    //        return null;
    //    }

    //    public async Task DeleteDocument(string docLink)
    //    {
    //        await dbConnection.DocDBClient.DeleteDocumentAsync(docLink).ConfigureAwait(continueOnCapturedContext: false);
    //    }
    //}
}
