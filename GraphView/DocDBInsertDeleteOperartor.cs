using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphView
{

    internal class InsertEdgeOperator : GraphViewOperator
    {
        public GraphViewOperator SelectInput;
        public string edge;
        public string source, sink;
        public GraphViewConnection dbConnection;
        private bool UploadFinish;
        internal Dictionary<string, string> map;
        private int thread_num;


        public InsertEdgeOperator(GraphViewConnection dbConnection, GraphViewOperator SelectInput, string edge, string source, string sink)
        {
            this.dbConnection = dbConnection;
            this.SelectInput = SelectInput;
            this.edge = edge;
            this.source = source;
            this.sink = sink;
            Open();
        }

        #region Unfinish GroupBy-method
        /*public override Record Next()
        {
            Dictionary<string, List<string>> groupBySource = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.List<string>>();
            Dictionary<string, List<string>> groupBySink = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.List<string>>();

            SelectInput.Open();
            while (SelectInput.Status())
            {
                Record rec = (Record)SelectInput.Next();
                string source = rec.RetriveData(null, 0);
                string sink = rec.RetriveData(null, 1);

                if (!groupBySource.ContainsKey(source))
                {
                    groupBySource[source] = new System.Collections.Generic.List<string>();
                }
                groupBySource[source].Add(sink);

                if (!groupBySink.ContainsKey(sink))
                {
                    groupBySink[sink] = new System.Collections.Generic.List<string>();
                }
                groupBySink[sink].Add(source);
            }
            SelectInput.Close();

            foreach (string source in groupBySource.Keys)
            {
                // Insert edges into the source doc
            }

            foreach (string sink in groupBySink.Keys)
            {
                // Insert reverse edges into the sink doc
            }

            return null;
        }*/
        #endregion

        public override RawRecord Next()
        {
            if (!Status()) return null;
            map = new Dictionary<string, string>();

            while (SelectInput.Status())
            {
                //get source and sink's id from SelectQueryBlock's TraversalProcessor 
                RawRecord rec = SelectInput.Next();
                if (rec == null) break;
                List<string> header = (SelectInput as OutputOperator).SelectedElement;
                string sourceid = rec.RetriveData(header, source);
                string sinkid = rec.RetriveData(header, sink);
                string source_tital = source.Substring(0,source.Length-3) + ".doc";
                string sink_tital = sink.Substring(0, source.Length - 3) + ".doc";

                string source_json_str = rec.RetriveData(header, source_tital);
                string sink_json_str = rec.RetriveData(header, sink_tital);
                
                InsertEdgeInMap(sourceid, sinkid,source_json_str, sink_json_str);
            }

            Upload();

            Close();
            return null;
        }

        internal void InsertEdgeInMap(string sourceid, string sinkid, string source_doc, string sink_doc)
        {
            if (!map.ContainsKey(sourceid))
                map[sourceid] = source_doc;
            
            if (!map.ContainsKey(sinkid))
                map[sinkid] = sink_doc;
            
            GraphViewDocDBCommand.INSERT_EDGE(map, edge, sourceid, sinkid);
        }

        internal void Upload()
        {
            UploadFinish = false;
            ReplaceDocument();

            //Wait until finish replacing.
            while (!UploadFinish)
                System.Threading.Thread.Sleep(10);
        }

        public async Task ReplaceDocument()
        {
            foreach (var cnt in map)
                await GraphViewDocDBCommand.ReplaceDocument(dbConnection, cnt.Key, cnt.Value);
            UploadFinish = true;
        }
    }

    internal class InsertEdgeFromTwoSourceOperator : GraphViewOperator
    {
        public GraphViewOperator SrcSelectInput;
        public GraphViewOperator DestSelectInput;

        public string edge;
        public string source, sink;
        public GraphViewConnection dbConnection;
        private bool UploadFinish;
        internal Dictionary<string, string> map;
        private int thread_num;


        public InsertEdgeFromTwoSourceOperator(GraphViewConnection dbConnection, GraphViewOperator pSrcSelectInput, GraphViewOperator pDestSelectInput, string edge, string source, string sink)
        {
            this.dbConnection = dbConnection;
            this.SrcSelectInput = pSrcSelectInput;
            this.DestSelectInput = pDestSelectInput;
            this.edge = edge;
            this.source = source;
            this.sink = sink;
            Open();
        }

        public override RawRecord Next()
        {
            if (!Status()) return null;
            map = new Dictionary<string, string>();

            List<RawRecord> SrcNode = new List<RawRecord>();
            List<RawRecord> DestNode = new List<RawRecord>();

            while(SrcSelectInput.Status()) SrcNode.Add(SrcSelectInput.Next());
            while(DestSelectInput.Status()) DestNode.Add(DestSelectInput.Next());

            foreach (var x in SrcNode)
            {
                foreach (var y in DestNode)
                {
                    //get source and sink's id from SelectQueryBlock's TraversalProcessor 
                    if (x == null || y == null) break;
                    List<string> headerx = (SrcSelectInput as OutputOperator).SelectedElement;
                    List<string> headery = (DestSelectInput as OutputOperator).SelectedElement;
                    string sourceid = x.RetriveData(headerx, source);
                    string sinkid = y.RetriveData(headery, sink);
                    string source_tital = source.Substring(0, source.Length - 3) + ".doc";
                    string sink_tital = sink.Substring(0, source.Length - 3) + ".doc";

                    string source_json_str = x.RetriveData(headerx, source_tital);
                    string sink_json_str = y.RetriveData(headery, sink_tital);

                    InsertEdgeInMap(sourceid, sinkid, source_json_str, sink_json_str);
                }
            }

            Upload();

            Close();
            return null;
        }

        internal void InsertEdgeInMap(string sourceid, string sinkid, string source_doc, string sink_doc)
        {
            if (!map.ContainsKey(sourceid))
                map[sourceid] = source_doc;

            if (!map.ContainsKey(sinkid))
                map[sinkid] = sink_doc;

            GraphViewDocDBCommand.INSERT_EDGE(map, edge, sourceid, sinkid);
        }

        internal void Upload()
        {
            UploadFinish = false;
            ReplaceDocument();

            //Wait until finish replacing.
            while (!UploadFinish)
                System.Threading.Thread.Sleep(10);
        }

        public async Task ReplaceDocument()
        {
            foreach (var cnt in map)
                await GraphViewDocDBCommand.ReplaceDocument(dbConnection, cnt.Key, cnt.Value);
            UploadFinish = true;
        }
    }

    internal class DeleteEdgeOperator : GraphViewOperator
    {
        public GraphViewOperator SelectInput;
        public string source, sink;
        public GraphViewConnection dbConnection;
        private bool UploadFinish;
        public string EdgeID_str;
        public string EdgeReverseID_str;
        internal Dictionary<string, string> map;

        public DeleteEdgeOperator(GraphViewConnection dbConnection, GraphViewOperator SelectInput,  string source, string sink, string EdgeID_str, string EdgeReverseID_str)
        {
            this.dbConnection = dbConnection;
            this.SelectInput = SelectInput;
            this.source = source;
            this.sink = sink;
            this.EdgeID_str = EdgeID_str;
            this.EdgeReverseID_str = EdgeReverseID_str;
            Open();
        }

        public override RawRecord Next()
        {
            if (!Status()) return null;
            map = new Dictionary<string, string>();

            while (SelectInput.Status())
            {
                //get source and sink's id from SelectQueryBlock's TraversalProcessor 
                RawRecord rec = SelectInput.Next();
                if (rec == null) break;
                List<string> header = (SelectInput as OutputOperator).SelectedElement;
                string sourceid = rec.RetriveData(header, source);
                string sinkid = rec.RetriveData(header, sink);
                //The "e" in the Record is "Reverse_e" in fact
                string EdgeReverseID = rec.RetriveData(header, EdgeID_str);
                string EdgeID = rec.RetriveData(header, EdgeReverseID_str);

                //get source.doc and sink.doc
                string source_tital = source.Substring(0, source.Length - 3) + ".doc";
                string sink_tital = sink.Substring(0, source.Length - 3) + ".doc";
                string source_json_str = rec.RetriveData(header, source_tital);
                string sink_json_str = rec.RetriveData(header, sink_tital);

                int ID, reverse_ID;
                int.TryParse(EdgeID, out ID);
                int.TryParse(EdgeReverseID, out reverse_ID);



                DeleteEdgeInMap(sourceid, sinkid, ID, reverse_ID, source_json_str, sink_json_str);
            }

            Upload();

            Close();

            return null;
        }

        internal void DeleteEdgeInMap(string sourceid, string sinkid, int ID, int reverse_ID, string source_json_str, string sink_json_str)
        {

            //Create one if a document not exist locally.
            if (!map.ContainsKey(sourceid))
                map[sourceid] = source_json_str;
            if (!map.ContainsKey(sinkid))
                map[sinkid] = sink_json_str;

            map[sourceid] = GraphViewJsonCommand.Delete_edge(map[sourceid], ID);
            map[sinkid] = GraphViewJsonCommand.Delete_reverse_edge(map[sinkid], reverse_ID);
        }

        internal void Upload()
        {
            UploadFinish = false;
            ReplaceDocument();
            //wait until finish replacing.
            while (!UploadFinish)
                System.Threading.Thread.Sleep(10);
        }

        public async Task ReplaceDocument()
        {
            foreach (var cnt in map)
                await GraphViewDocDBCommand.ReplaceDocument(dbConnection, cnt.Key, cnt.Value);
            UploadFinish = true;
        }
    }

    internal class InsertNodeOperator : GraphViewOperator
    {
        public string Json_str;
        public GraphViewConnection dbConnection;
        public bool UploadFinish;

        public InsertNodeOperator(GraphViewConnection dbConnection, string Json_str)
        {
            this.dbConnection = dbConnection;
            this.Json_str = Json_str;
            Open();
        }
        public override RawRecord Next()
        {
            if (!Status()) return null;

            var obj = JObject.Parse(Json_str);

            Upload(obj);

            Close();
            return null;
        }

        void Upload(JObject obj)
        {
            UploadFinish = false;
            CreateDocument(obj);

            //Wait until finish Creating documents.
            while (!UploadFinish)
                System.Threading.Thread.Sleep(10);
        }

        public async Task CreateDocument(JObject obj)
        {
            await dbConnection.DocDBclient.CreateDocumentAsync("dbs/" + dbConnection.DocDB_DatabaseId + "/colls/" + dbConnection.DocDB_CollectionId, obj);
            UploadFinish = true;
        }
    }
    internal class DeleteNodeOperator : GraphViewOperator
    {
        public WBooleanExpression search;
        public string Selectstr;
        public GraphViewConnection dbConnection;
        public bool UploadFinish;

        public DeleteNodeOperator(GraphViewConnection dbConnection,WBooleanExpression search, string Selectstr)
        {
            this.dbConnection = dbConnection;
            this.search = search;
            this.Selectstr = Selectstr;
            Open();
        }

        /// <summary>
        /// Check if there are some edges still connect to these nodes.
        /// </summary>
        /// <returns></returns>
        internal bool CheckNodes()
        {
            var sum_DeleteNode = dbConnection.DocDBclient.CreateDocumentQuery(
                                "dbs/" + dbConnection.DocDB_DatabaseId + "/colls/" + dbConnection.DocDB_CollectionId,
                                Selectstr);
            foreach (var DeleteNode in sum_DeleteNode)
                return false;
            return true;
        }

        /// <summary>
        /// Get those nodes.
        /// And then delete it.
        /// </summary>
        internal void DeleteNodes()
        {
            Selectstr = "SELECT * " + "FROM Node ";
            if (search != null)
                Selectstr += @"WHERE " + search.ToString();
            var sum_DeleteNode = dbConnection.DocDBclient.CreateDocumentQuery(
                "dbs/" + dbConnection.DocDB_DatabaseId + "/colls/" + dbConnection.DocDB_CollectionId,
                Selectstr);

            foreach (var DeleteNode in sum_DeleteNode)
            {
                UploadFinish = false;
                var docLink = string.Format("dbs/{0}/colls/{1}/docs/{2}", dbConnection.DocDB_DatabaseId,
                    dbConnection.DocDB_CollectionId, DeleteNode.id);
                DeleteDocument(docLink);
                //wait until finish deleting
                while (!UploadFinish)
                    System.Threading.Thread.Sleep(10);
            }
        }

        /// <summary>
        /// First check if there are some edges still connect to these nodes.
        /// If not, delete them.
        /// </summary>
        /// <returns></returns>
        public override RawRecord Next()
        {
            if (!Status())
                return null;
            
            if (CheckNodes())
            {
                DeleteNodes();
            }
            else
            {
                Close();
                throw new GraphViewException("There are some edges still connect to these nodes.");
            }
            Close();
            return null;
        }

        public async Task DeleteDocument(string docLink)
        {
            await dbConnection.DocDBclient.DeleteDocumentAsync(docLink);
            UploadFinish = true;
        }
    }

}
