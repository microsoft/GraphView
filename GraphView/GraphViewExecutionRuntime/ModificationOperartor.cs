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
    internal class DataModificationUtils
    {
        internal enum UpdatePropertyMode
        {
            Set,
            Append
        };

        internal static async Task ReplaceDocument(GraphViewConnection dbConnection, string documentId, string documentString)
        {
            var newDocument = JObject.Parse(documentString);
            await
                dbConnection.DocDBclient.ReplaceDocumentAsync(
                    UriFactory.CreateDocumentUri(dbConnection.DocDB_DatabaseId, dbConnection.DocDB_CollectionId,
                        documentId), newDocument);
        }

        internal static int InsertEdge(Dictionary<string, string> map, string Edge, string sourceid, string sinkid)
        {
            string source_str = map[sourceid];
            string sink_str = map[sinkid];
            var source_edge_num = GraphViewJsonCommand.get_edge_num(source_str);
            var sink_reverse_edge_num = GraphViewJsonCommand.get_reverse_edge_num(sink_str);

            Edge = GraphViewJsonCommand.insert_property(Edge, source_edge_num.ToString(), "_ID").ToString();
            Edge = GraphViewJsonCommand.insert_property(Edge, sink_reverse_edge_num.ToString(), "_reverse_ID").ToString();
            Edge = GraphViewJsonCommand.insert_property(Edge, '\"' + sinkid + '\"', "_sink").ToString();
            map[sourceid] = GraphViewJsonCommand.insert_edge(source_str, Edge, source_edge_num).ToString();
            //var new_source = JObject.Parse(source_str);

            Edge = GraphViewJsonCommand.insert_property(Edge, sink_reverse_edge_num.ToString(), "_ID").ToString();
            Edge = GraphViewJsonCommand.insert_property(Edge, source_edge_num.ToString(), "_reverse_ID").ToString();
            Edge = GraphViewJsonCommand.insert_property(Edge, '\"' + sourceid + '\"', "_sink").ToString();
            map[sinkid] = GraphViewJsonCommand.insert_reverse_edge(map[sinkid], Edge, sink_reverse_edge_num).ToString();

            return source_edge_num;
            //var new_sink = JObject.Parse(sink_str);

            //await conn.client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(conn.DocDB_DatabaseId, conn.DocDB_CollectionId, sourceid), new_source);
            //await conn.client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(conn.DocDB_DatabaseId, conn.DocDB_CollectionId, sinkid), new_sink);
        }

        internal static void UpdateNodeProperties(Dictionary<string, string> map, List<Tuple<string, string>> propList, string id, UpdatePropertyMode mode = UpdatePropertyMode.Set)
        {
            var document = JObject.FromObject(map[id]);

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

            map[id] = document.ToString();
        }

        internal static void UpdateEdgeProperties(Dictionary<string, string> map, List<Tuple<string, string>> propList, string id, string edgeIdStr, ref Dictionary<string, List<string>> revEdgeSyncDict, UpdatePropertyMode mode = UpdatePropertyMode.Set)
        {
            var document = JObject.FromObject(map[id]);
            var adj = (JArray)document["_edge"];
            var edgeId = int.Parse(edgeIdStr);

            foreach (var edge in adj.Children<JObject>())
            {
                if (int.Parse(edge["_ID"].ToString()) != edgeId) continue;

                // Store reverse edge's document id and edge id
                var sink = edge["_sink"].ToString();
                List<string> revEdgeList;
                if (!revEdgeSyncDict.TryGetValue(sink, out revEdgeList))
                    revEdgeSyncDict[sink] = new List<string>();
                revEdgeSyncDict[sink].Add(edge["_reverse_ID"].ToString());

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
                            edge[key] = document[key].ToString() + ", " + value;
                    }
                    
                }
                break;
            }

            map[id] = document.ToString();
        }

        internal static void UpdateRevEdgeProperties(Dictionary<string, string> map, List<Tuple<string, string>> propList, string id, string edgeIdStr, UpdatePropertyMode mode = UpdatePropertyMode.Set)
        {
            var document = JObject.FromObject(map[id]);
            var adj = (JArray)document["_reverse_edge"];
            var edgeId = int.Parse(edgeIdStr);

            foreach (var edge in adj.Children<JObject>())
            {
                if (int.Parse(edge["_reverse_ID"].ToString()) != edgeId) continue;

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
                            edge[key] = document[key].ToString() + ", " + value;
                    }
                }
                break;
            }

            map[id] = document.ToString();
        }
    }

    internal abstract class ModificationBaseOpertaor : GraphViewExecutionOperator
    {
        public GraphViewConnection dbConnection;
        public string source, sink;
        internal Dictionary<string, string> map;

        internal void Upload()
        {
            ReplaceDocument().Wait();
        }

        public async Task ReplaceDocument()
        {
            foreach (var cnt in map)
                await DataModificationUtils.ReplaceDocument(dbConnection, cnt.Key, cnt.Value)
                    .ConfigureAwait(continueOnCapturedContext: false);
        }
    }

    internal class InsertEdgeOperator : ModificationBaseOpertaor
    {
        public GraphViewExecutionOperator SelectInput;
        public string edge;
        public Queue<RawRecord> OutputBuffer;

        public InsertEdgeOperator(GraphViewConnection dbConnection, GraphViewExecutionOperator SelectInput, string edge, string source, string sink)
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
            if (OutputBuffer == null)
                OutputBuffer = new Queue<RawRecord>();
            if (OutputBuffer.Count != 0)
            {
                if (OutputBuffer.Count == 1)
                    this.Close();
                return OutputBuffer.Dequeue();
            }

            map = new Dictionary<string, string>();

            while (SelectInput.State())
            {
                //get source and sink's id from SelectQueryBlock's TraversalProcessor 
                RawRecord rec = SelectInput.Next();
                if (rec == null) break;
                List<string> header = (SelectInput as OutputOperator).SelectedElement;
                string sourceid = rec.RetriveData(header, source);
                string sinkid = rec.RetriveData(header, sink);
                string sourceDoc = source.Substring(0,source.Length-3) + ".doc";
                string sinkDoc = sink.Substring(0, source.Length - 3) + ".doc";

                string sourceJsonStr = rec.RetriveData(header, sourceDoc);
                string sinkJsonStr = rec.RetriveData(header, sinkDoc);
                
                int edgeId = InsertEdgeInMap(sourceid, sinkid, sourceJsonStr, sinkJsonStr);

                var record = new RawRecord(3)
                {
                    fieldValues =
                    {
                        [0] = sourceid,
                        [1] = sinkid,
                        [2] = edgeId.ToString()
                    }
                };

                OutputBuffer.Enqueue(record);
            }

            Upload();

            Close();

            if (OutputBuffer.Count <= 1) Close();
            if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
            return null;
        }

        internal int InsertEdgeInMap(string sourceid, string sinkid, string source_doc, string sink_doc)
        {
            if (!map.ContainsKey(sourceid))
                map[sourceid] = source_doc;
            
            if (!map.ContainsKey(sinkid))
                map[sinkid] = sink_doc;
            
            return DataModificationUtils.InsertEdge(map, edge, sourceid, sinkid);
        }
    }

    internal class InsertEdgeFromTwoSourceOperator : ModificationBaseOpertaor
    {
        public GraphViewExecutionOperator SrcSelectInput;
        public GraphViewExecutionOperator DestSelectInput;
        public string edge;
        public Queue<RawRecord> OutputBuffer;

        public InsertEdgeFromTwoSourceOperator(GraphViewConnection dbConnection, GraphViewExecutionOperator pSrcSelectInput, GraphViewExecutionOperator pDestSelectInput, string edge, string source, string sink)
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
            if (OutputBuffer == null)
                OutputBuffer = new Queue<RawRecord>();
            if (OutputBuffer.Count != 0)
            {
                if (OutputBuffer.Count == 1)
                    this.Close();
                return OutputBuffer.Dequeue();
            }

            map = new Dictionary<string, string>();

            List<RawRecord> SrcNode = new List<RawRecord>();
            List<RawRecord> DestNode = new List<RawRecord>();

            while(SrcSelectInput.State()) SrcNode.Add(SrcSelectInput.Next());
            while(DestSelectInput.State()) DestNode.Add(DestSelectInput.Next());

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
                    string sourceDoc = source.Substring(0, source.Length - 3) + ".doc";
                    string sinkDoc = sink.Substring(0, source.Length - 3) + ".doc";

                    string sourceJsonStr = x.RetriveData(headerx, sourceDoc);
                    string sinkJsonStr = y.RetriveData(headery, sinkDoc);

                    int edgeId = InsertEdgeInMap(sourceid, sinkid, sourceJsonStr, sinkJsonStr);

                    var record = new RawRecord(4)
                    {
                        fieldValues =
                        {
                            [0] = sourceid,
                            [1] = sinkid,
                            [2] = edgeId.ToString()
                        }
                    };
                    OutputBuffer.Enqueue(record);
                }
            }

            Upload();

            Close();

            if (OutputBuffer.Count <= 1) Close();
            if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
            return null;
        }

        internal int InsertEdgeInMap(string sourceid, string sinkid, string source_doc, string sink_doc)
        {
            if (!map.ContainsKey(sourceid))
                map[sourceid] = source_doc;

            if (!map.ContainsKey(sinkid))
                map[sinkid] = sink_doc;

            return DataModificationUtils.InsertEdge(map, edge, sourceid, sinkid);
        }
    }

    internal class InsertEdgeOperator2 : ModificationBaseOpertaor
    {
        private GraphViewExecutionOperator srcOp;
        private GraphViewExecutionOperator sinkOp;
        private string edgeBaseString;
        private Queue<RawRecord> outputBuffer;

        // The SELECT query selecting a collection of edges to be inserted. 
        // The first two columns of the returned results specify the source vertex ID and 
        // and the sink vertex ID of the edge to be inserted. The remaining columns specify
        // edge properties, whose names are given by edgeProperties.
        GraphViewExecutionOperator selectOp;
        List<string> edgeProperties;

        private string RetrieveDocumentById(string id)
        {
            var script = string.Format("SELECT * FROM Node WHERE Node.id = '{0}'", id);
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
            List<dynamic> result = dbConnection.DocDBclient.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(dbConnection.DocDB_DatabaseId, dbConnection.DocDB_CollectionId),
                script, queryOptions).ToList();

            if (result.Count == 0)
                return null;
            return ((JObject)result[0]).ToString();
        }

        public InsertEdgeOperator2(GraphViewConnection dbConnection, GraphViewExecutionOperator pSrcOp, GraphViewExecutionOperator pSinkOp, string edgeBaseString)
        {
            this.dbConnection = dbConnection;
            this.srcOp = pSrcOp;
            this.sinkOp = pSinkOp;
            this.edgeBaseString = edgeBaseString;
            Open();
        }

        public override RawRecord Next()
        {
            if (outputBuffer == null)
                outputBuffer = new Queue<RawRecord>();
            if (outputBuffer.Count != 0)
            {
                if (outputBuffer.Count == 1)
                    this.Close();
                return outputBuffer.Dequeue();
            }

            map = new Dictionary<string, string>();

            RawRecord srcRecord = null;
            RawRecord sinkRecord = null;

            List<RawRecord> srcRecords = new List<RawRecord>();
            List<RawRecord> sinkRecords = new List<RawRecord>();

            while (srcOp.State() && (srcRecord = srcOp.Next()) != null)
                srcRecords.Add(srcRecord);
            while (sinkOp.State() && (sinkRecord = sinkOp.Next()) != null)
                sinkRecords.Add(sinkRecord);

            foreach (var x in srcRecords)
            {
                foreach (var y in sinkRecords)
                {
                    string sourceid = x[0];
                    string sinkid = y[0];
                    string sourceJsonStr = !map.ContainsKey(sourceid) ? RetrieveDocumentById(sourceid) : map[sourceid];
                    string sinkJsonStr = !map.ContainsKey(sinkid) ? RetrieveDocumentById(sinkid) : map[sinkid];

                    int edgeId = InsertEdgeInMap(sourceid, sinkid, sourceJsonStr, sinkJsonStr);

                    var record = new RawRecord(3)
                    {
                        fieldValues =
                        {
                            [0] = sourceid,
                            [1] = sinkid,
                            [2] = edgeId.ToString()
                        }
                    };
                    outputBuffer.Enqueue(record);
                }
            }

            Upload();

            Close();

            if (outputBuffer.Count <= 1) Close();
            if (outputBuffer.Count != 0) return outputBuffer.Dequeue();
            return null;
        }

        internal int InsertEdgeInMap(string sourceid, string sinkid, string source_doc, string sink_doc)
        {
            if (!map.ContainsKey(sourceid))
                map[sourceid] = source_doc;

            if (!map.ContainsKey(sinkid))
                map[sinkid] = sink_doc;

            return DataModificationUtils.InsertEdge(map, edgeBaseString, sourceid, sinkid);
        }

        public override void ResetState()
        {
            srcOp.ResetState();
            sinkOp.ResetState();
            outputBuffer.Clear();
            map.Clear();
            this.Open();
        }
    }

    internal class DeleteEdgeOperator : ModificationBaseOpertaor
    {
        // The SELECT query that returns the edges to be deleted.
        // The query returns three columns: edge's source, edge's sink and edge's offset in the adjacency list
        GraphViewExecutionOperator edgeInput;

        string edgeSourceId;
        string edgeSinkId;

        public DeleteEdgeOperator(GraphViewConnection dbConnection, GraphViewExecutionOperator SelectInput,  string source, string sink, string EdgeID_str, string EdgeReverseID_str)
        {
            this.dbConnection = dbConnection;
            this.edgeInput = SelectInput;
            this.source = source;
            this.sink = sink;
            this.edgeSourceId = EdgeID_str;
            this.edgeSinkId = EdgeReverseID_str;
            Open();
        }

        public override RawRecord Next()
        {
            if (!State()) return null;
            map = new Dictionary<string, string>();

            while (edgeInput.State())
            {
                //get source and sink's id from SelectQueryBlock's TraversalProcessor 
                RawRecord rec = edgeInput.Next();
                if (rec == null) break;
                List<string> header = (edgeInput as OutputOperator).SelectedElement;
                string sourceid = rec.RetriveData(header, source);
                string sinkid = rec.RetriveData(header, sink);
                //The "e" in the Record is "Reverse_e" in fact
                string EdgeReverseID = rec.RetriveData(header, edgeSourceId);
                string EdgeID = rec.RetriveData(header, edgeSinkId);

                //get source.doc and sink.doc
                string sourceDoc = source.Substring(0, source.Length - 3) + ".doc";
                string sinkDoc = sink.Substring(0, source.Length - 3) + ".doc";
                string sourceJsonStr = rec.RetriveData(header, sourceDoc);
                string sinkJsonStr = rec.RetriveData(header, sinkDoc);

                int ID, reverse_ID;
                int.TryParse(EdgeID, out ID);
                int.TryParse(EdgeReverseID, out reverse_ID);

                DeleteEdgeInMap(sourceid, sinkid, ID, reverse_ID, sourceJsonStr, sinkJsonStr);
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
    }

    internal class InsertNodeOperator : ModificationBaseOpertaor
    {
        string Json_str;
        Document _createdDocument;

        public InsertNodeOperator(GraphViewConnection dbConnection, string Json_str)
        {
            this.dbConnection = dbConnection;
            this.Json_str = Json_str;
            Open();
        }
        public override RawRecord Next()
        {
            if (!State()) return null;

            var obj = JObject.Parse(Json_str);

            Upload(obj);

            Close();
            
            var result = new RawRecord(2);
            result.fieldValues[0] = _createdDocument.Id;
            result.fieldValues[1] = _createdDocument.ToString();
            return result;
        }

        internal void Upload(JObject obj)
        {
            CreateDocument(obj).Wait();
        }

        public async Task CreateDocument(JObject obj)
        {
            _createdDocument = await dbConnection.DocDBclient.CreateDocumentAsync("dbs/" + dbConnection.DocDB_DatabaseId + "/colls/" + dbConnection.DocDB_CollectionId, obj)
                .ConfigureAwait(continueOnCapturedContext: false);
        }
    }

    internal class DeleteNodeOperator : ModificationBaseOpertaor
    {
        public string Selectstr;

        // The SELECT query that returns the IDs of the vertices to be deleted.
        GraphViewExecutionOperator selectVertexOp;

        public DeleteNodeOperator(GraphViewConnection dbConnection, string Selectstr)
        {
            this.dbConnection = dbConnection;
            this.Selectstr = Selectstr;
            Open();
        }

        /// <summary>
        /// Get isolated nodes that satisifies the search condition and then delete them.
        /// </summary>
        internal void DeleteNodes()
        {
            var collectionLink = "dbs/" + dbConnection.DocDB_DatabaseId + "/colls/" + dbConnection.DocDB_CollectionId;
            var toBeDeletedNodes = SendQuery(Selectstr, dbConnection);

            foreach (var node in toBeDeletedNodes)
            {
                var docLink = collectionLink + "/docs/" + node.id;
                DeleteDocument(docLink).Wait();
            }
        }

        /// <summary>
        /// First check if there are some edges still connect to these nodes.
        /// If not, delete them.
        /// </summary>
        /// <returns></returns>
        public override RawRecord Next()
        {
            if (!State())
                return null;
            
            DeleteNodes();
            
            Close();
            return null;
        }

        public async Task DeleteDocument(string docLink)
        {
            await dbConnection.DocDBclient.DeleteDocumentAsync(docLink).ConfigureAwait(continueOnCapturedContext: false);
        }
    }

    internal class UpdateNodePropertiesOperator : ModificationBaseOpertaor
    {
        GraphViewExecutionOperator vertexInput;
        List<Tuple<string, string>> propertiesToBeUpdated;
        Queue<RawRecord> OutputBuffer;

        public UpdateNodePropertiesOperator(GraphViewConnection dbConnection, GraphViewExecutionOperator selectInput, string source, List<Tuple<string, string>> propertiesList)
        {
            this.dbConnection = dbConnection;
            this.vertexInput = selectInput;
            this.propertiesToBeUpdated = propertiesList;
            this.source = source;
            Open();
        }

        public override RawRecord Next()
        {
            if (OutputBuffer == null)
                OutputBuffer = new Queue<RawRecord>();
            if (OutputBuffer.Count != 0)
            {
                if (OutputBuffer.Count == 1)
                    this.Close();
                return OutputBuffer.Dequeue();
            }

            map = new Dictionary<string, string>();

            while (vertexInput.State())
            {
                //get target node id and document
                RawRecord rec = vertexInput.Next();
                if (rec == null) break;
                List<string> header = (vertexInput as OutputOperator).SelectedElement;

                string sourceid = rec.RetriveData(header, source);
                string sourceDoc = source.Substring(0, source.Length - 3) + ".doc";
                string sourceJsonStr = rec.RetriveData(header, sourceDoc);

                UpdatePropertiesofNode(sourceid, sourceJsonStr);

                rec.fieldValues[header.IndexOf(sourceJsonStr)] = map[sourceid];
                OutputBuffer.Enqueue(rec);
            }

            Upload();

            if (OutputBuffer.Count <= 1) Close();
            if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
            return null;
        }

        internal void UpdatePropertiesofNode(string id, string document)
        {
            if (!map.ContainsKey(id))
                map[id] = document;
            DataModificationUtils.UpdateNodeProperties(map, propertiesToBeUpdated, id);
        }
    }

    internal class UpdateEdgePropertiesOperator : ModificationBaseOpertaor
    {
        GraphViewExecutionOperator edgeInput;
        List<Tuple<string, string>> propertiesTobeUpdated;
        string EdgeIdStr;
        string RevEdgeIdStr;
        Queue<RawRecord> OutputBuffer; 

        public UpdateEdgePropertiesOperator(GraphViewConnection dbConnection, GraphViewExecutionOperator selectInput, string source, string edgeIdStr, string revEdgeIdStr, List<Tuple<string, string>> propertiesList)
        {
            this.dbConnection = dbConnection;
            this.edgeInput = selectInput;
            this.propertiesTobeUpdated = propertiesList;
            this.source = source;
            this.EdgeIdStr = edgeIdStr;
            this.RevEdgeIdStr = revEdgeIdStr;
            Open();
        }

        public override RawRecord Next()
        {
            if (OutputBuffer == null)
                OutputBuffer = new Queue<RawRecord>();
            if (OutputBuffer.Count != 0)
            {
                if (OutputBuffer.Count == 1)
                    this.Close();
                return OutputBuffer.Dequeue();
            }

            map = new Dictionary<string, string>();
            var revEdgeSyncDict = new Dictionary<string, List<string>>();

            while (edgeInput.State())
            {
                //get target node id and document
                RawRecord rec = edgeInput.Next();
                if (rec == null) break;
                List<string> header = (edgeInput as OutputOperator).SelectedElement;

                string sourceid = rec.RetriveData(header, source);
                string sourceDoc = source.Substring(0, source.Length - 3) + ".doc";
                string sourceJsonStr = rec.RetriveData(header, sourceDoc);
                string edgeIdStr = rec.RetriveData(header, EdgeIdStr);
                string revEdgeIdStr = rec.RetriveData(header, RevEdgeIdStr);

                UpdatePropertiesofEdge(sourceid, sourceJsonStr, edgeIdStr, revEdgeIdStr, ref revEdgeSyncDict);

                rec.fieldValues[header.IndexOf(sourceJsonStr)] = map[sourceid];
                OutputBuffer.Enqueue(rec);
            }

            UpdatePropertiesofRevEdges(revEdgeSyncDict);

            Upload();

            if (OutputBuffer.Count <= 1) Close();
            if (OutputBuffer.Count != 0) return OutputBuffer.Dequeue();
            return null;
        }

        internal void UpdatePropertiesofEdge(string id, string document, string edgeIdStr, string revEdgeIdStr, ref Dictionary<string, List<string>> revEdgeSyncDict)
        {
            if (!map.ContainsKey(id))
                map[id] = document;
            DataModificationUtils.UpdateEdgeProperties(map, propertiesTobeUpdated, id, edgeIdStr, ref revEdgeSyncDict);
        }

        internal void UpdatePropertiesofRevEdges(Dictionary<string, List<string>> revEdgeSyncDict)
        {
            string script = "SELECT * FROM Node WHERE Node.id = {0}";
            foreach (var pair in revEdgeSyncDict)
            {
                var id = pair.Key;
                // TODO: Retrieve from server
                if (!map.ContainsKey(id))
                    map[id] =
                        ((JObject) GraphViewExecutionOperator.SendQuery(string.Format(script, id), dbConnection).First())
                            .ToString();
                foreach (var edgeIdStr in pair.Value)
                    DataModificationUtils.UpdateRevEdgeProperties(map, propertiesTobeUpdated, id, edgeIdStr);
            }
        }
    }
}
