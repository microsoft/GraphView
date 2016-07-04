using System;
using System.Linq;
using System.Data;
using System.Collections.Generic;
// Add DocumentDB references
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Documents.Client;

namespace GraphView
{
    using AdjacentList = Dictionary<string, HashSet<string>>;

    /// <summary>
    /// Two Type of specifiers
    /// NodeQuery is a specification of a DocDB query retrieving individual nodes
    /// LinkQuery is specifiers for traversal of link between two nodes
    /// </summary>
    internal class ItemQuery { }
    internal class NodeQuery : ItemQuery
    {
        public NodeQuery(Dictionary<string, int> GraphDescription, MatchNode node)
        {
            NodeId = GraphDescription[node.NodeAlias];
            NodeAlias = node.NodeAlias;
            NodePredicate = node.DocDBQuery.Replace("'", "\"");
        }
        public NodeQuery() { }
        public int NodeId;
        public string NodeAlias;
        public string NodePredicate;
    }
    internal class LinkQuery : ItemQuery
    {
        public LinkQuery(Dictionary<string, int> GraphDescription, MatchNode pSrc, MatchNode pDest, MatchEdge Edge)
        {
            src = new NodeQuery();
            dest = new NodeQuery();
            src.NodeId = GraphDescription[pSrc.NodeAlias];
            src.NodeAlias = pSrc.NodeAlias;
            src.NodePredicate = pSrc.DocDBQuery.Replace("'", "\"");
            dest.NodeId = GraphDescription[pDest.NodeAlias];
            dest.NodeAlias = pDest.NodeAlias;
            dest.NodePredicate = pDest.DocDBQuery.Replace("'", "\"");
            EdgeAlias = new List<string>();
            foreach (var OutGoingEdge in pSrc.Neighbors)
            {
                if (OutGoingEdge.SinkNode.NodeAlias == dest.NodeAlias)
                    EdgeAlias.Add(OutGoingEdge.EdgeAlias);
            }
            EdgesToNeghbor = Edge.EdgeAlias;
        }
        public NodeQuery src;
        public NodeQuery dest;
        public List<string> EdgeAlias;
        public string EdgesToNeghbor;
    }
    /// <summary>
    /// QuerySpec is a set of specifiers.
    /// </summary>
    internal class QuerySpec
    {
        public QuerySpec()
        {
            lines = new List<ItemQuery>();
        }
        public void add(ItemQuery line)
        {
            lines.Add(line);
        }
        public int index()
        {
            return lines.Count - 1;
        }
        public List<ItemQuery> lines;
    }
    /// <summary>
    /// NodeFetchProcessor is used for the most basic interaction with server
    /// NodeFetchProcessor.Next() sends a query to server and return the result it fetched
    /// </summary>
    internal class NodeFetchProcessor : GraphViewOperator
    {
        string script;
        DocDBConnection connection;
        public NodeFetchProcessor(DocDBConnection pConnection, string pScript)
        {
            connection = pConnection;
            script = pScript;
        }
        override public object Next()
        {
            FeedOptions QueryOptions = new FeedOptions { MaxItemCount = -1 };
            IQueryable<dynamic> Result = connection.client.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(connection.DatabaseID, connection.CollectionID), script, QueryOptions);
            return Result;
        }
    }
    /// <summary>
    /// TraversalProcessor is used to traval a graph pattern and return asked result.
    /// TraversalProcessor.Next() returns one result of what its specifier specified.
    /// By connecting TraversalProcessor together it returns the final result.
    /// </summary>
    internal class TraversalProcessor : GraphViewOperator
    {
        static Record RecordZero;
        private Queue<Record> InputBuffer;
        private Queue<Record> OutputBuffer;
        private int InputBufferSize;
        private int OutputBufferSize;
        private GraphViewOperator ChildProcessor;

        //private string InRangeScript = "";
        private int StartOfResultField;

        private List<string> header;
        private DocDBConnection connection;

        private string src;
        private string dest;

        public TraversalProcessor(string pSrc, string pDest, List<string> pheader, int pStartOfResultField, int pInputBufferSize, int pOutputBufferSize)
        {
            this.Open();
            InputBufferSize = pInputBufferSize;
            OutputBufferSize = pOutputBufferSize;
            InputBuffer = new Queue<Record>();
            InputBuffer = new Queue<Record>();
            src = pSrc;
            dest = pDest;
            header = pheader;
            StartOfResultField = pStartOfResultField;
            if (RecordZero == null) RecordZero = new Record(pheader.Count);
        }
        override public object Next()
        {
            AdjacentList MapForCurrentStage = new AdjacentList();
            if (OutputBuffer == null)
                OutputBuffer = new Queue<Record>();
            if (OutputBuffer.Count != 0 && (OutputBuffer.Count > OutputBufferSize || (ChildProcessor != null && !ChildProcessor.Status())))
            {
                return OutputBuffer.Dequeue();
            }

            if (ChildProcessor == null)
            {
                if (OutputBuffer.Count == 0) InputBuffer.Enqueue(RecordZero);
            }
            else
                while (InputBuffer.Count() < InputBufferSize && ChildProcessor.Status())
                {
                    if (ChildProcessor != null && ChildProcessor.Status())
                    {
                        Record Result = (Record)ChildProcessor.Next();
                        if (Result == null) ChildProcessor.Close();
                        else
                            InputBuffer.Enqueue(Result);
                    }
                }

            foreach (Record record in InputBuffer)
            {
                List<string> ResultIndexToAppend = new List<string>();
                string ResultIndexString = " ,";
                foreach (string ResultIndex in header.GetRange(StartOfResultField, header.Count - StartOfResultField)
                {
                    if (ResultIndex.Substring(0, ResultIndex.IndexOf('.')) == dest ||
                        ResultIndex.Substring(0, ResultIndex.IndexOf('.')) == pLinkQuery.EdgeAlias[0])
                        ResultIndexToAppend.Add(ResultIndex);
                }
                foreach (string ResultIndex in ResultIndexToAppend)
                {
                    ResultIndexString += ResultIndex + " AS " + ResultIndex.Replace(".", "A") + ",";
                }
                if (ResultIndexString == " ,") ResultIndexString = "";
                ResultIndexString = CutTheTail(ResultIndexString);

                string ScriptBase = "SELECT {\"id\":node.id, \"edge\":node._edge, \"reverse\":node._reverse_edge} AS NodeInfo";
                string WhereClause = " " + pNodeQuery.NodePredicate;
                string NodeScript = ScriptBase.Replace("node", pNodeQuery.NodeAlias) + ResultIndexString;
                if (HasWhereClause(pNodeQuery.NodePredicate))
                    NodeScript += " " + WhereClause;
                else NodeScript += " From " + pNodeQuery.NodeAlias;
                IQueryable<dynamic> Node = (IQueryable<dynamic>)new NodeFetchProcessor(connection, NodeScript).Next();
                foreach (var item in Node)
                {
                    Tuple<string, string> ItemInfo = DecodeJObject((JObject)item);
                    Record ResultRecord = new Record();
                    foreach (string ResultIndex in ResultIndexToAppend)
                    {
                        ResultRecord.Fields[ResultRecord.GetIndex(ResultIndex, RecordFromLastTable.ResultsIndex)] =
                            ((JObject)item)[ResultIndex.Replace(".", "A")].ToString();
                    }
                    foreach (var record in RecordFromLastTable.records)
                    {
                        Record NewRecord = AddIfNotExist(ItemInfo, record, pNodeQuery, ResultRecord.Fields);
                        yield return NewRecord;
                    }
                }
                yield break;
            }
            if (OutputBuffer.Count != 0)
            {
                if (OutputBuffer.Count == 1) this.Close();
                return OutputBuffer.Dequeue();
            }
            return null;
        }


        private List<Record> ConvertFromBufferAndEmptyIt(Queue<Record> Buffer)
        {
            List<Record> result = new List<Record>();
            while (Buffer.Count != 0) result.Add(Buffer.Dequeue());
            return result;
        }
        private bool HasWhereClause(string SelectClause)
        {
            return !(SelectClause.Length < 6 || SelectClause.Substring(SelectClause.Length - 6, 5) == "Where");
        }
        /// <summary>
        /// Break down a JObject that return by server and extract the id and edge infomation from it.
        /// </summary>
        private Tuple<string, string> DecodeJObject(JObject Item, bool ShowEdge = false)
        {
            JToken NodeInfo = ((JObject)Item)["NodeInfo"];
            JToken edge = ((JObject)NodeInfo)["edge"];
            JToken id = NodeInfo["id"];
            JToken reverse = NodeInfo["reverse"];
            if (!ShowEdge) return new Tuple<string, string>(id.ToString(), "");
            else return new Tuple<string, string>(id.ToString(), edge["_sink"].ToString());
        }
        private Record AddIfNotExist(Tuple<string, string> ItemInfo, Record record, NodeQuery pQuery, List<string> Result = null)
        {
            List<string> NewBinding = new List<string>(record.Bindings);
            if (record.GetBinding(ItemInfo.Item1, BindingIndex) == -1)
            {
                NewBinding.Add(ItemInfo.Item1);
            }
            List<string> NewResult = new List<string>(record.Fields);
            if (Result != null)
                for (int i = 0; i < NewResult.Count; i++)
                {
                    if (NewResult[i] == "") NewResult[i] = Result[i];
                }
            Record NewRecord = new Record(NewBinding, NewResult);
            return NewRecord;
        }
        string CutTheTail(string InRangeScript)
        {
            if (InRangeScript.Length == 0) return "";
            return InRangeScript.Substring(0, InRangeScript.Length - 1);
        }
        /// <summary>
        /// Dealing with NodeQuery specifier, sending query to determine a set of nodes and bind them to a specific group
        /// </summary>
        private IEnumerable<Record> NodeQueryProcessor(TableBuffer RecordFromLastTable, NodeQuery pNodeQuery)
        {
            List<string> ResultIndexToAppend = new List<string>();
            string ResultIndexString = " ,";
            foreach (string ResultIndex in RecordFromLastTable.ResultsIndex)
            {
                if (ResultIndex.Substring(0, ResultIndex.IndexOf('.')) == pNodeQuery.NodeAlias)
                    ResultIndexToAppend.Add(ResultIndex);
            }
            foreach (string ResultIndex in ResultIndexToAppend)
            {
                ResultIndexString += ResultIndex + " AS " + ResultIndex.Replace(".", "A") + ",";
            }
            if (ResultIndexString == " ,") ResultIndexString = "";
            ResultIndexString = CutTheTail(ResultIndexString);

            string ScriptBase = "SELECT {\"id\":node.id, \"edge\":node._edge, \"reverse\":node._reverse_edge} AS NodeInfo";
            string WhereClause = " " + pNodeQuery.NodePredicate;
            string NodeScript = ScriptBase.Replace("node", pNodeQuery.NodeAlias) + ResultIndexString;
            if (HasWhereClause(pNodeQuery.NodePredicate))
                NodeScript += " " + WhereClause;
            else NodeScript += " From " + pNodeQuery.NodeAlias;
            IQueryable<dynamic> Node = (IQueryable<dynamic>)new NodeFetchProcessor(connection, NodeScript).Next();
            foreach (var item in Node)
            {
                Tuple<string, string> ItemInfo = DecodeJObject((JObject)item);
                Record ResultRecord = new Record();
                foreach (string ResultIndex in ResultIndexToAppend)
                {
                    ResultRecord.Fields[ResultRecord.GetIndex(ResultIndex, RecordFromLastTable.ResultsIndex)] =
                        ((JObject)item)[ResultIndex.Replace(".", "A")].ToString();
                }
                foreach (var record in RecordFromLastTable.records)
                {
                    Record NewRecord = AddIfNotExist(ItemInfo, record, pNodeQuery, ResultRecord.Fields);
                    yield return NewRecord;
                }
            }
            yield break;
        }
        /// <summary>
        /// Dealing with the source of a link
        /// sending query to determine a set of source nodes and bind them to a specific group
        /// Also giving a set of possible sink nodes for later use
        /// </summary>
        private IEnumerable<Record> QueryForSrcNodes(TableBuffer RecordFromLastTable, LinkQuery pLinkQuery, AdjacentList Map)
        {
            List<string> ResultIndexToAppend = new List<string>();
            string ResultIndexString = " ,";
            foreach (string ResultIndex in RecordFromLastTable.ResultsIndex)
            {
                if (ResultIndex.Substring(0, ResultIndex.IndexOf('.')) == pLinkQuery.src.NodeAlias ||
                    ResultIndex.Substring(0, ResultIndex.IndexOf('.')) == pLinkQuery.EdgeAlias[0])
                    ResultIndexToAppend.Add(ResultIndex);
            }
            foreach (string ResultIndex in ResultIndexToAppend)
            {
                ResultIndexString += ResultIndex + " AS " + ResultIndex.Replace(".", "A") + ",";
            }
            if (ResultIndexString == " ,") ResultIndexString = "";
            ResultIndexString = CutTheTail(ResultIndexString);

            string SrcScript = "";
            string EdgeAlias = "";
            foreach (var edge in pLinkQuery.EdgeAlias) EdgeAlias += edge.ToString();
            if (EdgeAlias.Length == 0) EdgeAlias = "node._edge";
            string ScriptBase = "SELECT {\"id\":node.id, \"edge\":" + EdgeAlias + ", \"reverse\":node._reverse_edge} AS NodeInfo ";
            string SrcWhereScript = " " + pLinkQuery.src.NodePredicate;
            SrcScript = ScriptBase.Replace("node", pLinkQuery.src.NodeAlias);
            if (HasWhereClause(pLinkQuery.src.NodePredicate))
            {
                SrcScript += ResultIndexString + " " + pLinkQuery.src.NodePredicate;
            }
            else SrcScript += ResultIndexString + " " + pLinkQuery.src.NodePredicate.Substring(0, pLinkQuery.src.NodePredicate.Length - 6);
            var src = (IQueryable<dynamic>)new NodeFetchProcessor(connection, SrcScript).Next();
            var LinkSet = new HashSet<string>();
            foreach (var item in src)
            {
                Tuple<string, string> ItemInfo = DecodeJObject((JObject)item, true);
                InRangeScript += "\"" + ItemInfo.Item2 + "\"" + ",";
                if (!LinkSet.Contains(ItemInfo.Item1))
                {

                    LinkSet.Add(ItemInfo.Item1);
                    Record ResultRecord = new Record();
                    foreach (string ResultIndex in ResultIndexToAppend)
                    {
                        var res = (((JObject)item)[ResultIndex.Replace(".", "A")]);
                        if (res != null)
                            ResultRecord.Fields[ResultRecord.GetIndex(ResultIndex, RecordFromLastTable.ResultsIndex)] =
                                res.ToString();
                    }
                    foreach (var record in RecordFromLastTable.records)
                    {
                        Record NewPath = AddIfNotExist(ItemInfo, record, pLinkQuery.src, ResultRecord.Fields);
                        yield return NewPath;
                    }
                }
                if (!Map.ContainsKey(ItemInfo.Item2))
                    Map.Add(ItemInfo.Item2, new HashSet<string>());
                Map[ItemInfo.Item2].Add(ItemInfo.Item1);
            }
            yield break;
        }
        /// <summary>
        /// Determine which nodes satisfied the predicates of sink nodes and also in the possible sink node set
        /// that generated by QueryForSrcNodes function.
        /// Bind them to sink node group
        /// </summary>
        private IEnumerable<Record> QueryForDestNodes(TableBuffer RecordFromLastTable, LinkQuery pLinkQuery, AdjacentList Map)
        {
            List<string> ResultIndexToAppend = new List<string>();
            string ResultIndexString = " ,";
            foreach (string ResultIndex in RecordFromLastTable.ResultsIndex)
            {
                if (ResultIndex.Substring(0, ResultIndex.IndexOf('.')) == pLinkQuery.dest.NodeAlias)
                    ResultIndexToAppend.Add(ResultIndex);
            }
            foreach (string ResultIndex in ResultIndexToAppend)
            {
                ResultIndexString += ResultIndex + " AS " + ResultIndex.Replace(".", "A") + ",";
            }
            if (ResultIndexString == " ,") ResultIndexString = "";
            ResultIndexString = CutTheTail(ResultIndexString);

            if (InRangeScript.Length == 0) yield break;
            InRangeScript = CutTheTail(InRangeScript);
            string ScriptBase = "SELECT {\"id\":node.id, \"edge\":node._edge, \"reverse\":node._reverse_edge} AS NodeInfo";
            string DestWhereScript = " " + pLinkQuery.dest.NodePredicate;
            if (HasWhereClause(DestWhereScript))
                DestWhereScript += " AND " + pLinkQuery.dest.NodeAlias + ".id IN (" + InRangeScript + ")";
            else DestWhereScript += pLinkQuery.dest.NodeAlias + ".id IN(" + InRangeScript + ")";
            string DestScript = ScriptBase + ResultIndexString + DestWhereScript;
            DestScript = DestScript.Replace("node", pLinkQuery.dest.NodeAlias);
            var dest = (IQueryable<dynamic>)new NodeFetchProcessor(connection, DestScript).Next();
            foreach (var item in dest)
            {
                Tuple<string, string> ItemInfo = DecodeJObject((JObject)item);
                Record ResultRecord = new Record();
                foreach (string ResultIndex in ResultIndexToAppend)
                {
                    var res = (((JObject)item)[ResultIndex.Replace(".", "A")]);
                    if (res != null)
                        ResultRecord.Fields[ResultRecord.GetIndex(ResultIndex, RecordFromLastTable.ResultsIndex)] =
                            res.ToString();
                }
                foreach (var record in RecordFromLastTable.records)
                {
                    if (record.GetId(pLinkQuery.src.NodeId, BindingIndex) != "")
                    {
                        foreach (var link in Map[ItemInfo.Item1])
                        {
                            if (link == record.GetId(pLinkQuery.src.NodeId, BindingIndex))
                            {
                                if (record.GetBinding(ItemInfo.Item1, BindingIndex) == pLinkQuery.dest.NodeId)
                                {
                                    yield return record;
                                }
                                else if (record.GetId(pLinkQuery.dest.NodeId, BindingIndex) == "")
                                {
                                    yield return AddIfNotExist(ItemInfo, record, pLinkQuery.dest, ResultRecord.Fields);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    /// <summary>
    /// SelectProcessor is used to handle a select query.
    /// It extracts needed information from WSelectQueryBlock and generates a set of specifiers.
    /// SelectProcessor.Next() feeds the specifiers it generated to a set of TraversalProcessor and return one result.
    /// </summary>
    public class WSelectQueryGenerate
    {

        /// <summary>
        /// Construct a spec that specific the step of querying.
        /// A spec is consist of two type of specifiers.
        /// One is NodeQuery, which is used to describe a query about a node with predicates.
        /// Another is LinkQuery, which is used to describe a link between two nodes and these nodes.
        /// </summary>
        private void ConstructQuerySpec()
        {
            HashSet<string> AddedNodes = new HashSet<string>();
            foreach (var node in NodeTable)
            {
                if (node.Value.Neighbors.Count() != 0)
                    foreach (var neighbor in node.Value.Neighbors)
                    {
                        spec.add(new LinkQuery(GraphDescription, node.Value, neighbor.SinkNode, neighbor));
                        AddedNodes.Add(node.Value.NodeAlias);
                        AddedNodes.Add(neighbor.SinkNode.NodeAlias);
                    }
            }
            foreach (var node in NodeTable)
            {
                if (node.Value.Neighbors.Count() == 0 && !AddedNodes.Contains(node.Value.NodeAlias))
                    spec.add(new NodeQuery(GraphDescription, node.Value));
            }
        }
        /// <summary>
        /// Consturct a Header for the table, which used to translate the infomation in it's records.
        /// </summary>


    }
}
