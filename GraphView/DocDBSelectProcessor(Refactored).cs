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
    /// NodeQuery is specifiers for traversal of isolated node
    /// LinkQuery is specifiers for traversal of link between two nodes
    /// </summary>
    internal class ItemQuery { }
    internal class NodeQuery : ItemQuery
    {
        public NodeQuery(Dictionary<string, int> GraphDescription, MatchNode node)
        {
            NodeNum = GraphDescription[node.NodeAlias];
            NodeAlias = node.NodeAlias;
            NodePredicate = node.DocDBQuery.Replace("'", "\"");
        }
        public NodeQuery() { }
        public int NodeNum;
        public string NodeAlias;
        public string NodePredicate;
    }
    internal class LinkQuery : ItemQuery
    {
        public LinkQuery(Dictionary<string, int> GraphDescription, MatchNode pSrc, MatchNode pDest, MatchEdge Edge)
        {
            src = new NodeQuery();
            dest = new NodeQuery();
            src.NodeNum = GraphDescription[pSrc.NodeAlias];
            src.NodeAlias = pSrc.NodeAlias;
            src.NodePredicate = pSrc.DocDBQuery.Replace("'", "\"");
            dest.NodeNum = GraphDescription[pDest.NodeAlias];
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
    internal class NodeFetchProcessor : DocDBOperatorProcessor
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
    internal class TraversalProcessor : DocDBOperatorProcessor
    {
        static Record RecordZero;
        private ItemQuery SpecForCurrent;
        private string InRangeScript = "";
        private List<int> BindingIndex;
        private List<string> ResultsIndex;
        private DocDBConnection connection;
        public TraversalProcessor(DocDBConnection pConnection, ItemQuery pSpec, DocDBOperatorProcessor pChildProcessor, List<int> pBindingIndex, List<string> pResultsIndex, int pInputBufferSize, int pOutputBufferSize)
        {
            this.Open();
            BindingIndex = pBindingIndex;
            ResultsIndex = pResultsIndex;
            SpecForCurrent = pSpec;
            InputBufferSize = pInputBufferSize;
            OutputBufferSize = pOutputBufferSize;
            ChildrenProcessor.Add(pChildProcessor);
            InputBuffer = new Queue<Record>();
            InputBuffer = new Queue<Record>();
            connection = pConnection;
            if (RecordZero == null) RecordZero = new Record(new List<string>(), new List<string>());
        }

        public TraversalProcessor(DocDBConnection pConnection, QuerySpec pSpecs, int index, List<int> pBindingIndex, List<string> pResultsIndex, int pInputBufferSize, int pOutputBufferSize)
        {
            this.Open();
            BindingIndex = pBindingIndex;
            ResultsIndex = pResultsIndex;
            ChildrenProcessor = new List<DocDBOperatorProcessor>();
            SpecForCurrent = pSpecs.lines[index];
            if (index > 0)
            {
                ChildrenProcessor.Add(new TraversalProcessor(pConnection, pSpecs, index - 1, pBindingIndex, pResultsIndex, pInputBufferSize, pOutputBufferSize));
            }
            InputBufferSize = pInputBufferSize;
            OutputBufferSize = pOutputBufferSize;
            InputBuffer = new Queue<Record>();
            InputBuffer = new Queue<Record>();
            connection = pConnection;
            if (RecordZero == null) RecordZero = new Record();
        }

        override public object Next()
        {
            AdjacentList MapForCurrentStage = new AdjacentList();
            Table StartTableFromLastStage = new Table(BindingIndex, ResultsIndex);
            Table TempRecordForCurrentStage = new Table(BindingIndex, ResultsIndex);
            if (OutputBuffer == null)
                OutputBuffer = new Queue<Record>();
            if (OutputBuffer.Count != 0 && (OutputBuffer.Count > OutputBufferSize || (ChildrenProcessor.Count > 0 && !ChildrenProcessor[0].Statue())))
            {
                return OutputBuffer.Dequeue();
            }

            if (ChildrenProcessor.Count == 0)
            {
                if (OutputBuffer.Count == 0) InputBuffer.Enqueue(RecordZero);
            }
            else
                while (InputBuffer.Count() < InputBufferSize && ChildrenProcessor[0].Statue())
                {
                    if (ChildrenProcessor.Count != 0 && ChildrenProcessor[0].Statue())
                    {
                        Record Result = (Record)ChildrenProcessor[0].Next();
                        if (Result == null) ChildrenProcessor[0].Close();
                        else
                            InputBuffer.Enqueue(Result);
                    }
                }

            StartTableFromLastStage.records = ConvertFromBufferAndEmptyIt(InputBuffer);

            if (SpecForCurrent is NodeQuery)
            {
                NodeQuery Query = SpecForCurrent as NodeQuery;
                foreach (var res in NodeQueryProcessor(StartTableFromLastStage, Query))
                    OutputBuffer.Enqueue(res);
            }
            else if (SpecForCurrent is LinkQuery)
            {
                LinkQuery Query = SpecForCurrent as LinkQuery;
                foreach (var res in QueryForSrcNodes(StartTableFromLastStage, Query, MapForCurrentStage))
                {
                    TempRecordForCurrentStage.records.Add(res);
                }

                foreach (var res in QueryForDestNodes(TempRecordForCurrentStage, Query, MapForCurrentStage))
                {
                    OutputBuffer.Enqueue(res);
                }
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
            List<string> NewResult = new List<string>(record.Results);
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
        private IEnumerable<Record> NodeQueryProcessor(Table RecordFromLastTable, NodeQuery pNodeQuery)
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
                    ResultRecord.Results[ResultRecord.GetIndex(ResultIndex, RecordFromLastTable.ResultsIndex)] =
                        ((JObject)item)[ResultIndex.Replace(".", "A")].ToString();
                }
                foreach (var record in RecordFromLastTable.records)
                {
                    Record NewRecord = AddIfNotExist(ItemInfo, record, pNodeQuery, ResultRecord.Results);
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
        private IEnumerable<Record> QueryForSrcNodes(Table RecordFromLastTable, LinkQuery pLinkQuery, AdjacentList Map)
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
                            ResultRecord.Results[ResultRecord.GetIndex(ResultIndex, RecordFromLastTable.ResultsIndex)] =
                                res.ToString();
                    }
                    foreach (var record in RecordFromLastTable.records)
                    {
                        Record NewPath = AddIfNotExist(ItemInfo, record, pLinkQuery.src, ResultRecord.Results);
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
        private IEnumerable<Record> QueryForDestNodes(Table RecordFromLastTable, LinkQuery pLinkQuery, AdjacentList Map)
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
                        ResultRecord.Results[ResultRecord.GetIndex(ResultIndex, RecordFromLastTable.ResultsIndex)] =
                            res.ToString();
                }
                foreach (var record in RecordFromLastTable.records)
                {
                    if (record.GetId(pLinkQuery.src.NodeNum, BindingIndex) != "")
                    {
                        foreach (var link in Map[ItemInfo.Item1])
                        {
                            if (link == record.GetId(pLinkQuery.src.NodeNum, BindingIndex))
                            {
                                if (record.GetBinding(ItemInfo.Item1, BindingIndex) == pLinkQuery.dest.NodeNum)
                                {
                                    yield return record;
                                }
                                else if (record.GetId(pLinkQuery.dest.NodeNum, BindingIndex) == "")
                                {
                                    yield return AddIfNotExist(ItemInfo, record, pLinkQuery.dest, ResultRecord.Results);
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
    internal class SelectProcessor : DocDBOperatorProcessor
    {

        private DocDBConnection connection;
        private WSelectQueryBlock SelectBlock;
        private MatchGraph SelectGraph;
        private Dictionary<string, int> GraphDescription;
        private Dictionary<string, MatchNode> NodeTable;
        private QuerySpec spec;
        private string SelectResult;
        private List<int> BindingHeader;
        private List<string> ResultHeader;
        public SelectProcessor(WSelectQueryBlock pSelectBlock, DocDBConnection pConnection)
        {
            SelectBlock = pSelectBlock;
            GraphDescription = new Dictionary<string, int>();
            spec = new QuerySpec();
            connection = pConnection;
            ConstructGraph(SelectBlock);
            ConstructQuerySpec();
            ConstructHeaderForTable();
            ResetRecordResultNumber();
            ConstructChildernProcessors();
        }

        override public object Next()
        {
            return ChildrenProcessor[0].Next();
        }

        private void ResetRecordResultNumber()
        {
            Record Instance = new Record();
            Instance.ReSetResultNumber(ResultHeader.Count);
        }
        private void ConstructChildernProcessors()
        {
            if (ChildrenProcessor == null) ChildrenProcessor = new List<DocDBOperatorProcessor>();
            ChildrenProcessor.Add(new TraversalProcessor(connection, spec, spec.index(), BindingHeader, ResultHeader, 10, 10));
        }
        /// <summary>
        /// Consturct a graph that describes the pattern of the select clause with all predicates attached on it's nodes.
        /// (The predicates of edges are attached to its source)
        /// </summary
        private void ConstructGraph(WSelectQueryBlock SelectQueryBlock)
        {
            SelectGraph = GraphViewDocDBCommand.DocDB_ConstructGraph(SelectQueryBlock);
            NodeTable = SelectGraph.ConnectedSubGraphs[0].Nodes;
            AttachWhereClauseVisitor AttachPredicateVistor = new AttachWhereClauseVisitor();
            WSqlTableContext Context = new WSqlTableContext();
            GraphMetaData GraphMeta = new GraphMetaData();
            Dictionary<string, string> ColumnTableMapping = Context.GetColumnToAliasMapping(GraphMeta.ColumnsOfNodeTables);
            if (SelectQueryBlock != null) AttachPredicateVistor.Invoke(SelectQueryBlock.WhereClause, SelectGraph, ColumnTableMapping);
            int GroupNumber = 0;
            foreach (var node in NodeTable)
            {
                GraphViewDocDBCommand.GetQuery(node.Value);
                if (!GraphDescription.ContainsKey(node.Value.NodeAlias))
                    GraphDescription[node.Value.NodeAlias] = ++GroupNumber;
            }
        }
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
        private void ConstructHeaderForTable()
        {
            BindingHeader = new List<int>();
            ResultHeader = new List<string>();
            foreach (var line in spec.lines)
            {
                if (line is NodeQuery) BindingHeader.Add((line as NodeQuery).NodeNum);
                if (line is LinkQuery)
                {
                    BindingHeader.Add((line as LinkQuery).src.NodeNum);
                    BindingHeader.Add((line as LinkQuery).dest.NodeNum);
                }
            }

            foreach (var element in SelectBlock.SelectElements)
            {
                if (element is WSelectScalarExpression)
                {
                    if ((element as WSelectScalarExpression).SelectExpr is WValueExpression) continue;
                    var expr = (element as WSelectScalarExpression).SelectExpr as WColumnReferenceExpression;
                    ResultHeader.Add(expr.MultiPartIdentifier.ToString());
                }
            }
        }

    }
}
