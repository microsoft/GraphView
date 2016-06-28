using System;
using System.Linq;
using System.Collections.Generic;
// Add DocumentDB references
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Documents.Client;


namespace GraphView
{

    using BindingStatue = Dictionary<string, int>;
    using BindingSet = HashSet<string>;
    using PathStatue = Tuple<Dictionary<string, int>, HashSet<string>>;
    using AdjacentList = Dictionary<string, HashSet<string>>;

    public class DocDBConnection
    {
        public DocDBConnection(int pMaxPacketSize, GraphViewConnection connection)
        {
            MaxPacketSize = pMaxPacketSize;
            EndPointUrl = connection.DocDB_Url;
            PrimaryKey = connection.DocDB_Key;
            DatabaseID = connection.DocDB_DatabaseId;
            CollectionID = connection.DocDB_CollectionId;
            client = connection.client;
        }
        public int MaxPacketSize;
        public string EndPointUrl;
        public string PrimaryKey;
        public string DatabaseID;
        public string CollectionID;
        public DocumentClient client;
    }

    public class ItemQuery { }
    public class NodeQuery : ItemQuery
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
    public class LinkQuery : ItemQuery
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
    public class QuerySpec
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
    public class SelectProcessor
    {
        static BindingSet ListZero = new BindingSet() { };
        static BindingStatue BindZero = new BindingStatue();
        static PathStatue PathZero = new Tuple<BindingStatue, BindingSet>(BindZero, ListZero);
        static List<PathStatue> StageZero = new List<PathStatue>() { PathZero, new PathStatue(new BindingStatue(), null) };

        private DocDBConnection connection;
        private WSelectQueryBlock SelectBlock;
        private MatchGraph SelectGraph;
        private Dictionary<string, int> GraphDescription;
        private Dictionary<string, MatchNode> NodeTable;
        private QuerySpec spec;
        private string SelectResult;
        private string InRangeScript = "";
        /// <summary>
        /// Execute a documentDB script on the connection that has already been established 
        /// and stored on (DocDBconnection) connection
        /// </summary>
        private IQueryable<dynamic> ExecuteQuery(string script)
        {
            FeedOptions QueryOptions = new FeedOptions { MaxItemCount = -1 };
            IQueryable<dynamic> Result = connection.client.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(connection.DatabaseID, connection.CollectionID), script, QueryOptions);
            return Result;
        }
        /// <summary>
        /// Consturct a graph that describes the pattern of the select clause with all predicates attached on its nodes.
        /// (The predicates of edges are attached to its source)
        /// </summary>
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
                else
                {
                    if (!AddedNodes.Contains(node.Value.NodeAlias))
                        spec.add(new NodeQuery(GraphDescription, node.Value));
                }
            }
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
        private PathStatue AddIfNotExist(Tuple<string, string> ItemInfo, PathStatue path, NodeQuery pQuery)
        {
            if (!path.Item1.ContainsKey(ItemInfo.Item1))
            {
                BindingStatue newBinding = new BindingStatue(path.Item1);
                newBinding.Add(ItemInfo.Item1, pQuery.NodeNum);
                BindingSet newLink = new BindingSet();
                foreach (var x in path.Item2)
                {
                    newLink.Add(x.ToString());
                }
                newLink.Add(pQuery.NodeNum.ToString());
                PathStatue newPath = new PathStatue(newBinding, newLink);
                return newPath;
            }
            return null;
        }
        string CutTheTail(string InRangeScript)
        {
            return InRangeScript.Substring(0, InRangeScript.Length - 1);
        }
        bool FindAnyThingOrNot(string InRangeScript)
        {
            return InRangeScript.Length > 0;
        }
        /// <summary>
        /// Dealing with NodeQuery specifier, sending query to determine a set of nodes and bind them to a specific group
        /// </summary>
        private IEnumerable<PathStatue> NodeQueryProcessor(List<PathStatue> PathsFromLastStage, NodeQuery pNodeQuery)
        {
            string ScriptBase = "SELECT {\"id\":node.id, \"edge\":node._edge, \"reverse\":node._reverse_edge} AS NodeInfo";
            string WhereClause = " " + pNodeQuery.NodePredicate;
            string NodeScript = ScriptBase.Replace("node", pNodeQuery.NodeAlias);
            if (HasWhereClause(pNodeQuery.NodePredicate))
                NodeScript += WhereClause;
            else NodeScript += " From " + pNodeQuery.NodeAlias;
            IQueryable<dynamic> Node = ExecuteQuery(NodeScript);
            foreach (var item in Node)
            {
                Tuple<string, string> ItemInfo = DecodeJObject((JObject)item);
                foreach (var path in PathsFromLastStage)
                {
                    PathStatue NewPath = AddIfNotExist(ItemInfo, path, pNodeQuery);
                    if (NewPath != null) yield return NewPath;
                    else yield return path;
                }
            }
            yield break;
        }

        private string GetListOfAlreadyBindedNodes(List<PathStatue> PathsFromLastStage, int GroupNumber)
        {
            string InRangeScript = "";
            HashSet<string> LinkSet = new HashSet<string>();
            foreach (var path in PathsFromLastStage)
            {
                foreach (var BindingPair in path.Item1)
                    if (BindingPair.Value == GroupNumber)
                    {
                        if (!LinkSet.Contains(BindingPair.Key))
                        {
                            InRangeScript += "\"" + BindingPair.Key + "\"" + ",";
                            LinkSet.Add(BindingPair.Key);
                        }
                    }
            }
            return InRangeScript;
        }
        /// <summary>
        /// Dealing with the source of a link
        /// sending query to determine a set of source nodes and bind them to a specific group
        /// Also giving a set of possible sink nodes for later use
        /// </summary>
        private IEnumerable<PathStatue> QueryForSrcNodes(List<PathStatue> PathsFromLastStage, LinkQuery pLinkQuery, AdjacentList Map)
        {
            string SrcScript = "";
            string EdgeAlias = "";
            foreach (var edge in pLinkQuery.EdgeAlias) EdgeAlias += edge.ToString();
            if (EdgeAlias.Length == 0) EdgeAlias = "node._edge";
            string ScriptBase = "SELECT {\"id\":node.id, \"edge\":" + EdgeAlias + ", \"reverse\":node._reverse_edge} AS NodeInfo ";
            string SrcWhereScript = " " + pLinkQuery.src.NodePredicate;
            SrcScript = ScriptBase.Replace("node", pLinkQuery.src.NodeAlias);
            if (HasWhereClause(pLinkQuery.src.NodePredicate))
            {
                SrcScript += pLinkQuery.src.NodePredicate;
            }
            else SrcScript += pLinkQuery.src.NodePredicate.Substring(pLinkQuery.src.NodePredicate.Length - 6, 5);
            var src = ExecuteQuery(SrcScript);
            var LinkSet = new HashSet<string>();
            foreach (var item in src)
            {
                Tuple<string, string> ItemInfo = DecodeJObject((JObject)item, true);
                InRangeScript += "\"" + ItemInfo.Item2 + "\"" + ",";
                if (!LinkSet.Contains(ItemInfo.Item1))
                {
                    LinkSet.Add(ItemInfo.Item1);
                    foreach (var path in PathsFromLastStage)
                    {
                        PathStatue NewPath = AddIfNotExist(ItemInfo, path, pLinkQuery.src);
                        if (NewPath != null) yield return NewPath;
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
        private IEnumerable<PathStatue> QueryForDestNodes(List<PathStatue> PathsFromLastStage, LinkQuery pLinkQuery, AdjacentList Map)
        {
            if (InRangeScript.Length == 0) yield break;
            InRangeScript = CutTheTail(InRangeScript);
            string ScriptBase = "SELECT {\"id\":node.id, \"edge\":node._edge, \"reverse\":node._reverse_edge} AS NodeInfo";
            string DestWhereScript = " " + pLinkQuery.dest.NodePredicate;
            if (HasWhereClause(DestWhereScript))
                DestWhereScript += " AND " + pLinkQuery.dest.NodeAlias + ".id IN (" + InRangeScript + ")";
            else DestWhereScript += pLinkQuery.dest.NodeAlias + ".id IN(" + InRangeScript + ")";
            string DestScript = ScriptBase + DestWhereScript;
            DestScript = DestScript.Replace("node", pLinkQuery.dest.NodeAlias);
            var dest = ExecuteQuery(DestScript);
            foreach (var node in dest)
            {
                Tuple<string, string> ItemInfo = DecodeJObject((JObject)node);
                foreach (var path in PathsFromLastStage)
                {
                    foreach (var binding in path.Item1)
                    {
                        if (binding.Value == pLinkQuery.src.NodeNum)
                            foreach (var link in Map[ItemInfo.Item1])
                            {
                                if (link == binding.Key)
                                {
                                    if (path.Item1.ContainsKey(ItemInfo.Item1) && path.Item1[ItemInfo.Item1] == pLinkQuery.dest.NodeNum)
                                    {
                                        BindingSet NewLink = new BindingSet();
                                        foreach (var x in path.Item2)
                                        {
                                            NewLink.Add(x);
                                        }
                                        yield return new PathStatue(path.Item1, NewLink);
                                    }
                                    else if (!path.Item2.Contains(pLinkQuery.dest.NodeNum.ToString()))
                                    {
                                        BindingStatue NewBinding = new BindingStatue(path.Item1);
                                        NewBinding.Add(ItemInfo.Item1, pLinkQuery.dest.NodeNum);
                                        BindingSet NewLink = new BindingSet();
                                        foreach (var x in path.Item2)
                                        {
                                            NewLink.Add(x);
                                        }
                                        NewLink.Add(pLinkQuery.dest.NodeNum.ToString());
                                        yield return new PathStatue(NewBinding, NewLink);
                                    }
                                }
                            }
                    }
                }
            }
        }
        PathStatue YieldTailFlag()
        {
            return new PathStatue(new BindingStatue(), null);
        }
        /// <summary>
        /// Read the spec line by line, if the line is NodeQuery Specifier, turn to NodeQueryProcessor
        /// otherwise turn to LinkQueryProcessor, which is QueryForSrcNodes and QueryForDestNodes function.
        /// Also do the packing thing, packing the result and send it to next line to be processed.
        /// </summary>
        private IEnumerable<PathStatue> FindNext(int index)
        {
            AdjacentList MapForCurrentStage = new AdjacentList();
            List<PathStatue> TempPathForCurrentStage = new List<PathStatue>();
            int PacketCnt = 0;
            List<PathStatue> PathPacket = new List<PathStatue>();
            IEnumerable<PathStatue> LastStage;
            if (index >= 1) LastStage = FindNext(index - 1);
            else LastStage = StageZero;
            foreach (var paths in LastStage)
            {
                if (PacketCnt < connection.MaxPacketSize && paths.Item2 != null)
                {
                    PathPacket.Add(paths);
                    PacketCnt += 1;
                }
                else if (spec.lines[index] is NodeQuery)
                {
                    NodeQuery Query = spec.lines[index] as NodeQuery;
                    foreach (var res in NodeQueryProcessor(PathPacket, Query))
                        yield return res;
                }
                else if (spec.lines[index] is LinkQuery)
                {
                    LinkQuery Query = spec.lines[index] as LinkQuery;

                    InRangeScript = GetListOfAlreadyBindedNodes(PathPacket, Query.src.NodeNum);
                    if (!FindAnyThingOrNot(InRangeScript))
                    {
                        foreach (var res in QueryForSrcNodes(PathPacket, Query, MapForCurrentStage))
                        {
                            TempPathForCurrentStage.Add(res);
                        }
                    }
                    foreach (var res in QueryForDestNodes(TempPathForCurrentStage, Query, MapForCurrentStage))
                    {
                        yield return res;
                    }
                }
            }
            yield return YieldTailFlag();
            yield break;
        }
        /// <summary>
        /// Extract result from the path that generated by FindNext function and presented as formated text.
        /// </summary>
        private string ExtractResult(WSelectQueryBlock SelectBlock, PathStatue path)
        {
            if (path.Item1.Count == 0) return "";
            Dictionary<string, HashSet<string>> BindingDic = new Dictionary<string, HashSet<string>>();
            var TargetElement = SelectBlock.SelectElements;
            var script = "";
            string QueryRange = "";
            string ResString = "";
            List<List<string>> Result = new List<List<string>>();
            foreach (var binding in path.Item1)
            {
                if (!BindingDic.ContainsKey(binding.Value.ToString()))
                {
                    HashSet<string> NewHashSet = new HashSet<string>();
                    BindingDic.Add(binding.Value.ToString(), NewHashSet);
                }
                BindingDic[binding.Value.ToString()].Add(binding.Key);
            }
            foreach (var element in TargetElement)
            {
                QueryRange = "";
                if (element is WSelectStarExpression)
                {
                    var star = element as WSelectStarExpression;
                    if (star.Qulifier == null)
                    {
                        foreach (var node in BindingDic)
                            foreach (var id in node.Value)
                                QueryRange += "\"" + id + "\",";
                        if (QueryRange.Length > 0) QueryRange = CutTheTail(QueryRange);
                        script = "SELECT NODE AS INFO " + "FROM NODE" + " WHERE NODE.id IN (" + QueryRange + ")";
                    }
                    else
                    {
                        var MainAlias = star.Qulifier.Identifiers[0].Value.ToString();
                        var alias = "";
                        foreach (var identifier in star.Qulifier.Identifiers)
                            alias += identifier.Value.ToString() + ".";
                        alias = CutTheTail(alias);
                        string TargetNode = GraphDescription[alias].ToString();
                        foreach (var node in BindingDic[TargetNode])
                            QueryRange += "\"" + node + "\",";
                        if (QueryRange.Length > 0) QueryRange = CutTheTail(QueryRange);
                        script = "SELECT " + alias + " AS INFO " + " FROM " + alias + " WHERE " + MainAlias + ".id IN (" + QueryRange + ")";
                    }
                }
                else
                {
                    var exprx = element as WSelectScalarExpression;
                    var expr = exprx.SelectExpr as WColumnReferenceExpression;
                    var identifiers = expr.MultiPartIdentifier.Identifiers;
                    if (GraphDescription.ContainsKey(identifiers[0].Value))
                    {
                        int MainNumber = GraphDescription[identifiers[0].Value];
                        foreach (var node in BindingDic[MainNumber.ToString()])
                            QueryRange += "\"" + node + "\",";
                        if (QueryRange.Length > 0) QueryRange = QueryRange.Substring(0, QueryRange.Length - 1);
                        script = "SELECT " + expr.MultiPartIdentifier.ToString() + " AS INFO " +
                            " FROM " + identifiers[0].Value +
                            " WHERE " + identifiers[0].Value + ".id IN (" + QueryRange + ")";
                    }
                    else
                    {
                        foreach (var line in spec.lines)
                            if (line is LinkQuery)
                            {
                                var link = line as LinkQuery;
                                foreach (var edge in link.EdgeAlias)
                                {
                                    if (edge == identifiers[0].Value)
                                    {
                                        string SrcRange = "";
                                        string DestRange = "";
                                        foreach (var node in BindingDic[link.src.NodeNum.ToString()])
                                            SrcRange += "\"" + node + "\",";
                                        foreach (var node in BindingDic[link.dest.NodeNum.ToString()])
                                            DestRange += "\"" + node + "\",";
                                        if (DestRange.Length != 0 && SrcRange.Length != 0)
                                        {
                                            script = "SELECT " + expr.MultiPartIdentifier.ToString() + " AS INFO " +
                                             " FROM " + link.src.NodeAlias + " JOIN " + edge +  " IN " + link.src.NodeAlias + "._edge " +
                                             " WHERE " + link.src.NodeAlias + ".id IN (" + CutTheTail(SrcRange) + ")" + " AND " + edge + "._sink IN (" + CutTheTail(DestRange) + ")";
                                        }
                                    }
                                }
                            }
                    }
                }
                var res = ExecuteQuery(script);
                foreach (var item in res)
                {
                    JToken obj = ((JObject)item)["INFO"];
                    if (!(obj == null))
                    {
                        string objstring = obj.ToString();
                        ResString += objstring + " ";
                    }
                }
            }
            return ResString;
        }
        public SelectProcessor(WSelectQueryBlock pSelectBlock, DocDBConnection pConnection)
        {
            SelectBlock = pSelectBlock;
            GraphDescription = new Dictionary<string, int>();
            spec = new QuerySpec();
            connection = pConnection;
            ConstructGraph(SelectBlock);
            ConstructQuerySpec();
        }
        public IEnumerable<string> Result()
        {
            foreach (var path in FindNext(spec.index()))
            {
                yield return ExtractResult(SelectBlock, path);
            }
            yield break;
        }
    }

}
