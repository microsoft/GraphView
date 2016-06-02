using System;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using System.Collections;
using System.IO;
using System.Text;

using System.CodeDom.Compiler;
using System.Collections.Generic;
using Microsoft.CSharp;
using Microsoft.SqlServer.TransactSql.ScriptDom;
// Add DocumentDB references
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Documents.Client;

using System.Net;
using Microsoft.Azure.Documents;
using GraphView;


using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace GraphView
{
    using BindingStatue = Dictionary<string, int>;
    using BindingList = HashSet<string>;
    using PathStatue = Tuple<Dictionary<string, int>, HashSet<string>>;
    using LinkStatue = Dictionary<string,HashSet<string>>;
    public class QueryComponent
    {
        //Database Client
        public static DocumentClient client = null;
        public static MatchGraph graph;
    
        //Initialization
        static BindingList ListZero = new BindingList() { };
        static BindingStatue BindZero = new BindingStatue();
        static PathStatue PathZero = new Tuple<BindingStatue, BindingList>(BindZero, ListZero);
        static List<PathStatue> StageZero = new List<PathStatue>() { PathZero, new PathStatue(new BindingStatue(), null) };

        //Configuration
        static private int MAX_PACKET_SIZE;
        static private string END_POINT_URL = "";
        static private string PRIMARY_KEY = "";


        static public void init(int MaxPacketsize, GraphViewConnection conn)
        {
            MAX_PACKET_SIZE = MaxPacketsize;
            END_POINT_URL = conn.DocDB_Url;
            PRIMARY_KEY = conn.DocDB_Key;
            client = conn.client;
        }
        static public IQueryable<dynamic> ExcuteQuery(string database, string collection, string script)
        {
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
            IQueryable<dynamic> Result = client.CreateDocumentQuery(
                    UriFactory.CreateDocumentCollectionUri(database, collection),
                    script,
                    queryOptions);
            return Result;
        }
        static public IEnumerable<HashSet<Tuple<string, string>>> SelectProcessor(WSelectQueryBlock SelectQueryBlock, string source = "", string sink = "")
        {
            graph = GraphViewDocDBCommand.DocDB_ConstructGraph(SelectQueryBlock);
            Dictionary<string, HashSet<string>> GroupDic = new Dictionary<string, HashSet<string>>();
            Dictionary<string, int> GraphInfo = new Dictionary<string, int>();
            int sum = 0;
            var NodesTable = graph.ConnectedSubGraphs[0].Nodes;
            var attachPredicateVisitor = new AttachWhereClauseVisitor();
            var _context = new WSqlTableContext();
            var _graphMetaData = new GraphMetaData();
            var columnTableMapping = _context.GetColumnToAliasMapping(_graphMetaData.ColumnsOfNodeTables);
            if (SelectQueryBlock != null)
                attachPredicateVisitor.Invoke(SelectQueryBlock.WhereClause, graph, columnTableMapping);
            foreach (var node in NodesTable)
            {
                BuildNodes(node.Value);
                if (!GraphInfo.ContainsKey(node.Value.NodeAlias))
                    GraphInfo[node.Value.NodeAlias] = ++sum;
            }
            List<DocDBMatchQuery> MatchList = new List<DocDBMatchQuery>();
            if (source != "" && sink != "")
            {
                MatchList.Add(new DocDBMatchQuery()
                {
                    source_num = GraphInfo[source],
                    sink_num = GraphInfo[sink],
                    source_alias = source,
                    sink_alias = sink,
                    edge_alias = new List<string>()
                });
            }
            foreach (var node in NodesTable)
            {
                int edge_source_num = GraphInfo[node.Value.NodeAlias];
                if (node.Value.Neighbors.Count != 0)
                {
                    for (int i = 0; i < node.Value.Neighbors.Count(); i++)
                    {
                        var edge = node.Value.Neighbors[i];
                        string edge_sink_alias = edge.SinkNode.NodeAlias;
                        int edge_sink_num = GraphInfo[edge_sink_alias];
                        DocDBMatchQuery NewItem = new DocDBMatchQuery()
                        {
                            source_num = edge_source_num,
                            sink_num = edge_sink_num,
                            source_SelectClause = node.Value.DocDBQuery.Replace("'", "\""),
                            sink_SelectClause = edge.SinkNode.DocDBQuery.Replace("'", "\""),
                            source_alias = node.Value.NodeAlias,
                            sink_alias = edge_sink_alias,
                            edge_alias = new List<string>()
                        };
                        foreach (var x in node.Value.Neighbors) {
                            if (x.SinkNode.NodeAlias == edge_sink_alias)
                            NewItem.edge_alias.Add(x.EdgeAlias);
                            }
                        MatchList.Add(NewItem);
                    }
                }
                else
                {
                    MatchList.Add(new DocDBMatchQuery()
                    {
                        source_num = edge_source_num,
                        sink_num = edge_source_num,
                        source_SelectClause = node.Value.DocDBQuery.Replace("'", "\""),
                        sink_SelectClause = node.Value.DocDBQuery.Replace("'", "\""),
                        source_alias = node.Value.NodeAlias,
                        sink_alias = node.Value.NodeAlias,
                        edge_alias = new List<string>()
                    });
                }
            }
            if (sink != "" && source != "")
            {
                foreach (var x in ExtractPairs(MatchList, 50))
                {
                    yield return x;
                }
            }
            else 
            {
                foreach (var x in ExtractNodes(MatchList, 50))

                {
                    foreach (var y in x)
                    {
                        if (!GroupDic.ContainsKey(y.Item2))
                        {
                            HashSet<string> NewHashSet = new HashSet<string>();
                            GroupDic.Add(y.Item2, NewHashSet);
                        }
                        GroupDic[y.Item2].Add(y.Item1);
                    }
                }
                foreach (var x in SelectQueryBlock.SelectElements)
                {
                    string QueryRange = "";
                    string script = "";
                    if (x is WSelectStarExpression)
                    {
                        var star = x as WSelectStarExpression;
                        if (star.Qulifier == null) {
                            foreach (var node in GroupDic)
                                foreach (var id in node.Value)
                                    QueryRange += "\"" + id + "\",";
                            if (QueryRange.Length > 0) QueryRange = QueryRange.Substring(0, QueryRange.Length - 1);
                            script = "SELECT NODE AS NODEINFO " +
                                "FROM NODE" +
                            " WHERE NODE.id IN (" + QueryRange + ")";
                        }
                        else
                        {
                            var alias = star.Qulifier.Identifiers[0].Value.ToString();
                            int TargetNode = GraphInfo[alias];
                            foreach (var node in GroupDic[TargetNode.ToString()])
                                QueryRange += "\"" + node + "\",";
                            if (QueryRange.Length > 0) QueryRange = QueryRange.Substring(0, QueryRange.Length - 1);
                            script = "SELECT " + alias + " AS NODEINFO " +
                                " FROM " + alias +
                                " WHERE " + alias + ".id IN (" + QueryRange + ")";
                        }
                    }
                    else
                    {
                        var exprx = x as WSelectScalarExpression;
                        var expr = exprx.SelectExpr as WColumnReferenceExpression;
                        var identifier = expr.MultiPartIdentifier.Identifiers;
                        int TargetNode = GraphInfo[identifier[0].Value];
                        foreach (var node in GroupDic[TargetNode.ToString()])
                            QueryRange += "\"" + node + "\",";
                        if (QueryRange.Length > 0) QueryRange = QueryRange.Substring(0, QueryRange.Length - 1);
                        script = "SELECT " + expr.MultiPartIdentifier.ToString() + " AS NODEINFO " +
                            " FROM " + identifier[0].Value +
                            " WHERE " + identifier[0].Value + ".id IN (" + QueryRange + ")";
                    }
                    var res = ExcuteQuery("GroupMatch", "GraphSix", script);
                    HashSet<Tuple<string, string>> result = new HashSet<Tuple<string, string>>();
                    string ResString = "";
                    foreach (var item in res)
                    {
                        JToken obj = ((JObject)item)["NODEINFO"];
                        string objstring = obj.ToString();
                        ResString += objstring;
                    }
                    Tuple<string, string> ResTuple = new Tuple<string, string>(ResString, "");
                    result.Add(ResTuple);
                    yield return result;
                }

            }
            yield break;
        }
        static private IEnumerable<PathStatue> FindNext(int index, List<DocDBMatchQuery> ParaPacket, HashSet<int> ReverseCheckSet = null)
        {
            // Initialization
            LinkStatue QueryResult = new LinkStatue();
            List<PathStatue> MiddleStage = new List<PathStatue>();
            List<PathStatue> PathPacket = new List<PathStatue>();
            Dictionary<string, HashSet<string>> QueryLink = new Dictionary<string, HashSet<string>>();

            int PacketCnt = 0;
            int to = ParaPacket[index].sink_num;
            int from = ParaPacket[index].source_num;
            string EdgeAlias = "";
            foreach(var x in ParaPacket[index].edge_alias)
            {
                EdgeAlias += x + ",";
            }
            if (EdgeAlias.Length > 0) EdgeAlias = EdgeAlias.Substring(0, EdgeAlias.Length - 1);
            else EdgeAlias = "node._edge";
            IEnumerable<PathStatue> LastStage;
            if (index != 1) LastStage = FindNext(index - 1, ParaPacket);
            else LastStage = StageZero;

            foreach (var paths in LastStage)
            {
                if (PacketCnt < MAX_PACKET_SIZE && paths.Item2 != null)
                {
                    PathPacket.Add(paths);
                    PacketCnt += 1;
                }
                else if (to == from)
                {
                    bool skipflag = false;
                    foreach (var path in PathPacket)
                        if (path.Item2.Contains(to.ToString()))
                        {
                            yield return path;
                            skipflag = true;
                        }
                    if (skipflag) continue;
                    string script = "SELECT {\"id\":node.id, \"edge\":node._edge, \"reverse\":node._reverse_edge} AS NodeInfo";
                    string NodeScript = script;
                    string NodeWhereScript = " " + ParaPacket[index].source_SelectClause;
                    NodeScript = NodeScript.Replace("node", ParaPacket[index].source_alias);
                    if (!(NodeWhereScript.Substring(NodeWhereScript.Length - 6, 5) == "Where"))
                    {
                        NodeScript += NodeWhereScript;
                    }
                    else NodeScript += " From " + ParaPacket[index].source_alias;
                    var start = ExcuteQuery("GroupMatch", "GraphSix", NodeScript);
                    foreach (var item in start)
                    {
                        JToken NodeInfo = ((JObject)item)["NodeInfo"];
                        var edge = NodeInfo["edge"];
                        var id = NodeInfo["id"];
                        var reverse = NodeInfo["reverse"];
                        foreach (var path in PathPacket)
                        {
                            if (!path.Item1.ContainsKey(id.ToString()))
                            {
                                BindingStatue newBinding = new BindingStatue(path.Item1);
                                newBinding.Add(id.ToString(), from);
                                BindingList newLink = new BindingList();
                                foreach (var x in path.Item2)
                                {
                                    newLink.Add(x);
                                }
                                newLink.Add(from.ToString());
                                PathStatue newPath = new PathStatue(newBinding, newLink);
                                yield return newPath;
                            }
                        }
                    }
                }
                else
                {
                    string AliasScript= "SELECT {\"id\":node.id, \"edge\":" + EdgeAlias + ", \"reverse\":node._reverse_edge} AS NodeInfo";
                    string script = "SELECT {\"id\":node.id, \"edge\":node._edge, \"reverse\":node._reverse_edge} AS NodeInfo";
                    MiddleStage = new List<PathStatue>();

                    string InRangeScript = "";

                    // To find nodes that has been binded with start group
                    HashSet<string> LinkSet = new HashSet<string>();
                    foreach (var path in PathPacket)
                    {
                        MiddleStage.Add(path);
                        foreach (var BindingPair in path.Item1)
                            if (BindingPair.Value == from)
                            {
                                if (!LinkSet.Contains(BindingPair.Key))
                                {
                                    InRangeScript += "\"" + BindingPair.Key + "\"" + ",";
                                    LinkSet.Add(BindingPair.Key);
                                }
                            }
                    }


                    bool NotYetBind = InRangeScript.Length == 0;

                    // To find not yet binded nodes, bind them to start group and generate new path
                    if (NotYetBind)
                    {
                        string StartScript = AliasScript;
                        string StartWhereScript = " " + ParaPacket[index].source_SelectClause;
                        StartScript = StartScript.Replace("node", ParaPacket[index].source_alias);
                        if (!(StartWhereScript.Substring(StartWhereScript.Length - 6, 5) == "Where"))
                        {
                            StartScript += StartWhereScript;
                        }
                        else StartScript += " From " + ParaPacket[index].source_alias;
                        var start = ExcuteQuery("GroupMatch", "GraphSix", StartScript);
                        foreach (var item in start)
                        {
                            JToken NodeInfo = ((JObject)item)["NodeInfo"];
                            var edge = NodeInfo["edge"];
                            var id = NodeInfo["id"];
                            var reverse = NodeInfo["reverse"];
                            if (!LinkSet.Contains(id.ToString()))
                            {
                                InRangeScript += "\"" + id.ToString() + "\"" + ",";
                                LinkSet.Add(id.ToString());
                            foreach (var path in PathPacket)
                            {
                                if (!path.Item1.ContainsKey(id.ToString()))
                                {
                                    BindingStatue newBinding = new BindingStatue(path.Item1);
                                    newBinding.Add(id.ToString(), from);
                                    BindingList newLink = new BindingList();
                                    foreach (var x in path.Item2)
                                    {
                                        newLink.Add(x);
                                    }
                                    newLink.Add(from.ToString());
                                    PathStatue newPath = new PathStatue(newBinding, newLink);
                                    MiddleStage.Add(newPath);
                                }
                            }
                        }
                        }
                    }

                    // To find possible end nodes
                    string LinkWhereClause = ParaPacket[index].source_SelectClause;
                    string LinkWhereScript = " " + LinkWhereClause +
                        ((LinkWhereClause.Substring(LinkWhereClause.Length - 6, 5) == "Where") ? "" : " AND ") +
                           ParaPacket[index].source_alias + ".id IN (" + InRangeScript.Substring(0, InRangeScript.Length - 1) + ")";
                    string LinkScript = AliasScript + LinkWhereScript;
                    LinkScript = LinkScript.Replace("node", ParaPacket[index].source_alias);
                    var LinkRes = ExcuteQuery("GroupMatch", "GraphSix", LinkScript);
                    InRangeScript = "";
                    foreach (var item in LinkRes)
                    {
                        JToken NodeInfo = ((JObject)item)["NodeInfo"];
                        var edge = ((JObject)NodeInfo)["edge"];
                        var id = NodeInfo["id"];
                        var reverse = NodeInfo["reverse"];
                        InRangeScript += "\"" + edge["_sink"].ToString() + "\"" + ",";
                        if (!QueryLink.ContainsKey(edge["_sink"].ToString()))
                            QueryLink.Add(edge["_sink"].ToString(), new HashSet<string>());
                        QueryLink[edge["_sink"].ToString()].Add(id.ToString());
                    }

                    // Query to determine which possible end nodes satisfied the WHERE Clause
                    string WhereClause = ParaPacket[index].sink_SelectClause;
                    string EndWhereScript = " " + WhereClause +
                        ((WhereClause.Substring(WhereClause.Length - 6, 5) == "Where") ? "" : " AND ") + (InRangeScript.Length == 0 ? ""
                           : ParaPacket[index].sink_alias + ".id IN (" + InRangeScript.Substring(0, InRangeScript.Length - 1) + ")");
                    if (EndWhereScript.Substring(EndWhereScript.Length - 6, 5) == "Where")
                        EndWhereScript = EndWhereScript.Substring(0, EndWhereScript.Length - 6);
                    if (EndWhereScript.Substring(EndWhereScript.Length - 4, 3) == "AND")
                        EndWhereScript = EndWhereScript.Substring(0, EndWhereScript.Length - 4);
                    string EndScript = script + EndWhereScript;
                    EndScript = EndScript.Replace("node", ParaPacket[index].sink_alias);
                    var res = ExcuteQuery("GroupMatch", "GraphSix", EndScript);

                    foreach (var item in res)
                    {
                        JToken NodeInfo = JObject.Parse(JsonConvert.SerializeObject(item))["NodeInfo"];
                        var edge = NodeInfo["edge"];
                        var id = NodeInfo["id"];
                        var reverse = NodeInfo["reverse"];
                        // For each path in current stage
                        foreach (var path in MiddleStage)
                        {
                            foreach (var BindingPair in path.Item1)
                            {
                                if (BindingPair.Value == from) {
                                    foreach (var Link in QueryLink[id.ToString()])
                                    {
                                        if(Link == BindingPair.Key)
                                        {
                                            if (path.Item1.ContainsKey(id.ToString()))
                                            {
                                                if (path.Item1[id.ToString()] == to)
                                                {
                                                    BindingList NewLink = new BindingList();
                                                    foreach (var x in path.Item2)
                                                    {
                                                        NewLink.Add(x);
                                                    }
                                                    yield return new PathStatue(path.Item1, NewLink);
                                                }
                                            }
                                            else
                                            {
                                                BindingStatue NewBinding = new BindingStatue(path.Item1);
                                                NewBinding.Add(id.ToString(), to);
                                                BindingList NewLink = new BindingList();
                                                foreach (var x in path.Item2)
                                                {
                                                    NewLink.Add(x);
                                                }
                                                NewLink.Add(to.ToString());
                                                yield return new PathStatue(NewBinding, NewLink);
                                            }
                                        }
                                    }
                                }
                            }
                         }
                        }
                    PathPacket.Clear();
                    PacketCnt = 0;
                }
            }
            yield return new PathStatue(new BindingStatue(), null);
            yield break;
        }
        static private IEnumerable<HashSet<Tuple<string, string>>> ExtractNodes(List<DocDBMatchQuery> ParaPacket, int PacketSize)
        {
            HashSet<Tuple<string, string>> PacketSet = new HashSet<Tuple<string, string>>();
            HashSet<Tuple<string, string>> packet = new HashSet<Tuple<string, string>>();
            int PacketCnt = 0;
            foreach (var path in FindNext(ParaPacket.Count - 1, ParaPacket))
            {
                foreach (var node in path.Item1)
                {
                    var NewTuple = new Tuple<string, string>(node.Key, node.Value.ToString());
                    if (PacketCnt >= PacketSize)
                    {
                        yield return packet;
                        packet = new HashSet<Tuple<string, string>>();
                        PacketCnt = 0;
                    }
                    if (!PacketSet.Contains(NewTuple))
                    {
                        packet.Add(NewTuple);
                        PacketSet.Add(NewTuple);
                    }
                    PacketCnt += 1;
                }
            }
            if (PacketCnt != 0) yield return packet;
            yield break;
        }
        static private IEnumerable<HashSet<Tuple<string, string>>> ExtractPairs(List<DocDBMatchQuery> ParaPacket, int PacketSize)
        {
            HashSet<Tuple<string, string>> PacketSet = new HashSet<Tuple<string, string>>();
            HashSet<Tuple<string, string>> packet = new HashSet<Tuple<string, string>>();
            int PacketCnt = 0;
            int first = ParaPacket[0].source_num;
            int second = ParaPacket[0].sink_num;
            string FirstGroup = "";
            string SecondGroup = "";
            foreach (var path in FindNext(ParaPacket.Count - 1, ParaPacket))
            {
                foreach (var node in path.Item1)
                {
                    if (PacketCnt >= PacketSize)
                    {
                        yield return packet;
                        packet = new HashSet<Tuple<string, string>>();
                        PacketCnt = 0;
                    }
                    if (node.Value == first) FirstGroup = node.Key;
                    if (node.Value == second) SecondGroup = node.Key;
                    if (FirstGroup.Length != 0 && SecondGroup.Length != 0 && !PacketSet.Contains(new Tuple<string, string>(FirstGroup, SecondGroup)))
                    {
                        PacketCnt += 1;
                        var NewPair = new Tuple<string, string>(FirstGroup, SecondGroup);
                        packet.Add(NewPair);
                        PacketSet.Add(NewPair);
                        FirstGroup = "";
                        SecondGroup = "";
                    }
                }
            }
            if (PacketCnt != 0) yield return packet;
            yield break;
        }
        static private void BuildNodes(MatchNode node)
        {
            string Query = "From " + node.NodeAlias;
            string Edgepredicate = "";
            int edge_predicate_num = 0;
            foreach (var edge in node.Neighbors)
            {
                if (edge.Predicates != null)
                {
                    if (edge_predicate_num != 0) Edgepredicate += " And ";
                    edge_predicate_num++;
                    Query += " Join " + edge.EdgeAlias + " in " + node.NodeAlias + "._edge ";
                    Edgepredicate += " (";
                    for (int i = 0; i < edge.Predicates.Count(); i++)
                    {
                        if (i != 0)
                            Edgepredicate += " And ";
                        Edgepredicate += "(" + edge.Predicates[i] + ")";
                    }
                    Edgepredicate += ") ";
                }
            }
            Query += " Where ";
            if (node.Predicates != null)
            {
                for (int i = 0; i < node.Predicates.Count(); i++)
                {
                    if (i != 0)
                        Query += " And ";
                    Query += node.Predicates[i];
                }
                if (Edgepredicate != "")
                    Query += " And ";
            }


            if (Edgepredicate != "")
            {
                Query += Edgepredicate;
            }

            node.DocDBQuery = Query;
        }
    }

}
