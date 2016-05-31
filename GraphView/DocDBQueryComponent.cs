using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Net;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using GraphView;
using Newtonsoft.Json.Linq;

using GraphView;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace GraphView
{
    using BindingStatue = Dictionary<string, int>;
    using LinkStatue = Dictionary<string, HashSet<string>>;
    using PathStatue = Tuple<Dictionary<string, int>, Dictionary<string, HashSet<string>>>;
    public class DocDBMatchQuery
    {
        public int source_num { get; set; }
        public int sink_num { get; set; }
        public string source_SelectClause { get; set; }
        public string sink_SelectClause { get; set; }
        public string source_alias { get; set; }
        public string sink_alias { get; set; }

    }
  class QueryComponentTest
    {
        static void Main(string[] args)
        {
            List<DocDBMatchQuery> list = new List<DocDBMatchQuery>();
            for (int i = 0; i < 5; i++) list.Add(new DocDBMatchQuery());
            #region ConstructTest
            list[0].sink_num = 2;
            list[0].sink_alias = "";
            list[0].sink_SelectClause = "";
            list[0].source_num = 1;
            list[0].source_alias = "";
            list[0].source_SelectClause = "";
            list[1].sink_num = 3;
            list[1].sink_alias = "B";
            list[1].sink_SelectClause = "From B Where ";
            list[1].source_num = 1;
            list[1].source_alias = "A";
            list[1].source_SelectClause = "From A Join g IN A._edge  Where A.age < 20 And  ((g.Long > 55)) ";
            list[2].sink_num = 2;
            list[2].sink_alias = "C";
            list[2].sink_SelectClause = "From C Where C.age > 55";
            list[2].source_num = 1;
            list[2].source_alias = "A";
            list[2].source_SelectClause = "From A Join g IN A._edge  Where A.age < 20 And  ((g.Long > 55)) ";
            list[3].sink_num = 4;
            list[3].sink_alias = "D";
            list[3].sink_SelectClause = "From D Where ";
            list[3].source_num = 1;
            list[3].source_alias = "A";
            list[3].source_SelectClause = "From A Join g IN A._edge  Where A.age < 20 And  ((g.Long > 55)) ";
            list[4].sink_num = 2;
            list[4].sink_alias = "C";
            list[4].sink_SelectClause = "From C Where C.age > 55";
            list[4].source_num = 3;
            list[4].source_alias = "B";
            list[4].source_SelectClause = "From B Where ";
            #endregion
            QueryComponent.init(50, "https://graphview.documents.azure.com:443/", "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==");
            QueryComponent.QueryTrianglePattern(list);
            Console.WriteLine("Ok!");
            Console.ReadKey();

        }
    }

    class QueryComponent
    {
        //DataBase Client
        static private DocumentClient client;

        //Initialization
        static List<string> ListZero = new List<string>() { };
        static LinkStatue LinkZero = new LinkStatue() { };
        static BindingStatue BindZero = new BindingStatue();
        static PathStatue PathZero = new Tuple<BindingStatue, LinkStatue>(BindZero, LinkZero);
        static List<PathStatue> StageZero = new List<PathStatue>() { PathZero, new PathStatue(new BindingStatue(), new LinkStatue()) };

        //Configuration
        static private int MAX_PACKET_SIZE;
        static private string END_POINT_URL = "";
        static private string PRIMARY_KEY = "";
        static public int QueryCount = 0;
        
        static public void init(int MaxPacketsize, string EndPointUrl, string PrimaryKey)
        {
            MAX_PACKET_SIZE = MaxPacketsize;
            END_POINT_URL = EndPointUrl;
            PRIMARY_KEY = PrimaryKey;
            LinkZero.Add("Bindings", new HashSet<string>());
            client = new DocumentClient(new Uri(END_POINT_URL), PRIMARY_KEY);
        }
        static private IQueryable<dynamic> ExcuteQuery(string database, string collection, string script)
        {
            QueryCount++;
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
            IQueryable<dynamic> Result = client.CreateDocumentQuery(
                    UriFactory.CreateDocumentCollectionUri(database, collection),
                    script,
                    queryOptions);
            return Result;
        }
        static public void ShowAll() { 
            int y = 1;
            var all = ExcuteQuery("GroupMatch", "GraphSix", "SELECT * FROM ALL");
            foreach (var x in all)  y++;
        }

        static private IEnumerable<PathStatue> FindLink(int index, List<DocDBMatchQuery> ParaPacket, HashSet<int> ReverseCheckSet = null)//,string From, string where)
        {
            LinkStatue QueryResult = new LinkStatue();
            List<PathStatue> MiddleStage = new List<PathStatue>();
            List<PathStatue> PathPacket = new List<PathStatue>();
            // For start nodes which has been binded
            int PacketCnt = 0;
            int to = ParaPacket[index].sink_num;
            int from = ParaPacket[index].source_num;
            IEnumerable<PathStatue> LastStage;
            if (index != 1) LastStage = FindLink(index - 1, ParaPacket);
            else LastStage = StageZero;
            foreach (var paths in LastStage)
            {
                if (PacketCnt < MAX_PACKET_SIZE && paths.Item2.Count != 0)
                {
                    PathPacket.Add(paths);
                    PacketCnt += 1;
                }
                else
                {
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
                        string StartScript = script;
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
                                //InRangeScript += "\"" + id.ToString() + "\"" + ",";
                                foreach(var y in edge)
                                {
                                    InRangeScript += "\"" + y["_sink"].ToString() + "\"" + ",";
                                }
                                LinkSet.Add(id.ToString());
                            }
                            foreach (var path in PathPacket)
                            {
                                if (!path.Item1.ContainsKey(id.ToString()))
                                {
                                    BindingStatue newBinding = new BindingStatue(path.Item1);
                                    newBinding.Add(id.ToString(), from);
                                    LinkStatue newLink = new LinkStatue();
                                    HashSet<string> newList;
                                    foreach (var x in path.Item2)
                                    {
                                        newList = new HashSet<string>(x.Value);
                                        newLink.Add(x.Key, newList);
                                    }
                                    newLink["Bindings"].Add(from.ToString());
                                    PathStatue newPath = new PathStatue(newBinding, newLink);
                                    MiddleStage.Add(newPath);
                                }
                            }
                        }
                    }

                    // To find possible end nodes
                    if (!NotYetBind)
                    {
                        string LinkWhereClause = ParaPacket[index].source_SelectClause;
                        string LinkWhereScript = " " + LinkWhereClause +
                            ((LinkWhereClause.Substring(LinkWhereClause.Length - 6, 5) == "Where") ? "" : " AND ") +
                               ParaPacket[index].source_alias + ".id IN (" + InRangeScript.Substring(0, InRangeScript.Length - 1) + ")";
                        string LinkScript = script + LinkWhereScript;
                        LinkScript = LinkScript.Replace("node", ParaPacket[index].source_alias);
                        var LinkRes = ExcuteQuery("GroupMatch", "GraphSix", LinkScript);
                        InRangeScript = "";
                        foreach (var item in LinkRes)
                        {
                            JToken NodeInfo = ((JObject)item)["NodeInfo"];
                            var edge = NodeInfo["edge"];
                            var id = NodeInfo["id"];
                            var reverse = NodeInfo["reverse"];
                            foreach (var y in edge)
                                InRangeScript += "\"" + y["_sink"].ToString() + "\"" + ",";
                        }
                    }
                    // Query to determine which possible end nodes satisfied the WHERE Clause
                    string WhereClause = ParaPacket[index].sink_SelectClause;
                    string EndWhereScript = " " + WhereClause + 
                        ((WhereClause.Substring(WhereClause.Length - 6, 5) == "Where") ? "" : " AND ") +
                           ParaPacket[index].sink_alias + ".id IN (" + InRangeScript.Substring(0, InRangeScript.Length - 1) + ")";
                    string EndScript = script + EndWhereScript;
                    EndScript = EndScript.Replace("node", ParaPacket[index].sink_alias);
                    var res = ExcuteQuery("GroupMatch", "GraphSix", EndScript);

                    foreach (var item in res)
                    {
                        JToken NodeInfo = ((JObject)item)["NodeInfo"];
                        var edge = NodeInfo["edge"];
                        var id = NodeInfo["id"];
                        var reverse = NodeInfo["reverse"];
                        List<string> RevList = new List<string>();
                        foreach (var x in reverse) RevList.Add(x["_sink"].ToString());
                        // For each path in current stage
                        foreach (var path in MiddleStage)
                        {
                            // For each binded start node
                            foreach (var BindingPair in path.Item1)
                                if (BindingPair.Value == from)
                                {
                                    if (RevList.Contains(BindingPair.Key))
                                    {
                                        if (path.Item1.ContainsKey(id.ToString()))
                                        {
                                            if (path.Item1[id.ToString()] == to)
                                            {
                                                LinkStatue NewLink = new LinkStatue();
                                                foreach (var x in path.Item2)
                                                {
                                                    NewLink.Add(x.Key, x.Value);
                                                }
                                                if (NewLink.ContainsKey(BindingPair.Key))
                                                {
                                                    NewLink[BindingPair.Key].Add(id.ToString());
                                                }
                                                else
                                                {
                                                    HashSet<string> NewList = new HashSet<string> { id.ToString() };
                                                    NewLink.Add(BindingPair.Key, NewList);
                                                }
                                                yield return new PathStatue(path.Item1, NewLink);
                                            }
                                        }
                                        else
                                        {
                                            BindingStatue NewBinding = new BindingStatue(path.Item1);
                                            NewBinding.Add(id.ToString(), to);
                                            LinkStatue NewLink = new LinkStatue();
                                            foreach (var x in path.Item2)
                                            {
                                                NewLink.Add(x.Key, x.Value);
                                            }
                                            if (NewLink.ContainsKey(BindingPair.Key))
                                            {
                                                NewLink[BindingPair.Key].Add(id.ToString());
                                            }
                                            else
                                            {
                                                HashSet<string> NewList = new HashSet<string> { id.ToString() };
                                                NewLink.Add(BindingPair.Key, NewList);
                                            }
                                            NewLink["Bindings"].Add(to.ToString());
                                            yield return new PathStatue(NewBinding, NewLink);
                                        }
                                    }
                                }
                        }
                    }
                    PathPacket.Clear();
                    PacketCnt = 0;
                }
            }
            yield return new PathStatue(new BindingStatue(), new LinkStatue());
            yield break;
        }
        static public IEnumerable<HashSet<string>> ExtractNodes(List<DocDBMatchQuery> ParaPacket, int PacketSize)
        {
            HashSet<string> PacketSet = new HashSet<string>();
            HashSet<string> packet = new HashSet<string>();
            int PacketCnt = 0;
            foreach (var path in FindLink(ParaPacket.Count - 1, ParaPacket))
            {
                foreach (var node in path.Item1)
                {
                    if (PacketCnt >= PacketSize)
                    {
                        yield return packet;
                        packet = new HashSet<string>();
                        PacketCnt = 0;
                    }
                    if (!PacketSet.Contains(node.Key))
                    {
                        packet.Add(node.Key);
                        PacketSet.Add(node.Key);
                    }
                    PacketCnt += 1;
                }
            }
            if (PacketCnt != 0) yield return packet;
            yield break;
        }
        static public IEnumerable<HashSet<Tuple<string, string>>> ExtractPairs(List<DocDBMatchQuery> ParaPacket, int PacketSize)
        {
            HashSet<Tuple<string, string>> packet = new HashSet<Tuple<string, string>>();
            HashSet<Tuple<string, string>> PacketSet = new HashSet<Tuple<string, string>>();
            int PacketCnt = 0;
            int first = ParaPacket[0].source_num;
            int second = ParaPacket[0].sink_num;
            string FirstGroup = "";
            string SecondGroup = "";
            foreach (var path in FindLink(ParaPacket.Count - 1, ParaPacket))
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
        static public void QueryTrianglePattern(List<DocDBMatchQuery> list)
        {
            foreach (var x in ExtractPairs(list, 50))
                foreach(var y in x)
                Console.WriteLine(y);
        }
    }
}

