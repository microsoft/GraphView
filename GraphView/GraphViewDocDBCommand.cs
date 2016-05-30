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




namespace GraphView
{
    using BindingStatue = Dictionary<string, int>;
    using LinkStatue = Dictionary<string, HashSet<string>>;
    using PathStatue = Tuple<Dictionary<string, int>, Dictionary<string, HashSet<string>>>;
    
    public class GraphViewDocDBCommand
    {

        public static CompilerResults CompileFromSource(string source)
        {
            var codeProvider = new CSharpCodeProvider();
            var parameters = new CompilerParameters
            {
                GenerateExecutable = false,
#if DEBUG
                IncludeDebugInformation = true,
#else
                IncludeDebugInformation = false,
#endif
            };
            parameters.ReferencedAssemblies.Add("Microsoft.Azure.Documents.Client.dll");
            parameters.ReferencedAssemblies.Add("Microsoft.CSharp.dll");
            parameters.ReferencedAssemblies.Add(@"D:\source\graphview\packages\Newtonsoft.Json.6.0.8\lib\net45\Newtonsoft.Json.dll");
            parameters.ReferencedAssemblies.Add("System.dll");
            parameters.ReferencedAssemblies.Add("System.Core.dll");
            parameters.ReferencedAssemblies.Add("System.Data.dll");
            parameters.ReferencedAssemblies.Add("System.Data.DataSetExtensions.dll");
            parameters.ReferencedAssemblies.Add("System.Xml.dll");
            parameters.ReferencedAssemblies.Add("System.Xml.Linq.dll");

            parameters.CompilerOptions = "/optimize";
            var result = codeProvider.CompileAssemblyFromSource(parameters, source);

            return result;
        }
        public static CompilerResults CompileFromFile(string path)
        {
            var codeProvider = new CSharpCodeProvider();
            var parameters = new CompilerParameters
            {
                GenerateExecutable = false,
#if DEBUG
                IncludeDebugInformation = true,
#else
                IncludeDebugInformation = false,
#endif
            };
            parameters.ReferencedAssemblies.Add("Microsoft.Azure.Documents.Client.dll");
            parameters.ReferencedAssemblies.Add("Microsoft.CSharp.dll");
            parameters.ReferencedAssemblies.Add(@"D:\source\graphview\packages\Newtonsoft.Json.6.0.8\lib\net45\Newtonsoft.Json.dll");
            parameters.ReferencedAssemblies.Add("System.dll");
            parameters.ReferencedAssemblies.Add("System.Core.dll");
            parameters.ReferencedAssemblies.Add("System.Data.dll");
            parameters.ReferencedAssemblies.Add("System.Data.DataSetExtensions.dll");
            parameters.ReferencedAssemblies.Add("System.Xml.dll");
            parameters.ReferencedAssemblies.Add("System.Xml.Linq.dll");

            parameters.CompilerOptions = "/optimize";
            var result = codeProvider.CompileAssemblyFromFile(parameters, path);

            return result;
        }
        public static MatchGraph DocDB_ConstructGraph(WSelectQueryBlock query)
        {
            if (query == null || query.WhereClause.SearchCondition == null)
                return null;

            var edgeColumnToAliasesDict = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            var pathDictionary = new Dictionary<string, MatchPath>(StringComparer.OrdinalIgnoreCase);

            var unionFind = new UnionFind();
            var nodes = new Dictionary<string, MatchNode>(StringComparer.OrdinalIgnoreCase);
            var connectedSubGraphs = new List<ConnectedComponent>();
            var subGrpahMap = new Dictionary<string, ConnectedComponent>(StringComparer.OrdinalIgnoreCase);
            var parent = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            unionFind.Parent = parent;

            foreach (var cnt in query.SelectElements)
            {
                if (cnt == null) continue;
                var cnt2 = (cnt as WSelectScalarExpression).SelectExpr as WColumnReferenceExpression;
                if (cnt2 == null) continue;
                nodes.GetOrCreate(cnt2.MultiPartIdentifier.Identifiers[0].Value);
            }
            if (query.MatchClause != null)
            {
                if (query.MatchClause.Paths.Count > 0)
                {
                    foreach (var path in query.MatchClause.Paths)
                    {
                        var index = 0;
                        MatchEdge preEdge = null;
                        for (var count = path.PathEdgeList.Count; index < count; ++index)
                        {
                            var currentNodeTableRef = path.PathEdgeList[index].Item1;
                            var currentEdgeColumnRef = path.PathEdgeList[index].Item2;
                            var currentNodeExposedName = currentNodeTableRef.BaseIdentifier.Value;
                            var nextNodeTableRef = index != count - 1
                                ? path.PathEdgeList[index + 1].Item1
                                : path.Tail;
                            var nextNodeExposedName = nextNodeTableRef.BaseIdentifier.Value;
                            var patternNode = nodes.GetOrCreate(currentNodeExposedName);
                            if (patternNode.NodeAlias == null)
                            {
                                patternNode.NodeAlias = currentNodeExposedName;
                                patternNode.Neighbors = new List<MatchEdge>();
                                patternNode.External = false;

                            }

                            string edgeAlias = currentEdgeColumnRef.Alias;
                            if (edgeAlias == null)
                            {
                                bool isReversed = path.IsReversed;
                                var currentEdgeName = currentEdgeColumnRef.MultiPartIdentifier.Identifiers.Last().Value;
                                //var originalSourceName =
                                //    (_context[nextNodeExposedName] as WNamedTableReference).TableObjectName.BaseIdentifier.Value;
                                string originalEdgeName = null;

                                //if (isReversed)
                                //{
                                //    var i = currentEdgeName.IndexOf(originalSourceName, StringComparison.OrdinalIgnoreCase) +
                                //        originalSourceName.Length;
                                //    originalEdgeName = currentEdgeName.Substring(i + 1,
                                //        currentEdgeName.Length - "Reversed".Length - i - 1);
                                //}

                                edgeAlias = string.Format("{0}_{1}_{2}", currentNodeExposedName, currentEdgeName,
                                    nextNodeExposedName);

                                // when current edge is a reversed edge, the key should still be the original edge name
                                var edgeNameKey = isReversed ? originalEdgeName : currentEdgeName;
                                if (edgeColumnToAliasesDict.ContainsKey(edgeNameKey))
                                {
                                    edgeColumnToAliasesDict[edgeNameKey].Add(edgeAlias);
                                }
                                else
                                {
                                    edgeColumnToAliasesDict.Add(edgeNameKey, new List<string> { edgeAlias });
                                }
                            }

                            //Identifier edgeIdentifier = currentEdgeColumnRef.MultiPartIdentifier.Identifiers.Last();
                            //string schema = patternNode.NodeTableObjectName.SchemaIdentifier.Value.ToLower();
                            //string nodeTableName = patternNode.NodeTableObjectName.BaseIdentifier.Value;
                            //string bindTableName =
                            //    _context.EdgeNodeBinding[
                            //        new Tuple<string, string>(nodeTableName.ToLower(), edgeIdentifier.Value.ToLower())].ToLower();
                            MatchEdge edge;
                            if (currentEdgeColumnRef.MinLength == 1 && currentEdgeColumnRef.MaxLength == 1)
                            {
                                edge = new MatchEdge
                                {
                                    SourceNode = patternNode,
                                    EdgeColumn = currentEdgeColumnRef,
                                    EdgeAlias = edgeAlias,
                                    BindNodeTableObjName =
                                        new WSchemaObjectName(
                                            //new Identifier { Value = schema }
                                            //new Identifier { Value = bindTableName }
                                            ),
                                };
                                //_context.AddEdgeReference(edge);
                            }
                            else
                            {
                                MatchPath matchPath = new MatchPath
                                {
                                    SourceNode = patternNode,
                                    EdgeColumn = currentEdgeColumnRef,
                                    EdgeAlias = edgeAlias,
                                    BindNodeTableObjName =
                                        new WSchemaObjectName(
                                            //new Identifier { Value = schema }
                                            //new Identifier { Value = bindTableName }
                                            ),
                                    MinLength = currentEdgeColumnRef.MinLength,
                                    MaxLength = currentEdgeColumnRef.MaxLength,
                                    ReferencePathInfo = false,
                                    AttributeValueDict = currentEdgeColumnRef.AttributeValueDict
                                };
                                //_context.AddEdgeReference(matchPath);
                                pathDictionary[edgeAlias] = matchPath;
                                edge = matchPath;
                            }

                            if (preEdge != null)
                            {
                                preEdge.SinkNode = patternNode;
                            }
                            preEdge = edge;
                            if (!parent.ContainsKey(currentNodeExposedName))
                                parent[currentNodeExposedName] = currentNodeExposedName;
                            if (!parent.ContainsKey(nextNodeExposedName))
                                parent[nextNodeExposedName] = nextNodeExposedName;

                            unionFind.Union(currentNodeExposedName, nextNodeExposedName);


                            patternNode.Neighbors.Add(edge);

                        }
                        var tailExposedName = path.Tail.BaseIdentifier.Value;
                        var tailNode = nodes.GetOrCreate(tailExposedName);
                        if (tailNode.NodeAlias == null)
                        {
                            tailNode.NodeAlias = tailExposedName;
                            tailNode.Neighbors = new List<MatchEdge>();
                        }
                        if (preEdge != null)
                            preEdge.SinkNode = tailNode;
                    }

                }
            }
            // Puts nodes into subgraphs
            foreach (var node in nodes)
            {
                string root;

                /////////////////////////////////////////////////////////////
                
                root = "DocDB_graph";  // put them into the same graph
                
                var patternNode = node.Value;

                //update node's info
                //work when v1.0
                if (patternNode.NodeAlias == null)
                {
                    patternNode.NodeAlias = node.Key;
                    patternNode.Neighbors = new List<MatchEdge>();
                    patternNode.External = false;
                }

                if (!subGrpahMap.ContainsKey(root))
                {
                    var subGraph = new ConnectedComponent();
                    subGraph.Nodes[node.Key] = node.Value;
                    foreach (var edge in node.Value.Neighbors)
                    {
                        subGraph.Edges[edge.EdgeAlias] = edge;
                    }
                    subGrpahMap[root] = subGraph;
                    connectedSubGraphs.Add(subGraph);
                    subGraph.IsTailNode[node.Value] = false;
                }
                else
                {
                    var subGraph = subGrpahMap[root];
                    subGraph.Nodes[node.Key] = node.Value;
                    foreach (var edge in node.Value.Neighbors)
                    {
                        subGraph.Edges[edge.EdgeAlias] = edge;
                    }
                    subGraph.IsTailNode[node.Value] = false;
                }
            }

            var graph = new MatchGraph
            {
                ConnectedSubGraphs = connectedSubGraphs,
            };

            return graph;
        }

        private class UnionFind
        {
            public Dictionary<string, string> Parent;

            public string Find(string x)
            {
                string k, j, r;
                r = x;
                while (Parent[r] != r)
                {
                    r = Parent[r];
                }
                k = x;
                while (k != r)
                {
                    j = Parent[k];
                    Parent[k] = r;
                    k = j;
                }
                return r;
            }

            public void Union(string a, string b)
            {
                string aRoot = Find(a);
                string bRoot = Find(b);
                if (aRoot == bRoot)
                    return;
                Parent[aRoot] = bRoot;
            }
        }

        static void GetQuery(MatchNode node)
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
        public static List<DocDBMatchQuery> BuildFind(MatchGraph graph,string source,string sink)
        {
            Dictionary<string,int> map = new Dictionary<string, int>();
            int sum = 0;
            var nodes = graph.ConnectedSubGraphs[0].Nodes;
            foreach (var node in nodes)
            {
                GetQuery(node.Value);
                if (!map.ContainsKey(node.Value.NodeAlias))
                    map[node.Value.NodeAlias] = ++sum;
            }

            List<DocDBMatchQuery> ans = new List<DocDBMatchQuery>();
            ans.Add(new DocDBMatchQuery()
                    {
                        source_num = map[source],
                        sink_num = map[sink]
                    }
            );

            foreach (var node in nodes)
            {
                int edge_source_num = map[node.Value.NodeAlias];
                if (node.Value.Neighbors != null)
                {
                    for (int i = 0; i < node.Value.Neighbors.Count(); i++)
                    {
                        var edge = node.Value.Neighbors[i];
                        string edge_sink_alia = edge.SinkNode.NodeAlias;
                        int edge_sink_num = map[edge_sink_alia];
                        ans.Add(new DocDBMatchQuery()
                                {
                                    source_num = edge_source_num,
                                    sink_num = edge_sink_num,
                                    source_SelectClause = node.Value.DocDBQuery.Replace("'", "\\\""),
                                    sink_SelectClause = edge.SinkNode.DocDBQuery.Replace("'", "\\\""),
                                    source_alias = node.Value.NodeAlias,
                                    sink_alias = edge_sink_alia
                                }
                            );
                    }
                }
            }

            return ans ;
        }
    }
    public class DocDBMatchQuery
    {
        public int source_num { get; set; }
        public int sink_num { get; set; }
        public string source_SelectClause { get; set; }
        public string sink_SelectClause { get; set; }
        public string source_alias { get; set; }
        public string sink_alias { get; set; }

    }


    /*
     * Minghua's work
     */

    public class QueryComponent
    {
        //Database Client
        public static DocumentClient client;

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


        static public void init(int MaxPacketsize, GraphViewConnection conn)
        {
            MAX_PACKET_SIZE = MaxPacketsize;
            END_POINT_URL = conn.DocDB_Url;
            PRIMARY_KEY = conn.DocDB_Key;
            LinkZero.Add("Bindings", new HashSet<string>());
            client = conn.client;
        }
        static private IQueryable<dynamic> ExcuteQuery(string database, string collection, string script)
        {
            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };
            IQueryable<dynamic> Result = client.CreateDocumentQuery(
                    UriFactory.CreateDocumentCollectionUri(database, collection),
                    script,
                    queryOptions);
            return Result;
        }
        static public void ShowAll()
        {
            var all = ExcuteQuery("GroupMatch", "GraphSix", "SELECT * FROM ALL");
            foreach (var x in all) Console.Write(x);
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
                                InRangeScript += "\"" + id.ToString() + "\"" + ",";
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

                    // Query to determine which possible end nodes satisfied the WHERE Clause
                    string WhereClause = ParaPacket[index].sink_SelectClause;
                    string EndWhereScript = " " + WhereClause +
                        ((WhereClause.Substring(WhereClause.Length - 6, 5) == "Where") ? "" : " AND ") +  (InRangeScript.Length  == 0 ? ""
                           :ParaPacket[index].sink_alias + ".id IN (" + InRangeScript.Substring(0, InRangeScript.Length - 1) + ")");
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
                foreach (var y in x)
                    Console.WriteLine(y);
        }
    }

    public class DocDBDocumentCommand
    {
        public static void INSERT_EDGE(Dictionary<string,string> map, string Edge, string sourceid, string sinkid)
        {
            string source_str = map[sourceid];
            string sink_str = map[sinkid];
            var source_edge_num = GraphViewJsonCommand.get_edge_num(source_str);
            var sink_reverse_edge_num = GraphViewJsonCommand.get_reverse_edge_num(sink_str);

            Edge = GraphViewJsonCommand.insert_property(Edge, source_edge_num.ToString(), "_ID").ToString();
            Edge = GraphViewJsonCommand.insert_property(Edge, sink_reverse_edge_num.ToString(), "_reverse_ID").ToString();
            Edge = GraphViewJsonCommand.insert_property(Edge, '\"' + sinkid + '\"', "_sink").ToString();
            source_str = GraphViewJsonCommand.insert_edge(source_str, Edge, source_edge_num).ToString();
            //var new_source = JObject.Parse(source_str);

            Edge = GraphViewJsonCommand.insert_property(Edge, sink_reverse_edge_num.ToString(), "_ID").ToString();
            Edge = GraphViewJsonCommand.insert_property(Edge, source_edge_num.ToString(), "_reverse_ID").ToString();
            Edge = GraphViewJsonCommand.insert_property(Edge, '\"' + sourceid + '\"', "_sink").ToString();
            sink_str = GraphViewJsonCommand.insert_reverse_edge(sink_str, Edge, sink_reverse_edge_num).ToString();
            //var new_sink = JObject.Parse(sink_str);

            map[sourceid] = source_str;
            map[sinkid] = sink_str;

            //await conn.client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(conn.DocDB_DatabaseId, conn.DocDB_CollectionId, sourceid), new_source);
            //await conn.client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(conn.DocDB_DatabaseId, conn.DocDB_CollectionId, sinkid), new_sink);
        }

        public static async Task Delete_Node(GraphViewConnection conn, string id)
        {
            var docLink = string.Format("dbs/{0}/colls/{1}/docs/{2}", conn.DocDB_DatabaseId, conn.DocDB_CollectionId, id);
            await conn.client.DeleteDocumentAsync(docLink);
        }

        public static async Task ReplaceDocument(GraphViewConnection conn, string Documentid, string DocumentString)
        {
            var new_source = JObject.Parse(DocumentString);
            await conn.client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(conn.DocDB_DatabaseId, conn.DocDB_CollectionId, Documentid), new_source);
            
        }
    }

}
