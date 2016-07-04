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
            if (query == null)
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
                if (cnt is WSelectStarExpression) continue;
                if (cnt == null) continue;
                var cnt2 = (cnt as WSelectScalarExpression).SelectExpr as WColumnReferenceExpression;
                if (cnt2 == null) continue;
                //nodes.GetOrCreate(cnt2.MultiPartIdentifier.Identifiers[0].Value);
            }
            if (query.FromClause != null)
            {
                foreach (WTableReferenceWithAlias cnt in query.FromClause.TableReferences)
                {
                    nodes.GetOrCreate(cnt.Alias.Value);
                }
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
                if (node.Value.Neighbors.Count != 0)
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
                                    source_SelectClause = node.Value.DocDBQuery.Replace("'", "\""),
                                    sink_SelectClause = edge.SinkNode.DocDBQuery.Replace("'", "\""),
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
        public string neighbor_edge { get; set; }
        public List<string> edge_alias { get; set; }
    }


    /*
     * Minghua's work
     */


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