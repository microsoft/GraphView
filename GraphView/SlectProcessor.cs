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
    using BindingSet = HashSet<string>;
    using PathStatue = Tuple<Dictionary<string, int>, HashSet<string>>;
    public class DocumentDBConnection
    {
        DocumentDBConnection(int pMaxPacketSize, GraphViewConnection conn)
        {
            MaxPacketSize = pMaxPacketSize;
            EndPointUrl = conn.DocDB_Url;
            PrimaryKey = conn.DocDB_Key;
            client = conn.client;
            DatabaseID = conn.DocDB_DatabaseId;
            CollectionID = conn.DocDB_CollectionId;
        }
        public int MaxPacketSize;
        public string EndPointUrl;
        public string PrimaryKey;
        public DocumentClient client;
        public string DatabaseID;
        public string CollectionID;
    }
    public class QueryItem
    {
    }
    public class NodeQuery : QueryItem
    {
        public NodeQuery(Dictionary<string, int> GraphDescription, MatchNode node)
        {
            NodeNum = GraphDescription[node.NodeAlias];
            NodeAlias = node.NodeAlias;
            NodeSlectClause = node.DocDBQuery.Replace("'", "\"");
        }
        int NodeNum;
        string NodeAlias;
        string NodeSlectClause;
    }
    public class LinkQuery : QueryItem
    {
        public LinkQuery(Dictionary<string, int> GraphDescription, MatchNode Src, MatchNode Dest, string EdgeAlias)
        {
            SrcNum = GraphDescription[Src.NodeAlias];
            SrcAlias = Src.NodeAlias;
            SrcSelectClause = Src.DocDBQuery.Replace("'", "\"");
            DestNum = GraphDescription[Dest.NodeAlias];
            DestAlias = Dest.NodeAlias;
            DestSelectClause = Dest.DocDBQuery.Replace("'", "\"");
            EdgeSpecifier = EdgeAlias;
            EdgeList = new List<string>();
            foreach (var EdgeToNeighbor in Src.Neighbors)
            {
                if (EdgeToNeighbor.SinkNode.NodeAlias == DestAlias)
                    EdgeList.Add(EdgeToNeighbor.EdgeAlias);
            }
        }
        int SrcNum;
        string SrcSelectClause;
        string SrcAlias;
        int DestNum;
        string DestSelectClause;
        string DestAlias;
        List<string> EdgeList;
        string EdgeSpecifier;
    }
    public class QuerySpec
    {
        public void add(QueryItem line)
        {
            Lines.Add(line);
        }
        List<QueryItem> Lines;
    }
    public class SelectProcessor
    {
        private DocumentDBConnection connection;
        private MatchGraph QueryGraph;
        private Dictionary<string, int> GraphDescription;
        private WSelectQueryBlock SelectQueryBlock;
        private QuerySpec Spec;
        private Dictionary<string, MatchNode> NodeTable;
        private IQueryable<dynamic> ExcuteQuery(string script)
        {
            FeedOptions QueryOptions = new FeedOptions { MaxItemCount = -1 };
            IQueryable<dynamic> QueryResult = connection.client.CreateDocumentQuery(
                UriFactory.CreateDocumentCollectionUri(connection.DatabaseID, connection.CollectionID),
                script,
                QueryOptions);
            return QueryResult;
        }
        private void ConstructSelectGraph()
        {
            QueryGraph = GraphViewDocDBCommand.DocDB_ConstructGraph(SelectQueryBlock);
            NodeTable = QueryGraph.ConnectedSubGraphs[0].Nodes;
            var AttachPredicateVisitor = new AttachWhereClauseVisitor();
            var TableContext = new WSqlTableContext();
            var GraphMeta = new GraphMetaData();
            var columnTableMapping = TableContext.GetColumnToAliasMapping(GraphMeta.ColumnsOfNodeTables);
            if (SelectQueryBlock != null) AttachPredicateVisitor.Invoke(SelectQueryBlock.WhereClause, QueryGraph, columnTableMapping);
            int GroupNumber = 0;
            foreach (var node in NodeTable)
            {
                GraphViewDocDBCommand.GetQuery(node.Value);
                if (!GraphDescription.ContainsKey(node.Value.NodeAlias))
                    GraphDescription[node.Value.NodeAlias] = ++GroupNumber;
            }
        }
        private void QuerySpecGenerator()
        {
            Spec = new QuerySpec();
            foreach (var node in NodeTable)
            {
                if (node.Value.Neighbors.Count != 0)
                    foreach (var neighbor in node.Value.Neighbors)
                        Spec.add(new LinkQuery(GraphDescription, node.Value, neighbor.SinkNode, neighbor.EdgeAlias));
                else Spec.add(new NodeQuery(GraphDescription, node.Value));
            }
        }
        public Class1()
        {
        }
    }
}
