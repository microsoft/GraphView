// GraphView
// 
// Copyright (c) 2015 Microsoft Corporation
// 
// All rights reserved. 
// 
// MIT License
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
// 
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GraphView
{
    public abstract partial class WDataModificationSpecification : WSqlStatement
    {
        // Omit the OUTPUT clause
        public WTableReference Target { get; set; }
        public WTopRowFilter TopRowFilter { get; set; }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (Target != null)
                Target.Accept(visitor);
            if (TopRowFilter != null)
                TopRowFilter.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public abstract partial class WUpdateDeleteSpecificationBase : WDataModificationSpecification
    {
        public WFromClause FromClause { get; set; }
        public WWhereClause WhereClause { get; set; }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (FromClause != null)
                FromClause.Accept(visitor);
            if (WhereClause != null)
                WhereClause.Accept(visitor);
            base.AcceptChildren(visitor);
        }
    }

    public partial class WInsertSpecification : WDataModificationSpecification
    {
        public InsertOption InsertOption { get; set; }
        public WInsertSource InsertSource { get; set; }

        public IList<WColumnReferenceExpression> Columns { get; set; }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0}INSERT INTO {1}", indent, Target.ToString());
            if (Columns != null && Columns.Count > 0)
            {
                sb.AppendFormat(" ({0}", Columns[0].ToString(indent));
                for (var i = 1; i < Columns.Count; ++i)
                {
                    sb.AppendFormat(", {0}", Columns[i].ToString(indent));
                }
                sb.Append(")");
            }
            sb.Append("\r\n");
            sb.Append(InsertSource.ToString(indent));

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (InsertSource != null)
                InsertSource.Accept(visitor);

            var index = 0;
            if (Columns!=null)
                for (var count = Columns.Count; index < count; ++index)
                    Columns[index].Accept(visitor);

            base.AcceptChildren(visitor);
        }
    }

    public partial class WInsertNodeSpecification : WInsertSpecification
    {
        public WInsertNodeSpecification(WInsertSpecification insertSpec)
        {
            InsertOption = insertSpec.InsertOption;
            InsertSource = insertSpec.InsertSource;
            FirstTokenIndex = insertSpec.FirstTokenIndex;
            LastTokenIndex = insertSpec.LastTokenIndex;
            TopRowFilter = insertSpec.TopRowFilter;
            Target = insertSpec.Target;
            Columns = new List<WColumnReferenceExpression>();
            foreach (var col in insertSpec.Columns)
                Columns.Add(col);
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        //need : Url , Key , DatabaseID , CollectionID
        public override string ToDocDbScript(GraphViewConnection docDbConnection)
        {
            string Json_str = "{}";
            var cnt = InsertSource as WValuesInsertSource;
            for (int i = 0; i < Columns.Count(); i++)
            {
                string s1 = Columns[i].MultiPartIdentifier.Identifiers[0].Value;
                var cnt2 = (cnt.RowValues[0].ColumnValues[i] as WValueExpression);
                string s2 =cnt2.Value;
                if (cnt2.SingleQuoted)
                    s2 = '\"' + s2 + '\"';

                if (s2[0] == '@') return "";
                Json_str = GraphViewJsonCommand.insert_property(Json_str, s2, s1).ToString();
            }
            Json_str = GraphViewJsonCommand.insert_property(Json_str, "[]", "_edge").ToString();
            Json_str = GraphViewJsonCommand.insert_property(Json_str, "[]", "_reverse_edge").ToString();
            Json_str = Json_str.Replace("\"", "\"\"");

            var InsertNodeGenerator = new DocDBInsertNodeTemplate()
            {
                json_str = Json_str,
                EndpointUrl = docDbConnection.DocDB_Url,
                AuthorizationKey = docDbConnection.DocDB_Key,
                DatabaseID = docDbConnection.DocDB_DatabaseId,
                CollectionID = docDbConnection.DocDB_CollectionId
            };

            string code = InsertNodeGenerator.TransformText();

            return code;
        }
        public override async Task RunDocDbScript(GraphViewConnection docDbConnection)
        {
            string Json_str = "{}";
            docDbConnection.DocDB_finish = false;

            var cnt = InsertSource as WValuesInsertSource;
            for (int i = 0; i < Columns.Count(); i++)
            {
                string s1 = Columns[i].MultiPartIdentifier.Identifiers[0].Value;
                var cnt2 = (cnt.RowValues[0].ColumnValues[i] as WValueExpression);
                string s2 = cnt2.Value;
                if (cnt2.SingleQuoted)
                    s2 = '\"' + s2 + '\"';
                Json_str = GraphViewJsonCommand.insert_property(Json_str, s2, s1).ToString();
            }
            Json_str = GraphViewJsonCommand.insert_property(Json_str, "[]", "_edge").ToString();
            Json_str = GraphViewJsonCommand.insert_property(Json_str, "[]", "_reverse_edge").ToString();

            var obj = JObject.Parse(Json_str);
            await docDbConnection.client.CreateDocumentAsync("dbs/" + docDbConnection.DocDB_DatabaseId + "/colls/" + docDbConnection.DocDB_CollectionId, obj);
            docDbConnection.DocDB_finish = true;
        }
    }

    public partial class WInsertEdgeSpecification : WInsertSpecification
    {
        public WSelectInsertSource SelectInsertSource { get; set; }
        public WColumnReferenceExpression EdgeColumn { get; set; }

        public WInsertEdgeSpecification(WInsertSpecification insertSpec)
        {
            SelectInsertSource = insertSpec.InsertSource as WSelectInsertSource;
            if (SelectInsertSource == null)
            {
                throw new SyntaxErrorException("The insert source of the INSERT EDGE statement must be a SELECT statement.");
            }

            InsertOption = insertSpec.InsertOption;
            FirstTokenIndex = insertSpec.FirstTokenIndex;
            LastTokenIndex = insertSpec.LastTokenIndex;
            TopRowFilter = insertSpec.TopRowFilter;
            Target = insertSpec.Target;
            Columns = new List<WColumnReferenceExpression>();
            foreach (var col in insertSpec.Columns)
                Columns.Add(col);
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (SelectInsertSource != null)
                SelectInsertSource.Accept(visitor);
            base.AcceptChildren(visitor);
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0}INSERT EDGE INTO {1}.{2}\r\n", indent, Target.ToString(), EdgeColumn.ToString());
            sb.Append(SelectInsertSource.ToString(indent));
            return sb.ToString();
        }

        public override string ToDocDbScript(GraphViewConnection docDbConnection)
        {
            var DocDB_graph = new MatchGraph();
            var SelectQueryBlock = SelectInsertSource.Select as WSelectQueryBlock;
            DocDB_graph = GraphViewDocDBCommand.DocDB_ConstructGraph(SelectQueryBlock);

            var InsertEdgeGenerator = new DocDBInsertEdgeTemplate()
            {
                EndpointUrl = docDbConnection.DocDB_Url,
                AuthorizationKey = docDbConnection.DocDB_Key,
                DatabaseID = docDbConnection.DocDB_DatabaseId,
                CollectionID = docDbConnection.DocDB_CollectionId,
                SelectQuery = new List<DocDBSelectQuery>()
            };

            var attachPredicateVisitor = new AttachWhereClauseVisitor();
            var _context = new WSqlTableContext();
            var _graphMetaData = new GraphMetaData();
            var columnTableMapping = _context.GetColumnToAliasMapping(_graphMetaData.ColumnsOfNodeTables);
            if (SelectQueryBlock != null)
                attachPredicateVisitor.Invoke(SelectQueryBlock.WhereClause, DocDB_graph, columnTableMapping);
            
            var query_nodes = DocDB_graph.ConnectedSubGraphs[0].Nodes;
            foreach (var query_node in query_nodes)
            {
                string predicate = "";
                if (query_node.Value.Predicates != null)
                {
                    for (int i = 0; i < query_node.Value.Predicates.Count(); i++)
                    {
                        if (i != 0) predicate += " and ";
                        predicate += query_node.Value.Predicates[i].ToString();
                    }
                    predicate = predicate.Replace("\'", "\\\"");
                }
                InsertEdgeGenerator.SelectQuery.Add(new DocDBSelectQuery(query_node.Key , predicate));
            }

            //build up Edge(string)
            string Edge = "{}";
            Edge = GraphViewJsonCommand.insert_property(Edge, "", "_ID").ToString();
            Edge = GraphViewJsonCommand.insert_property(Edge, "", "_reverse_ID").ToString();
            Edge = GraphViewJsonCommand.insert_property(Edge, "", "_sink").ToString();

            var Columns = this.Columns;
            var Values = new List<WValueExpression>();
            var source = "";
            var sink = "";

            foreach (var SelectElement in SelectQueryBlock.SelectElements)
            {
                var SelectScalar = SelectElement as WSelectScalarExpression;
                if (SelectScalar != null)
                {
                    if (SelectScalar.SelectExpr is WValueExpression)
                    {
                        var ValueExpression = SelectScalar.SelectExpr as WValueExpression;
                        Values.Add(ValueExpression);
                    }
                    else if (SelectScalar.SelectExpr is WColumnReferenceExpression)
                    {
                        var ColumnReferenceExpression = SelectScalar.SelectExpr as WColumnReferenceExpression;
                        if (source == "") source = ColumnReferenceExpression.ToString();
                        else
                        {
                            if(sink == "")
                                sink = ColumnReferenceExpression.ToString();
                        }
                    }
                }
            }
            if (Values.Count() != Columns.Count())
                throw new SyntaxErrorException("Columns and Values not match");

            for (var index = 0; index < Columns.Count(); index++)
            {
                Edge = GraphViewJsonCommand.insert_property(Edge, Values[index].ToString(),
                        Columns[index].ToString()).ToString();
            }
            

            if (SelectQueryBlock.MatchClause != null)
            {
                var Find = GraphViewDocDBCommand.BuildFind(DocDB_graph, source, sink);
                QueryComponent.init(50, docDbConnection);
                Dictionary<string,string> map = new Dictionary<string, string>();
                foreach (var x in QueryComponent.ExtractPairs(Find, 50))
                    foreach (var y in x)
                    {
                        if (!map.ContainsKey(y.Item1))
                        {
                            var documents =
                                docDbConnection.client.CreateDocumentQuery(
                                    "dbs/" + docDbConnection.DocDB_DatabaseId + "/colls/" +
                                    docDbConnection.DocDB_CollectionId,
                                    "SELECT * " +
                                    string.Format("FROM doc WHERE doc.id = \"{0}\"", y.Item1));
                            foreach (var doc in documents)
                                map[y.Item1] = JsonConvert.SerializeObject(doc);
                        }
                        if (!map.ContainsKey(y.Item2))
                        {
                            var documents =
                                docDbConnection.client.CreateDocumentQuery(
                                    "dbs/" + docDbConnection.DocDB_DatabaseId + "/colls/" +
                                    docDbConnection.DocDB_CollectionId,
                                    "SELECT * " +
                                    string.Format("FROM doc WHERE doc.id = \"{0}\"", y.Item2));
                            foreach (var doc in documents)
                                map[y.Item2] = JsonConvert.SerializeObject(doc);
                        }
                        DocDBDocumentCommand.INSERT_EDGE(map, Edge, y.Item1, y.Item2);
                    }
                foreach (var cnt in map)
                {
                    DocDBDocumentCommand.ReplaceDocument(docDbConnection, cnt.Key, cnt.Value);
                }
            }
            




            

            Edge = Edge.Replace("\"", "\"\"");
            InsertEdgeGenerator.Edge = Edge;
            InsertEdgeGenerator.source = source;
            InsertEdgeGenerator.sink = sink;

            string code = InsertEdgeGenerator.TransformText();

            return code;
        }
        public override async Task RunDocDbScript(GraphViewConnection docDbConnection)
        {
            docDbConnection.DocDB_finish = false;

            var DocDB_graph = new MatchGraph();
            var SelectQueryBlock = SelectInsertSource.Select as WSelectQueryBlock;
            DocDB_graph = GraphViewDocDBCommand.DocDB_ConstructGraph(SelectQueryBlock);

            var attachPredicateVisitor = new AttachWhereClauseVisitor();
            var _context = new WSqlTableContext();
            var _graphMetaData = new GraphMetaData();
            var columnTableMapping = _context.GetColumnToAliasMapping(_graphMetaData.ColumnsOfNodeTables);
            if (SelectQueryBlock != null)
                attachPredicateVisitor.Invoke(SelectQueryBlock.WhereClause, DocDB_graph, columnTableMapping);

            //build up Edge(string)
            string Edge = "{}";
            Edge = GraphViewJsonCommand.insert_property(Edge, "", "_ID").ToString();
            Edge = GraphViewJsonCommand.insert_property(Edge, "", "_reverse_ID").ToString();
            Edge = GraphViewJsonCommand.insert_property(Edge, "", "_sink").ToString();

            var Columns = this.Columns;
            var Values = new List<WValueExpression>();
            var source = "";
            var sink = "";

            foreach (var SelectElement in SelectQueryBlock.SelectElements)
            {
                var SelectScalar = SelectElement as WSelectScalarExpression;
                if (SelectScalar != null)
                {
                    if (SelectScalar.SelectExpr is WValueExpression)
                    {
                        var ValueExpression = SelectScalar.SelectExpr as WValueExpression;
                        Values.Add(ValueExpression);
                    }
                    else if (SelectScalar.SelectExpr is WColumnReferenceExpression)
                    {
                        var ColumnReferenceExpression = SelectScalar.SelectExpr as WColumnReferenceExpression;
                        if (source == "") source = ColumnReferenceExpression.ToString();
                        else
                        {
                            if (sink == "")
                                sink = ColumnReferenceExpression.ToString();
                        }
                    }
                }
            }
            if (Values.Count() != Columns.Count())
                throw new SyntaxErrorException("Columns and Values not match");

            for (var index = 0; index < Columns.Count(); index++)
            {
                Edge = GraphViewJsonCommand.insert_property(Edge, Values[index].ToString(),
                        Columns[index].ToString()).ToString();
            }

            if (SelectQueryBlock.MatchClause != null)
            {
                var Find = GraphViewDocDBCommand.BuildFind(DocDB_graph, source, sink);
                QueryComponent.init(50, docDbConnection);
                Dictionary<string, string> map = new Dictionary<string, string>();
                foreach (var x in QueryComponent.ExtractPairs(Find, 50))
                    foreach (var y in x)
                    {
                        if (!map.ContainsKey(y.Item1))
                        {
                            var documents =
                                docDbConnection.client.CreateDocumentQuery(
                                    "dbs/" + docDbConnection.DocDB_DatabaseId + "/colls/" +
                                    docDbConnection.DocDB_CollectionId,
                                    "SELECT * " +
                                    string.Format("FROM doc WHERE doc.id = \"{0}\"", y.Item1));
                            foreach (var doc in documents)
                                map[y.Item1] = JsonConvert.SerializeObject(doc);
                        }
                        if (!map.ContainsKey(y.Item2))
                        {
                            var documents =
                                docDbConnection.client.CreateDocumentQuery(
                                    "dbs/" + docDbConnection.DocDB_DatabaseId + "/colls/" +
                                    docDbConnection.DocDB_CollectionId,
                                    "SELECT * " +
                                    string.Format("FROM doc WHERE doc.id = \"{0}\"", y.Item2));
                            foreach (var doc in documents)
                                map[y.Item2] = JsonConvert.SerializeObject(doc);
                        }
                        DocDBDocumentCommand.INSERT_EDGE(map, Edge, y.Item1, y.Item2);
                    }
                foreach (var cnt in map)
                    await DocDBDocumentCommand.ReplaceDocument(docDbConnection, cnt.Key, cnt.Value);
            }
            docDbConnection.DocDB_finish = true;
        }
    }

    
    
       
    


public partial class WDeleteSpecification : WUpdateDeleteSpecificationBase
    {
        public WDeleteSpecification()
        {
            WhereClause = new WWhereClause();
        }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder(128);

            sb.AppendFormat("{0}DELETE", indent);
            if (TopRowFilter != null)
            {
                if (TopRowFilter.OneLine())
                {
                    sb.Append(TopRowFilter.ToString(""));
                }
                else
                {
                    sb.Append("\r\n");
                    sb.Append(TopRowFilter.ToString(indent + "  "));
                }
            }

            sb.AppendFormat(" FROM {0}", Target.ToString());

            if (FromClause != null)
            {
                sb.Append("\r\n");
                sb.Append(FromClause.ToString(indent));
            }

            if (WhereClause != null && WhereClause.SearchCondition != null)
            {
                sb.Append("\r\n");
                sb.Append(WhereClause.ToString(indent));
            }

            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }
    }

    public partial class WDeleteNodeSpecification : WDeleteSpecification
    {
        public WDeleteNodeSpecification(WDeleteSpecification deleteSpec)
        {
            FirstTokenIndex = deleteSpec.FirstTokenIndex;
            LastTokenIndex = deleteSpec.LastTokenIndex;
            TopRowFilter = deleteSpec.TopRowFilter;
            Target = deleteSpec.Target;
            WhereClause = deleteSpec.WhereClause;
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        internal override bool OneLine()
        {
            return WhereClause == null;
        }

        internal override string ToString(string indent)
        {
            StringBuilder sb = new StringBuilder(128);

            sb.AppendFormat("{0}DELETE NODE", indent);
            if (TopRowFilter != null)
            {
                if (TopRowFilter.OneLine())
                {
                    sb.Append(TopRowFilter.ToString(""));
                }
                else
                {
                    sb.Append("\r\n");
                    sb.Append(TopRowFilter.ToString(indent + "  "));
                }
            }
            sb.AppendFormat(" FROM {0}", Target.ToString());

            if (FromClause != null)
            {
                sb.Append("\r\n");
                sb.Append(FromClause.ToString(indent));
            }

            if (WhereClause != null && WhereClause.SearchCondition != null)
            {
                sb.Append("\r\n");
                sb.Append(WhereClause.ToString(indent));
            }

            return sb.ToString();
        }

        public override string ToDocDbScript(GraphViewConnection docDbConnection)
        {
            var DeleteNodeGenerator = new DocDBDeleteNodeTemplate()
            {
                EndpointUrl = docDbConnection.DocDB_Url,
                AuthorizationKey = docDbConnection.DocDB_Key,
                DatabaseID = docDbConnection.DocDB_DatabaseId,
                CollectionID = docDbConnection.DocDB_CollectionId,
                search = WhereClause.SearchCondition
            };
            string code = DeleteNodeGenerator.TransformText();
            
            //QueryComponent.SelectProcessor(,,)

            return code;
        }
    }

    public partial class WDeleteEdgeSpecification : WDeleteSpecification
    {
        public WSelectQueryBlock SelectDeleteExpr { get; set; }
        public WEdgeColumnReferenceExpression EdgeColumn { get; set; } 
        public WDeleteEdgeSpecification(WSelectQueryBlock deleteSpec)
        {
            SelectDeleteExpr = deleteSpec;
            //FromClause = new WFromClause
            //{
            //    TableReferences = new List<WTableReference>
            //    {
            //        new WQueryDerivedTable
            //        {
            //            QueryExpr = deleteSpec
            //        }
            //    }
            //};
            EdgeColumn = deleteSpec.MatchClause.Paths[0].PathEdgeList[0].Item2;
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {

            if (EdgeColumn != null)
                EdgeColumn.Accept(visitor);
            if (SelectDeleteExpr != null)
                SelectDeleteExpr.Accept(visitor);
            base.AcceptChildren(visitor);
        }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            StringBuilder sb = new StringBuilder(128);

            WSelectElement sourceElement = SelectDeleteExpr.SelectElements[0];
            WSelectElement sinkElement = SelectDeleteExpr.SelectElements[1];
            sb.AppendFormat("{0}DELETE EDGE {1}-{2}->{3}",
                indent,
                sourceElement.ToString(""),
                EdgeColumn.ToString(""),
                sinkElement.ToString(""));

            sb.Append("\r\n");
            sb.Append(SelectDeleteExpr.FromClause.ToString(indent));

            // For the DELETE EDGE statement, the first path in the parsed MATCH clause is 
            // the one-hop path, i.e., the edge, to be deleted. 
            if (SelectDeleteExpr.MatchClause.Paths.Count > 1)
            {
                sb.Append("\r\n");
                sb.AppendFormat("{0}MATCH {1}", indent, SelectDeleteExpr.MatchClause.Paths[1].ToString(""));

                for (int i = 2; i < SelectDeleteExpr.MatchClause.Paths.Count; i++)
                {
                    sb.Append("\r\n");
                    sb.AppendFormat("  {0}{1}", indent, SelectDeleteExpr.MatchClause.Paths[i].ToString(""));
                }
            }

            if (SelectDeleteExpr.WhereClause != null && SelectDeleteExpr.WhereClause.SearchCondition != null)
            {
                sb.Append("\r\n");
                sb.Append(SelectDeleteExpr.WhereClause.ToString(indent));
            }

            return sb.ToString();
        }

        public override string ToDocDbScript(GraphViewConnection docDbConnection)
        {
            var DocDB_graph = new MatchGraph();
            var SelectQueryBlock = SelectDeleteExpr as WSelectQueryBlock;
            DocDB_graph = GraphViewDocDBCommand.DocDB_ConstructGraph(SelectQueryBlock);

            var DeleteEdgeGenerator = new DocDBDeleteEdgeTemplate()
            {
                EndpointUrl = docDbConnection.DocDB_Url,
                AuthorizationKey = docDbConnection.DocDB_Key,
                DatabaseID = docDbConnection.DocDB_DatabaseId,
                CollectionID = docDbConnection.DocDB_CollectionId,
                SelectQuery = new List<DocDBSelectQuery>()
            };

            var attachPredicateVisitor = new AttachWhereClauseVisitor();
            var _context = new WSqlTableContext();
            var _graphMetaData = new GraphMetaData();
            var columnTableMapping = _context.GetColumnToAliasMapping(_graphMetaData.ColumnsOfNodeTables);
            if (SelectQueryBlock != null)
                attachPredicateVisitor.Invoke(SelectQueryBlock.WhereClause, DocDB_graph, columnTableMapping);

            var query_nodes = DocDB_graph.ConnectedSubGraphs[0].Nodes;
            foreach (var query_node in query_nodes)
            {
                string DocDB_from = "Select ";
                string DocDB_select = "From ";
                string DocDB_where = "";
                if (query_node.Value.Predicates != null)
                {
                    DocDB_where = "WHERE ";

                    var node = query_node.Value;
                    for (int i = 0; i < node.Predicates.Count(); i++)
                    {
                        if (i != 0) DocDB_where += " and ";
                        DocDB_where += "(" + query_node.Value.Predicates[i].ToString() + ")";
                    }

                    DocDB_select += query_node.Key + " As Doc " ;
                    DocDB_from += query_node.Key + " ";

                    if (node.Neighbors != null)
                    {
                        for (int i = 0; i < node.Neighbors.Count(); i++)
                        {
                            DocDB_select += ", " + node.Neighbors[i].EdgeAlias + " As Edge ";
                            DocDB_from += "Join " + node.Neighbors[i].EdgeAlias + " in " + node.NodeAlias + "._edge ";
                            if (node.Neighbors[i].Predicates != null)
                            {
                                if (DocDB_where != "")
                                    DocDB_where += " and ";
                                DocDB_where += "( ";
                                for (int j = 0; j < node.Neighbors[i].Predicates.Count(); j++)
                                {
                                    if (j != 0) DocDB_where += " and ";
                                    DocDB_where += node.Neighbors[i].Predicates[j].ToString();
                                }
                                DocDB_where += " ) ";
                            }
                        }
                    }
                    DocDB_where = DocDB_where.Replace("\'", "\\\"");
                }
                DeleteEdgeGenerator.SelectQuery.Add(new DocDBSelectQuery(query_node.Key,DocDB_select,DocDB_from, DocDB_where, query_node.Value));
            }
            var source = "";
            var sink = "";

            foreach (var SelectElement in SelectQueryBlock.SelectElements)
            {
                var SelectScalar = SelectElement as WSelectScalarExpression;
                if (SelectScalar != null)
                {
                    if (SelectScalar.SelectExpr is WColumnReferenceExpression)
                    {
                        var ColumnReferenceExpression = SelectScalar.SelectExpr as WColumnReferenceExpression;
                        if (source == "") source = ColumnReferenceExpression.ToString();
                        else sink = ColumnReferenceExpression.ToString();
                    }
                }
            }
            DeleteEdgeGenerator.source = source;
            DeleteEdgeGenerator.sink = sink;

            

            return "";
        }
    }

    public partial class WUpdateSpecification : WUpdateDeleteSpecificationBase
    {
        public IList<WSetClause> SetClauses { get; set; }

        public WUpdateSpecification()
        {
            WhereClause = new WWhereClause();
        }

        internal override bool OneLine()
        {
            return false;
        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0}UPDATE ", indent);
            if (TopRowFilter != null)
                sb.Append(TopRowFilter.ToString(indent));

            sb.AppendFormat(" {0} SET \r\n", Target.ToString(indent));
            
            var first = true;
            if (SetClauses != null)
            {
                foreach (var setclause in SetClauses)
                {
                    if (first)
                    {
                        first = false;
                        sb.AppendFormat("{0}{1}", indent, setclause.ToString(indent));
                    }
                    else
                    {
                        sb.AppendFormat(", {0}", setclause.ToString(indent));
                    }
                }
            }

            if (FromClause != null)
                sb.AppendFormat("\r\n{0}{1}", indent, FromClause.ToString(indent));
            if (WhereClause != null && WhereClause.SearchCondition!=null) 
                sb.AppendFormat("\r\n{0}{1}", indent, WhereClause.ToString(indent));
            return sb.ToString();
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {
            if (SetClauses != null)
            {
                var index = 0;
                for (var count = SetClauses.Count; index < count; ++index)
                    SetClauses[index].Accept(visitor);
            }

            base.AcceptChildren(visitor);
        }
    }

}