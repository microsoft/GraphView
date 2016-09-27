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
            if (Columns != null)
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

        /// <summary>
        /// Construct a Json's string which contains all the information about the new node.
        /// </summary>
        /// <returns></returns>
        public string ConstructNode()
        {
            string Json_str = "{}";

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
            //Insert "_edge" & "_reverse_edge" into the string.
            Json_str = GraphViewJsonCommand.insert_property(Json_str, "[]", "_edge").ToString();
            Json_str = GraphViewJsonCommand.insert_property(Json_str, "[]", "_reverse_edge").ToString();

            return Json_str;
        }

        /// <summary>
        /// Construct a Json's string which contains all the information about the new node.
        /// And then Create a InsertNodeOperator with this string
        /// </summary>
        /// <param name="docDbConnection">The Connection</param>
        /// <returns></returns>
        internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        {
            string Json_str = ConstructNode();

            InsertNodeOperator InsertOp = new InsertNodeOperator(dbConnection,Json_str);

            return InsertOp;
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

        /// <summary>
        /// Construct an edge's string with all informations.
        /// </summary>
        /// <returns></returns>
        public string ConstructEdge()
        {
            var SelectQueryBlock = SelectInsertSource.Select as WSelectQueryBlock;

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

            //Add properties to Edge
            for (var index = 0; index < Columns.Count(); index++)
            {
                Edge = GraphViewJsonCommand.insert_property(Edge, Values[index].ToString(),
                        Columns[index].ToString()).ToString();
            }
            return Edge;
        }

        internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        {
            var SelectQueryBlock = SelectInsertSource.Select as WSelectQueryBlock;
            
            string Edge = ConstructEdge();

            //Add "id" after each identifier
            var iden = new Identifier();
            iden.Value = "id";
            var dic_iden = new Identifier();
            dic_iden.Value = "doc";

            var n1 = SelectQueryBlock.SelectElements[0] as WSelectScalarExpression;
            var identifiers1 = (n1.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers;
            identifiers1.Add(iden);

            var n2 = SelectQueryBlock.SelectElements[1] as WSelectScalarExpression;
            var identifiers2 = (n2.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers;
            identifiers2.Add(iden);

            var n3 = new WSelectScalarExpression(); SelectQueryBlock.SelectElements.Add(n3);
            var n3_SelectExpr = new WColumnReferenceExpression();
            n3.SelectExpr = n3_SelectExpr;
            n3_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n3_SelectExpr.MultiPartIdentifier.Identifiers.Add((n1.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers[0]);
            n3_SelectExpr.MultiPartIdentifier.Identifiers.Add(dic_iden);

            var n4 = new WSelectScalarExpression(); SelectQueryBlock.SelectElements.Add(n4);
            var n4_SelectExpr = new WColumnReferenceExpression();
            n4.SelectExpr = n4_SelectExpr;
            n4_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n4_SelectExpr.MultiPartIdentifier.Identifiers.Add((n2.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers[0]);
            n4_SelectExpr.MultiPartIdentifier.Identifiers.Add(dic_iden);

            GraphViewExecutionOperator input = SelectQueryBlock.Generate(dbConnection);
            if (input == null)
                throw new GraphViewException("The insert source of the INSERT EDGE statement is invalid.");
            
            InsertEdgeOperator InsertOp = new InsertEdgeOperator(dbConnection, input, Edge, n1.ToString(), n2.ToString());
            
            return InsertOp;
        }
    }



    public partial class WInsertEdgeFromTwoSourceSpecification : WInsertSpecification
    {
        public WSelectInsertSource SrcInsertSource { get; set; }
        public WSelectQueryBlock DestInsertSource { get; set; }

        public WColumnReferenceExpression EdgeColumn { get; set; }

        public GraphTraversal.direction dir { get; set; }

        public WInsertEdgeFromTwoSourceSpecification(WSqlStatement SrcSpec, WSqlStatement DestSpec, GraphTraversal.direction pDir)
        {
            dir = pDir;
            SrcInsertSource = (SrcSpec as WInsertEdgeSpecification).SelectInsertSource as WSelectInsertSource;
            DestInsertSource = DestSpec as WSelectQueryBlock;

            if (SrcInsertSource == null || DestInsertSource == null)
            {
                throw new SyntaxErrorException("The insert source of the INSERT EDGE statement must be a SELECT statement.");
            }

            Target = (SrcSpec as WInsertEdgeSpecification).Target;
            Columns = new List<WColumnReferenceExpression>();
            foreach (var col in (SrcSpec as WInsertEdgeSpecification).Columns)
                Columns.Add(col);
        }

        public override void Accept(WSqlFragmentVisitor visitor)
        {
            if (visitor != null)
                visitor.Visit(this);
        }

        public override void AcceptChildren(WSqlFragmentVisitor visitor)
        {

        }

        internal override string ToString(string indent)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("{0}INSERT EDGE INTO {1}.{2}\r\n", indent, Target.ToString(), EdgeColumn.ToString());
            return sb.ToString();
        }

        /// <summary>
        /// Construct an edge's string with all informations.
        /// </summary>
        /// <returns></returns>
        public string ConstructEdge()
        {
            var SelectQueryBlock = SrcInsertSource.Select as WSelectQueryBlock;

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

            //Add properties to Edge
            for (var index = 0; index < Columns.Count(); index++)
            {
                Edge = GraphViewJsonCommand.insert_property(Edge, Values[index].ToString(),
                        Columns[index].ToString()).ToString();
            }
            return Edge;
        }

        internal override GraphViewExecutionOperator Generate(GraphViewConnection pConnection)
        {
            WSelectQueryBlock SrcSelect;
            WSelectQueryBlock DestSelect;
            if (dir == GraphTraversal.direction.In)
            {
                SrcSelect = DestInsertSource;
                DestSelect = SrcInsertSource.Select as WSelectQueryBlock;
            }
            else
            {
                SrcSelect = SrcInsertSource.Select as WSelectQueryBlock;
                DestSelect = DestInsertSource;
            }
                
            string Edge = ConstructEdge();

            //Add "id" after each identifier
            var iden = new Identifier();
            iden.Value = "id";
            var dic_iden = new Identifier();
            dic_iden.Value = "doc";

            var n1 = SrcSelect.SelectElements[0] as WSelectScalarExpression;
            var identifiers1 = (n1.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers;
            identifiers1.Add(iden);

            var n2 = DestSelect.SelectElements[0] as WSelectScalarExpression;
            var identifiers2 = (n2.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers;
            identifiers2.Add(iden);

            var n3 = new WSelectScalarExpression(); SrcSelect.SelectElements.Add(n3);
            var n3_SelectExpr = new WColumnReferenceExpression();
            n3.SelectExpr = n3_SelectExpr;
            n3_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n3_SelectExpr.MultiPartIdentifier.Identifiers.Add((n1.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers[0]);
            n3_SelectExpr.MultiPartIdentifier.Identifiers.Add(dic_iden);

            var n4 = new WSelectScalarExpression(); DestSelect.SelectElements.Add(n4);
            var n4_SelectExpr = new WColumnReferenceExpression();
            n4.SelectExpr = n4_SelectExpr;
            n4_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n4_SelectExpr.MultiPartIdentifier.Identifiers.Add((n2.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers[0]);
            n4_SelectExpr.MultiPartIdentifier.Identifiers.Add(dic_iden);

            GraphViewExecutionOperator SrcInput = SrcSelect.Generate(pConnection);
            GraphViewExecutionOperator DestInput = DestSelect.Generate(pConnection);
            if (SrcInput == null || DestInput == null)
                throw new GraphViewException("The insert source of the INSERT EDGE statement is invalid.");

            InsertEdgeFromTwoSourceOperator InsertOp = new InsertEdgeFromTwoSourceOperator(pConnection, SrcInput,DestInput, Edge, n1.ToString(), n2.ToString());

            return InsertOp;
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

        public WDeleteNodeSpecification()
        {
            
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

        /// <summary>
        /// Check if there is eligible nodes with edges.
        /// If there is , stop delete nodes.
        /// Else , create a DeleteNodeOperator.
        /// </summary>
        /// <param name="docDbConnection">The Connection</param>
        /// <returns></returns>
        internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        {
            var search = WhereClause.SearchCondition;
            //build up the query
            string Selectstr = "SELECT * " + "FROM N_0 ";
            if (search == null)
            {
                Selectstr += @"WHERE ARRAY_LENGTH(N_0._edge)>0 or ARRAY_LENGTH(N_0._reverse_edge)>0 ";
            }
            else
            {
                Selectstr += @"WHERE " + search.ToString() +
                             @" and (ARRAY_LENGTH(N_0._edge)>0 or ARRAY_LENGTH(N_0._reverse_edge)>0)  ";
            }
            
            DeleteNodeOperator Deleteop = new DeleteNodeOperator(dbConnection, search, Selectstr);

            return Deleteop;
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

        internal void ChangeSelectQuery()
        {
            var SelectQueryBlock = SelectDeleteExpr as WSelectQueryBlock;
            var edgealias = SelectDeleteExpr.MatchClause.Paths[0].PathEdgeList[0].Item2.Alias;

            #region Add "id" after identifiers
            //Add "id" after identifiers
            var iden = new Identifier();
            iden.Value = "id";

            var n1 = SelectQueryBlock.SelectElements[0] as WSelectScalarExpression;
            var identifiers1 = (n1.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers;
            identifiers1.Add(iden);

            var n2 = SelectQueryBlock.SelectElements[1] as WSelectScalarExpression;
            var identifiers2 = (n2.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers;
            identifiers2.Add(iden);

            #endregion

            #region Add "edge._ID" & "edge._reverse_ID" in Select
            //Add "edge._ID" & "edge._reverse_ID" in Select
            var edge_name = new Identifier();
            var edge_id = new Identifier();
            var edge_reverse_id = new Identifier();
            edge_name.Value = edgealias;
            edge_id.Value = "_ID";
            edge_reverse_id.Value = "_reverse_ID";

            var n3 = new WSelectScalarExpression(); SelectQueryBlock.SelectElements.Add(n3);
            var n3_SelectExpr = new WColumnReferenceExpression();
            n3.SelectExpr = n3_SelectExpr;
            n3_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n3_SelectExpr.MultiPartIdentifier.Identifiers.Add(edge_name);
            n3_SelectExpr.MultiPartIdentifier.Identifiers.Add(edge_id);

            var n4 = new WSelectScalarExpression(); SelectQueryBlock.SelectElements.Add(n4);
            var n4_SelectExpr = new WColumnReferenceExpression();
            n4.SelectExpr = n4_SelectExpr;
            n4_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n4_SelectExpr.MultiPartIdentifier.Identifiers.Add(edge_name);
            n4_SelectExpr.MultiPartIdentifier.Identifiers.Add(edge_reverse_id);
            #endregion

            #region Add ".doc" in Select
            var dic_iden = new Identifier();
            dic_iden.Value = "doc";
            var n5 = new WSelectScalarExpression(); SelectQueryBlock.SelectElements.Add(n5);
            var n5_SelectExpr = new WColumnReferenceExpression();
            n5.SelectExpr = n5_SelectExpr;
            n5_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n5_SelectExpr.MultiPartIdentifier.Identifiers.Add((n1.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers[0]);
            n5_SelectExpr.MultiPartIdentifier.Identifiers.Add(dic_iden);

            var n6 = new WSelectScalarExpression(); SelectQueryBlock.SelectElements.Add(n6);
            var n6_SelectExpr = new WColumnReferenceExpression();
            n6.SelectExpr = n6_SelectExpr;
            n6_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n6_SelectExpr.MultiPartIdentifier.Identifiers.Add((n2.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers[0]);
            n6_SelectExpr.MultiPartIdentifier.Identifiers.Add(dic_iden);
            #endregion
        }

        internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        {
            ChangeSelectQuery();

            var SelectQueryBlock = SelectDeleteExpr as WSelectQueryBlock;

            var n1 = SelectQueryBlock.SelectElements[0] as WSelectScalarExpression;

            var n2 = SelectQueryBlock.SelectElements[1] as WSelectScalarExpression;

            var n3 = SelectQueryBlock.SelectElements[2] as WSelectScalarExpression;

            var n4 = SelectQueryBlock.SelectElements[3] as WSelectScalarExpression;

            GraphViewExecutionOperator input = SelectQueryBlock.Generate(dbConnection);
            if (input == null)
            {
                throw new GraphViewException("The delete source of the DELETE EDGE statement is invalid.");
            }
            DeleteEdgeOperator DeleteOp = new DeleteEdgeOperator(dbConnection, input, n1.ToString(), n2.ToString(), n3.ToString(), n4.ToString());

            return DeleteOp;
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
            if (WhereClause != null && WhereClause.SearchCondition != null)
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