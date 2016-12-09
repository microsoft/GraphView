using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    partial class WInsertNodeSpecification
    {
        /// <summary>
        /// Construct a Json's string which contains all the information about the new node.
        /// And then Create a InsertNodeOperator with this string
        /// </summary>
        /// <param name="docDbConnection">The Connection</param>
        /// <returns></returns>
        internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        {
            string Json_str = ConstructNode();

            InsertNodeOperator InsertOp = new InsertNodeOperator(dbConnection, Json_str);

            return InsertOp;
        }
    }

    partial class WInsertEdgeSpecification
    {
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
            n3.ColumnName = n3_SelectExpr.MultiPartIdentifier.ToString() + ".doc";

            var n4 = new WSelectScalarExpression(); SelectQueryBlock.SelectElements.Add(n4);
            var n4_SelectExpr = new WColumnReferenceExpression();
            n4.SelectExpr = n4_SelectExpr;
            n4_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n4_SelectExpr.MultiPartIdentifier.Identifiers.Add((n2.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers[0]);
            n4.ColumnName = n4_SelectExpr.MultiPartIdentifier.ToString() + ".doc";

            GraphViewExecutionOperator input = SelectQueryBlock.Generate(dbConnection);
            if (input == null)
                throw new GraphViewException("The insert source of the INSERT EDGE statement is invalid.");

            InsertEdgeOperator InsertOp = new InsertEdgeOperator(dbConnection, input, Edge, n1.ToString(), n2.ToString());

            return InsertOp;
        }
    }

    partial class WInsertEdgeFromTwoSourceSpecification
    {
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
            n3.ColumnName = n3_SelectExpr.MultiPartIdentifier.ToString() + ".doc";

            var n4 = new WSelectScalarExpression(); DestSelect.SelectElements.Add(n4);
            var n4_SelectExpr = new WColumnReferenceExpression();
            n4.SelectExpr = n4_SelectExpr;
            n4_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n4_SelectExpr.MultiPartIdentifier.Identifiers.Add((n2.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers[0]);
            n4.ColumnName = n4_SelectExpr.MultiPartIdentifier.ToString() + ".doc";

            GraphViewExecutionOperator SrcInput = SrcSelect.Generate(pConnection);
            GraphViewExecutionOperator DestInput = DestSelect.Generate(pConnection);
            if (SrcInput == null || DestInput == null)
                throw new GraphViewException("The insert source of the INSERT EDGE statement is invalid.");

            InsertEdgeFromTwoSourceOperator InsertOp = new InsertEdgeFromTwoSourceOperator(pConnection, SrcInput, DestInput, Edge, n1.ToString(), n2.ToString());

            return InsertOp;
        }
    }

    partial class WDeleteEdgeSpecification
    {
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
            var n5 = new WSelectScalarExpression(); SelectQueryBlock.SelectElements.Add(n5);
            var n5_SelectExpr = new WColumnReferenceExpression();
            n5.SelectExpr = n5_SelectExpr;
            n5_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n5_SelectExpr.MultiPartIdentifier.Identifiers.Add((n1.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers[0]);
            n5.ColumnName = n5_SelectExpr.MultiPartIdentifier.ToString() + ".doc";

            var n6 = new WSelectScalarExpression(); SelectQueryBlock.SelectElements.Add(n6);
            var n6_SelectExpr = new WColumnReferenceExpression();
            n6.SelectExpr = n6_SelectExpr;
            n6_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n6_SelectExpr.MultiPartIdentifier.Identifiers.Add((n2.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers[0]);
            n6.ColumnName = n6_SelectExpr.MultiPartIdentifier.ToString() + ".doc";

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

    partial class WDeleteNodeSpecification
    {
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
            var target = (Target as WNamedTableReference).ToStringWithoutRange();

            if (search != null)
            {
                // Make sure every column is bound with the target table
                var modifyTableNameVisitor = new ModifyTableNameVisitor();
                modifyTableNameVisitor.Invoke(search, target);
            }

            DocDbScript script = new DocDbScript
            {
                ScriptBase = "SELECT *",
                FromClause = new DFromClause { TableReference = target, FromClauseString = "" },
                WhereClause = new WWhereClause()
            };

            // Build up the query to select all the nodes can be deleted
            string Selectstr;
            string IsolatedCheck = string.Format("(ARRAY_LENGTH({0}._edge) = 0 AND ARRAY_LENGTH({0}._reverse_edge) = 0) ", target);

            if (search == null)
            {
                Selectstr = script.ToString() + @"WHERE " + IsolatedCheck;
            }
            else
            {
                script.WhereClause.SearchCondition =
                    WBooleanBinaryExpression.Conjunction(script.WhereClause.SearchCondition,
                        new WBooleanParenthesisExpression { Expression = search });
                Selectstr = script.ToString() + @" AND " + IsolatedCheck;
            }

            DeleteNodeOperator Deleteop = new DeleteNodeOperator(dbConnection, Selectstr);

            return Deleteop;
        }
    }
}
