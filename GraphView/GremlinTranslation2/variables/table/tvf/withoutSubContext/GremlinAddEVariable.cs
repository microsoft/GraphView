using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAddEVariable: GremlinEdgeTableVariable
    {
        public GremlinVariable InputVariable { get; set; }
        public GremlinToSqlContext FromVertexContext { get; set; }
        public GremlinToSqlContext ToVertexContext { get; set; }
        public Dictionary<string, object> Properties { get; set; }
        public string EdgeLabel { get; set; }

        //public override List<WSqlStatement> ToSetVariableStatements()
        //{
        //    List<WSqlStatement> statementList = new List<WSqlStatement>();

        //    var columnK = new List<WColumnReferenceExpression>();
        //    var selectBlock = new WSelectQueryBlock()
        //    {
        //        FromClause = new WFromClause()
        //    };

        //    selectBlock.FromClause.TableReferences.Add(FromVariable.ToTableReference());
        //    selectBlock.FromClause.TableReferences.Add(ToVariable.ToTableReference());

        //    var fromVarExpr = SqlUtil.GetColumnReferenceExpr(FromVariable.VariableName, "id");
        //    selectBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(fromVarExpr));

        //    var toVarExpr = SqlUtil.GetColumnReferenceExpr(ToVariable.VariableName, "id");
        //    selectBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(toVarExpr));


        //    //Add edge key-value
        //    WScalarExpression valueExpr;
        //    if (EdgeLabel != null)
        //    {
        //        columnK.Add(SqlUtil.GetColumnReferenceExpr("label"));
        //        valueExpr = SqlUtil.GetValueExpr(EdgeLabel);
        //        selectBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(valueExpr));

        //    }
        //    foreach (var property in Properties)
        //    {
        //        columnK.Add(SqlUtil.GetColumnReferenceExpr(property.Key));
        //        valueExpr = SqlUtil.GetValueExpr(property.Value.ToString());
        //        selectBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(valueExpr));
        //    }

        //    var insertStatement = new WInsertSpecification()
        //    {
        //        Columns = columnK,
        //        InsertSource = new WSelectInsertSource() { Select = selectBlock },
        //        Target = SqlUtil.GetNamedTableReference("Edge")
        //    };

        //    var addEStatement = new WInsertEdgeSpecification(insertStatement)
        //    {
        //        SelectInsertSource = new WSelectInsertSource() { Select = selectBlock }
        //    };

        //    var setStatement = new WSetVariableStatement()
        //    {
        //        Expression = new WScalarSubquery()
        //        {
        //            SubQueryExpr = addEStatement
        //        },
        //        Variable = SqlUtil.GetVariableReference(VariableName)
        //    };

        //    statementList.Add(setStatement);
        //    return statementList;
        //}

        public GremlinAddEVariable(GremlinVariable inputVariable, string edgeLabel)
        {
            Properties = new Dictionary<string, object>();
            EdgeLabel = edgeLabel;
            InputVariable = inputVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(GetSelectQueryBlock(FromVertexContext)));
            parameters.Add(SqlUtil.GetScalarSubquery(GetSelectQueryBlock(ToVertexContext)));
            if (EdgeLabel != null)
            {
                parameters.Add(SqlUtil.GetValueExpr(GremlinKeyword.Label));
                parameters.Add(SqlUtil.GetValueExpr(EdgeLabel));
            }
            foreach (var property in Properties)
            {
                parameters.Add(SqlUtil.GetValueExpr(property.Key));
                parameters.Add(SqlUtil.GetValueExpr(property.Value));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.AddE, parameters, this, VariableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        private WSelectQueryBlock GetSelectQueryBlock(GremlinToSqlContext context)
        {
            if (context == null)
            {
                return SqlUtil.GetSimpleSelectQueryBlock(InputVariable.VariableName, new List<string>() { GremlinKeyword.NodeID }); ;
            }
            else
            {
                return context.ToSelectQueryBlock();
            } 
        }


        internal override void From(GremlinToSqlContext currentContext, string label)
        {
            throw new NotImplementedException();
        }

        internal override void From(GremlinToSqlContext currentContext, GremlinToSqlContext fromVertexContext)
        {
            FromVertexContext = fromVertexContext;
        }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            foreach (var pair in properties)
            {
                Properties[pair.Key] = pair.Value;
            }
        }

        internal override void To(GremlinToSqlContext currentContext, string label)
        {
            throw new NotImplementedException();
        }

        internal override void To(GremlinToSqlContext currentContext, GremlinToSqlContext toVertexContext)
        {
            ToVertexContext = toVertexContext;
        }
    }
}
