using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAddVVariable: GremlinVertexTableVariable
    {
        public Dictionary<string, object> Properties { get; set; }
        public string VertexLabel { get; set; }
        public bool IsFirstTableReference { get; set; }

        //public override List<WSqlStatement> ToSetVariableStatements()
        //{
        //    List<WSqlStatement> statementList = new List<WSqlStatement>();

        //    var columnK = new List<WColumnReferenceExpression>();
        //    var columnV = new List<WScalarExpression>();

        //    if (VertexLabel != null)
        //    {
        //        columnK.Add(SqlUtil.GetColumnReferenceExpr("label"));
        //        columnV.Add(SqlUtil.GetValueExpr(VertexLabel));
        //    }

        //    foreach (var property in Properties)
        //    {
        //        columnK.Add(SqlUtil.GetColumnReferenceExpr(property.Key));
        //        columnV.Add(SqlUtil.GetValueExpr(property.Value));
        //    }

        //    var row = new List<WRowValue>() { new WRowValue() { ColumnValues = columnV } };
        //    var source = new WValuesInsertSource() { RowValues = row };

        //    var insertStatement = new WInsertSpecification()
        //    {
        //        Columns = columnK,
        //        InsertSource = source,
        //        Target = SqlUtil.GetNamedTableReference("Node")
        //    };

        //    var addVStatement = new WInsertNodeSpecification(insertStatement);

        //    var setStatement = new WSetVariableStatement()
        //    {
        //        Expression = new WScalarSubquery()
        //        {
        //            SubQueryExpr = addVStatement
        //        },
        //        Variable = SqlUtil.GetVariableReference(VariableName)
        //    };

        //    statementList.Add(setStatement);
        //    return statementList;
        //}

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            if (VertexLabel != null)
            {
                parameters.Add(SqlUtil.GetValueExpr(GremlinKeyword.Label));
                parameters.Add(SqlUtil.GetValueExpr(VertexLabel));
            }
            foreach (var property in Properties)
            {
                parameters.Add(SqlUtil.GetValueExpr(property.Key));
                parameters.Add(SqlUtil.GetValueExpr(property.Value));
            }
            var firstTableRef = IsFirstTableReference ? SqlUtil.GetDerivedTable(SqlUtil.GetSimpleSelectQueryBlock("1"), "_") : null;
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.AddV, parameters, VariableName);

            return SqlUtil.GetCrossApplyTableReference(firstTableRef, secondTableRef);
        }

        public GremlinAddVVariable(string vertexLabel, bool isFirstTableReference = false)
        {
            Properties = new Dictionary<string, object>();
            VertexLabel = vertexLabel;
            IsFirstTableReference = isFirstTableReference;
        }

        public GremlinAddVVariable()
        {
            Properties = new Dictionary<string, object>();
        }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            foreach (var pair in properties)
            {
                Properties[pair.Key] = pair.Value;
            }
        }
    }
}
