using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAddVVariable: GremlinVariableReference
    {
        private static long _count = 0;

        internal override string GenerateTableAlias()
        {
            return "AddV_" + _count++;
        }

        public Dictionary<string, object> Properties { get; set; }
        public string VertexLabel { get; set; }

        public override WTableReference ToTableReference()
        {
            return new WVariableTableReference()
            {
                Variable = GremlinUtil.GetVariableReference(VariableName),
                Alias = GremlinUtil.GetIdentifier(VariableName)
            };
        }

        public override List<WSqlStatement> ToSetVariableStatements()
        {
            List<WSqlStatement> statementList = new List<WSqlStatement>();

            var columnK = new List<WColumnReferenceExpression>();
            var columnV = new List<WScalarExpression>();

            if (VertexLabel != null)
            {
                columnK.Add(GremlinUtil.GetColumnReferenceExpr("label"));
                columnV.Add(GremlinUtil.GetValueExpr(VertexLabel));
            }

            foreach (var property in Properties)
            {
                columnK.Add(GremlinUtil.GetColumnReferenceExpr(property.Key));
                columnV.Add(GremlinUtil.GetValueExpr(property.Value));
            }

            var row = new List<WRowValue>() { new WRowValue() { ColumnValues = columnV } };
            var source = new WValuesInsertSource() { RowValues = row };

            var insertStatement = new WInsertSpecification()
            {
                Columns = columnK,
                InsertSource = source,
                Target = GremlinUtil.GetNamedTableReference("Node")
            };

            var addVStatement = new WInsertNodeSpecification(insertStatement);

            var setStatement = new WSetVariableStatement()
            {
                Expression = new WScalarSubquery()
                {
                    SubQueryExpr = addVStatement
                },
                Variable = GremlinUtil.GetVariableReference(VariableName)
            };

            statementList.Add(setStatement);
            return statementList;
        }

        public GremlinAddVVariable(string vertexLabel)
        {
            VariableName = GenerateTableAlias();
            Properties = new Dictionary<string, object>();
            VertexLabel = vertexLabel;
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
