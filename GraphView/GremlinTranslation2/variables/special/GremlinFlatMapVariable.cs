using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFlatMapVariable: GremlinTableVariable
    {
        public GremlinToSqlContext FlatMapContext;

        public GremlinFlatMapVariable(GremlinToSqlContext flatMapContext)
        {
            FlatMapContext = flatMapContext;
            VariableName = GenerateTableAlias();
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            WSelectQueryBlock queryBlock = FlatMapContext.ToSelectQueryBlock();
            foreach (var projectProperty in projectedProperties)
            {
                queryBlock.SelectElements.Add(new WSelectScalarExpression()
                {
                    SelectExpr = GremlinUtil.GetColumnReferenceExpression(FlatMapContext.PivotVariable.VariableName, projectProperty)
                });
            }
            PropertyKeys.Add(GremlinUtil.GetScalarSubquery(queryBlock));
            var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("flatMap", PropertyKeys);
            secondTableRef.Alias = GremlinUtil.GetIdentifier(VariableName);
            return GremlinUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
