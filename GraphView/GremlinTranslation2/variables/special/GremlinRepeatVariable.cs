using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinRepeatVariable : GremlinTableVariable
    {
        private GremlinVariable2 inputVariable;
        private GremlinToSqlContext context;

        public GremlinRepeatVariable(GremlinVariable2 inputVariable, GremlinToSqlContext context)
        {
            VariableName = GenerateTableAlias();
            this.context = context;
            this.inputVariable = inputVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            PropertyKeys.Add(GremlinUtil.GetScalarSubquery(context.ToSelectQueryBlock()));
            var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("repeat", PropertyKeys);
            secondTableRef.Alias = GremlinUtil.GetIdentifier(VariableName);
            return GremlinUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
