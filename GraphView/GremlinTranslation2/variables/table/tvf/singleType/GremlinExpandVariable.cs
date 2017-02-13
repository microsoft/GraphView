using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinExpandVariable: GremlinTableVariable
    {
        public GremlinVariable ExpandVariable { get; set; }

        public GremlinExpandVariable(GremlinVariable expandVariable)
        {
            ExpandVariable = expandVariable;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(ExpandVariable.DefaultVariableProperty().ToScalarExpression());
            foreach (var property in ProjectedProperties)
            {
                parameters.Add(SqlUtil.GetValueExpr(property));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Properties, parameters, this, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
