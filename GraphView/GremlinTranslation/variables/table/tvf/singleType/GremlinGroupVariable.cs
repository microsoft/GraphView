using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinGroupVariable: GremlinScalarTableVariable
    {
        public List<object> Parameters { get; set; }
        public string SideEffectKey { get; set; }
        public GremlinVariable PrimaryVariable { get; set; }

        public GremlinGroupVariable(GremlinVariable primaryVariable, string sideEffectKey, List<object> parameters)
        {
            PrimaryVariable = primaryVariable;
            SideEffectKey = sideEffectKey;
            Parameters = new List<object>(parameters);
            foreach (var parameter in parameters)
            {
                if (parameter is GremlinToSqlContext)
                {
                    (parameter as GremlinToSqlContext).HomeVariable = this;
                }
            }
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(SideEffectKey));
            for (var i = 0; i < Parameters.Count; i++)
            {
                if (Parameters[i] is GremlinToSqlContext)
                {
                    parameters.Add(SqlUtil.GetScalarSubquery((Parameters[i] as GremlinToSqlContext).ToSelectQueryBlock()));
                }
                else
                {
                    parameters.Add(PrimaryVariable.GetVariableProperty(Parameters[i] as string).ToScalarExpression());
                }
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Group, parameters, this, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
