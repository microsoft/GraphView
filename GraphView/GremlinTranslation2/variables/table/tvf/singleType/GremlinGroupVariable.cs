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

        public GremlinGroupVariable(string sideEffectKey, List<object> parameters)
        {
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
            foreach (var parameter in Parameters)
            {
                if (parameter is GremlinToSqlContext)
                {
                    parameters.Add(SqlUtil.GetScalarSubquery((parameter as GremlinToSqlContext).ToSelectQueryBlock()));
                }
                else
                {
                    parameters.Add(SqlUtil.GetValueExpr(parameter));
                }
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Group, parameters, this, VariableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
