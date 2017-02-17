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
            foreach (var parameter in Parameters)
            {
                if (parameter is GremlinToSqlContext)
                {
                    parameters.Add(SqlUtil.GetScalarSubquery((parameter as GremlinToSqlContext).ToSelectQueryBlock()));
                }
                else
                {
                    parameters.Add(SqlUtil.GetScalarSubquery(
                        SqlUtil.GetSimpleSelectQueryBlock(PrimaryVariable.GetVariableProperty(parameter as string))));
                }
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Group, parameters, this, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
