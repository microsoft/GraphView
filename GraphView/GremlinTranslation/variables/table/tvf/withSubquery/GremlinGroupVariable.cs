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

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            foreach (var parameter in Parameters)
            {
                if (parameter is GremlinToSqlContext)
                {
                    variableList.AddRange((parameter as GremlinToSqlContext).FetchVarsFromCurrAndChildContext());
                }
            }
            return variableList;
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
                else if (Parameters[i] is GremlinVariableProperty)
                {
                    parameters.Add((Parameters[i] as GremlinVariableProperty).ToScalarExpression());
                }
                else
                {
                    throw new QueryCompilationException();
                }
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Group, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
