using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinDedupVariable : GremlinTableVariable
    {
        public GremlinVariable InputVariable { get; set; }
        public List<GremlinVariable> DedupVariables { get; set; }
        public GremlinToSqlContext DedupContext { get; set; }
        public GremlinKeyword.Scope Scope { get; set; }

        public GremlinDedupVariable(GremlinVariable inputVariable, 
                                    List<GremlinVariable> dedupVariables, 
                                    GremlinToSqlContext dedupContext,
                                    GremlinKeyword.Scope scope) : base(GremlinVariableType.Table)
        {
            InputVariable = inputVariable;
            DedupVariables = new List<GremlinVariable>(dedupVariables);
            DedupContext = dedupContext;
            Scope = scope;
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            return DedupContext == null ? new List<GremlinVariable>() : DedupContext.FetchVarsFromCurrAndChildContext();
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            if (DedupVariables.Count > 0)
            {
                foreach (var dedupVariable in DedupVariables)
                {
                    parameters.Add(dedupVariable.DefaultVariableProperty().ToScalarExpression());
                }
            }
            else if (DedupContext != null)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(DedupContext.ToSelectQueryBlock()));
            }
            else
            {
                parameters.Add(InputVariable.DefaultVariableProperty().ToScalarExpression());
            }

            var tableRef = SqlUtil.GetFunctionTableReference(
                Scope == GremlinKeyword.Scope.global ? GremlinKeyword.func.DedupGlobal : GremlinKeyword.func.DedupLocal,
                parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
