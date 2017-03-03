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

        public GremlinDedupVariable(GremlinVariable inputVariable, List<GremlinVariable> dedupVariables, GremlinToSqlContext dedupContext)
        {
            InputVariable = inputVariable;
            DedupVariables = new List<GremlinVariable>(dedupVariables);
            DedupContext = dedupContext;
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

            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Dedup, parameters, this, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
