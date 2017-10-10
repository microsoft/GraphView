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

        internal override void Populate(string property)
        {
            InputVariable?.Populate(property);
            foreach (var variable in DedupVariables)
            {
                variable.Populate(property);
            }
            base.Populate(property);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.Add(InputVariable);
            variableList.AddRange(DedupVariables);
            if (DedupContext != null)
                variableList.AddRange(DedupContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            if (DedupContext != null)
                variableList.AddRange(DedupContext.FetchAllTableVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            if (DedupVariables.Count > 0)
            {
                foreach (var dedupVariable in DedupVariables)
                {
                    parameters.Add(dedupVariable.DefaultProjection().ToScalarExpression());
                }
            }
            else
            {
                parameters.Add(SqlUtil.GetScalarSubquery(DedupContext.ToSelectQueryBlock()));
            }

            var tableRef = SqlUtil.GetFunctionTableReference(
                Scope == GremlinKeyword.Scope.Global ? GremlinKeyword.func.DedupGlobal : GremlinKeyword.func.DedupLocal,
                parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
