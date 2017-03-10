using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinGroupVariable: GremlinScalarTableVariable
    {
        public GremlinToSqlContext GroupByContext { get; set; }
        public GremlinToSqlContext ProjectByContext { get; set; }
        public bool IsProjectByString { get; set; }
        public string SideEffectKey { get; set; }
        public GremlinVariable PrimaryVariable { get; set; }

        public GremlinGroupVariable(GremlinVariable primaryVariable, string sideEffectKey, GremlinToSqlContext groupByContext,
            GremlinToSqlContext projectByContext, bool isProjectByString)
        {
            PrimaryVariable = primaryVariable;
            SideEffectKey = sideEffectKey;
            GroupByContext = groupByContext;
            ProjectByContext = projectByContext;
            IsProjectByString = isProjectByString;

            GroupByContext.HomeVariable = this;
            ProjectByContext.HomeVariable = this;
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            if (GroupByContext != null)
                variableList.AddRange(GroupByContext.FetchVarsFromCurrAndChildContext());
            if (ProjectByContext != null)
                variableList.AddRange(ProjectByContext.FetchVarsFromCurrAndChildContext());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(SideEffectKey));
            parameters.Add(SqlUtil.GetScalarSubquery(GroupByContext.ToSelectQueryBlock()));
            parameters.Add(SqlUtil.GetScalarSubquery(ProjectByContext.ToSelectQueryBlock()));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Group, parameters, GetVariableName());
            ((WGroupTableReference) tableRef).IsProjectByString = IsProjectByString;
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
