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
        public bool IsProjectingACollection { get; set; }
        public string SideEffectKey { get; set; }
        public GremlinVariable PrimaryVariable { get; set; }

        public GremlinGroupVariable(GremlinVariable primaryVariable, string sideEffectKey, GremlinToSqlContext groupByContext,
            GremlinToSqlContext projectByContext, bool isProjectingACollection)
        {
            PrimaryVariable = primaryVariable;
            SideEffectKey = sideEffectKey;
            GroupByContext = groupByContext;
            ProjectByContext = projectByContext;
            IsProjectingACollection = isProjectingACollection;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.Add(PrimaryVariable);
            variableList.AddRange(GroupByContext.FetchAllVars());
            variableList.AddRange(ProjectByContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinVariable> FetchAllTableVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(GroupByContext.FetchAllTableVars());
            variableList.AddRange(ProjectByContext.FetchAllTableVars());
            return variableList;
        }

        internal override void Populate(string property)
        {
            ProjectByContext.Populate(property);
            base.Populate(property);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(SideEffectKey));

            WSelectQueryBlock groupBlock = GroupByContext.ToSelectQueryBlock(true);
            parameters.Add(SqlUtil.GetScalarSubquery(groupBlock));

            WSelectQueryBlock projectBlock = ProjectByContext.ToSelectQueryBlock(true);
            parameters.Add(SqlUtil.GetScalarSubquery(projectBlock));

            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Group, parameters, GetVariableName());
            ((WGroupTableReference) tableRef).IsProjectingACollection = IsProjectingACollection;
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
