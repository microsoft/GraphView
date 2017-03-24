using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAggregateVariable : GremlinScalarTableVariable
    {
        public string SideEffectKey { get; set; }
        public GremlinToSqlContext ProjectContext { get; set; }

        public GremlinAggregateVariable(GremlinToSqlContext projectContext, string sideEffectKey)
        {
            ProjectContext = projectContext;
            SideEffectKey = sideEffectKey;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(ProjectContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinVariable> FetchAllTableVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(ProjectContext.FetchAllTableVars());
            return variableList;
        }

        internal override void Populate(string property)
        {
            ProjectContext.Populate(property);
            base.Populate(property);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            //TODO: refactor
            WSelectQueryBlock selectQueryBlock = ProjectContext.ToSelectQueryBlock(true);
            parameters.Add(SqlUtil.GetScalarSubquery(selectQueryBlock));
            parameters.Add(SqlUtil.GetValueExpr(SideEffectKey));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Aggregate, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
