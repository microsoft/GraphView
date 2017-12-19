using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAggregateVariable : GremlinListTableVariable
    {
        public string SideEffectKey { get; set; }
        public GremlinToSqlContext ProjectContext { get; set; }

        public GremlinAggregateVariable(GremlinToSqlContext projectContext, string sideEffectKey)
        {
            this.ProjectContext = projectContext;
            this.SideEffectKey = sideEffectKey;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.ProjectContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            variableList.AddRange(this.ProjectContext.FetchAllTableVars());
            return variableList;
        }

        internal override bool Populate(string property, string label = null)
        {
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label) || this.SideEffectKey == label)
            {
                populateSuccessfully = true;
                this.ProjectContext.Populate(property, null);
            }
            else
            {
                populateSuccessfully |= this.ProjectContext.Populate(property, label);
            }
            return populateSuccessfully;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            //TODO: refactor
            WSelectQueryBlock selectQueryBlock = this.ProjectContext.ToSelectQueryBlock(true);
            parameters.Add(SqlUtil.GetScalarSubquery(selectQueryBlock));
            parameters.Add(SqlUtil.GetValueExpr(this.SideEffectKey));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Aggregate, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
