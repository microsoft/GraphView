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
        public GremlinContextVariable PrimaryVariable { get; set; }

        public GremlinGroupVariable(GremlinVariable primaryVariable, string sideEffectKey, GremlinToSqlContext groupByContext,
            GremlinToSqlContext projectByContext, bool isProjectingACollection)
        {
            this.PrimaryVariable = new GremlinContextVariable(primaryVariable);
            this.SideEffectKey = sideEffectKey;
            this.GroupByContext = groupByContext;
            this.ProjectByContext = projectByContext;
            this.IsProjectingACollection = isProjectingACollection;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.Add(this.PrimaryVariable);
            variableList.AddRange(this.GroupByContext.FetchAllVars());
            variableList.AddRange(this.ProjectByContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            variableList.AddRange(this.GroupByContext.FetchAllTableVars());
            variableList.AddRange(this.ProjectByContext.FetchAllTableVars());
            return variableList;
        }

        internal override bool Populate(string property, string label = null)
        {
            if (base.Populate(property, label))
            {
                this.GroupByContext.Populate(property, null);
                this.ProjectByContext.Populate(property, null);
                return true;
            }
            else
            {
                bool populateSuccess = false;
                populateSuccess |= this.GroupByContext.Populate(property, label);
                populateSuccess |= this.ProjectByContext.Populate(property, label);
                if (populateSuccess)
                {
                    base.Populate(property, null);
                }
                return populateSuccess;
            }
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(this.SideEffectKey));

            WSelectQueryBlock groupBlock = this.GroupByContext.ToSelectQueryBlock(true);
            parameters.Add(SqlUtil.GetScalarSubquery(groupBlock));

            WSelectQueryBlock projectBlock = this.ProjectByContext.ToSelectQueryBlock(true);
            parameters.Add(SqlUtil.GetScalarSubquery(projectBlock));

            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Group, parameters, GetVariableName());
            ((WGroupTableReference) tableRef).IsProjectingACollection = this.IsProjectingACollection;
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
