using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinProjectVariable: GremlinMapTableVariable
    {
        public List<string> ProjectKeys { get; set; }
        public List<GremlinToSqlContext> ProjectContextList { get; set; }

        public GremlinProjectVariable(List<string> projectKeys, List<GremlinToSqlContext> byContexts)
        {
            this.ProjectKeys = new List<string>(projectKeys);
            this.ProjectContextList = byContexts;
        }

        internal override bool Populate(string property, string label = null)
        {
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label) || this.ProjectKeys.Contains(label))
            {
                populateSuccessfully = true;
                foreach (GremlinToSqlContext context in this.ProjectContextList)
                {
                    context.Populate(property, null);
                }
            }
            else
            {
                foreach (GremlinToSqlContext context in this.ProjectContextList)
                {
                    populateSuccessfully |= context.Populate(property, label);
                }
            }
            return populateSuccessfully;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            foreach (var context in this.ProjectContextList)
            {
                variableList.AddRange(context.FetchAllVars());
            }
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            foreach (var context in this.ProjectContextList)
            {
                variableList.AddRange(context.FetchAllTableVars());
            }
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            for (var i = 0; i < this.ProjectKeys.Count; i++)
            {
                WSelectQueryBlock selectBlock = this.ProjectContextList[i % this.ProjectContextList.Count].ToSelectQueryBlock(true);
                parameters.Add(SqlUtil.GetScalarSubquery(selectBlock));
                parameters.Add(SqlUtil.GetValueExpr(this.ProjectKeys[i]));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Project, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
