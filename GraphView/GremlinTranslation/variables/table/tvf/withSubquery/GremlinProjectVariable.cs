using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinProjectVariable: GremlinScalarTableVariable
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
            if (base.Populate(property, label))
            {
                foreach (var context in this.ProjectContextList)
                {
                    context.Populate(property, null);
                }
                return true;
            }
            else if (this.ProjectKeys.Contains(label))
            {
                foreach (var context in this.ProjectContextList)
                {
                    context.Populate(property, null);
                }
                return base.Populate(property, null);
            }
            else
            {
                bool populateSuccess = false;
                foreach (var context in this.ProjectContextList)
                {
                    populateSuccess |= context.Populate(property, label);
                }
                if (populateSuccess)
                {
                    base.Populate(property, null);
                }
                return populateSuccess;
            }
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
