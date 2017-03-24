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
            ProjectKeys = new List<string>(projectKeys);
            ProjectContextList = byContexts;
        }

        internal override void Populate(string property)
        {
            foreach (var context in ProjectContextList)
            {
                context.Populate(property);
            }
            base.Populate(property);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            foreach (var context in ProjectContextList)
            {
                variableList.AddRange(context.FetchAllVars());
            }
            return variableList;
        }

        internal override List<GremlinVariable> FetchAllTableVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            foreach (var context in ProjectContextList)
            {
                variableList.AddRange(context.FetchAllTableVars());
            }
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            for (var i = 0; i < ProjectKeys.Count; i++)
            {
                WSelectQueryBlock selectBlock = ProjectContextList[i % ProjectContextList.Count].ToSelectQueryBlock(true);
                parameters.Add(SqlUtil.GetScalarSubquery(selectBlock));
                parameters.Add(SqlUtil.GetValueExpr(ProjectKeys[i]));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Project, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
