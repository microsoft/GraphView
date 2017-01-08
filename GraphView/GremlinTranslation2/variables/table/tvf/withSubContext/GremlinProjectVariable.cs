using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinProjectVariable: GremlinScalarTableVariable
    {
        public GremlinToSqlContext ParentContext { get; set; }
        public List<string> ProjectKeys { get; set; }
        public List<GremlinToSqlContext> ProjectContextList { get; set; }

        public GremlinProjectVariable(GremlinToSqlContext parentContext, List<string> projectKeys)
        {
            ParentContext = parentContext;

            ProjectKeys = new List<string>(projectKeys);
            ProjectContextList = new List<GremlinToSqlContext>();

            foreach (var projectKey in projectKeys)
            {
                Populate(projectKey);
            }
        }

        internal override void Populate(string property)
        {
            foreach (var context in ProjectContextList)
            {
                context.Populate(property);
            }
        }

        internal override void By(GremlinToSqlContext currentContext, GraphTraversal2 byTraversal)
        {
            byTraversal.GetStartOp().InheritedVariableFromParent(ParentContext);
            GremlinToSqlContext byContext = byTraversal.GetEndOp().GetContext();
            ProjectContextList.Add(byContext);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            for (var i = 0; i < ProjectKeys.Count; i++)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(ProjectContextList[i % ProjectContextList.Count].ToSelectQueryBlock()));
                parameters.Add(SqlUtil.GetValueExpr(ProjectKeys[i]));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference("project", parameters, VariableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }
}
