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
            byContext.ParentVariable = this;
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
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Project, parameters, this, VariableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        internal override void Select(GremlinToSqlContext currentContext, string label)
        {
            int index = ProjectKeys.FindIndex(p=> p == label);
            if (index < 0)
            {
                throw new QueryCompilationException(string.Format("The specified tag \"{0}\" is not defined.", label));
            }
            GremlinGhostVariable newVariable = GremlinGhostVariable.Create(ProjectContextList[index % ProjectContextList.Count].PivotVariable, this, label);
            currentContext.VariableList.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }
    }
}
