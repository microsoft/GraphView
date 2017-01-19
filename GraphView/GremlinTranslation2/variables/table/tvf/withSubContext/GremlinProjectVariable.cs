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
        }

        //internal override void By(GremlinToSqlContext currentContext, GraphTraversal2 byTraversal)
        //{
        //    byTraversal.GetStartOp().InheritedVariableFromParent(ParentContext);
        //    GremlinToSqlContext byContext = byTraversal.GetEndOp().GetContext();
        //    byContext.ParentVariable = this;
        //    ProjectContextList.Add(byContext);
        //}

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            for (var i = 0; i < ProjectKeys.Count; i++)
            {
                //List<string> defaultProjectProperty = new List<string>()
                //{
                //    ProjectContextList[i%ProjectContextList.Count].PivotVariable.DefaultVariableProperty()
                //        .VariableProperty
                //};
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
                base.Select(currentContext, label);
            }
            else
            {
                if (ProjectContextList[index % ProjectContextList.Count].PivotVariable is GremlinGhostVariable)
                {
                    var ghostVar =
                        ProjectContextList[index%ProjectContextList.Count].PivotVariable as GremlinGhostVariable;
                    var newGhostVar = GremlinGhostVariable.Create(ghostVar.RealVariable, ghostVar.AttachedVariable,
                        label);
                    currentContext.VariableList.Add(newGhostVar);
                    currentContext.SetPivotVariable(newGhostVar);
                }
                else
                {
                    GremlinGhostVariable newVariable = GremlinGhostVariable.Create(ProjectContextList[index % ProjectContextList.Count].PivotVariable, this, label);
                    currentContext.VariableList.Add(newVariable);
                    currentContext.SetPivotVariable(newVariable);
                }
            }
        }
    }
}
