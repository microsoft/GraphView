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
            foreach (var context in ProjectContextList)
            {
                context.HomeVariable = this;
            }
        }

        internal override List<GremlinVariable> PopulateAllTaggedVariable(string label)
        {
            //Project step should be regarded as one step, so we can't populate the tagged variable of ProjectContextList 
            return base.PopulateAllTaggedVariable(label);
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            foreach (var context in ProjectContextList)
            {
                variableList.AddRange(context.FetchVarsFromCurrAndChildContext());
            }
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            for (var i = 0; i < ProjectKeys.Count; i++)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(ProjectContextList[i % ProjectContextList.Count].ToSelectQueryBlock()));
                parameters.Add(SqlUtil.GetValueExpr(ProjectKeys[i]));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Project, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
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
