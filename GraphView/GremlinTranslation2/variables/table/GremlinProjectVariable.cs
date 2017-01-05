using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinProjectVariable: GremlinTableVariable
    {
        public GremlinToSqlContext SubqeryContext { get; set; }
        public List<string> ProjectKeys { get; set; }
        public List<GremlinToSqlContext> ProjectContextList { get; set; }

        public GremlinProjectVariable(GremlinToSqlContext subqueryContext, List<string> projectKeys)
        {
            VariableName = GenerateTableAlias();
            SubqeryContext = subqueryContext;
            ProjectKeys = new List<string>(projectKeys);
            ProjectContextList = new List<GremlinToSqlContext>();

            foreach (var projectKey in projectKeys)
            {
                Populate(projectKey);
            }
        }

        internal override void By(GremlinToSqlContext currentContext, GraphTraversal2 byTraversal)
        {
            GremlinUtil.InheritedVariableFromParent(byTraversal, SubqeryContext);
            GremlinToSqlContext byContext = byTraversal.GetEndOp().GetContext();
            currentContext.SetVariables.AddRange(byContext.SetVariables);
            ProjectContextList.Add(byContext);
        }

        public override WTableReference ToTableReference()
        {
            WSelectQueryBlock selectQueryBlock = SubqeryContext.ToSelectQueryBlock();
            selectQueryBlock.SelectElements.Clear();

            for (var i = 0; i < ProjectKeys.Count; i++)
            {
                WSelectQueryBlock projectQueryBlock = ProjectContextList[i % ProjectContextList.Count].ToSelectQueryBlock();
                WScalarExpression scalarExpr = GremlinUtil.GetScalarSubquery(projectQueryBlock);
                WSelectScalarExpression selectScalarExpr = GremlinUtil.GetSelectScalarExpression(scalarExpr, ProjectKeys[i]);
                selectQueryBlock.SelectElements.Add(selectScalarExpr);
            }

            return GremlinUtil.GetDerivedTable(selectQueryBlock, VariableName);
        }
    }
}
