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

        internal override void Populate(string property)
        {
            SubqeryContext.Populate(property);
            foreach (var context in ProjectContextList)
            {
                context.Populate(property);
            }
        }

        internal override void By(GremlinToSqlContext currentContext, GraphTraversal2 byTraversal)
        {
            byTraversal.GetStartOp().InheritedVariableFromParent(SubqeryContext);
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
                WScalarExpression scalarExpr = SqlUtil.GetScalarSubquery(projectQueryBlock);
                WSelectScalarExpression selectScalarExpr = SqlUtil.GetSelectScalarExpr(scalarExpr, ProjectKeys[i]);
                selectQueryBlock.SelectElements.Add(selectScalarExpr);
            }

            return SqlUtil.GetDerivedTable(selectQueryBlock, VariableName);
        }
    }
}
