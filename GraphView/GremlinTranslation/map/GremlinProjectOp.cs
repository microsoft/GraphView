using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslation
{
    internal class GremlinProjectOp: GremlinTranslationOperator, IGremlinByModulating
    {
        public List<string> ProjectKeys { get; set; }
        public List<GraphTraversal2> TraversalList { get; set; }

        public GremlinProjectOp(params string[] projectKeys)
        {
            TraversalList = new List<GraphTraversal2>();
            ProjectKeys = new List<string>();
            foreach (var projectKey in projectKeys)
            {
                ProjectKeys.Add(projectKey);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            List<WScalarExpression> parameterList = new List<WScalarExpression>();

            for (var i = 0; i < ProjectKeys.Count; i++)
            {
                inputContext.SaveCurrentState();
                GremlinUtil.InheritedVariableFromParent(TraversalList[i % TraversalList.Count], inputContext);
                WSelectQueryBlock selectQueryBlock = TraversalList[i % TraversalList.Count].GetEndOp().GetContext().ToSelectQueryBlock();
                inputContext.ResetSavedState();

                WScalarExpression scalarExpr = new WScalarSubquery()
                {
                    SubQueryExpr = selectQueryBlock
                };
                parameterList.Add(scalarExpr);
                parameterList.Add(GremlinUtil.GetValueExpr(ProjectKeys[i]));
            }
            WFunctionCall projectFunctionCall = GremlinUtil.GetFunctionCall("project", parameterList);

            FunctionCallProjection newFunctionCallProjection = new FunctionCallProjection(projectFunctionCall);
            inputContext.SetCurrProjection(newFunctionCallProjection);

            return inputContext;
        }
        public void ModulateBy()
        {

        }
        public void ModulateBy(GraphTraversal2 traversal)
        {
            TraversalList.Add(traversal);
        }

        public void ModulateBy(string key)
        {
        }

        public void ModulateBy(GremlinKeyword.Order order)
        {
        }
    }
}
