using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinOptionalOp: GremlinTranslationOperator
    {
        public GraphTraversal2 TraversalOption;

        public GremlinOptionalOp(GraphTraversal2 traversalOption)
        {
            TraversalOption = traversalOption;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinUtil.InheritedVariableFromParent(TraversalOption, inputContext);
            var optionalExpr = new WOptional()
            {
                SqlStatement = TraversalOption.GetEndOp().GetContext().ToSelectQueryBlock()
            };

            GremlinOptionalVariable newVariable = new GremlinOptionalVariable(optionalExpr);
            inputContext.AddNewVariable(newVariable, Labels);
            inputContext.SetCurrVariable(newVariable);
            inputContext.SetDefaultProjection(newVariable);

            return inputContext;
        }
    }
}
