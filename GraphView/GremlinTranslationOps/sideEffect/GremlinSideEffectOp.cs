using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.GremlinTranslationOps.filter;

namespace GraphView.GremlinTranslationOps.sideEffect
{
    internal class GremlinSideEffectOp: GremlinTranslationOperator
    {
        public GraphTraversal2 SideEffectTraversal;

        public GremlinSideEffectOp(GraphTraversal2 sideEffectTraversal)
        {
            SideEffectTraversal = sideEffectTraversal;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinUtil.InheritedContextFromParent(SideEffectTraversal, inputContext);

            //inputContext.AddContextStatements(SideEffectTraversal.GetEndOp().GetContext());
            GremlinVariable currVariable = inputContext.CurrVariable;
            List<GremlinVariable> currVariableList = inputContext.RemainingVariableList.Copy();
            List<Projection> currProjection = inputContext.ProjectionList.Copy();
            List<Tuple<GremlinVariable, GremlinVariable, GremlinVariable>> currPath = inputContext.Paths.Copy();
            inputContext.Statements.Add(SideEffectTraversal.GetEndOp().GetContext().ToSelectQueryBlock());

            inputContext.SetCurrVariable(currVariable);
            inputContext.ProjectionList = currProjection;
            inputContext.RemainingVariableList = currVariableList;
            inputContext.Paths = currPath;
            return inputContext;

        }
    }
}
