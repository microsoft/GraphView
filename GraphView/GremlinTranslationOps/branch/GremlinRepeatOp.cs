using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.branch
{
    internal class GremlinRepeatOp: GremlinTranslationOperator
    {
        public GraphTraversal2 RepeatTraversal;
        public Predicate ConditionPredicate;
        public GraphTraversal2 ConditionTraversal;

        public bool IsEmitTrue = false;
        public bool IsEmitBefore = false;
        public bool IsEmitAfter = false;
        public bool IsUntilBefore = false;
        public bool IsUntilAfter = false;
        public bool IsTimes = false;
        public long Times;

        public GremlinRepeatOp(GraphTraversal2 repeatTraversal)
        {
            RepeatTraversal = repeatTraversal;
        }

        public GremlinRepeatOp()
        {
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinUtil.InheritedVariableFromParent(RepeatTraversal, inputContext);

            WRepeatPath repeatPath = new WRepeatPath() {};
            repeatPath.IsUntilBefore = IsUntilBefore;
            repeatPath.IsUntilAfter = IsUntilAfter;
            repeatPath.IsEmitTrue = IsEmitTrue;
            repeatPath.IsEmitBefore = IsEmitBefore;
            repeatPath.IsEmitAfter = IsEmitAfter;
            repeatPath.IsTimes = IsTimes;
            repeatPath.Times = Times;
            
            if (ConditionTraversal != null)
            {
                inputContext.SaveCurrentState();
                repeatPath.ConditionSubQueryBlock = ConditionTraversal.GetEndOp().GetContext().ToSelectQueryBlock();
                inputContext.ResetSavedState();
            }
            if (ConditionPredicate != null)
            {
                throw new NotImplementedException();
            }
            inputContext.SaveCurrentState();
            repeatPath.SubQueryExpr = RepeatTraversal.GetEndOp().GetContext().ToSelectQueryBlock();
            inputContext.ResetSavedState();

            if (inputContext.CurrVariable is GremlinVertexVariable)
            {
                GremlinPathVariable newEdgeVar = new GremlinPathVariable(WEdgeType.Path);
                inputContext.AddNewVariable(newEdgeVar);

                GremlinVertexVariable sinkVar = new GremlinVertexVariable();
                inputContext.AddPaths(inputContext.CurrVariable, newEdgeVar, sinkVar);
                inputContext.AddNewVariable(sinkVar);
                inputContext.SetDefaultProjection(sinkVar);
                inputContext.SetCurrVariable(sinkVar);

                inputContext.WithPaths[newEdgeVar.VariableName] = repeatPath;
            }
            else if (inputContext.CurrVariable is GremlinEdgeVariable)
            {

            }
            else
            {
                throw new NotImplementedException();
            }



            return inputContext;
        }

    }
}
