using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinRepeatOp: GremlinTranslationOperator
    {
        public Predicate TerminationPredicate { get; set; }
        public Predicate EmitPredicate { get; set; }
        public GraphTraversal2 TerminationTraversal { get; set; }
        public GraphTraversal2 EmitTraversal { get; set; }
        public GraphTraversal2 RepeatTraversal { get; set; }
        public int RepeatTimes { get; set; }
        public bool StartFromContext { get; set; }
        public bool EmitContext { get; set; }
        public bool IsEmit { get; set; }
        // i.e. times().repeat()   or   until().repeat()   or   emit().repeat()
        public bool IsFake { get; set; }

        public GremlinRepeatOp(GraphTraversal2 repeatTraversal)
        {
            RepeatTraversal = repeatTraversal;
            IsFake = false;
            RepeatTimes = -1;
        }

        public GremlinRepeatOp()
        {
            IsFake = true;
            RepeatTimes = -1;
        }

        internal override GremlinToSqlContext GetContext()
        {
            if (this.IsFake)
            {
                throw new TranslationException("The fake repeat operator can not get context");
            }

            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            RepeatTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
            GremlinToSqlContext repeatContext = RepeatTraversal.GetEndOp().GetContext();

            foreach (var variable in repeatContext.TableReferences)
            {
                if (variable is GremlinFoldVariable
                    || variable is GremlinCountVariable
                    || variable is GremlinMinVariable
                    || variable is GremlinMaxVariable
                    || variable is GremlinSumVariable
                    || variable is GremlinMeanVariable
                    || variable is GremlinTreeVariable)
                {
                    throw new SyntaxErrorException($"The parent of a reducing barrier can not be repeat()-step: {variable.GetType()}");
                }
                var group = variable as GremlinGroupVariable;
                if (group != null && group.SideEffectKey == null)
                {
                    throw new SyntaxErrorException($"The parent of a reducing barrier can not be repeat()-step: {variable.GetType()}");
                }
            }
            foreach (var variable in repeatContext.FetchAllTableVars())
            {
                if (variable is GremlinRepeatVariable) {
                    throw new SyntaxErrorException("The repeat()-step can't include another nesting repeat()-step:");
                }
            }

            RepeatCondition repeatCondition = new RepeatCondition();
            repeatCondition.StartFromContext = StartFromContext;
            repeatCondition.IsEmitContext = EmitContext;
            if (IsEmit)
            {
                GremlinToSqlContext emitContext = new GremlinToSqlContext();
                emitContext.AddPredicate(SqlUtil.GetTrueBooleanComparisonExpr());
                repeatCondition.EmitContext = emitContext;
            }
            if (TerminationPredicate != null)
            {
                throw new NotImplementedException();
            }
            if (TerminationTraversal != null)
            {
                this.TerminationTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                repeatCondition.TerminationContext = this.TerminationTraversal.GetEndOp().GetContext();
            }
            if (EmitPredicate != null)
            {
                throw new NotImplementedException();
            }
            if (EmitTraversal != null)
            {
                this.EmitTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                repeatCondition.EmitContext = this.EmitTraversal.GetEndOp().GetContext();
            }
            if (RepeatTimes != -1)
            {
                repeatCondition.RepeatTimes = RepeatTimes;
            }

            inputContext.PivotVariable.Repeat(inputContext, repeatContext, repeatCondition);

            return inputContext;
        }

    }

    public class RepeatCondition
    {
        internal bool StartFromContext { get; set; }
        internal bool IsEmitContext { get; set; }
        internal int RepeatTimes { get; set; }
        internal GremlinToSqlContext EmitContext { get; set; }
        internal GremlinToSqlContext TerminationContext { get; set; }

        public RepeatCondition()
        {
            RepeatTimes = -1;
            StartFromContext = false;
            IsEmitContext = false;
        }
    } 
}
