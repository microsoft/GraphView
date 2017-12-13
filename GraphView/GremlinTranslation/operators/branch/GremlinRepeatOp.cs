using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Diagnostics;
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
        public GraphTraversal TerminationTraversal { get; set; }
        public GraphTraversal EmitTraversal { get; set; }
        public GraphTraversal RepeatTraversal { get; set; }
        public int RepeatTimes { get; set; }
        public bool StartFromContext { get; set; }
        public bool EmitContext { get; set; }
        public bool IsEmit { get; set; }
        // i.e. times().repeat()   or   until().repeat()   or   emit().repeat()
        public bool IsFake { get; set; }

        public GremlinRepeatOp(GraphTraversal repeatTraversal)
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
                throw new TranslationException("The PivotVariable of repeat()-step can't be null.");
            }

            // Convert GremlinParentContextOp to GremlinRepeatParentContextOp
            // Because It is different for "__" in Repeat-traversal and in other operator-traversal such as FlatMap.
            var oldStartOp = RepeatTraversal.GetStartOp() as GremlinParentContextOp;
            Debug.Assert(oldStartOp != null);
            RepeatTraversal.ReplaceGremlinOperator(0, new GremlinRepeatParentContextOp(oldStartOp));

            RepeatTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
            GremlinToSqlContext repeatContext = RepeatTraversal.GetEndOp().GetContext();

            foreach (var variable in repeatContext.TableReferencesInFromClause)
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
                // Convert GremlinParentContextOp to GremlinUntilParentContextOp
                // Because It is different for "__" in Until-traversal and in other operator-traversal such as FlatMap.
                var old = TerminationTraversal.GetStartOp() as GremlinParentContextOp;
                Debug.Assert(old != null);
                TerminationTraversal.ReplaceGremlinOperator(0, new GremlinUntilParentContextOp(old));

                this.TerminationTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                repeatCondition.TerminationContext = this.TerminationTraversal.GetEndOp().GetContext();
            }
            if (EmitPredicate != null)
            {
                throw new NotImplementedException();
            }
            if (EmitTraversal != null)
            {
                // Convert GremlinParentContextOp to GremlinEmitParentContextOp
                // Because It is different for "__" in Emit-traversal and in other operator-traversal such as FlatMap.
                var old = EmitTraversal.GetStartOp() as GremlinParentContextOp;
                Debug.Assert(old != null);
                EmitTraversal.ReplaceGremlinOperator(0, new GremlinEmitParentContextOp(old));

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

    internal class RepeatCondition
    {
        /// <summary>
        /// When this variable is true, the repeat step corresponds to the while-do semantics. 
        /// That is: the input will be evaluated against the until condition first,
        /// if there is any, before it is fed into the repeat body. 
        /// Or, the repeat step corresponds to the do-while semantics.
        /// </summary>
        internal bool StartFromContext { get; set; }
        /// <summary>
        /// When this variable is true, the repeat step will always projects the step's input.
        /// </summary>
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
