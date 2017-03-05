using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFlatMapOp: GremlinTranslationOperator
    {
        internal GraphTraversal2 FlatMapTraversal { get; set; }

        public GremlinFlatMapOp(GraphTraversal2 flatMapTraversal)
        {
            FlatMapTraversal = flatMapTraversal;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            FlatMapTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
            GremlinToSqlContext flatMapContext = FlatMapTraversal.GetEndOp().GetContext();
            inputContext.PivotVariable.FlatMap(inputContext, flatMapContext);

            return inputContext;
        }
    }
}
