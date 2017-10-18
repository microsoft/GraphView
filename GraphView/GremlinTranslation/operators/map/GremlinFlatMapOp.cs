using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFlatMapOp: GremlinTranslationOperator
    {
        internal GraphTraversal FlatMapTraversal { get; set; }

        public GremlinFlatMapOp(GraphTraversal flatMapTraversal)
        {
            FlatMapTraversal = flatMapTraversal;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of flatMap()-step can't be null.");
            }

            FlatMapTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
            GremlinToSqlContext flatMapContext = FlatMapTraversal.GetEndOp().GetContext();
            inputContext.PivotVariable.FlatMap(inputContext, flatMapContext);

            return inputContext;
        }
    }
}
