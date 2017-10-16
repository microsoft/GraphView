using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinMapOp: GremlinTranslationOperator
    {
        public GraphTraversal MapTraversal { get; set; }

        public GremlinMapOp(GraphTraversal mapTraversal)
        {
            MapTraversal = mapTraversal;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new TranslationException("The PivotVariable of map()-step can't be null.");
            }

            MapTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
            inputContext.PivotVariable.Map(inputContext, MapTraversal.GetEndOp().GetContext());

            return inputContext;
        }
    }
}
