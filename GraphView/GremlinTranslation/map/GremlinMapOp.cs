using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinMapOp: GremlinTranslationOperator
    {
        public GraphTraversal2 MapTraversal { get; set; }

        public GremlinMapOp(GraphTraversal2 mapTraversal2)
        {
            MapTraversal = mapTraversal2;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            MapTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
            inputContext.PivotVariable.Map(inputContext, MapTraversal.GetEndOp().GetContext());

            return inputContext;
        }
    }
}
