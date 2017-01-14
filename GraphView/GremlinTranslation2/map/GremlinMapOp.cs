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

            MapTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
            inputContext.PivotVariable.Map(inputContext, MapTraversal.GetEndOp().GetContext());

            return inputContext;
        }
    }
}
