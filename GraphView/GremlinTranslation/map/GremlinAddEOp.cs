using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAddEOp: GremlinTranslationOperator
    {
        internal string EdgeLabel { get; set; }

        public GremlinAddEOp() {}

        public GremlinAddEOp(string label)
        {
            EdgeLabel = label;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            inputContext.PivotVariable.AddE(inputContext, EdgeLabel);

            return inputContext;
        }
    }

    internal class GremlinFromOp : GremlinTranslationOperator
    {
        public GraphTraversal2 FromVertexTraversal { get; set; }

        public GremlinFromOp(string stepLabel)
        {
            FromVertexTraversal = GraphTraversal2.__().Select(GremlinKeyword.Pop.last, stepLabel);
        }

        public GremlinFromOp(GraphTraversal2 fromVertexTraversal)
        {
            FromVertexTraversal = fromVertexTraversal;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            FromVertexTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
            GremlinToSqlContext fromVertexContext = FromVertexTraversal.GetEndOp().GetContext();
            inputContext.PivotVariable.From(inputContext, fromVertexContext);

            return inputContext;
        }
    }

    internal class GremlinToOp : GremlinTranslationOperator
    {
        public GraphTraversal2 ToVertexTraversal { get; set; }

        public GremlinToOp(string stepLabel)
        {
            ToVertexTraversal = GraphTraversal2.__().Select(stepLabel);
        }

        public GremlinToOp(GraphTraversal2 toVertexTraversal)
        {
            ToVertexTraversal = toVertexTraversal;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            ToVertexTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
            GremlinToSqlContext toVertexContext = ToVertexTraversal.GetEndOp().GetContext();
            inputContext.PivotVariable.To(inputContext, toVertexContext);

            return inputContext;
        }
    }
}

