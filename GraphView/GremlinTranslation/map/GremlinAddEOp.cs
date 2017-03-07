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

            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("AddE should follow by a Vertex");
            } 

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
            var gremlinAddETableVariable = inputContext.PivotVariable as GremlinAddETableVariable;
            if (gremlinAddETableVariable != null)
                gremlinAddETableVariable.From(inputContext, fromVertexContext);
            else
                throw new QueryCompilationException("From step only can follow by AddE step.");

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
            var gremlinAddETableVariable = inputContext.PivotVariable as GremlinAddETableVariable;
            if (gremlinAddETableVariable != null)
                gremlinAddETableVariable.To(inputContext, toVertexContext);
            else
                throw new QueryCompilationException("To step only can follow by AddE step.");
            return inputContext;
        }
    }
}

