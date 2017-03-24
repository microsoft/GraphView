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
        public GraphTraversal2 FromVertexTraversal { get; set; }
        public GraphTraversal2 ToVertexTraversal { get; set; }
        public List<GremlinProperty> EdgeProperties { get; set; }

        public GremlinAddEOp(string label)
        {
            EdgeLabel = label;
            EdgeProperties = new List<GremlinProperty>();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("AddE should follow by a Vertex");
            }

            FromVertexTraversal?.GetStartOp().InheritedVariableFromParent(inputContext);
            GremlinToSqlContext fromVertexContext = FromVertexTraversal?.GetEndOp().GetContext();

            ToVertexTraversal?.GetStartOp().InheritedVariableFromParent(inputContext);
            GremlinToSqlContext toVertexContext = ToVertexTraversal?.GetEndOp().GetContext();

            inputContext.PivotVariable.AddE(inputContext, EdgeLabel, EdgeProperties, fromVertexContext, toVertexContext);

            return inputContext;
        }
    }
}

