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

        public GremlinAddEOp(string label)
        {
            EdgeLabel = label;
        }
        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            throw new NotImplementedException();
            return inputContext;
        }
    }

    internal class GremlinFromOp : GremlinTranslationOperator
    {
        internal string StepLabel;
        public GraphTraversal2 FromVertexTraversal;
        public FromType Type;

        public GremlinFromOp(string stepLabel)
        {
            StepLabel = stepLabel;
            Type = FromType.FromStepLabel;
        }

        public GremlinFromOp(GraphTraversal2 fromVertexTraversal)
        {
            FromVertexTraversal = fromVertexTraversal;
            Type = FromType.FromVertexTraversal;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            
            return inputContext;
        }

        public enum FromType
        {
            FromStepLabel,
            FromVertexTraversal
        }
    }

    internal class GremlinToOp : GremlinTranslationOperator
    {
        public string StepLabel;
        public GraphTraversal2 ToVertexTraversal;
        public ToType Type;

        public GremlinToOp(string stepLabel)
        {
            StepLabel = stepLabel;
            Type = ToType.ToStepLabel; 
        }

        public GremlinToOp(GraphTraversal2 toVertexTraversal)
        {
            ToVertexTraversal = toVertexTraversal;
            Type = ToType.ToVertexTraversal;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            
            return inputContext;
        }

        public enum ToType 
        {
            ToStepLabel,
            ToVertexTraversal
        }
    }
}

