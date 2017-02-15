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
        public FromType Type { get; set; }
        public string StepLabel { get; set; }
        public GraphTraversal2 FromVertexTraversal { get; set; }

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

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            switch (Type)
            {
                case FromType.FromStepLabel:
                    inputContext.PivotVariable.From(inputContext, StepLabel);
                    break;
                case FromType.FromVertexTraversal:
                    FromVertexTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                    GremlinToSqlContext fromVertexContext = FromVertexTraversal.GetEndOp().GetContext();
                    inputContext.PivotVariable.From(inputContext, fromVertexContext);
                    break;
            }

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
        public string StepLabel { get; set; }
        public GraphTraversal2 ToVertexTraversal { get; set; }
        public ToType Type { get; set; }

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

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            switch (Type)
            {
                case ToType.ToStepLabel:
                    inputContext.PivotVariable.To(inputContext, StepLabel);
                    break;
                case ToType.ToVertexTraversal:
                    ToVertexTraversal.GetStartOp().InheritedVariableFromParent(inputContext);
                    GremlinToSqlContext toVertexContext = ToVertexTraversal.GetEndOp().GetContext();
                    inputContext.PivotVariable.To(inputContext, toVertexContext);
                    break;
            }

            return inputContext;
        }

        public enum ToType 
        {
            ToStepLabel,
            ToVertexTraversal
        }
    }
}

