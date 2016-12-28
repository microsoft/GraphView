using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinAddEOp: GremlinTranslationOperator
    {
        internal string EdgeLabel;

        public GremlinAddEOp(string label)
        {
            EdgeLabel = label;
        }
        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            //GremlinUtil.CheckIsGremlinVertexVariable(inputContext.CurrVariable);
            WSetVariableStatement statement = inputContext.GetOrCreateSetVariableStatement();
            var newVariableReference = new GremlinVariableReference(statement);
            newVariableReference.Type = VariableType.Vertex;

            GremlinAddEVariable newAddEVar = new GremlinAddEVariable(EdgeLabel, newVariableReference);
            inputContext.AddNewVariable(newAddEVar);
            inputContext.SetCurrVariable(newAddEVar);
            inputContext.SetDefaultProjection(newAddEVar);

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

            GremlinUtil.CheckIsGremlinAddEVariable(inputContext.CurrVariable);
            GremlinVariable fromVariable = null;

            if (Type == FromType.FromStepLabel) {
                fromVariable = inputContext.AliasToGremlinVariableList[StepLabel].Last();
            }
            else if (Type == FromType.FromVertexTraversal)
            {
                GremlinUtil.InheritedContextFromParent(FromVertexTraversal, inputContext);

                inputContext.SaveCurrentState();
                var context = FromVertexTraversal.GetEndOp().GetContext();
                WSetVariableStatement statement = context.GetOrCreateSetVariableStatement();
                fromVariable = new GremlinVariableReference(statement);
                fromVariable.Type = VariableType.Vertex;
                inputContext.AddNewVariable(fromVariable);
                inputContext.ResetSavedState();

                if (!(FromVertexTraversal.GetStartOp() is GremlinParentContextOp))
                {
                    foreach (var s in context.Statements)
                    {
                        inputContext.Statements.Add(s);
                    }
                }
            }

            (inputContext.CurrVariable as GremlinAddEVariable).FromVariable = fromVariable;

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

            GremlinVariable toVariable = null;
            if (Type == ToType.ToStepLabel)
            {
                toVariable = inputContext.AliasToGremlinVariableList[StepLabel].Last();
            }
            else if (Type == ToType.ToVertexTraversal)
            {
                GremlinUtil.InheritedContextFromParent(ToVertexTraversal, inputContext);

                inputContext.SaveCurrentState();
                var context = ToVertexTraversal.GetEndOp().GetContext();
                WSetVariableStatement statement = context.GetOrCreateSetVariableStatement();
                toVariable = new GremlinVariableReference(statement);
                toVariable.Type = VariableType.Vertex;
                inputContext.AddNewVariable(toVariable);
                inputContext.ResetSavedState();

                if (!(ToVertexTraversal.GetStartOp() is GremlinParentContextOp))
                {
                    foreach (var s in context.Statements)
                    {
                        inputContext.Statements.Add(s);
                    }
                }
            }
            (inputContext.CurrVariable as GremlinAddEVariable).ToVariable = toVariable;
            return inputContext;
        }

        public enum ToType 
        {
            ToStepLabel,
            ToVertexTraversal
        }
    }
}

