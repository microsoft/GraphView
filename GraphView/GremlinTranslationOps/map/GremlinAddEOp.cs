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

            GremlinUtil.CheckIsGremlinVertexVariable(inputContext.CurrVariable);
            GremlinAddEVariable newAddEVar = new GremlinAddEVariable(EdgeLabel, inputContext.CurrVariable as GremlinVertexVariable);
            newAddEVar.EdgeLabel = EdgeLabel;
            inputContext.AddNewVariable(newAddEVar, Labels);
            inputContext.SetCurrVariable(newAddEVar);
            inputContext.ClearProjection();

            return inputContext;
        }
    }

    internal class GremlinFromOp : GremlinTranslationOperator
    {
        internal string StepLabel;

        public GremlinFromOp(string stepLabel)
        {
            StepLabel = stepLabel;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinUtil.CheckIsGremlinAddEVariable(inputContext.CurrVariable);
            GremlinVariable fromVariable = null;
            foreach (var aliasToGremlinVariable in inputContext.AliasToGremlinVariableList)
            {
                if (aliasToGremlinVariable.Item1 == StepLabel)
                {
                    GremlinUtil.CheckIsGremlinVertexVariable(aliasToGremlinVariable.Item2);
                    fromVariable = aliasToGremlinVariable.Item2;
                }
            }

            (inputContext.CurrVariable as GremlinAddEVariable).FromVariable = fromVariable as GremlinVertexVariable;
            return inputContext;
        }
    }

    internal class GremlinToOp : GremlinTranslationOperator
    {
        internal string StepLabel;

        public GremlinToOp(string stepLabel)
        {
            StepLabel = stepLabel;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinUtil.CheckIsGremlinAddEVariable(inputContext.CurrVariable);
            GremlinVariable toVariable = null;
            foreach (var aliasToGremlinVariable in inputContext.AliasToGremlinVariableList)
            {
                if (aliasToGremlinVariable.Item1 == StepLabel)
                {
                    GremlinUtil.CheckIsGremlinVertexVariable(aliasToGremlinVariable.Item2);
                    toVariable = aliasToGremlinVariable.Item2;
                }
            }
            (inputContext.CurrVariable as GremlinAddEVariable).ToVariable =toVariable as GremlinVertexVariable;
            return inputContext;
        }
    }
}

