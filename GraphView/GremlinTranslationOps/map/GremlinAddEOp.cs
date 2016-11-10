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
            inputContext.AddNewVariable(newAddEVar);
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
            GremlinUtil.CheckIsGremlinVertexVariable(inputContext.AliasToGremlinVariable[StepLabel]);

            (inputContext.CurrVariable as GremlinAddEVariable).FromVariable =
                inputContext.AliasToGremlinVariable[StepLabel] as GremlinVertexVariable;
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
            GremlinUtil.CheckIsGremlinVertexVariable(inputContext.AliasToGremlinVariable[StepLabel]);

            (inputContext.CurrVariable as GremlinAddEVariable).ToVariable =
                inputContext.AliasToGremlinVariable[StepLabel] as GremlinVertexVariable;
            return inputContext;
        }
    }
}

