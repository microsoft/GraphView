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
            GremlinAddEVariable newAddEVar = new GremlinAddEVariable(EdgeLabel, inputContext.CurrVariable);

            //inputContext.AddPaths(newAddEVar.FromVariable, newAddEVar, newAddEVar.ToVariable);
            inputContext.AddNewVariable(newAddEVar, Labels);
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
                //GremlinUtil.CheckIsGremlinVertexVariable(aliasToGremlinVariable.Item2);
                fromVariable = inputContext.AliasToGremlinVariableList[StepLabel].Last();
            }
            else if (Type == FromType.FromVertexTraversal)
            {
                GremlinUtil.InheritedVariableFromParent(FromVertexTraversal, inputContext);

                WQueryDerivedTable queryDerivedTable = new WQueryDerivedTable()
                {
                    QueryExpr = FromVertexTraversal.GetEndOp().GetContext().ToSqlQuery() as WSelectQueryBlock
                };

                fromVariable = new GremlinDerivedVariable(queryDerivedTable, "from");
                (inputContext.CurrVariable as GremlinAddEVariable).IsNewFromVariable = true;
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

            GremlinUtil.CheckIsGremlinAddEVariable(inputContext.CurrVariable);

            GremlinVariable toVariable = null;
            if (Type == ToType.ToStepLabel)
            {
                toVariable = inputContext.AliasToGremlinVariableList[StepLabel].Last();
            }
            else if (Type == ToType.ToVertexTraversal)
            {
                GremlinUtil.InheritedVariableFromParent(ToVertexTraversal, inputContext);

                WQueryDerivedTable queryDerivedTable = new WQueryDerivedTable()
                {
                    QueryExpr = ToVertexTraversal.GetEndOp().GetContext().ToSqlQuery() as WSelectQueryBlock
                };

                toVariable = new GremlinDerivedVariable(queryDerivedTable, "to");
                (inputContext.CurrVariable as GremlinAddEVariable).IsNewToVariable = true;
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

