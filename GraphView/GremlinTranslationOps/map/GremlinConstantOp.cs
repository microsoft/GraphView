using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinConstantOp: GremlinTranslationOperator
    {
        public object Constant;

        public GremlinConstantOp(object constant)
        {
            Constant = constant;
        }

        public override GremlinToSqlContext GetContext()
        { 
            GremlinToSqlContext inputContext = GetInputContext();

            WQueryDerivedTable queryDerivedTable = GremlinUtil.GetConstantQueryDerivedTable(Constant);

            GremlinDerivedVariable newVariable = new GremlinDerivedVariable(queryDerivedTable);
            inputContext.AddNewVariable(newVariable, Labels);
            inputContext.SetCurrVariable(newVariable);
            inputContext.SetStarProjection(Constant);

            return inputContext;
        }
    }
}
