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

            GremlinConstantVariable newConstantVar = new GremlinConstantVariable(Constant);
            inputContext.AddNewVariable(newConstantVar);
            inputContext.SetCurrVariable(newConstantVar);

            return inputContext;
        }
    }
}
