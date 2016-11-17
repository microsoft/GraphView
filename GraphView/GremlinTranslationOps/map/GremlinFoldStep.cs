using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinFoldStep: GremlinTranslationOperator
    {
        public GremlinFoldStep() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            //inputContext.SetCurrProjection(GremlinUtil.GetFunctionCall("fold"));

            GremlinToSqlContext newContext = new GremlinToSqlContext();
            GremlinListVariable newDerivedVariable = new GremlinListVariable(inputContext.ToSqlQuery());
            newContext.AddNewVariable(newDerivedVariable);
            newContext.SetDefaultProjection(newDerivedVariable);
            newContext.SetCurrVariable(newDerivedVariable);

            //TODO: inherit some variable?

            return newContext;
        }
    }
}
