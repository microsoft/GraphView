using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinUnfoldOp: GremlinTranslationOperator
    {
        public GremlinUnfoldOp() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            //var functionTableReference = GremlinUtil.GetSchemaObjectFunctionTableReference("unfold");

            //GremlinDerivedVariable newVariable = new GremlinDerivedVariable(functionTableReference, "unfold");
            //inputContext.AddNewVariable(newVariable);
            //inputContext.SetCurrVariable(newVariable);
            //inputContext.SetDefaultProjection(newVariable);

            return inputContext;
        }
    }
}
