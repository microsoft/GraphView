using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinSumOp: GremlinTranslationOperator
    {
        public GremlinSumOp() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            return GremlinUtil.ProcessByFunctionStep("sum", inputContext, Labels);
            //var functionTableReference = GremlinUtil.GetSchemaObjectFunctionTableReference("sum");

            //GremlinDerivedVariable newVariable = new GremlinDerivedVariable(functionTableReference, "sum");

            //inputContext.AddNewVariable(newVariable, Labels);
            //inputContext.SetDefaultProjection(newVariable);
            //inputContext.SetCurrVariable(newVariable);

            //return inputContext;
        }
    }
}