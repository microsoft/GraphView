using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinMaxOp: GremlinTranslationOperator
    {
        public Scope Scope;
        public GremlinMaxOp() { }
        public GremlinMaxOp(Scope scope)
        {
            Scope = scope;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            //return GremlinUtil.ProcessByFunctionStep("max", inputContext, Labels);

            var functionTableReference = GremlinUtil.GetSchemaObjectFunctionTableReference("max");

            GremlinDerivedVariable newVariable = new GremlinDerivedVariable(functionTableReference);

            inputContext.AddNewVariable(newVariable, Labels);
            inputContext.SetDefaultProjection(newVariable);
            inputContext.SetCurrVariable(newVariable);

            return inputContext;
        }
    }
}
