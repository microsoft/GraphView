using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinCountOp: GremlinTranslationOperator
    {
        public GremlinCountOp() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            //inputContext.SetCurrProjection(GremlinUtil.GetFunctionCall("count"));

            //GremlinToSqlContext newContext = new GremlinToSqlContext();
            //GremlinScalarVariable newScalarVariable = new GremlinScalarVariable(inputContext.ToSqlQuery());

            var functionTableReference = GremlinUtil.GetSchemaObjectFunctionTableReference("count");

            GremlinDerivedVariable newVariable = new GremlinDerivedVariable(functionTableReference);

            inputContext.AddNewVariable(newVariable, Labels);
            inputContext.SetDefaultProjection(newVariable);
            inputContext.SetCurrVariable(newVariable);

            return inputContext;
        }
    }
}
