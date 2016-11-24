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

            var functionTableReference = GremlinUtil.GetSchemaObjectFunctionTableReference("fold");

            GremlinDerivedVariable newVariable = new GremlinDerivedVariable(functionTableReference);
            inputContext.AddNewVariable(newVariable, Labels);
            inputContext.SetCurrVariable(newVariable);
            inputContext.SetDefaultProjection(newVariable);

            return inputContext;
        }
    }
}
