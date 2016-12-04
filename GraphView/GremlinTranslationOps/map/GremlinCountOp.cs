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

            return GremlinUtil.ProcessByFunctionStep("count", inputContext, Labels);

            //inputContext.SetCurrProjection(GremlinUtil.GetFunctionCall("count"));

            //GremlinToSqlContext newContext = new GremlinToSqlContext();
            //WQueryDerivedTable queryDerivedTable = new WQueryDerivedTable()
            //{
            //    QueryExpr = inputContext.ToSelectQueryBlock() as WSelectQueryBlock
            //};
            //GremlinDerivedVariable newVariable = new GremlinDerivedVariable(queryDerivedTable, "count");
            //newContext.AddNewVariable(newVariable, Labels);
            //newContext.SetCurrVariable(newVariable);
            //newContext.SetDefaultProjection(newVariable);

            //return newContext;

            //var functionTableReference = GremlinUtil.GetSchemaObjectFunctionTableReference("count");

            //GremlinDerivedVariable newVariable = new GremlinDerivedVariable(functionTableReference, "count");

            //inputContext.AddNewVariable(newVariable, Labels);
            //inputContext.SetDefaultProjection(newVariable);
            //inputContext.SetCurrVariable(newVariable);

            //return inputContext;
        }
    }
}
