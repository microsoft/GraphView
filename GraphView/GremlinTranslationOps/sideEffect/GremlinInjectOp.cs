using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GremlinTranslationOps.sideEffect
{
    internal class GremlinInjectOp: GremlinTranslationOperator
    {
        public object[] Injections;

        public GremlinInjectOp(params object[] injections)
        {
            Injections = injections;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            //var functionTableReference = GremlinUtil.GetSchemaObjectFunctionTableReference("inject", Injections);

            //GremlinDerivedVariable newVariable = new GremlinDerivedVariable(functionTableReference, "inject");
            //inputContext.AddNewVariable(newVariable, Labels);
            //inputContext.SetCurrVariable(newVariable);
            //inputContext.SetDefaultProjection(newVariable);

            return inputContext;
        }
    }
}
