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
        public List<object> Injections;

        public GremlinInjectOp(params object[] injections)
        {
            Injections = new List<object>();
            foreach (var injection in injections)
            {
                Injections.Add(injection);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            List<WScalarExpression> parameters = new List<WScalarExpression>();
            foreach (var injection in Injections)
            {
                parameters.Add(GremlinUtil.GetValueExpression(injection));
            }
            WSchemaObjectFunctionTableReference functionTableReference = new WSchemaObjectFunctionTableReference()
            {
                SchemaObject = new WSchemaObjectName(GremlinUtil.GetIdentifier("inject")),
                Parameters = parameters
            };

            GremlinDerivedVariable newVariable = new GremlinDerivedVariable(functionTableReference);
            inputContext.AddNewVariable(newVariable, Labels);
            inputContext.SetCurrVariable(newVariable);
            inputContext.SetDefaultProjection(newVariable);

            return inputContext;
        }
    }
}
