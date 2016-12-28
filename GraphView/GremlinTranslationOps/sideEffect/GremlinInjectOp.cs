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
        public object[] Injections { get; set; }

        public GremlinInjectOp(params object[] injections)
        {
            Injections = injections;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            if (inputContext.NewVariableList.Count == 0)
            {
                WSqlStatement statement = GremlinUtil.GetInjectStatement(Injections);
                GremlinDerivedVariable newVariable = new GremlinDerivedVariable(statement, "inject");

                inputContext.AddNewVariable(newVariable);
                inputContext.SetCurrVariable(newVariable);
                inputContext.SetDefaultProjection(newVariable);
            }
            else
            {
                //TODO
                throw new NotImplementedException();
            }

            return inputContext;
        }
    }
}
