using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinInjectOp: GremlinTranslationOperator
    {
        public List<object> Injections { get; set; }

        public GremlinInjectOp(params object[] injections)
        {
            Injections = new List<object>(injections);
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            if (inputContext.VariableList.Count == 0)
            {
                GremlinInjectVariable injectVar = new GremlinInjectVariable(null, Injections);
                inputContext.VariableList.Add(injectVar);
                inputContext.TableReferences.Add(injectVar);
                inputContext.PivotVariable = injectVar;
            }
            else
            {
                inputContext.PivotVariable.Inject(inputContext, Injections);
            }
            return inputContext;
        }
    }
}
