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
        public object Injection { get; set; }

        public GremlinInjectOp(object injection)
        {
            Injection = injection;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            if (inputContext.VariableList.Count == 0)
            {
                GremlinInjectVariable injectVar = new GremlinInjectVariable(null, Injection);
                inputContext.VariableList.Add(injectVar);
                inputContext.TableReferences.Add(injectVar);
                inputContext.SetPivotVariable(injectVar);
            }
            else
            {
                inputContext.PivotVariable.Inject(inputContext, Injection);
            }
            return inputContext;
        }
    }
}
