using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinValuesOp : GremlinTranslationOperator
    {
        public List<string> PropertyKeys { get; set; }

        public GremlinValuesOp(params string[] propertyKeys)
        {
            PropertyKeys = new List<string>();
            foreach (var propertyKey in propertyKeys)
            {
                PropertyKeys.Add(propertyKey);
            }
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            inputContext.PivotVariable.Values(inputContext, PropertyKeys);

            return inputContext;
        }
    }
}
