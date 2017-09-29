using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinOutOp: GremlinTranslationOperator
    {
        internal  List<string> EdgeLabels { get; set; }

        public GremlinOutOp(params string[] labels)
        {
            EdgeLabels = new List<string>();
            foreach (var label in labels)
            {
                EdgeLabels.Add(label);
            }
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("This step should follow by a vertex variable.");
            }

            inputContext.PivotVariable.Out(inputContext, EdgeLabels);

            return inputContext;
        }
    }
}
