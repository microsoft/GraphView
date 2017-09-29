using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinBothOp: GremlinTranslationOperator
    {
        public List<string> EdgeLabels { get; set; }

        public GremlinBothOp(params string[] edgeLabels)
        {
            EdgeLabels = new List<string>();
            foreach (var edgeLabel in edgeLabels)
            {
                EdgeLabels.Add(edgeLabel);
            }
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            inputContext.PivotVariable.Both(inputContext, EdgeLabels);

            return inputContext;
        }
    }
}
