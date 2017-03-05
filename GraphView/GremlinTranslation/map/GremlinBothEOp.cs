using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinBothEOp: GremlinTranslationOperator
    {
        public List<string> EdgeLabels { get; set; }

        public GremlinBothEOp(params string[] edgelabels)
        {
            EdgeLabels = new List<string>();
            foreach (var edgeLabel in edgelabels)
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

            inputContext.PivotVariable.BothE(inputContext, EdgeLabels);

            return inputContext;
        }
    }
}
