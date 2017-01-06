using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAsOp: GremlinTranslationOperator
    {
        public List<string> Labels;
        public GremlinAsOp(params string[] labels)
        {
            Labels = new List<string>();
            foreach (var label in labels)
            {
                Labels.Add(label);
            }
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            inputContext.PivotVariable.As(inputContext, Labels);
            return inputContext;
        }
    }
}
