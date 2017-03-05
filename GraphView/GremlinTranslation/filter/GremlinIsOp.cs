using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinIsOp: GremlinTranslationOperator
    {
        public object Value { get; set; }
        public Predicate Predicate { get; set; }

        public GremlinIsOp(object value)
        {
            Value = value;
        }

        public GremlinIsOp(Predicate predicate)
        {
            Predicate = predicate;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            if (Value != null)
            {
                inputContext.PivotVariable.Is(inputContext, Value);
            }
            else
            {
                inputContext.PivotVariable.Is(inputContext, Predicate);
            }

            return inputContext;
        }
    }
}
