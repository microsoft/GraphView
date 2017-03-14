using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinValueMapOp: GremlinTranslationOperator
    {
        public bool IsIncludeTokens { get; set; }
        public List<string> PropertyKeys { get; set; }

        public GremlinValueMapOp(bool isIncludeTokens, params string[] propertyKeys)
        {
            IsIncludeTokens = isIncludeTokens;
            PropertyKeys = new List<string>(propertyKeys);
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            inputContext.PivotVariable.ValueMap(inputContext, IsIncludeTokens, PropertyKeys);

            return inputContext;
        }
    }
}
