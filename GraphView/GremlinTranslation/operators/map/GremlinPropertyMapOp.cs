using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPropertyMapOp : GremlinTranslationOperator
    {
        public List<string> PropertyKeys { get; set; }

        public GremlinPropertyMapOp(params string[] propertyKeys)
        {
            PropertyKeys = new List<string>(propertyKeys);
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            inputContext.PivotVariable.PropertyMap(inputContext, PropertyKeys);

            return inputContext;
        }
    }
}
