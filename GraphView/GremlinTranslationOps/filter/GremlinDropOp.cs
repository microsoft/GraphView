using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.filter
{
    internal class GremlinDropOp: GremlinTranslationOperator
    {
        public GremlinDropOp() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetContext();

            //remove element and properties from the graph
            return new GremlinToSqlContext();
        }
        public override WSqlFragment ToWSqlFragment()
        {
            return GetInputContext().ToSqlDelete();
        }
    }
}
