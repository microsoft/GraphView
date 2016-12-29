using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
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
        //public override WSqlScript ToSqlScript()
        //{
        //    return GetInputContext().ToSqlDelete();
        //}
    }
}
