using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.sideEffect
{
    internal class GremlinInjectOp: GremlinTranslationOperator
    {
        public List<object> Injections;

        public GremlinInjectOp(params object[] injections)
        {
            Injections = new List<object>();
            foreach (var injection in injections)
            {
                Injections.Add(injection);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            return inputContext;
        }
    }
}
