using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinSelectOp: GremlinTranslationOperator
    {
        public List<string> SelectKeys;

        public GremlinSelectOp(string selectKey, params string[] selectKeys)
        {
            SelectKeys = new List<string>();
            SelectKeys.Add(selectKey);

            foreach (var key in selectKeys)
            {
                SelectKeys.Add(key);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();



            return inputContext;
        }
    }
}
