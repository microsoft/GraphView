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

        public GremlinSelectOp(GremlinKeyword.Pop pop, params string[] selectKeys)
        {
            SelectKeys = new List<string>();
            foreach (var key in selectKeys)
            {
                SelectKeys.Add(key);
            }
        }

        public GremlinSelectOp(params string[] selectKeys)
        {
            SelectKeys = new List<string>();
            foreach (var key in selectKeys)
            {
                SelectKeys.Add(key);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            if (SelectKeys.Count == 0)
            {

            }
            else if (SelectKeys.Count == 1)
            {
                //GremlinVariable selectVar = inputContext.AliasToGremlinVariable[SelectKeys.First()];
                //inputContext.SetCurrVariable(selectVar);
            }
            else
            {
                foreach (var selectKey in SelectKeys)
                {

                }
            }
            
            return inputContext;
        }
    }
}
