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
        public GremlinKeyword.Pop Pop;

        public GremlinSelectOp(GremlinKeyword.Pop pop, params string[] selectKeys)
        {
            SelectKeys = new List<string>();
            foreach (var key in selectKeys)
            {
                SelectKeys.Add(key);
            }
            Pop = pop;
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
                if (Pop == GremlinKeyword.Pop.first)
                {
                    inputContext.SetCurrVariable(inputContext.AliasToGremlinVariableList[SelectKeys.First()].First());
                }
                else if (Pop == GremlinKeyword.Pop.last)
                {
                    inputContext.SetCurrVariable(inputContext.AliasToGremlinVariableList[SelectKeys.First()].Last());
                }
                else
                {
                    throw new NotImplementedException();
                }
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
