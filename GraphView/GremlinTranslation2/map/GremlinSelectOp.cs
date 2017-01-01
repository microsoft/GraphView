using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSelectOp: GremlinTranslationOperator
    {
        public List<string> SelectKeys { get; set; }
        public GremlinKeyword.Pop Pop { get; set; }

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

            if (Pop != null)
            {
                inputContext.PivotVariable.Select(inputContext, Pop, SelectKeys.First());
            }
            else
            {
                throw new NotImplementedException();
            }

            return inputContext;
        }
    }
}
