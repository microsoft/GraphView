using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslation
{
    internal class GremlinGroupOp: GremlinTranslationOperator, IGremlinByModulating
    {
        public List<object> ByList { get; set; }
        public string SideEffect { get; set; }

        public GremlinGroupOp()
        {
            ByList = new List<object>();
        }

        public GremlinGroupOp(string sideEffect)
        {
            ByList = new List<object>();
            SideEffect = sideEffect;
        }

        public void ModulateBy() { }

        public void ModulateBy(GraphTraversal2 paramOp) { }

        public void ModulateBy(string key)
        {
            ByList.Add(key);
        }
        public void ModulateBy(GremlinKeyword.Order order) { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            inputContext.GroupByVariable = new Tuple<GremlinVariable, GroupByRecord>(inputContext.CurrVariable, new GroupByRecord());

            foreach (var key in ByList)
            {
                inputContext.GroupByVariable.Item2.GroupingSpecList.Add(GremlinUtil.GetGroupingSpecification(key as string));
            }

            return inputContext;
        }
    }
}
