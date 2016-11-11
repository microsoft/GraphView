using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinOrderOp: GremlinTranslationOperator
    {
        //public Order Order;
        public List<string> KeyList;
        //public GremlinTranslationOperator ParamOp; 

        public GremlinOrderOp() { }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            inputContext.OrderByVariable = new Tuple<GremlinVariable, OrderByRecord>(inputContext.CurrVariable, new OrderByRecord());

            foreach (var key in KeyList)
            {
                inputContext.OrderByVariable.Item2.SortOrderList.Add(GremlinUtil.GetExpressionWithSortOrder(key, Order.Desr));
            }

            return inputContext;
        }
    }

    public enum Order
    {
        Shuffle,
        Desr,
        Incr
    }
}
