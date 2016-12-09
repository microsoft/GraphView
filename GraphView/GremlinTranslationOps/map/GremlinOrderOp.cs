using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinOrderOp: GremlinTranslationOperator, IGremlinByModulating
    {
        public List<Order> OrderList;
        public List<string> KeyList;
        public List<GraphTraversal2> TraversalList;

        public GremlinOrderOp()
        {
            OrderList = new List<Order>();
            KeyList = new List<string>();
            TraversalList = new List<GraphTraversal2>();
        }

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

        public void ModulateBy()
        {
            
        }

        public void ModulateBy(GraphTraversal2 traversal)
        {
            TraversalList.Add(traversal);
        }

        public void ModulateBy(string key)
        {
            KeyList.Add(key);
        }

        public void ModulateBy(Order order)
        {
            OrderList.Add(order);
        }
    }

    public enum Order
    {
        Shuffle,
        Desr,
        Incr
    }
}
