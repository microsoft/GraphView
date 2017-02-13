using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinOrderOp: GremlinTranslationOperator, IGremlinByModulating
    {
        public List<string> KeyList { get; set; }
        public List<GremlinKeyword.Order> OrderList { get; set; }
        public List<GraphTraversal2> TraversalList { get; set; }

        public GremlinOrderOp()
        {
            KeyList = new List<string>();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            throw new NotImplementedException();
        }

        public void ModulateBy()
        {
            throw new NotImplementedException();
        }

        public void ModulateBy(GraphTraversal2 traversal)
        {
            throw new NotImplementedException();
        }

        public void ModulateBy(string key)
        {
            throw new NotImplementedException();
        }

        public void ModulateBy(GremlinKeyword.Order order)
        {
            throw new NotImplementedException();
        }
    }
}
