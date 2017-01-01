using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinLocalOp: GremlinTranslationOperator
    {
        public GraphTraversal2 LocalTraversal { get; set; }
        public List<object> PropertyKeys { get; set; }

        public GremlinLocalOp(GraphTraversal2 localTraversal)
        {
            LocalTraversal = localTraversal;
            PropertyKeys = new List<object>();
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            throw new NotImplementedException();
            return inputContext;
        }
    }
}
