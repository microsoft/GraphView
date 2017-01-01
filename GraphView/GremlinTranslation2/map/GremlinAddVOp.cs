using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAddVOp: GremlinTranslationOperator
    {
        public Dictionary<string, object> Properties { get; set; }
        public string VertexLabel { get; set; }

        public GremlinAddVOp() { }

        public GremlinAddVOp(params Object[] propertyKeyValues)
        {
            
        }

        public GremlinAddVOp(string vertexLabel)
        {
            VertexLabel = vertexLabel;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            throw new NotImplementedException();
            return inputContext;
        }
    }
}
