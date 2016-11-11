using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinAddVOp: GremlinTranslationOperator
    {
        public Dictionary<string, object> Properties;

        public string VertexLabel;
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

            GremlinUtil.CheckIsGremlinVertexVariable(inputContext.CurrVariable);
            GremlinAddVVariable newAddEVar = new GremlinAddVVariable();
            inputContext.AddNewVariable(newAddEVar);
            inputContext.SetCurrVariable(newAddEVar);
            inputContext.ClearProjection();

            return inputContext;
        }
    }
}
