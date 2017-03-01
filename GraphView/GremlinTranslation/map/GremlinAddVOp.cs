using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAddVOp: GremlinTranslationOperator
    {
        public string VertexLabel { get; set; }
        public List<object> PropertyKeyValues { get; set; }

        public GremlinAddVOp()
        {
            PropertyKeyValues = new List<object>();
            VertexLabel = "vertex";
        }

        public GremlinAddVOp(params object[] propertyKeyValues)
        {
            PropertyKeyValues = new List<object>(propertyKeyValues);
            VertexLabel = "vertex";
        }

        public GremlinAddVOp(string vertexLabel)
        {
            VertexLabel = vertexLabel;
            PropertyKeyValues = new List<object>();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            if (inputContext.PivotVariable == null)
            {
                GremlinAddVVariable newVariable = new GremlinAddVVariable(VertexLabel, PropertyKeyValues, true);
                inputContext.VariableList.Add(newVariable);
                inputContext.TableReferences.Add(newVariable);
                inputContext.SetPivotVariable(newVariable);
            }
            else
            {
                inputContext.PivotVariable.AddV(inputContext, VertexLabel, PropertyKeyValues);
            }

            return inputContext;
        }
    }
}
