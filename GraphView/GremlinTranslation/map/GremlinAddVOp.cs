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
        public List<GremlinProperty> PropertyKeyValues { get; set; }

        public GremlinAddVOp()
        {
            PropertyKeyValues = new List<GremlinProperty>();
            VertexLabel = "vertex";
        }

        public GremlinAddVOp(params object[] propertyKeyValues)
        {
            if (propertyKeyValues.Length > 1 && propertyKeyValues.Length % 2 != 0) throw new Exception("The parameter of property should be even");
            PropertyKeyValues = new List<GremlinProperty>();
            for (var i = 0; i < propertyKeyValues.Length; i += 2)
            {
                PropertyKeyValues.Add(new GremlinProperty(GremlinKeyword.PropertyCardinality.list, 
                                                                propertyKeyValues[i] as string,
                                                                propertyKeyValues[i+1], null));
            }
            VertexLabel = "vertex";
        }

        public GremlinAddVOp(string vertexLabel)
        {
            VertexLabel = vertexLabel;
            PropertyKeyValues = new List<GremlinProperty>();
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
