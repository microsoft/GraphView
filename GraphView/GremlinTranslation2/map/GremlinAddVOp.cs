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

        public GremlinAddVOp()
        {
            throw new NotImplementedException();
        }

        public GremlinAddVOp(params Object[] propertyKeyValues)
        {
            throw new NotImplementedException();
        }

        public GremlinAddVOp(string vertexLabel)
        {
            VertexLabel = vertexLabel;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            if (inputContext.PivotVariable == null)
            {
                GremlinAddVVariable newVariable = new GremlinAddVVariable(VertexLabel);
                inputContext.VariableList.Add(newVariable);
                inputContext.TableReferences.Add(newVariable);
                inputContext.SetVariables.Add(newVariable);
                inputContext.PivotVariable = newVariable;
            }
            else
            {
                inputContext.PivotVariable.AddV(inputContext, VertexLabel);
            }

            return inputContext;
        }
    }
}
