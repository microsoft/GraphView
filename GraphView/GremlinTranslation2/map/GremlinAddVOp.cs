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

        public GremlinAddVOp() {}

        public GremlinAddVOp(params object[] propertyKeyValues)
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
                GremlinAddVVariable newVariable = new GremlinAddVVariable(VertexLabel, true);
                inputContext.VariableList.Add(newVariable);
                inputContext.TableReferences.Add(newVariable);
                inputContext.SetPivotVariable(newVariable);
            }
            else
            {
                inputContext.PivotVariable.AddV(inputContext, VertexLabel);
            }

            return inputContext;
        }
    }
}
