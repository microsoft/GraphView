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

            GremlinAddVVariable newAddVVar = new GremlinAddVVariable(VertexLabel);
            inputContext.CurrVariable = newAddVVar;
            inputContext.SaveCurrentState();
            WSetVariableStatement statement = inputContext.ToSetVariableStatement();
            inputContext.ResetSavedState();
            inputContext.Statements.Add(statement);
            var newVar = new GremlinVariableReference(statement);
            newVar.Type = VariableType.Vertex;
            inputContext.AddNewVariable(newVar);
            inputContext.SetCurrVariable(newVar);
            inputContext.SetDefaultProjection(newVar);

            return inputContext;
        }
    }
}
