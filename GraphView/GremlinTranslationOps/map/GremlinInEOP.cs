using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinInEOP: GremlinTranslationOperator
    {
        internal List<string> EdgeLabels;

        public GremlinInEOP(params string[] labels)
        {
            EdgeLabels = new List<string>();
            foreach (var label in labels)
            {
                EdgeLabels.Add(label);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            GremlinEdgeVariable newEdgeVar = new GremlinEdgeVariable(GremlinEdgeType.InEdge);
            inputContext.AddNewVariable(newEdgeVar);
            inputContext.SetDefaultProjection(newEdgeVar);
            inputContext.AddLabelsPredicatesToEdge(EdgeLabels, newEdgeVar);

            GremlinVertexVariable sourceVar = null;
            foreach (var currVertex in inputContext.CurrVariableList)
            {
                GremlinUtil.CheckIsGremlinVertexVariable(currVertex);
                sourceVar = new GremlinVertexVariable();
                inputContext.AddNewVariable(sourceVar);
                inputContext.AddPaths(sourceVar, newEdgeVar, currVertex);
            }

            inputContext.SetCurrentVariable(newEdgeVar);
            return inputContext;          
        }
    }
}
