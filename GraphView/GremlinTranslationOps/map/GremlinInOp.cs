using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinInOp: GremlinTranslationOperator
    {
        internal List<string> EdgeLabels;

        public GremlinInOp(params string[] labels)
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
            GremlinEdgeVariable newEdgeVar = new GremlinEdgeVariable(GremlinEdgeType.OutEdge);
            GremlinVertexVariable sourceVar = null;
            inputContext.AddNewVariable(newEdgeVar);
            inputContext.AddLabelsPredicatesToEdge(EdgeLabels, newEdgeVar);

            List<GremlinVertexVariable> tempNewVariable = new List<GremlinVertexVariable>();
            foreach (var currVertex in inputContext.CurrVariableList)
            {
                GremlinUtil.CheckIsGremlinVertexVariable(currVertex);
                sourceVar = new GremlinVertexVariable();
                tempNewVariable.Add(sourceVar);
                inputContext.AddNewVariable(sourceVar);
                inputContext.SetDefaultProjection(sourceVar);
                inputContext.AddPaths(currVertex, newEdgeVar, sourceVar);
            }

            inputContext.ClearCurrentVariable();
            foreach (var newVar in tempNewVariable)
            {
                inputContext.AddCurrentVariable(newVar);
            }

            return inputContext;
        }
    }
}
