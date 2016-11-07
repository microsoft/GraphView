using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps
{
    internal class GremlinOutOp: GremlinTranslationOperator
    {
        internal  List<string> EdgeLabels;
        public GremlinOutOp(params string[] labels)
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
            GremlinVertexVariable sinkVar = null;
            inputContext.AddNewVariable(newEdgeVar);
            inputContext.AddLabelsPredicatesToEdge(EdgeLabels, newEdgeVar);

            List<GremlinVertexVariable> tempNewVariable = new List<GremlinVertexVariable>();
            foreach (var currVertex in inputContext.CurrVariableList)
            {
                GremlinUtil.CheckIsGremlinVertexVariable(currVertex);
                sinkVar = new GremlinVertexVariable();
                tempNewVariable.Add(sinkVar);
                inputContext.AddNewVariable(sinkVar);
                inputContext.SetDefaultProjection(sinkVar);
                inputContext.AddPaths(currVertex, newEdgeVar, sinkVar);
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
