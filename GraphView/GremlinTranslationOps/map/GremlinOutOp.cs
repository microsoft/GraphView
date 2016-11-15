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

            GremlinUtil.CheckIsGremlinVertexVariable(inputContext.CurrVariable);

            GremlinEdgeVariable newEdgeVar = new GremlinEdgeVariable(GremlinEdgeType.OutEdge);
            inputContext.AddNewVariable(newEdgeVar);
            inputContext.AddLabelsPredicatesToEdge(EdgeLabels, newEdgeVar);

            GremlinVertexVariable sinkVar = new GremlinVertexVariable();
            inputContext.AddNewVariable(sinkVar);
            inputContext.SetDefaultProjection(sinkVar);
            
            inputContext.AddPaths(inputContext.CurrVariable, newEdgeVar, sinkVar);

            inputContext.SetCurrVariable(sinkVar);

            return inputContext;
        }
    }
}
