using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslation
{
    internal class GremlinOutOp: GremlinTranslationOperator
    {
        internal  List<string> EdgeLabels { get; set; }

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

            //GremlinUtil.CheckIsGremlinVertexVariable(inputContext.CurrVariable);

            GremlinEdgeVariable newEdgeVar = new GremlinEdgeVariable(inputContext.CurrVariable, WEdgeType.OutEdge);
            inputContext.AddNewVariable(newEdgeVar);
            inputContext.AddLabelsPredicatesToEdge(EdgeLabels, newEdgeVar);

            GremlinVertexVariable sinkVar = new GremlinVertexVariable();
            inputContext.AddPaths(inputContext.CurrVariable, newEdgeVar, sinkVar);
            inputContext.AddNewVariable(sinkVar);
            inputContext.SetDefaultProjection(sinkVar);
            inputContext.SetCurrVariable(sinkVar);

            return inputContext;
        }
    }
}
