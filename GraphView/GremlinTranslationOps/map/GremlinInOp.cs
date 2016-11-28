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

            //GremlinUtil.CheckIsGremlinVertexVariable(inputContext.CurrVariable);

            GremlinEdgeVariable newEdgeVar = new GremlinEdgeVariable(WEdgeType.OutEdge);
            inputContext.AddNewVariable(newEdgeVar, Labels);
            inputContext.AddLabelsPredicatesToEdge(EdgeLabels, newEdgeVar);

            GremlinVertexVariable sourceVar = new GremlinVertexVariable();
            inputContext.AddNewVariable(sourceVar, Labels);
            inputContext.SetDefaultProjection(sourceVar);

            inputContext.AddPaths(sourceVar, newEdgeVar, inputContext.CurrVariable);

            inputContext.SetCurrVariable(sourceVar);

            return inputContext;
        }
    }
}
