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

            GremlinUtil.CheckIsGremlinVertexVariable(inputContext.CurrVariable);

            GremlinEdgeVariable newEdgeVar = new GremlinEdgeVariable(GremlinEdgeType.OutEdge);
            inputContext.AddNewVariable(newEdgeVar);
            inputContext.AddLabelsPredicatesToEdge(EdgeLabels, newEdgeVar);

            GremlinVertexVariable sourceVar = new GremlinVertexVariable();
            inputContext.AddNewVariable(sourceVar);
            inputContext.SetDefaultProjection(sourceVar);

            inputContext.AddPaths(sourceVar, newEdgeVar, inputContext.CurrVariable);

            inputContext.SetCurrentVariable(sourceVar);

            return inputContext;
        }
    }
}
