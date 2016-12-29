using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinInOp: GremlinTranslationOperator
    {
        internal List<string> EdgeLabels { get; set; }

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

            GremlinVertexVariable sourceVar = new GremlinVertexVariable();
            inputContext.AddNewVariable(sourceVar);
            inputContext.SetDefaultProjection(sourceVar);

            GremlinEdgeVariable newEdgeVar = new GremlinEdgeVariable(sourceVar, WEdgeType.OutEdge);
            inputContext.AddNewVariable(newEdgeVar);
            inputContext.AddLabelsPredicatesToEdge(EdgeLabels, newEdgeVar);
            inputContext.AddPaths(sourceVar, newEdgeVar, inputContext.CurrVariable);

            inputContext.SetCurrVariable(sourceVar);

            return inputContext;
        }
    }
}
