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
            GremlinVertexVariable newVertexVar = new GremlinVertexVariable();
            GremlinEdgeVariable newEdgeVar = new GremlinEdgeVariable(GremlinEdgeType.InEdge);
            inputContext.AddGremlinVariable(newEdgeVar);
            inputContext.AddGremlinVariable(newVertexVar);
            inputContext.SetDefaultProjection(newVertexVar);
            inputContext.AddLabelsPredicatesToEdge(EdgeLabels, newEdgeVar);
            return inputContext;
        }
    }
}
