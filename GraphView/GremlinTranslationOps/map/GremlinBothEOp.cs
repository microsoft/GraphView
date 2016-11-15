using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.map
{
    internal class GremlinBothEOp: GremlinTranslationOperator
    {
        public List<string> EdgeLabels;

        public GremlinBothEOp(params string[] edgelabels)
        {
            EdgeLabels = new List<string>();
            foreach (var edgeLabel in edgelabels)
            {
                EdgeLabels.Add(edgeLabel);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            GremlinVertexVariable asSource = new GremlinVertexVariable();
            GremlinVertexVariable asTarget = new GremlinVertexVariable();
            GremlinEdgeVariable outE = new GremlinEdgeVariable(GremlinEdgeType.OutEdge);
            GremlinEdgeVariable inE = new GremlinEdgeVariable(GremlinEdgeType.InEdge);
            inputContext.AddNewVariable(asSource);
            inputContext.AddNewVariable(asTarget);
            inputContext.AddPaths(inputContext.CurrVariable, outE, asTarget);
            inputContext.AddPaths(asSource, inE, inputContext.CurrVariable);

            GremlinJoinEdgeVariable newVariable = new GremlinJoinEdgeVariable(outE, inE);
            inputContext.AddNewVariable(newVariable);
            inputContext.SetDefaultProjection(newVariable);
            inputContext.SetCurrVariable(newVariable);
            return inputContext;
        }
    }
}
