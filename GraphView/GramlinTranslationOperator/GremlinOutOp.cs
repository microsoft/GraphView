using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GramlinTranslationOperator
{
    internal class GremlinOutOp: GremlinTranslationOperator
    {
        public List<string> _labels;
        public GremlinOutOp(params string[] labels)
        {
            _labels = new List<string>;
            foreach (var label in labels)
            {
                _labels.Add(label);
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();
            GremlinVertexVariable newVertexVar = new GremlinVertexVariable();
            GremlinEdgeVariable newEdgeVar = new GremlinEdgeVariable();
            inputContext.RemainingVariableList.Add(newVertexVar);
            inputContext.SetDefaultProjection(newVertexVar);

            // Add paths
            inputContext.AddPaths(inputContext.LastVariable, newEdgeVar, newVertexVar);


            return inputContext;
        }
    }
}
