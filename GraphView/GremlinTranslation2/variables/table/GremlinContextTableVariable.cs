using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextTableVariable: GremlinContextVariable
    {
        public GremlinContextTableVariable(GremlinVariable2 contextVariable): base(contextVariable) {}

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_edge");
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(ContextVariable, adjacencyList);
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);

            //add Predicate to edge
            AddLabelPredicateToEdge(currentContext, inEdge, edgeLabels);

            currentContext.PivotVariable = inEdge;
        }
    }
}
