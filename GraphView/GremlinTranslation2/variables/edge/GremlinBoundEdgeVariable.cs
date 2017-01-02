using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinBoundEdgeVariable : GremlinEdgeVariable2
    {
        protected static int _count = 0;

        internal override string GenerateTableAlias()
        {
            return "E_" + _count++;
        }

        private GremlinVariableProperty adjacencyList;
        // A list of edge properties to project for this edge table
        //private List<string> projectedProperties;

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            var valueExpr = GremlinUtil.GetValueExpression(adjacencyList.ToScalarExpression());
            PropertyKeys.Add(valueExpr);
            var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("E", PropertyKeys);
            secondTableRef.Alias = GremlinUtil.GetIdentifier(VariableName);
            return GremlinUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        public GremlinBoundEdgeVariable(GremlinVariableProperty adjacencyList)
        {
            VariableName = GenerateTableAlias();
            this.adjacencyList = adjacencyList;
        }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            GremlinVariable2 inVertex = currentContext.GetSinkVertex(currentContext.PivotVariable as GremlinBoundEdgeVariable);
            if (inVertex == null)
            {
                inVertex = new GremlinFreeVertexVariable();
                currentContext.TableReferences.Add(inVertex as GremlinFreeVertexVariable);
                currentContext.VariableList.Add(inVertex);
                currentContext.Paths.Add(new GremlinMatchPath(null,
                                              currentContext.PivotVariable,
                                              inVertex));
            }

            currentContext.PivotVariable = inVertex;
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            // A naive implementation would be: add a new bound/free vertex to the variable list. 
            // A better implementation should reason the status of the edge variable and only reset
            // the pivot variable if possible, thereby avoiding adding a new vertex variable  
            // and reducing one join.

            GremlinVariable2 outVertex = currentContext.GetSourceVertex(currentContext.PivotVariable as GremlinBoundEdgeVariable);
            if (outVertex == null)
            {
                outVertex = new GremlinFreeVertexVariable();
                currentContext.TableReferences.Add(outVertex as GremlinFreeVertexVariable);
                currentContext.VariableList.Add(outVertex);
                currentContext.Paths.Add(new GremlinMatchPath(outVertex,
                                              currentContext.PivotVariable,
                                              null));
            }

            currentContext.PivotVariable = outVertex;
        }
    }
}
