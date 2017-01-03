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
            PropertyKeys.Add(GremlinUtil.GetValueExpression("id"));
            foreach (var property in projectedProperties)
            {
                PropertyKeys.Add(GremlinUtil.GetValueExpression(property));
            }
            var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("E", PropertyKeys);
            secondTableRef.Alias = GremlinUtil.GetIdentifier(VariableName);
            return GremlinUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        public GremlinBoundEdgeVariable(GremlinVariableProperty adjacencyList, WEdgeType edgeType = WEdgeType.OutEdge)
        {
            VariableName = GenerateTableAlias();
            this.adjacencyList = adjacencyList;
            EdgeType = edgeType;
        }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            GremlinVertexVariable2 inVertex = currentContext.GetSinkVertex(this);
            if (inVertex == null)
            {
                inVertex = new GremlinFreeVertexVariable();
                currentContext.TableReferences.Add(inVertex as GremlinFreeVertexVariable);
                currentContext.VariableList.Add(inVertex);
                currentContext.Paths.Find(p => p.EdgeVariable == this).SinkVariable = inVertex;
            }

            currentContext.PivotVariable = inVertex;
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            // A naive implementation would be: add a new bound/free vertex to the variable list. 
            // A better implementation should reason the status of the edge variable and only reset
            // the pivot variable if possible, thereby avoiding adding a new vertex variable  
            // and reducing one join.

            GremlinVertexVariable2 outVertex = currentContext.GetSourceVertex(this);
            if (outVertex == null)
            {
                outVertex = new GremlinFreeVertexVariable();
                currentContext.TableReferences.Add(outVertex as GremlinFreeVertexVariable);
                currentContext.VariableList.Add(outVertex);
                currentContext.Paths.Find(p => p.EdgeVariable == this).SinkVariable = outVertex;
            }

            currentContext.PivotVariable = outVertex;
        }
    }
}
