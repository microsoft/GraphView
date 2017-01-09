using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFreeEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinFreeEdgeVariable(WEdgeType edgeType)
        {
            EdgeType = edgeType;
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            GremlinVariable inVertex = currentContext.GetSinkVertex(this);
            if (inVertex == null)
            {
                var path = currentContext.GetPathWithEdge(this);
                GremlinFreeVertexVariable newVertex = new GremlinFreeVertexVariable();
                path.SetSinkVariable(newVertex);
                currentContext.TableReferences.Add(newVertex);
                currentContext.VariableList.Add(newVertex);
                currentContext.PivotVariable = newVertex;
            }
            else
            {
                currentContext.PivotVariable = inVertex;
            }
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            // A naive implementation would be: add a new bound/free vertex to the variable list. 
            // A better implementation should reason the status of the edge variable and only reset
            // the pivot variable if possible, thereby avoiding adding a new vertex variable  
            // and reducing one join.
            GremlinVariable outVertex = currentContext.GetSourceVertex(this);
            if (outVertex == null)
            {
                var path = currentContext.GetPathWithEdge(this);
                GremlinFreeVertexVariable newVertex = new GremlinFreeVertexVariable();
                path.SetSourceVariable(newVertex);
                currentContext.TableReferences.Add(newVertex);
                currentContext.VariableList.Add(newVertex);
                currentContext.PivotVariable = newVertex;
            }
            else
            {
                currentContext.PivotVariable = outVertex;
            }
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            var path = currentContext.GetPathWithEdge(this);

            if (path == null)
            {
                throw new QueryCompilationException("Can't find a path");
            }

            if (EdgeType == WEdgeType.InEdge)
            {
                OutV(currentContext);
            }
            else if (EdgeType == WEdgeType.OutEdge)
            {
                InV(currentContext);
            }
            else if (EdgeType == WEdgeType.BothEdge)
            {
                throw new QueryCompilationException();
            }
            else
            {
                throw new NotImplementedException();
            }
        }
    }
}
