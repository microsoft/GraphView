using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinBoundEdgeVariable : GremlinEdgeVariable
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
            var valueExpr = GremlinUtil.GetValueExpr(adjacencyList.ToScalarExpression());
            PropertyKeys.Add(valueExpr);
            Populate("id");
            foreach (var property in ProjectedProperties)
            {
                PropertyKeys.Add(GremlinUtil.GetValueExpr(property));
            }
            var secondTableRef = GremlinUtil.GetFunctionTableReference("E", PropertyKeys, VariableName);
            return GremlinUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        public GremlinBoundEdgeVariable(GremlinVariable sourceVariable, GremlinVariableProperty adjacencyList, WEdgeType edgeType = WEdgeType.OutEdge)
        {
            SourceVariable = sourceVariable;
            VariableName = GenerateTableAlias();
            this.adjacencyList = adjacencyList;
            EdgeType = edgeType;
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            GremlinVariable inVertex = currentContext.GetSinkVertex(this);
            if (inVertex == null)
            {
                var path = currentContext.Paths.Find(p => p.EdgeVariable == this);
                if (path != null)
                {
                    GremlinFreeVertexVariable newVertex = new GremlinFreeVertexVariable();
                    path.SinkVariable = newVertex;
                    currentContext.TableReferences.Add(newVertex);
                    currentContext.VariableList.Add(newVertex);
                    currentContext.PivotVariable = newVertex;
                }
                else
                {
                    Populate("_sink");
                    GremlinBoundVertexVariable newVertex =
                        new GremlinBoundVertexVariable(new GremlinVariableProperty(this, "_sink"));
                    currentContext.VariableList.Add(newVertex);
                    currentContext.TableReferences.Add(newVertex);
                    currentContext.PivotVariable = newVertex;
                }
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
                var path = currentContext.Paths.Find(p => p.EdgeVariable == this);
                if (path != null)
                {
                    GremlinFreeVertexVariable newVertex = new GremlinFreeVertexVariable();
                    path.SourceVariable = newVertex;
                    currentContext.TableReferences.Add(newVertex);
                    currentContext.VariableList.Add(newVertex);
                    currentContext.PivotVariable = newVertex;
                }
                else
                {
                    //The edge in DocDB doesn't have a source_id
                    throw new NotImplementedException();
                    //Populate("_sink");
                    //GremlinBoundVertexVariable newVertex =
                    //    new GremlinBoundVertexVariable(new GremlinVariableProperty(this, "_sink"));
                    //currentContext.VariableList.Add(newVertex);
                    //currentContext.TableReferences.Add(newVertex);
                    //currentContext.PivotVariable = newVertex;
                }
            }
            else
            {
                currentContext.PivotVariable = outVertex;
            }
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            var path = currentContext.Paths.Find(p => p.EdgeVariable == this);

            if (path == null)
            {
                throw new QueryCompilationException("Can't find a path");
            }

            if (path.SourceVariable == SourceVariable)
            {
                InV(currentContext);
            }
            else if (path.SinkVariable == SourceVariable)
            {
                OutV(currentContext);
            }
            else
            {
                throw new NotImplementedException();
            }
        }
    }
}
