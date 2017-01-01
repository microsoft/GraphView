using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextVertexVariable : GremlinVertexVariable2
    {
        GremlinVertexVariable2 contextVariable;

        public GremlinContextVertexVariable(GremlinVertexVariable2 contextVariable)
        {
            this.contextVariable = contextVariable;
            VariableName = contextVariable.VariableName;
        }

        public override GremlinVariableType GetVariableType()
        {
            return contextVariable.GetVariableType();
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return contextVariable.DefaultProjection();
        }

        internal override void Populate(string name)
        {
            contextVariable.Populate(name);
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("BothAdjacencyList");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "BothAdjacencyList");
            GremlinEdgeVariable2 bothEdge = new GremlinBoundEdgeVariable(adjacencyList);
            bothEdge.Populate("_sink");
            currentContext.VariableList.Add(bothEdge);

            GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(bothEdge, "_sink"));
            currentContext.VariableList.Add(bothVertex);

            currentContext.TableReferences.Add(bothEdge);
            currentContext.TableReferences.Add(bothVertex);

            //add Predicate to edge
            foreach (var edgeLabel in edgeLabels)
            {
                var firstExpr = GremlinUtil.GetColumnReferenceExpression(bothEdge.VariableName, "label");
                var secondExpr = GremlinUtil.GetValueExpression(edgeLabel);
                currentContext.AddEqualPredicate(firstExpr, secondExpr);
            }

            currentContext.PivotVariable = bothVertex;
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "_edge");
            GremlinEdgeVariable2 outEdge = new GremlinBoundEdgeVariable(adjacencyList);
            outEdge.Populate("_sink");
            currentContext.VariableList.Add(outEdge);

            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(outEdge, "_sink"));
            currentContext.VariableList.Add(outVertex);

            currentContext.TableReferences.Add(outEdge);
            currentContext.TableReferences.Add(outVertex);

            //add Predicate to edge
            foreach (var edgeLabel in edgeLabels)
            {
                var firstExpr = GremlinUtil.GetColumnReferenceExpression(outVertex.VariableName, "label");
                var secondExpr = GremlinUtil.GetValueExpression(edgeLabel);
                currentContext.AddEqualPredicate(firstExpr, secondExpr);
            }

            currentContext.PivotVariable = outVertex;
        }
    }
}
