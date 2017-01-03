using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextVertexVariable : GremlinContextVariable
    {
        public GremlinContextVertexVariable(GremlinVariable2 contextVariable):base(contextVariable) {}

        public override GremlinVariableType GetVariableType()
        {
            return ContextVariable.GetVariableType();
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("BothAdjacencyList");

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
                bothEdge.Populate("label");
            }

            currentContext.PivotVariable = bothVertex;
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_edge");

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
                var firstExpr = GremlinUtil.GetColumnReferenceExpression(outEdge.VariableName, "label");
                var secondExpr = GremlinUtil.GetValueExpression(edgeLabel);
                currentContext.AddEqualPredicate(firstExpr, secondExpr);
                outEdge.Populate("label");
            }

            currentContext.PivotVariable = outVertex;
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "_edge");
            GremlinEdgeVariable2 inEdge = new GremlinBoundEdgeVariable(adjacencyList);
            inEdge.Populate("_sink");
            currentContext.VariableList.Add(inEdge);

            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(inEdge, "_sink"));
            currentContext.VariableList.Add(outVertex);

            currentContext.TableReferences.Add(inEdge);
            currentContext.TableReferences.Add(outVertex);

            //add Predicate to edge
            foreach (var edgeLabel in edgeLabels)
            {
                var firstExpr = GremlinUtil.GetColumnReferenceExpression(inEdge.VariableName, "label");
                var secondExpr = GremlinUtil.GetValueExpression(edgeLabel);
                currentContext.AddEqualPredicate(firstExpr, secondExpr);
                inEdge.Populate("label");
            }

            currentContext.PivotVariable = outVertex;
        }
    }
}
