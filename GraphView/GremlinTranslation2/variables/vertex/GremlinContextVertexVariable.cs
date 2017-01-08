using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextVertexVariable : GremlinContextVariable
    {
        public GremlinContextVertexVariable(GremlinVariable contextVariable):base(contextVariable) {}

        internal override GremlinVariableType GetVariableType()
        {
            return ContextVariable.GetVariableType();
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_reverse_edge");
            ContextVariable.Populate("_edge");

            GremlinVariableProperty adjReverseEdge = new GremlinVariableProperty(ContextVariable, "_reverse_edge");
            GremlinVariableProperty adjEdge = new GremlinVariableProperty(this, "_edge");
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(this, adjEdge, adjReverseEdge, WEdgeType.BothEdge);
            bothEdge.Populate("_sink");
            currentContext.VariableList.Add(bothEdge);
            currentContext.TableReferences.Add(bothEdge);
            currentContext.AddLabelPredicateForEdge(bothEdge, edgeLabels);

            GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(bothEdge, "_sink"));
            currentContext.VariableList.Add(bothVertex);
            currentContext.TableReferences.Add(bothVertex);
            currentContext.PivotVariable = bothVertex;
        }

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_reverse_edge");
            ContextVariable.Populate("_edge");

            GremlinVariableProperty adjReverseEdge = new GremlinVariableProperty(ContextVariable, "_reverse_edge");
            GremlinVariableProperty adjEdge = new GremlinVariableProperty(this, "_edge");
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(this, adjEdge, adjReverseEdge, WEdgeType.BothEdge);
            bothEdge.Populate("_sink");
            currentContext.VariableList.Add(bothEdge);
            currentContext.TableReferences.Add(bothEdge);
            currentContext.AddLabelPredicateForEdge(bothEdge, edgeLabels);
            currentContext.PivotVariable = bothEdge;
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_edge");
            GremlinEdgeVariable inEdge = new GremlinBoundEdgeVariable(ContextVariable, adjacencyList);
            inEdge.Populate("_sink");
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(inEdge, "_sink"));
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);
            currentContext.PivotVariable = outVertex;
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_reverse_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_reverse_edge");
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(ContextVariable, adjacencyList);
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);
            currentContext.PivotVariable = inEdge;
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_edge");
            GremlinEdgeVariable outEdge = new GremlinBoundEdgeVariable(ContextVariable, adjacencyList);
            outEdge.Populate("_sink");
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(outEdge, "_sink"));
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);
            currentContext.PivotVariable = outVertex;
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_edge");
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(ContextVariable, adjacencyList);
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);
            currentContext.PivotVariable = inEdge;
        }
    }
}
