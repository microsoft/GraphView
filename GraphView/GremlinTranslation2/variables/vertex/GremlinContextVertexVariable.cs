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
            ContextVariable.Populate("BothAdjacencyList");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "BothAdjacencyList");
            GremlinEdgeVariable bothEdge = new GremlinBoundEdgeVariable(this, adjacencyList, WEdgeType.BothEdge);
            bothEdge.Populate("_sink");
            currentContext.VariableList.Add(bothEdge);

            GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(bothEdge, "_sink"));
            currentContext.VariableList.Add(bothVertex);

            currentContext.TableReferences.Add(bothEdge);
            currentContext.TableReferences.Add(bothVertex);

            //add Predicate to edge
            AddLabelPredicateToEdge(currentContext, bothEdge, edgeLabels);
            
            currentContext.PivotVariable = bothVertex;
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_edge");
            GremlinEdgeVariable outEdge = new GremlinBoundEdgeVariable(ContextVariable, adjacencyList);
            outEdge.Populate("_sink");
            currentContext.VariableList.Add(outEdge);

            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(outEdge, "_sink"));
            currentContext.VariableList.Add(outVertex);

            currentContext.TableReferences.Add(outEdge);
            currentContext.TableReferences.Add(outVertex);

            //add Predicate to edge
            AddLabelPredicateToEdge(currentContext, outEdge, edgeLabels);

            currentContext.PivotVariable = outVertex;
        }

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

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_edge");
            GremlinEdgeVariable inEdge = new GremlinBoundEdgeVariable(ContextVariable, adjacencyList);
            inEdge.Populate("_sink");
            currentContext.VariableList.Add(inEdge);

            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(inEdge, "_sink"));
            currentContext.VariableList.Add(outVertex);

            currentContext.TableReferences.Add(inEdge);
            currentContext.TableReferences.Add(outVertex);

            //add Predicate to edge
            AddLabelPredicateToEdge(currentContext, inEdge, edgeLabels);

            currentContext.PivotVariable = outVertex;
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_reverse_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_reverse_edge");
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(ContextVariable, adjacencyList);
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);

            //add Predicate to edge
            AddLabelPredicateToEdge(currentContext, inEdge, edgeLabels);

            currentContext.PivotVariable = inEdge;
        }
    }
}
