using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinContextTableVariable: GremlinContextVariable
    {
        public GremlinContextTableVariable(GremlinVariable contextVariable): base(contextVariable) {}

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

            currentContext.Paths.Add(new GremlinMatchPath(ContextVariable as GremlinTableVariable, bothEdge, bothVertex));

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

            currentContext.Paths.Add(new GremlinMatchPath(ContextVariable as GremlinTableVariable, bothEdge, null));

            currentContext.PivotVariable = bothEdge;
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_reverse_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_reverse_edge");
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(ContextVariable, adjacencyList);
            inEdge.Populate("_sink");
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(inEdge, "_sink"));
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);

            currentContext.Paths.Add(new GremlinMatchPath(outVertex, inEdge, ContextVariable as GremlinTableVariable));

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

            currentContext.Paths.Add(new GremlinMatchPath(null, inEdge, ContextVariable as GremlinTableVariable));

            currentContext.PivotVariable = inEdge;
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            GremlinVariable inVertex = currentContext.GetSinkVertex(ContextVariable);
            if (inVertex == null)
            {
                Populate("_sink");
                var path = currentContext.Paths.Find(p => p.EdgeVariable == ContextVariable);
                if (path == null)
                {
                    throw new QueryCompilationException();
                }
                GremlinBoundVertexVariable newVertex =
                    new GremlinBoundVertexVariable(new GremlinVariableProperty(ContextVariable, "_sink"));
                path.SinkVariable = newVertex;
                currentContext.VariableList.Add(newVertex);
                currentContext.TableReferences.Add(newVertex);
                currentContext.PivotVariable = newVertex;
            }
            else
            {
                GremlinContextVariable newVariable = Create(inVertex);
                currentContext.VariableList.Add(newVariable);
                currentContext.PivotVariable = newVariable;
            }
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_edge");
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(ContextVariable, adjacencyList);
            outEdge.Populate("_sink");
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(outEdge, "_sink"));
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);

            currentContext.Paths.Add(new GremlinMatchPath(ContextVariable as GremlinTableVariable, outEdge, outVertex));

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

            currentContext.Paths.Add(new GremlinMatchPath(ContextVariable as GremlinTableVariable, inEdge, null));

            currentContext.PivotVariable = inEdge;
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            GremlinVariable outVertex = currentContext.GetSourceVertex(ContextVariable);
            if (outVertex == null)
            {
                var path = currentContext.Paths.Find(p => p.EdgeVariable == ContextVariable);
                if (path == null)
                {
                    throw new QueryCompilationException();
                }
                Populate("_sink");
                GremlinBoundVertexVariable newVertex =
                    new GremlinBoundVertexVariable(new GremlinVariableProperty(ContextVariable, "_sink"));
                path.SourceVariable = newVertex;
                currentContext.VariableList.Add(newVertex);
                currentContext.TableReferences.Add(newVertex);
                currentContext.PivotVariable = newVertex;
            }
            else
            {
                GremlinContextVariable newVariable = Create(outVertex);
                currentContext.VariableList.Add(newVariable);
                currentContext.PivotVariable = newVariable;
            }
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            ContextVariable.OtherV(currentContext);
        }
    }
}
