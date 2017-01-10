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
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(adjEdge, adjReverseEdge, WEdgeType.BothEdge);
            bothEdge.Populate("_sink");
            currentContext.VariableList.Add(bothEdge);
            currentContext.TableReferences.Add(bothEdge);
            currentContext.AddLabelPredicateForEdge(bothEdge, edgeLabels);

            GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(bothEdge, "_sink"));
            currentContext.VariableList.Add(bothVertex);
            currentContext.TableReferences.Add(bothVertex);

            currentContext.AddPath(new GremlinMatchPath(ContextVariable as GremlinTableVariable, bothEdge, bothVertex));

            currentContext.PivotVariable = bothVertex;
        }

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate(GremlinKeyword.NodeID);
            ContextVariable.Populate("_reverse_edge");
            ContextVariable.Populate("_edge");

            GremlinVariableProperty sourceNode = new GremlinVariableProperty(this, GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = new GremlinVariableProperty(ContextVariable, "_reverse_edge");
            GremlinVariableProperty adjEdge = new GremlinVariableProperty(this, "_edge");
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(sourceNode, adjEdge, adjReverseEdge, WEdgeType.BothEdge);
            bothEdge.Populate("_source");
            bothEdge.Populate("_sink");
            currentContext.VariableList.Add(bothEdge);
            currentContext.TableReferences.Add(bothEdge);
            currentContext.AddLabelPredicateForEdge(bothEdge, edgeLabels);

            currentContext.AddPath(new GremlinMatchPath(ContextVariable as GremlinTableVariable, bothEdge, null));

            currentContext.PivotVariable = bothEdge;
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_reverse_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_reverse_edge");
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(adjacencyList, WEdgeType.InEdge);
            inEdge.Populate("_sink");
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(inEdge, "_sink"));
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);

            currentContext.AddPath(new GremlinMatchPath(ContextVariable as GremlinTableVariable, inEdge, outVertex));

            currentContext.PivotVariable = outVertex;
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_reverse_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_reverse_edge");
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(adjacencyList, WEdgeType.InEdge);
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

            currentContext.AddPath(new GremlinMatchPath(ContextVariable as GremlinTableVariable, inEdge, null));

            currentContext.PivotVariable = inEdge;
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_edge");
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(adjacencyList, WEdgeType.OutEdge);
            outEdge.Populate("_sink");
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(outEdge, "_sink"));
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);

            currentContext.AddPath(new GremlinMatchPath(ContextVariable as GremlinTableVariable, outEdge, outVertex));

            currentContext.PivotVariable = outVertex;
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            ContextVariable.Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_edge");
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(adjacencyList, WEdgeType.OutEdge);
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

            currentContext.AddPath(new GremlinMatchPath(ContextVariable as GremlinTableVariable, inEdge, null));

            currentContext.PivotVariable = inEdge;
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            if ((ContextVariable as GremlinEdgeTableVariable).EdgeType == WEdgeType.BothEdge)
            {
                Populate("_sink");

                GremlinBoundVertexVariable outVertex =
                    new GremlinBoundVertexVariable(new GremlinVariableProperty(ContextVariable, "_sink"));
                currentContext.VariableList.Add(outVertex);
                currentContext.TableReferences.Add(outVertex);

                //currentContext.AddPath(new GremlinMatchPath(this, outEdge, outVertex));

                currentContext.PivotVariable = outVertex;
            }
            else
            {
                GremlinVariable inVertex = currentContext.GetSinkVertex(ContextVariable);
                if (inVertex == null)
                {
                    //It's a forward edge
                    Populate("_sink");
                    var path = currentContext.GetPathFromPathList(ContextVariable as GremlinTableVariable);
                    GremlinBoundVertexVariable newVertex =
                        new GremlinBoundVertexVariable(new GremlinVariableProperty(ContextVariable, "_sink"));
                    path.SetSinkVariable(newVertex);
                    currentContext.VariableList.Add(newVertex);
                    currentContext.TableReferences.Add(newVertex);
                    currentContext.PivotVariable = newVertex;
                }
                else
                {
                    //It's a reversed edge
                    GremlinContextVariable newVariable = Create(inVertex);
                    currentContext.VariableList.Add(newVariable);
                    currentContext.PivotVariable = newVariable;
                }
            }
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            if ((ContextVariable as GremlinEdgeTableVariable).EdgeType == WEdgeType.BothEdge)
            {
                Populate("_source");

                GremlinBoundVertexVariable outVertex =
                    new GremlinBoundVertexVariable(new GremlinVariableProperty(ContextVariable, "_source"));
                currentContext.VariableList.Add(outVertex);
                currentContext.TableReferences.Add(outVertex);

                //currentContext.AddPath(new GremlinMatchPath(this, outEdge, outVertex));

                currentContext.PivotVariable = outVertex;
            }
            else
            {
                GremlinVariable outVertex = currentContext.GetSourceVertex(ContextVariable);
                if (outVertex == null)
                {
                    var path = currentContext.GetPathFromPathList(ContextVariable as GremlinTableVariable);
                    Populate("_sink");
                    GremlinBoundVertexVariable newVertex =
                        new GremlinBoundVertexVariable(new GremlinVariableProperty(ContextVariable, "_sink"));
                    path.SetSourceVariable(newVertex);
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
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            if ((ContextVariable as GremlinEdgeTableVariable).EdgeType == WEdgeType.BothEdge)
            {
                Populate("_other");

                GremlinBoundVertexVariable outVertex =
                    new GremlinBoundVertexVariable(new GremlinVariableProperty(ContextVariable, "_other"));
                currentContext.VariableList.Add(outVertex);
                currentContext.TableReferences.Add(outVertex);

                currentContext.PivotVariable = outVertex;
            }
            else
            {
                var path = currentContext.GetPathFromPathList(ContextVariable as GremlinTableVariable);
                var edge = (path.EdgeVariable as GremlinEdgeTableVariable).EdgeType;

                if (path == null)
                {
                    throw new QueryCompilationException("Can't find a path");
                }

                if (edge == WEdgeType.InEdge)
                {
                    OutV(currentContext);
                }
                else if (edge == WEdgeType.OutEdge)
                {
                    InV(currentContext);
                }
                else if (edge == WEdgeType.BothEdge)
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
}
