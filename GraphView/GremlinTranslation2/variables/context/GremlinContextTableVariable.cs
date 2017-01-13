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
            Populate("_reverse_edge");
            Populate("_edge");

            GremlinVariableProperty adjReverseEdge = new GremlinVariableProperty(ContextVariable, "_reverse_edge");
            GremlinVariableProperty adjEdge = new GremlinVariableProperty(this, "_edge");
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(adjEdge, adjReverseEdge, WEdgeType.BothEdge, GremlinEdgeType.BothE);
            currentContext.VariableList.Add(bothEdge);
            currentContext.TableReferences.Add(bothEdge);
            currentContext.AddLabelPredicateForEdge(bothEdge, edgeLabels);

            bothEdge.Populate("_sink");
            GremlinVariableProperty sinkProperty = new GremlinVariableProperty(bothEdge, "_sink");
            GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(sinkProperty);
            currentContext.VariableList.Add(bothVertex);
            currentContext.TableReferences.Add(bothVertex);

            currentContext.AddPath(new GremlinMatchPath(ContextVariable as GremlinTableVariable, bothEdge, bothVertex));

            currentContext.SetPivotVariable(bothVertex);
        }

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate("_reverse_edge");
            Populate("_edge");

            GremlinVariableProperty sourceNode = new GremlinVariableProperty(this, GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = new GremlinVariableProperty(ContextVariable, "_reverse_edge");
            GremlinVariableProperty adjEdge = new GremlinVariableProperty(this, "_edge");
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(sourceNode, adjEdge, adjReverseEdge, WEdgeType.BothEdge, GremlinEdgeType.BothForwardE);
            bothEdge.Populate("_source");
            bothEdge.Populate("_sink");
            currentContext.VariableList.Add(bothEdge);
            currentContext.TableReferences.Add(bothEdge);
            currentContext.AddLabelPredicateForEdge(bothEdge, edgeLabels);

            currentContext.AddPath(new GremlinMatchPath(ContextVariable as GremlinTableVariable, bothEdge, null));

            currentContext.SetPivotVariable(bothEdge);
        }

        internal override void BothV(GremlinToSqlContext currentContext)
        {
            if ((ContextVariable as GremlinEdgeTableVariable).EdgeType == WEdgeType.BothEdge)
            {
                Populate("_sink");
                Populate("_source");
                GremlinVariableProperty sinkProperty = new GremlinVariableProperty(ContextVariable, "_sink");
                GremlinVariableProperty sourceProperty = new GremlinVariableProperty(ContextVariable, "_source");
                GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(sinkProperty, sourceProperty);
                currentContext.VariableList.Add(bothVertex);
                currentContext.TableReferences.Add(bothVertex);

                currentContext.SetPivotVariable(bothVertex);
            }
            else
            {
                Populate("_sink");
                var path = currentContext.GetPathFromPathList(ContextVariable as GremlinEdgeTableVariable);
                path.SourceVariable.Populate(GremlinKeyword.NodeID);
                GremlinVariableProperty sourceProperty = new GremlinVariableProperty(path.SourceVariable, GremlinKeyword.NodeID);
                GremlinVariableProperty sinkProperty = new GremlinVariableProperty(ContextVariable, "_sink");
                GremlinBoundVertexVariable newVertex = new GremlinBoundVertexVariable(sourceProperty, sinkProperty);

                currentContext.VariableList.Add(newVertex);
                currentContext.TableReferences.Add(newVertex);
                currentContext.SetPivotVariable(newVertex);
            }
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate("_reverse_edge");

            GremlinVariableProperty sourceProperty = new GremlinVariableProperty(ContextVariable, GremlinKeyword.NodeID);
            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_reverse_edge");
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(sourceProperty, adjacencyList, WEdgeType.InEdge, GremlinEdgeType.InE);
            inEdge.Populate("_sink");
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

            GremlinVariableProperty sinkProperty = new GremlinVariableProperty(inEdge, "_sink");
            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(sinkProperty);
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);

            currentContext.AddPath(new GremlinMatchPath(ContextVariable as GremlinTableVariable, inEdge, outVertex));

            currentContext.SetPivotVariable(outVertex);
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate("_reverse_edge");

            GremlinVariableProperty sourceProperty = new GremlinVariableProperty(ContextVariable, GremlinKeyword.NodeID);
            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_reverse_edge");
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(sourceProperty, adjacencyList, WEdgeType.InEdge, GremlinEdgeType.InForwardE);
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

            currentContext.AddPath(new GremlinMatchPath(ContextVariable as GremlinTableVariable, inEdge, null));

            currentContext.SetPivotVariable(inEdge);
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate("_edge");
            GremlinVariableProperty sourceProperty = new GremlinVariableProperty(ContextVariable, GremlinKeyword.NodeID);
            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_edge");
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjacencyList, WEdgeType.OutEdge, GremlinEdgeType.OutE);
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            outEdge.Populate("_sink");
            GremlinVariableProperty sinkProperty = new GremlinVariableProperty(outEdge, "_sink");
            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(sinkProperty);
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);

            currentContext.AddPath(new GremlinMatchPath(ContextVariable as GremlinTableVariable, outEdge, outVertex));

            currentContext.SetPivotVariable(outVertex);
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate("_edge");

            GremlinVariableProperty sourceProperty = new GremlinVariableProperty(ContextVariable, GremlinKeyword.NodeID);
            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(ContextVariable, "_edge");
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(sourceProperty, adjacencyList, WEdgeType.OutEdge, GremlinEdgeType.OutE);
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

            currentContext.AddPath(new GremlinMatchPath(ContextVariable as GremlinTableVariable, inEdge, null));

            currentContext.SetPivotVariable(inEdge);
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            var path = currentContext.GetPathFromPathList(ContextVariable as GremlinEdgeTableVariable);
            GremlinVariableProperty sinkProperty = new GremlinVariableProperty(this, GremlinKeyword.EdgeSinkId);
            GremlinBoundVertexVariable newVertex = new GremlinBoundVertexVariable(sinkProperty);

            switch ((ContextVariable as GremlinEdgeTableVariable).EdgeType)
            {
                case WEdgeType.BothEdge:
                    Populate(GremlinKeyword.EdgeSinkId);
                    currentContext.VariableList.Add(newVertex);
                    currentContext.TableReferences.Add(newVertex);
                    currentContext.SetPivotVariable(newVertex);
                    break;
                case WEdgeType.OutEdge:
                case WEdgeType.InEdge:
                    if (path != null || path.SinkVariable != null)
                    {
                        if (currentContext.IsVariableInCurrentContext(path.SinkVariable))
                        {
                            currentContext.SetPivotVariable(path.SinkVariable);
                        }
                        else
                        {
                            GremlinContextVariable newContextVariable = Create(path.SinkVariable);
                            currentContext.VariableList.Add(newContextVariable);
                            currentContext.SetPivotVariable(newContextVariable);
                        }
                    }
                    else
                    {
                        Populate(GremlinKeyword.EdgeSinkId);
                        if (path != null) path.SetSinkVariable(newVertex);

                        currentContext.VariableList.Add(newVertex);
                        currentContext.TableReferences.Add(newVertex);
                        currentContext.SetPivotVariable(newVertex);
                    }
                    break;
            }
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            var path = currentContext.GetPathFromPathList(ContextVariable as GremlinEdgeTableVariable);
            GremlinVariableProperty sourceProperty = new GremlinVariableProperty(this, GremlinKeyword.EdgeSourceId);
            GremlinBoundVertexVariable newVertex = new GremlinBoundVertexVariable(sourceProperty);

            switch ((ContextVariable as GremlinEdgeTableVariable).EdgeType)
            {
                case WEdgeType.BothEdge:
                    Populate(GremlinKeyword.EdgeSourceId);
                    currentContext.VariableList.Add(newVertex);
                    currentContext.TableReferences.Add(newVertex);
                    currentContext.SetPivotVariable(newVertex);
                    break;
                case WEdgeType.OutEdge:
                case WEdgeType.InEdge:
                    if (path != null || path.SourceVariable != null)
                    {
                        if (currentContext.IsVariableInCurrentContext(path.SourceVariable))
                        {
                            currentContext.SetPivotVariable(path.SourceVariable);
                        }
                        else
                        {
                            GremlinContextVariable newContextVariable = Create(path.SourceVariable);
                            currentContext.VariableList.Add(newContextVariable);
                            currentContext.SetPivotVariable(newContextVariable);
                        }
                    }
                    else
                    {
                        Populate(GremlinKeyword.EdgeSourceId);
                        if (path != null) path.SetSourceVariable(newVertex);

                        currentContext.VariableList.Add(newVertex);
                        currentContext.TableReferences.Add(newVertex);
                        currentContext.SetPivotVariable(newVertex);
                    }
                    break;
            }
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            if ((ContextVariable as GremlinEdgeTableVariable).EdgeType == WEdgeType.BothEdge)
            {
                Populate("_other");
                GremlinVariableProperty otherProperty = new GremlinVariableProperty(ContextVariable, "_other");
                GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(otherProperty);
                currentContext.VariableList.Add(outVertex);
                currentContext.TableReferences.Add(outVertex);

                currentContext.SetPivotVariable(outVertex);
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

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            foreach (var property in propertyKeys)
            {
                Populate(property);
            }
            GremlinPropertiesVariable newVariable = new GremlinPropertiesVariable(ContextVariable as GremlinTableVariable, propertyKeys);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            if (propertyKeys.Count == 1)
            {
                Populate(propertyKeys.First());
                GremlinVariableProperty newVariableProperty = new GremlinVariableProperty(ContextVariable as GremlinTableVariable, propertyKeys.First());
                currentContext.VariableList.Add(newVariableProperty);
                currentContext.SetPivotVariable(newVariableProperty);
            }
            else
            {
                foreach (var property in propertyKeys)
                {
                    Populate(property);
                }
                GremlinValuesVariable newVariable = new GremlinValuesVariable(ContextVariable as GremlinTableVariable, propertyKeys);
                currentContext.VariableList.Add(newVariable);
                currentContext.TableReferences.Add(newVariable);
                currentContext.SetPivotVariable(newVariable);
            }
        }
    }
}
