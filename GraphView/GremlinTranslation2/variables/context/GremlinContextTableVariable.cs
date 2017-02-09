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
            Populate(GremlinKeyword.NodeID);
            Populate(GremlinKeyword.EdgeAdj);
            Populate(GremlinKeyword.ReverseEdgeAdj);
            Populate(GremlinKeyword.Label);
            if (RealVariable is GremlinContextVariable)
            {
                RealVariable.Both(currentContext, edgeLabels);
            }
            else
            {
                GremlinVariableProperty sourceProperty = RealVariable.GetVariableProperty(GremlinKeyword.NodeID);
                GremlinVariableProperty adjReverseEdge = RealVariable.GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
                GremlinVariableProperty adjEdge = RealVariable.GetVariableProperty(GremlinKeyword.EdgeAdj);
                GremlinVariableProperty labelProperty = GetVariableProperty(GremlinKeyword.Label);
                GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, adjReverseEdge, labelProperty, WEdgeType.BothEdge);
                currentContext.VariableList.Add(bothEdge);
                currentContext.TableReferences.Add(bothEdge);
                currentContext.AddLabelPredicateForEdge(bothEdge, edgeLabels);

                bothEdge.Populate(GremlinKeyword.EdgeOtherV);
                GremlinVariableProperty otherProperty = new GremlinVariableProperty(bothEdge, GremlinKeyword.EdgeOtherV);
                GremlinBoundVertexVariable otherVertex = new GremlinBoundVertexVariable(otherProperty);
                currentContext.VariableList.Add(otherVertex);
                currentContext.TableReferences.Add(otherVertex);

                currentContext.SetPivotVariable(otherVertex);
            }
        }

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate(GremlinKeyword.EdgeAdj);
            Populate(GremlinKeyword.ReverseEdgeAdj);
            Populate(GremlinKeyword.Label);
            if (RealVariable is GremlinContextVariable)
            {
                RealVariable.BothE(currentContext, edgeLabels);
            }
            else
            {
                GremlinVariableProperty sourceProperty = RealVariable.GetVariableProperty(GremlinKeyword.NodeID);
                GremlinVariableProperty adjReverseEdge = RealVariable.GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
                GremlinVariableProperty adjEdge = RealVariable.GetVariableProperty(GremlinKeyword.EdgeAdj);
                GremlinVariableProperty labelProperty = GetVariableProperty(GremlinKeyword.Label);
                GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, adjReverseEdge, labelProperty, WEdgeType.BothEdge);
                currentContext.VariableList.Add(bothEdge);
                currentContext.TableReferences.Add(bothEdge);
                currentContext.AddLabelPredicateForEdge(bothEdge, edgeLabels);

                currentContext.SetPivotVariable(bothEdge);
            }
        }

        internal override void BothV(GremlinToSqlContext currentContext)
        {
            Populate(GremlinKeyword.EdgeSourceV);
            Populate(GremlinKeyword.EdgeSinkV);
            Populate(GremlinKeyword.Label);
            if (RealVariable is GremlinContextVariable)
            {
                RealVariable.BothV(currentContext);
            }
            else
            {
                GremlinVariableProperty sourceProperty = RealVariable.GetVariableProperty(GremlinKeyword.EdgeSourceV);
                GremlinVariableProperty sinkProperty = RealVariable.GetVariableProperty(GremlinKeyword.EdgeSinkV);
                GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(sourceProperty, sinkProperty);

                currentContext.VariableList.Add(bothVertex);
                currentContext.TableReferences.Add(bothVertex);
                currentContext.SetPivotVariable(bothVertex);
            }
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate(GremlinKeyword.ReverseEdgeAdj);
            Populate(GremlinKeyword.Label);
            if (RealVariable is GremlinContextVariable)
            {
                RealVariable.In(currentContext, edgeLabels);
            }
            else
            {
                GremlinVariableProperty sourceProperty = RealVariable.GetVariableProperty(GremlinKeyword.NodeID);
                GremlinVariableProperty adjReverseEdge = RealVariable.GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
                GremlinVariableProperty labelProperty = RealVariable.GetVariableProperty(GremlinKeyword.Label);
                GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(sourceProperty, adjReverseEdge, labelProperty, WEdgeType.InEdge);
                currentContext.VariableList.Add(inEdge);
                currentContext.TableReferences.Add(inEdge);
                currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

                inEdge.Populate(GremlinKeyword.EdgeSourceV);

                GremlinVariableProperty edgeProperty = new GremlinVariableProperty(inEdge, GremlinKeyword.EdgeSourceV);
                GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(edgeProperty);
                currentContext.VariableList.Add(outVertex);
                currentContext.TableReferences.Add(outVertex);

                currentContext.AddPath(new GremlinMatchPath(outVertex, inEdge, RealVariable as GremlinTableVariable));

                currentContext.SetPivotVariable(outVertex);
            }
            
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate(GremlinKeyword.ReverseEdgeAdj);
            Populate(GremlinKeyword.Label);
            if (RealVariable is GremlinContextVariable)
            {
                RealVariable.InE(currentContext, edgeLabels);
            }
            else
            {
                GremlinVariableProperty sourceProperty = RealVariable.GetVariableProperty(GremlinKeyword.NodeID);
                GremlinVariableProperty adjReverseEdge = RealVariable.GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
                GremlinVariableProperty labelProperty = RealVariable.GetVariableProperty(GremlinKeyword.Label);
                GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjReverseEdge, labelProperty, WEdgeType.InEdge);
                currentContext.VariableList.Add(outEdge);
                currentContext.TableReferences.Add(outEdge);
                currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

                currentContext.AddPath(new GremlinMatchPath(null, outEdge, RealVariable as GremlinTableVariable));
                currentContext.SetPivotVariable(outEdge);
            }
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate(GremlinKeyword.EdgeAdj);
            Populate(GremlinKeyword.Label);
            if (RealVariable is GremlinContextVariable)
            {
                RealVariable.Out(currentContext, edgeLabels);
            }
            else
            {
                GremlinVariableProperty sourceProperty = RealVariable.GetVariableProperty(GremlinKeyword.NodeID);
                GremlinVariableProperty adjEdge = RealVariable.GetVariableProperty(GremlinKeyword.EdgeAdj);
                GremlinVariableProperty labelProperty = RealVariable.GetVariableProperty(GremlinKeyword.Label);
                GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, labelProperty, WEdgeType.OutEdge);
                currentContext.VariableList.Add(outEdge);
                currentContext.TableReferences.Add(outEdge);
                currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

                outEdge.Populate(GremlinKeyword.EdgeSinkV);
                GremlinVariableProperty edgeProperty = new GremlinVariableProperty(outEdge, GremlinKeyword.EdgeSinkV);
                GremlinBoundVertexVariable inVertex = new GremlinBoundVertexVariable(edgeProperty);
                currentContext.VariableList.Add(inVertex);
                currentContext.TableReferences.Add(inVertex);

                currentContext.AddPath(new GremlinMatchPath(RealVariable as GremlinTableVariable, outEdge, inVertex));

                currentContext.SetPivotVariable(inVertex);
            }
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate(GremlinKeyword.EdgeAdj);
            Populate(GremlinKeyword.Label);
            if (RealVariable is GremlinContextVariable)
            {
                RealVariable.OutE(currentContext, edgeLabels);
            }
            else
            {
                GremlinVariableProperty sourceProperty = RealVariable.GetVariableProperty(GremlinKeyword.NodeID);
                GremlinVariableProperty adjEdge = RealVariable.GetVariableProperty(GremlinKeyword.EdgeAdj);
                GremlinVariableProperty labelProperty = RealVariable.GetVariableProperty(GremlinKeyword.Label);
                GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, labelProperty, WEdgeType.OutEdge);
                currentContext.VariableList.Add(outEdge);
                currentContext.TableReferences.Add(outEdge);
                currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

                currentContext.AddPath(new GremlinMatchPath(RealVariable as GremlinTableVariable, outEdge, null));
                currentContext.SetPivotVariable(outEdge);
            }
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            Populate(GremlinKeyword.EdgeSinkV); //TODO: maybe shouldn't populate
            if (RealVariable is GremlinContextVariable)
            {
                RealVariable.InV(currentContext);
            }
            else
            {
                GremlinVariableProperty sinkProperty = RealVariable.GetVariableProperty(GremlinKeyword.EdgeSinkV);
                GremlinBoundVertexVariable newVertex = new GremlinBoundVertexVariable(sinkProperty);

                switch ((RealVariable as GremlinEdgeTableVariable).EdgeType)
                {
                    case WEdgeType.BothEdge:
                        Populate(GremlinKeyword.EdgeSinkV);
                        currentContext.VariableList.Add(newVertex);
                        currentContext.TableReferences.Add(newVertex);
                        currentContext.SetPivotVariable(newVertex);
                        break;
                    case WEdgeType.OutEdge:
                    case WEdgeType.InEdge:
                        var path = currentContext.GetPathFromPathList(RealVariable as GremlinTableVariable);
                        if (path != null && path.SinkVariable != null)
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
                            Populate(GremlinKeyword.EdgeSinkV);
                            if (path != null) path.SetSinkVariable(newVertex);

                            currentContext.VariableList.Add(newVertex);
                            currentContext.TableReferences.Add(newVertex);
                            currentContext.SetPivotVariable(newVertex);
                        }
                        break;
                }
            }
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            Populate(GremlinKeyword.EdgeSourceV); //TODO: maybe shouldn't populate
            if (RealVariable is GremlinContextVariable)
            {
                RealVariable.OutV(currentContext);
            }
            else
            {
                GremlinVariableProperty sourceProperty = RealVariable.GetVariableProperty(GremlinKeyword.EdgeSourceV);
                GremlinBoundVertexVariable newVertex = new GremlinBoundVertexVariable(sourceProperty);

                switch ((RealVariable as GremlinEdgeTableVariable).EdgeType)
                {
                    case WEdgeType.BothEdge:
                        Populate(GremlinKeyword.EdgeSourceV);
                        currentContext.VariableList.Add(newVertex);
                        currentContext.TableReferences.Add(newVertex);
                        currentContext.SetPivotVariable(newVertex);
                        break;
                    case WEdgeType.OutEdge:
                    case WEdgeType.InEdge:
                        var path = currentContext.GetPathFromPathList(RealVariable as GremlinTableVariable);

                        if (path != null && path.SourceVariable != null)
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
                            Populate(GremlinKeyword.EdgeSourceV);
                            if (path != null) path.SetSourceVariable(newVertex);

                            currentContext.VariableList.Add(newVertex);
                            currentContext.TableReferences.Add(newVertex);
                            currentContext.SetPivotVariable(newVertex);
                        }
                        break;
                }
            }
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            Populate(GremlinKeyword.EdgeOtherV); //TODO: maybe shouldn't populate
            if (RealVariable is GremlinContextVariable)
            {
                RealVariable.OtherV(currentContext);
            }
            else
            {
                switch ((RealVariable as GremlinEdgeTableVariable).EdgeType)
                {
                    case WEdgeType.BothEdge:
                        Populate(GremlinKeyword.EdgeOtherV);
                        GremlinVariableProperty otherProperty = RealVariable.GetVariableProperty(GremlinKeyword.EdgeOtherV);
                        GremlinBoundVertexVariable otherVertex = new GremlinBoundVertexVariable(otherProperty);
                        currentContext.VariableList.Add(otherVertex);
                        currentContext.TableReferences.Add(otherVertex);
                        currentContext.SetPivotVariable(otherVertex);
                        break;
                    case WEdgeType.InEdge:
                        OutV(currentContext);
                        break;
                    case WEdgeType.OutEdge:
                        InV(currentContext);
                        break;
                }
            }
        }

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            foreach (var property in propertyKeys)
            {
                Populate(property);
            }
            if (RealVariable is GremlinContextVariable)
            {
                RealVariable.Properties(currentContext, propertyKeys);
            }
            else
            {
                GremlinPropertiesVariable newVariable = new GremlinPropertiesVariable(RealVariable, propertyKeys);
                currentContext.VariableList.Add(newVariable);
                currentContext.TableReferences.Add(newVariable);
                currentContext.SetPivotVariable(newVariable);
            }
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            foreach (var property in propertyKeys)
            {
                Populate(property);
            }
            if (RealVariable is GremlinContextVariable)
            {
                RealVariable.Values(currentContext, propertyKeys);
            }
            else
            {
                GremlinValuesVariable newVariable = new GremlinValuesVariable(RealVariable as GremlinTableVariable, propertyKeys);
                currentContext.VariableList.Add(newVariable);
                currentContext.TableReferences.Add(newVariable);
                currentContext.SetPivotVariable(newVariable);
            }
        }
    }
}
