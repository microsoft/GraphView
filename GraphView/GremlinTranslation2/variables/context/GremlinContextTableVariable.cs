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

            GremlinVariableProperty sourceProperty = ContextVariable.GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = ContextVariable.GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty adjEdge = ContextVariable.GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, adjReverseEdge, WEdgeType.BothEdge);
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

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate(GremlinKeyword.EdgeAdj);
            Populate(GremlinKeyword.ReverseEdgeAdj);

            GremlinVariableProperty sourceProperty = ContextVariable.GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = ContextVariable.GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty adjEdge = ContextVariable.GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, adjReverseEdge, WEdgeType.BothEdge);
            currentContext.VariableList.Add(bothEdge);
            currentContext.TableReferences.Add(bothEdge);
            currentContext.AddLabelPredicateForEdge(bothEdge, edgeLabels);

            currentContext.SetPivotVariable(bothEdge);
        }

        internal override void BothV(GremlinToSqlContext currentContext)
        {
            Populate(GremlinKeyword.EdgeSourceV);
            Populate(GremlinKeyword.EdgeSinkV);

            GremlinVariableProperty sourceProperty = ContextVariable.GetVariableProperty(GremlinKeyword.EdgeSourceV);
            GremlinVariableProperty sinkProperty = ContextVariable.GetVariableProperty(GremlinKeyword.EdgeSinkV);
            GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(sourceProperty, sinkProperty);

            currentContext.VariableList.Add(bothVertex);
            currentContext.TableReferences.Add(bothVertex);
            currentContext.SetPivotVariable(bothVertex);
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate(GremlinKeyword.ReverseEdgeAdj);

            GremlinVariableProperty sourceProperty = ContextVariable.GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = ContextVariable.GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(sourceProperty, adjReverseEdge, WEdgeType.InEdge);
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

            inEdge.Populate(GremlinKeyword.EdgeSourceV);

            GremlinVariableProperty edgeProperty = new GremlinVariableProperty(inEdge, GremlinKeyword.EdgeSourceV);
            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(edgeProperty);
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);

            currentContext.AddPath(new GremlinMatchPath(outVertex, inEdge, ContextVariable as GremlinTableVariable));

            currentContext.SetPivotVariable(outVertex);
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate(GremlinKeyword.ReverseEdgeAdj);

            GremlinVariableProperty sourceProperty = ContextVariable.GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = ContextVariable.GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjReverseEdge, WEdgeType.InEdge);
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            currentContext.AddPath(new GremlinMatchPath(null, outEdge, ContextVariable as GremlinTableVariable));
            currentContext.SetPivotVariable(outEdge);
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate(GremlinKeyword.EdgeAdj);

            GremlinVariableProperty sourceProperty = ContextVariable.GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjEdge = ContextVariable.GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, WEdgeType.OutEdge);
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            outEdge.Populate(GremlinKeyword.EdgeSinkV);
            GremlinVariableProperty edgeProperty = new GremlinVariableProperty(outEdge, GremlinKeyword.EdgeSinkV);
            GremlinBoundVertexVariable inVertex = new GremlinBoundVertexVariable(edgeProperty);
            currentContext.VariableList.Add(inVertex);
            currentContext.TableReferences.Add(inVertex);

            currentContext.AddPath(new GremlinMatchPath(ContextVariable as GremlinTableVariable, outEdge, inVertex));

            currentContext.SetPivotVariable(inVertex);
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate(GremlinKeyword.EdgeAdj);

            GremlinVariableProperty sourceProperty = ContextVariable.GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjEdge = ContextVariable.GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, WEdgeType.OutEdge);
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            currentContext.AddPath(new GremlinMatchPath(ContextVariable as GremlinTableVariable, outEdge, null));
            currentContext.SetPivotVariable(outEdge);
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            GremlinVariableProperty sinkProperty = ContextVariable.GetVariableProperty(GremlinKeyword.EdgeSinkV);
            GremlinBoundVertexVariable newVertex = new GremlinBoundVertexVariable(sinkProperty);

            switch ((ContextVariable as GremlinEdgeTableVariable).EdgeType)
            {
                case WEdgeType.BothEdge:
                    Populate(GremlinKeyword.EdgeSinkV);
                    currentContext.VariableList.Add(newVertex);
                    currentContext.TableReferences.Add(newVertex);
                    currentContext.SetPivotVariable(newVertex);
                    break;
                case WEdgeType.OutEdge:
                case WEdgeType.InEdge:
                    var path = currentContext.GetPathFromPathList(ContextVariable as GremlinTableVariable);
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

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            // A naive implementation would be: add a new bound/free vertex to the variable list. 
            // A better implementation should reason the status of the edge variable and only reset
            // the pivot variable if possible, thereby avoiding adding a new vertex variable  
            // and reducing one join.
            GremlinVariableProperty sourceProperty = ContextVariable.GetVariableProperty(GremlinKeyword.EdgeSourceV);
            GremlinBoundVertexVariable newVertex = new GremlinBoundVertexVariable(sourceProperty);

            switch ((ContextVariable as GremlinEdgeTableVariable).EdgeType)
            {
                case WEdgeType.BothEdge:
                    Populate(GremlinKeyword.EdgeSourceV);
                    currentContext.VariableList.Add(newVertex);
                    currentContext.TableReferences.Add(newVertex);
                    currentContext.SetPivotVariable(newVertex);
                    break;
                case WEdgeType.OutEdge:
                case WEdgeType.InEdge:
                    var path = currentContext.GetPathFromPathList(ContextVariable as GremlinTableVariable);

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

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            switch ((ContextVariable as GremlinEdgeTableVariable).EdgeType)
            {
                case WEdgeType.BothEdge:
                    Populate(GremlinKeyword.EdgeOtherV);
                    GremlinVariableProperty otherProperty = new GremlinVariableProperty(ContextVariable, GremlinKeyword.EdgeOtherV);
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
