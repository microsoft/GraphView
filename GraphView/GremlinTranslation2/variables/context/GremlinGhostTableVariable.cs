using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinGhostTableVariable : GremlinGhostVariable
    {
        public GremlinGhostTableVariable(GremlinVariable ghostVariable, GremlinVariable attachedVariable, string label)
            : base(ghostVariable, attachedVariable, label) { }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty adjEdge = GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, adjReverseEdge, WEdgeType.BothEdge);
            currentContext.VariableList.Add(bothEdge);
            currentContext.TableReferences.Add(bothEdge);
            currentContext.AddLabelPredicateForEdge(bothEdge, edgeLabels);

            GremlinVariableProperty otherProperty = bothEdge.GetVariableProperty(GremlinKeyword.EdgeOtherV);
            GremlinBoundVertexVariable otherVertex = new GremlinBoundVertexVariable(otherProperty);
            currentContext.VariableList.Add(otherVertex);
            currentContext.TableReferences.Add(otherVertex);

            currentContext.SetPivotVariable(otherVertex);
        }

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty adjEdge = GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, adjReverseEdge, WEdgeType.BothEdge);
            currentContext.VariableList.Add(bothEdge);
            currentContext.TableReferences.Add(bothEdge);
            currentContext.AddLabelPredicateForEdge(bothEdge, edgeLabels);

            currentContext.SetPivotVariable(bothEdge);
        }

        internal override void BothV(GremlinToSqlContext currentContext)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.EdgeSourceV);
            GremlinVariableProperty sinkProperty = GetVariableProperty(GremlinKeyword.EdgeSinkV);
            GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(sourceProperty, sinkProperty);

            currentContext.VariableList.Add(bothVertex);
            currentContext.TableReferences.Add(bothVertex);
            currentContext.SetPivotVariable(bothVertex);
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty labelProperty = GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(sourceProperty, adjReverseEdge, labelProperty, WEdgeType.InEdge);
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

            GremlinVariableProperty edgeProperty = inEdge.GetVariableProperty(GremlinKeyword.EdgeSourceV);
            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(edgeProperty);
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);

            currentContext.AddPath(new GremlinMatchPath(outVertex, inEdge, RealVariable as GremlinTableVariable));

            currentContext.SetPivotVariable(outVertex);
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty labelProperty = GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjReverseEdge, labelProperty, WEdgeType.InEdge);
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            currentContext.AddPath(new GremlinMatchPath(null, outEdge, RealVariable as GremlinTableVariable));
            currentContext.SetPivotVariable(outEdge);
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjEdge = GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinVariableProperty labelProperty = GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, labelProperty, WEdgeType.OutEdge);
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            GremlinVariableProperty edgeProperty = outEdge.GetVariableProperty(GremlinKeyword.EdgeSinkV);
            GremlinBoundVertexVariable inVertex = new GremlinBoundVertexVariable(edgeProperty);
            currentContext.VariableList.Add(inVertex);
            currentContext.TableReferences.Add(inVertex);

            currentContext.AddPath(new GremlinMatchPath(RealVariable as GremlinTableVariable, outEdge, inVertex));

            currentContext.SetPivotVariable(inVertex);
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjEdge = GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinVariableProperty labelProperty = GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, labelProperty, WEdgeType.OutEdge);
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            currentContext.AddPath(new GremlinMatchPath(RealVariable as GremlinTableVariable, outEdge, null));
            currentContext.SetPivotVariable(outEdge);
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            //GremlinVariableProperty sinkProperty = RealVariable.GetVariableProperty(GremlinKeyword.EdgeSinkV);
            //GremlinBoundVertexVariable newVertex = new GremlinBoundVertexVariable(sinkProperty);

            //switch ((RealVariable as GremlinEdgeTableVariable).EdgeType)
            //{
            //    case WEdgeType.BothEdge:
            //        Populate(GremlinKeyword.EdgeSinkV);
            //        currentContext.VariableList.Add(newVertex);
            //        currentContext.TableReferences.Add(newVertex);
            //        currentContext.SetPivotVariable(newVertex);
            //        break;
            //    case WEdgeType.OutEdge:
            //    case WEdgeType.InEdge:
            //        var path = currentContext.GetPathFromPathList(RealVariable as GremlinTableVariable);
            //        if (path != null && path.SinkVariable != null)
            //        {
            //            if (currentContext.IsVariableInCurrentContext(path.SinkVariable))
            //            {
            //                currentContext.SetPivotVariable(path.SinkVariable);
            //            }
            //            else
            //            {
            //                GremlinRealVariable newRealVariable = Create(path.SinkVariable);
            //                currentContext.VariableList.Add(newRealVariable);
            //                currentContext.SetPivotVariable(newRealVariable);
            //            }
            //        }
            //        else
            //        {
            //            Populate(GremlinKeyword.EdgeSinkV);
            //            if (path != null) path.SetSinkVariable(newVertex);

            //            currentContext.VariableList.Add(newVertex);
            //            currentContext.TableReferences.Add(newVertex);
            //            currentContext.SetPivotVariable(newVertex);
            //        }
            //        break;
            //}
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            //GremlinVariableProperty sourceProperty = RealVariable.GetVariableProperty(GremlinKeyword.EdgeSourceV);
            //GremlinBoundVertexVariable newVertex = new GremlinBoundVertexVariable(sourceProperty);

            //switch ((RealVariable as GremlinEdgeTableVariable).EdgeType)
            //{
            //    case WEdgeType.BothEdge:
            //        Populate(GremlinKeyword.EdgeSourceV);
            //        currentContext.VariableList.Add(newVertex);
            //        currentContext.TableReferences.Add(newVertex);
            //        currentContext.SetPivotVariable(newVertex);
            //        break;
            //    case WEdgeType.OutEdge:
            //    case WEdgeType.InEdge:
            //        var path = currentContext.GetPathFromPathList(RealVariable as GremlinTableVariable);

            //        if (path != null && path.SourceVariable != null)
            //        {
            //            if (currentContext.IsVariableInCurrentContext(path.SourceVariable))
            //            {
            //                currentContext.SetPivotVariable(path.SourceVariable);
            //            }
            //            else
            //            {
            //                GremlinRealVariable newRealVariable = GremlinRealVariable.Create(path.SourceVariable);
            //                currentContext.VariableList.Add(newRealVariable);
            //                currentContext.SetPivotVariable(newRealVariable);
            //            }
            //        }
            //        else
            //        {
            //            Populate(GremlinKeyword.EdgeSourceV);
            //            if (path != null) path.SetSourceVariable(newVertex);

            //            currentContext.VariableList.Add(newVertex);
            //            currentContext.TableReferences.Add(newVertex);
            //            currentContext.SetPivotVariable(newVertex);
            //        }
            //        break;
            //}
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            switch ((RealVariable as GremlinEdgeTableVariable).EdgeType)
            {
                case WEdgeType.BothEdge:
                    GremlinVariableProperty otherProperty = GetVariableProperty(GremlinKeyword.EdgeOtherV);
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
            throw new NotImplementedException();
            //foreach (var property in propertyKeys)
            //{
            //    Populate(property);
            //}
            //GremlinPropertiesVariable newVariable = new GremlinPropertiesVariable(RealVariable as GremlinTableVariable, propertyKeys);
            //currentContext.VariableList.Add(newVariable);
            //currentContext.TableReferences.Add(newVariable);
            //currentContext.SetPivotVariable(newVariable);
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            throw new NotImplementedException();
            //if (propertyKeys.Count == 1)
            //{
            //    Populate(propertyKeys.First());
            //    GremlinVariableProperty newVariableProperty = new GremlinVariableProperty(RealVariable as GremlinTableVariable, propertyKeys.First());
            //    currentContext.VariableList.Add(newVariableProperty);
            //    currentContext.SetPivotVariable(newVariableProperty);
            //}
            //else
            //{
            //    foreach (var property in propertyKeys)
            //    {
            //        Populate(property);
            //    }
            //    GremlinValuesVariable newVariable = new GremlinValuesVariable(RealVariable as GremlinTableVariable, propertyKeys);
            //    currentContext.VariableList.Add(newVariable);
            //    currentContext.TableReferences.Add(newVariable);
            //    currentContext.SetPivotVariable(newVariable);
            //}
        }
    }
}
