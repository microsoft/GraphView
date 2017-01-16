using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinGhostTableVariable : GremlinGhostVariable
    {
        public GremlinGhostTableVariable(GremlinVariable ghostVariable, GremlinVariable attachedVariable) : base(ghostVariable, attachedVariable) { }

        internal override void Both(GremlinToSqlContext currentGhost, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate(GremlinKeyword.EdgeAdj);
            Populate(GremlinKeyword.ReverseEdgeAdj);

            GremlinVariableProperty sourceProperty = new GremlinVariableProperty(RealVariable, GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = new GremlinVariableProperty(RealVariable, GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty adjEdge = new GremlinVariableProperty(RealVariable, GremlinKeyword.EdgeAdj);
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, adjReverseEdge, WEdgeType.BothEdge);
            currentGhost.VariableList.Add(bothEdge);
            currentGhost.TableReferences.Add(bothEdge);
            currentGhost.AddLabelPredicateForEdge(bothEdge, edgeLabels);

            bothEdge.Populate(GremlinKeyword.EdgeOtherV);
            GremlinVariableProperty otherProperty = new GremlinVariableProperty(bothEdge, GremlinKeyword.EdgeOtherV);
            GremlinBoundVertexVariable otherVertex = new GremlinBoundVertexVariable(otherProperty);
            currentGhost.VariableList.Add(otherVertex);
            currentGhost.TableReferences.Add(otherVertex);

            currentGhost.SetPivotVariable(otherVertex);
        }

        internal override void BothE(GremlinToSqlContext currentGhost, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate(GremlinKeyword.EdgeAdj);
            Populate(GremlinKeyword.ReverseEdgeAdj);

            GremlinVariableProperty sourceProperty = new GremlinVariableProperty(RealVariable, GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = new GremlinVariableProperty(RealVariable, GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty adjEdge = new GremlinVariableProperty(RealVariable, GremlinKeyword.EdgeAdj);
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, adjReverseEdge, WEdgeType.BothEdge);
            currentGhost.VariableList.Add(bothEdge);
            currentGhost.TableReferences.Add(bothEdge);
            currentGhost.AddLabelPredicateForEdge(bothEdge, edgeLabels);

            currentGhost.SetPivotVariable(bothEdge);
        }

        internal override void BothV(GremlinToSqlContext currentGhost)
        {
            Populate(GremlinKeyword.EdgeSourceV);
            Populate(GremlinKeyword.EdgeSinkV);

            GremlinVariableProperty sourceProperty = new GremlinVariableProperty(RealVariable, GremlinKeyword.EdgeSourceV);
            GremlinVariableProperty sinkProperty = new GremlinVariableProperty(RealVariable, GremlinKeyword.EdgeSinkV);
            GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(sourceProperty, sinkProperty);

            currentGhost.VariableList.Add(bothVertex);
            currentGhost.TableReferences.Add(bothVertex);
            currentGhost.SetPivotVariable(bothVertex);
        }

        internal override void In(GremlinToSqlContext currentGhost, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate(GremlinKeyword.ReverseEdgeAdj);

            GremlinVariableProperty sourceProperty = new GremlinVariableProperty(RealVariable, GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = new GremlinVariableProperty(RealVariable, GremlinKeyword.ReverseEdgeAdj);
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(sourceProperty, adjReverseEdge, WEdgeType.InEdge);
            currentGhost.VariableList.Add(inEdge);
            currentGhost.TableReferences.Add(inEdge);
            currentGhost.AddLabelPredicateForEdge(inEdge, edgeLabels);

            inEdge.Populate(GremlinKeyword.EdgeSourceV);

            GremlinVariableProperty edgeProperty = new GremlinVariableProperty(inEdge, GremlinKeyword.EdgeSourceV);
            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(edgeProperty);
            currentGhost.VariableList.Add(outVertex);
            currentGhost.TableReferences.Add(outVertex);

            currentGhost.AddPath(new GremlinMatchPath(outVertex, inEdge, RealVariable as GremlinTableVariable));

            currentGhost.SetPivotVariable(outVertex);
        }

        internal override void InE(GremlinToSqlContext currentGhost, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate(GremlinKeyword.ReverseEdgeAdj);

            GremlinVariableProperty sourceProperty = new GremlinVariableProperty(RealVariable, GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = new GremlinVariableProperty(RealVariable, GremlinKeyword.ReverseEdgeAdj);
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjReverseEdge, WEdgeType.InEdge);
            currentGhost.VariableList.Add(outEdge);
            currentGhost.TableReferences.Add(outEdge);
            currentGhost.AddLabelPredicateForEdge(outEdge, edgeLabels);

            currentGhost.AddPath(new GremlinMatchPath(null, outEdge, RealVariable as GremlinTableVariable));
            currentGhost.SetPivotVariable(outEdge);
        }

        internal override void Out(GremlinToSqlContext currentGhost, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate(GremlinKeyword.EdgeAdj);

            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjEdge = GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, WEdgeType.OutEdge);
            currentGhost.VariableList.Add(outEdge);
            currentGhost.TableReferences.Add(outEdge);
            currentGhost.AddLabelPredicateForEdge(outEdge, edgeLabels);

            outEdge.Populate(GremlinKeyword.EdgeSinkV);
            GremlinVariableProperty edgeProperty = new GremlinVariableProperty(outEdge, GremlinKeyword.EdgeSinkV);
            GremlinBoundVertexVariable inVertex = new GremlinBoundVertexVariable(edgeProperty);
            currentGhost.VariableList.Add(inVertex);
            currentGhost.TableReferences.Add(inVertex);

            currentGhost.AddPath(new GremlinMatchPath(RealVariable as GremlinTableVariable, outEdge, inVertex));

            currentGhost.SetPivotVariable(inVertex);
        }

        internal override void OutE(GremlinToSqlContext currentGhost, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate(GremlinKeyword.EdgeAdj);

            GremlinVariableProperty sourceProperty = new GremlinVariableProperty(RealVariable, GremlinKeyword.NodeID);
            GremlinVariableProperty adjEdge = new GremlinVariableProperty(RealVariable, GremlinKeyword.EdgeAdj);
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, WEdgeType.OutEdge);
            currentGhost.VariableList.Add(outEdge);
            currentGhost.TableReferences.Add(outEdge);
            currentGhost.AddLabelPredicateForEdge(outEdge, edgeLabels);

            currentGhost.AddPath(new GremlinMatchPath(RealVariable as GremlinTableVariable, outEdge, null));
            currentGhost.SetPivotVariable(outEdge);
        }

        internal override void InV(GremlinToSqlContext currentGhost)
        {
            //GremlinVariableProperty sinkProperty = new GremlinVariableProperty(RealVariable, GremlinKeyword.EdgeSinkV);
            //GremlinBoundVertexVariable newVertex = new GremlinBoundVertexVariable(sinkProperty);

            //switch ((RealVariable as GremlinEdgeTableVariable).EdgeType)
            //{
            //    case WEdgeType.BothEdge:
            //        Populate(GremlinKeyword.EdgeSinkV);
            //        currentGhost.VariableList.Add(newVertex);
            //        currentGhost.TableReferences.Add(newVertex);
            //        currentGhost.SetPivotVariable(newVertex);
            //        break;
            //    case WEdgeType.OutEdge:
            //    case WEdgeType.InEdge:
            //        var path = currentGhost.GetPathFromPathList(RealVariable as GremlinTableVariable);
            //        if (path != null && path.SinkVariable != null)
            //        {
            //            if (currentGhost.IsVariableInCurrentContext(path.SinkVariable))
            //            {
            //                currentGhost.SetPivotVariable(path.SinkVariable);
            //            }
            //            else
            //            {
            //                GremlinRealVariable newRealVariable = Create(path.SinkVariable);
            //                currentGhost.VariableList.Add(newRealVariable);
            //                currentGhost.SetPivotVariable(newRealVariable);
            //            }
            //        }
            //        else
            //        {
            //            Populate(GremlinKeyword.EdgeSinkV);
            //            if (path != null) path.SetSinkVariable(newVertex);

            //            currentGhost.VariableList.Add(newVertex);
            //            currentGhost.TableReferences.Add(newVertex);
            //            currentGhost.SetPivotVariable(newVertex);
            //        }
            //        break;
            //}
        }

        internal override void OutV(GremlinToSqlContext currentGhost)
        {
            //GremlinVariableProperty sourceProperty = new GremlinVariableProperty(RealVariable, GremlinKeyword.EdgeSourceV);
            //GremlinBoundVertexVariable newVertex = new GremlinBoundVertexVariable(sourceProperty);

            //switch ((RealVariable as GremlinEdgeTableVariable).EdgeType)
            //{
            //    case WEdgeType.BothEdge:
            //        Populate(GremlinKeyword.EdgeSourceV);
            //        currentGhost.VariableList.Add(newVertex);
            //        currentGhost.TableReferences.Add(newVertex);
            //        currentGhost.SetPivotVariable(newVertex);
            //        break;
            //    case WEdgeType.OutEdge:
            //    case WEdgeType.InEdge:
            //        var path = currentGhost.GetPathFromPathList(RealVariable as GremlinTableVariable);

            //        if (path != null && path.SourceVariable != null)
            //        {
            //            if (currentGhost.IsVariableInCurrentContext(path.SourceVariable))
            //            {
            //                currentGhost.SetPivotVariable(path.SourceVariable);
            //            }
            //            else
            //            {
            //                GremlinRealVariable newRealVariable = GremlinRealVariable.Create(path.SourceVariable);
            //                currentGhost.VariableList.Add(newRealVariable);
            //                currentGhost.SetPivotVariable(newRealVariable);
            //            }
            //        }
            //        else
            //        {
            //            Populate(GremlinKeyword.EdgeSourceV);
            //            if (path != null) path.SetSourceVariable(newVertex);

            //            currentGhost.VariableList.Add(newVertex);
            //            currentGhost.TableReferences.Add(newVertex);
            //            currentGhost.SetPivotVariable(newVertex);
            //        }
            //        break;
            //}
        }

        internal override void OtherV(GremlinToSqlContext currentGhost)
        {
            switch ((RealVariable as GremlinEdgeTableVariable).EdgeType)
            {
                case WEdgeType.BothEdge:
                    Populate(GremlinKeyword.EdgeOtherV);
                    GremlinVariableProperty otherProperty = new GremlinVariableProperty(RealVariable, GremlinKeyword.EdgeOtherV);
                    GremlinBoundVertexVariable otherVertex = new GremlinBoundVertexVariable(otherProperty);
                    currentGhost.VariableList.Add(otherVertex);
                    currentGhost.TableReferences.Add(otherVertex);
                    currentGhost.SetPivotVariable(otherVertex);
                    break;
                case WEdgeType.InEdge:
                    OutV(currentGhost);
                    break;
                case WEdgeType.OutEdge:
                    InV(currentGhost);
                    break;
            }
        }

        internal override void Properties(GremlinToSqlContext currentGhost, List<string> propertyKeys)
        {
            foreach (var property in propertyKeys)
            {
                Populate(property);
            }
            GremlinPropertiesVariable newVariable = new GremlinPropertiesVariable(RealVariable as GremlinTableVariable, propertyKeys);
            currentGhost.VariableList.Add(newVariable);
            currentGhost.TableReferences.Add(newVariable);
            currentGhost.SetPivotVariable(newVariable);
        }

        internal override void Values(GremlinToSqlContext currentGhost, List<string> propertyKeys)
        {
            if (propertyKeys.Count == 1)
            {
                Populate(propertyKeys.First());
                GremlinVariableProperty newVariableProperty = new GremlinVariableProperty(RealVariable as GremlinTableVariable, propertyKeys.First());
                currentGhost.VariableList.Add(newVariableProperty);
                currentGhost.SetPivotVariable(newVariableProperty);
            }
            else
            {
                foreach (var property in propertyKeys)
                {
                    Populate(property);
                }
                GremlinValuesVariable newVariable = new GremlinValuesVariable(RealVariable as GremlinTableVariable, propertyKeys);
                currentGhost.VariableList.Add(newVariable);
                currentGhost.TableReferences.Add(newVariable);
                currentGhost.SetPivotVariable(newVariable);
            }
        }
    }
}
