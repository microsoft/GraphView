using System;
using System.Collections.Generic;
using System.Linq;

namespace GraphView
{
    internal abstract class GremlinTableVariable : GremlinVariable
    {
        public GremlinUpdatePropertiesVariable UpdateVariable { get; set; }

        protected static int _count = 0;

        internal virtual string GenerateTableAlias()
        {
            return "R_" + _count++;
        }

        public GremlinSqlTableVariable SqlTableVariable { get; set; }

        public GremlinTableVariable()
        {
            VariableName = GenerateTableAlias();
        }

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            throw new NotImplementedException();
            //return GetVariableProperty( GremlinKeyword.TableValue);
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            throw new NotImplementedException();
            //return GetVariableProperty( GremlinKeyword.TableValue);
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }

        internal override List<GremlinVariable> FetchAllVariablesInCurrAndChildContext()
        {
            return SqlTableVariable?.FetchAllVariablesInCurrAndChildContext();
        }

        public virtual WTableReference ToTableReference()
        {
            if (SqlTableVariable != null)
            {
                return SqlTableVariable.ToTableReference(ProjectedProperties, VariableName, this);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        internal override void Populate(string property)
        {
            base.Populate(property);
            SqlTableVariable?.Populate(property);
        }

        internal override void PopulateGremlinPath()
        {
            SqlTableVariable?.PopulateGremlinPath();
        }

        internal override List<GremlinVariable> PopulateAllTaggedVariable(string label)
        {
            if (SqlTableVariable != null)
                return SqlTableVariable.PopulateAllTaggedVariable(label, this);
            else
                return base.PopulateAllTaggedVariable(label);
        }


        internal override bool ContainsLabel(string label)
        {
            if (base.ContainsLabel(label)) return true;
            if (SqlTableVariable != null)
            {
                return SqlTableVariable.ContainsLabel(label);

            }
            return false;
        }

        internal override GremlinVariableProperty GetPath()
        {
            if (SqlTableVariable != null) return GetVariableProperty(GremlinKeyword.Path);
            return base.GetPath();
        }

        internal override void Range(GremlinToSqlContext currentContext, int low, int high)
        {
            Low = low;
            High = high;
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, object value)
        {
            Populate(propertyKey);
            WScalarExpression firstExpr = SqlUtil.GetColumnReferenceExpr(VariableName, propertyKey);
            WScalarExpression secondExpr = SqlUtil.GetValueExpr(value);
            currentContext.AddPredicate(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, object value)
        {
            Has(currentContext, GremlinKeyword.Label, label);
            Has(currentContext, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
            Populate(propertyKey);
            WScalarExpression firstExpr = SqlUtil.GetColumnReferenceExpr(VariableName, propertyKey);
            currentContext.AddPredicate(SqlUtil.GetBooleanComparisonExpr(firstExpr, null, predicate));
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey,
            Predicate predicate)
        {
            Has(currentContext, GremlinKeyword.Label, label);
            Has(currentContext, propertyKey, predicate);
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty adjEdge = GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, adjReverseEdge,
                WEdgeType.BothEdge);
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
            GremlinVariableProperty adjEdge = GetVariableProperty( GremlinKeyword.EdgeAdj);
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, adjReverseEdge,
                WEdgeType.BothEdge);
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
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(sourceProperty, adjReverseEdge,
                WEdgeType.InEdge);
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

            GremlinVariableProperty edgeProperty = inEdge.GetVariableProperty(GremlinKeyword.EdgeSourceV);
            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(edgeProperty);
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);

            currentContext.AddPath(new GremlinMatchPath(outVertex, inEdge, this));

            currentContext.SetPivotVariable(outVertex);

        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(sourceProperty, adjReverseEdge,
                WEdgeType.InEdge);
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

            currentContext.AddPath(new GremlinMatchPath(null, inEdge, this));
            currentContext.SetPivotVariable(inEdge);
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjEdge = GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, WEdgeType.OutEdge);
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            GremlinVariableProperty sinkProperty = outEdge.GetVariableProperty(GremlinKeyword.EdgeSinkV);
            GremlinBoundVertexVariable sinkVariable = new GremlinBoundVertexVariable(sinkProperty);
            currentContext.VariableList.Add(sinkVariable);
            currentContext.TableReferences.Add(sinkVariable);

            currentContext.AddPath(new GremlinMatchPath(this, outEdge, sinkVariable));

            currentContext.SetPivotVariable(sinkVariable);
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjEdge = GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjEdge, WEdgeType.OutEdge);
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            currentContext.AddPath(new GremlinMatchPath(this, outEdge, null));
            currentContext.SetPivotVariable(outEdge);
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            GremlinVariableProperty sinkProperty = GetVariableProperty(GremlinKeyword.EdgeSinkV);
            GremlinBoundVertexVariable newVertex = new GremlinBoundVertexVariable(sinkProperty);

            switch ((this as GremlinEdgeTableVariable).EdgeType)
            {
                case WEdgeType.BothEdge:
                    currentContext.VariableList.Add(newVertex);
                    currentContext.TableReferences.Add(newVertex);
                    currentContext.SetPivotVariable(newVertex);
                    break;
                case WEdgeType.OutEdge:
                case WEdgeType.InEdge:
                    var path = currentContext.GetPathFromPathList(this);
                    if (path != null && path.SinkVariable != null)
                    {
                        if (currentContext.IsVariableInCurrentContext(path.SinkVariable))
                        {
                            currentContext.SetPivotVariable(path.SinkVariable);
                        }
                        else
                        {
                            GremlinContextVariable newContextVariable = GremlinContextVariable.Create(path.SinkVariable);
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
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.EdgeSourceV);
            GremlinBoundVertexVariable newVertex = new GremlinBoundVertexVariable(sourceProperty);

            switch ((this as GremlinEdgeTableVariable).EdgeType)
            {
                case WEdgeType.BothEdge:
                    currentContext.VariableList.Add(newVertex);
                    currentContext.TableReferences.Add(newVertex);
                    currentContext.SetPivotVariable(newVertex);
                    break;
                case WEdgeType.OutEdge:
                case WEdgeType.InEdge:
                    var path = currentContext.GetPathFromPathList(this);

                    if (path != null && path.SourceVariable != null)
                    {
                        if (currentContext.IsVariableInCurrentContext(path.SourceVariable))
                        {
                            currentContext.SetPivotVariable(path.SourceVariable);
                        }
                        else
                        {
                            GremlinContextVariable newContextVariable =
                                GremlinContextVariable.Create(path.SourceVariable);
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
            switch ((this as GremlinEdgeTableVariable).EdgeType)
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
            foreach (var property in propertyKeys)
            {
                Populate(property);
            }
            if (propertyKeys.Count == 0)
            {
                Populate("*");
            }
            GremlinPropertiesVariable newVariable = new GremlinPropertiesVariable(this, propertyKeys);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            foreach (var property in propertyKeys)
            {
                Populate(property);
            }
            if (propertyKeys.Count == 0)
            {
                Populate("*");
            }
            GremlinValuesVariable newVariable = new GremlinValuesVariable(this, propertyKeys);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
            //}
        }
    }

    internal abstract class GremlinScalarTableVariable : GremlinTableVariable
    {
        internal override void Populate(string property)
        {
            if (property == GremlinKeyword.EdgeID
                || property == GremlinKeyword.EdgeAdj
                || property == GremlinKeyword.ReverseEdgeAdj
                || property == GremlinKeyword.EdgeSourceV
                || property == GremlinKeyword.EdgeSinkV
                || property == GremlinKeyword.EdgeOtherV
                || property == GremlinKeyword.NodeID
            )
            {
                return;
            }
            base.Populate(property);
            SqlTableVariable?.Populate(property);
        }

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            Populate(GremlinKeyword.ScalarValue);
            return GetVariableProperty( GremlinKeyword.ScalarValue);
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            Populate(GremlinKeyword.ScalarValue);
            return GetVariableProperty( GremlinKeyword.ScalarValue);
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            throw new QueryCompilationException("The OutV() step can only be applied to edges or vertex.");
        }
    }

    internal abstract class GremlinVertexTableVariable : GremlinTableVariable
    {
        protected static int _count = 0;

        internal override string GenerateTableAlias()
        {
            return "N_" + _count++;
        }

        internal override void Populate(string property)
        {
            if (property == GremlinKeyword.EdgeID
                || property == GremlinKeyword.EdgeSourceV
                || property == GremlinKeyword.EdgeSinkV
                || property == GremlinKeyword.EdgeOtherV
            )
            {
                return;
            }
            base.Populate(property);
            SqlTableVariable?.Populate(property);
        }

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            Populate(GremlinKeyword.NodeID);
            return GetVariableProperty( GremlinKeyword.NodeID);
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            Populate(GremlinKeyword.Star);
            return GetVariableProperty( GremlinKeyword.Star);
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Vertex;
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            GremlinVariableProperty variableProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinDropVertexVariable newVariable = new GremlinDropVertexVariable(variableProperty);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            if (UpdateVariable == null)
            {
                GremlinVariableProperty variableProperty = GetVariableProperty(GremlinKeyword.NodeID);
                UpdateVariable = new GremlinUpdateNodePropertiesVariable(variableProperty, properties);
                currentContext.VariableList.Add(UpdateVariable);
                currentContext.TableReferences.Add(UpdateVariable);
            }
            else
            {
                UpdateVariable.Property(currentContext, properties);
            }
        }

        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            foreach (var value in values)
            {
                Has(currentContext, GremlinKeyword.NodeID, value);
            }
        }
    }

    internal abstract class GremlinEdgeTableVariable : GremlinTableVariable
    {
        protected static int _count = 0;

        internal override string GenerateTableAlias()
        {
            return "E_" + _count++;
        }

        public WEdgeType EdgeType { get; set; }

        internal override void Populate(string property)
        {
            if (property == GremlinKeyword.NodeID
                || property == GremlinKeyword.EdgeAdj
                || property == GremlinKeyword.ReverseEdgeAdj)
            {
                property = null;
            }
            base.Populate(property);
            SqlTableVariable?.Populate(property);
        }

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            Populate(GremlinKeyword.EdgeID);
            return GetVariableProperty( GremlinKeyword.EdgeID);
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            Populate(GremlinKeyword.Star);
            return GetVariableProperty( GremlinKeyword.Star);
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            var sourceProperty = GetVariableProperty(GremlinKeyword.EdgeSourceV);
            var edgeProperty = GetVariableProperty(GremlinKeyword.EdgeID);
            GremlinDropEdgeVariable newVariable = new GremlinDropEdgeVariable(sourceProperty, edgeProperty);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            if (UpdateVariable == null)
            {
                var sourceProperty = GetVariableProperty(GremlinKeyword.EdgeSourceV);
                var edgeProperty = GetVariableProperty(GremlinKeyword.EdgeID);
                UpdateVariable = new GremlinUpdateEdgePropertiesVariable(sourceProperty, edgeProperty, properties);
                currentContext.VariableList.Add(UpdateVariable);
                currentContext.TableReferences.Add(UpdateVariable);
            }
            else
            {
                UpdateVariable.Property(currentContext, properties);
            }
        }

        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            foreach (var value in values)
            {
                Has(currentContext, GremlinKeyword.EdgeID, value);
            }
        }
    }

    internal abstract class GremlinDropTableVariable : GremlinTableVariable
    {
        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.NULL;
        }
    }
}
