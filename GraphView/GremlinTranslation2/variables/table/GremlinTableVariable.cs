using System;
using System.Collections.Generic;
using System.Linq;

namespace GraphView
{
    internal abstract class GremlinTableVariable : GremlinVariable
    {
        public virtual void test1() { }

        public WEdgeType EdgeType { get; set; }
        public GremlinVariableType VariableType { get; set; }

        public GremlinTableVariable(GremlinVariableType variableType)
        {
            SetVariableTypeAndGenerateName(variableType);
        }

        public GremlinTableVariable()
        {
            SetVariableTypeAndGenerateName(GremlinVariableType.Table);
        }

        internal override WEdgeType GetEdgeType()
        {
            if (EdgeType == null)
            {
                throw new QueryCompilationException("EdgeType can't be null");
            }
            return EdgeType;
        }

        public void SetVariableTypeAndGenerateName(GremlinVariableType variableType)
        {
            VariableType = variableType;
            VariableName = GremlinUtil.GenerateTableAlias(VariableType);
        }

        internal override void Populate(string property)
        {
            switch (GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    if (GremlinUtil.IsEdgeProperty(property)) return;
                    break;
                case GremlinVariableType.Edge:
                    if (GremlinUtil.IsVertexProperty(property)) return;
                    break;
                case GremlinVariableType.Scalar:
                    if (GremlinUtil.IsVertexProperty(property) || GremlinUtil.IsEdgeProperty(property)) return;
                    break;
            }
            base.Populate(property);
        }

        public virtual WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            switch (VariableType)
            {
                case GremlinVariableType.Edge:
                    return GetVariableProperty(GremlinKeyword.EdgeID);
                case GremlinVariableType.Scalar:
                    return GetVariableProperty(GremlinKeyword.ScalarValue);
                case GremlinVariableType.Vertex:
                    return GetVariableProperty(GremlinKeyword.NodeID);
            }
            return new GremlinVariableProperty(this, GremlinKeyword.TableDefaultColumnName);
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            switch (VariableType)
            {
                case GremlinVariableType.Edge:
                    return GetVariableProperty(GremlinKeyword.Star);
                case GremlinVariableType.Scalar:
                    return GetVariableProperty(GremlinKeyword.ScalarValue);
                case GremlinVariableType.Vertex:
                    return GetVariableProperty(GremlinKeyword.Star);
            }
            return new GremlinVariableProperty(this, GremlinKeyword.TableDefaultColumnName);
        }

        internal override GremlinVariableType GetVariableType()
        {
            return VariableType;
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

        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            string compareKey = "";
            switch (VariableType)
            {
                case GremlinVariableType.Edge:
                    Populate(GremlinKeyword.EdgeID);
                    compareKey = GremlinKeyword.EdgeID;
                    break;
                case GremlinVariableType.Vertex:
                    Populate(GremlinKeyword.NodeID);
                    compareKey = GremlinKeyword.NodeID;
                    break;
            }
            List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
            foreach (var value in values)
            {
                WScalarExpression firstExpr = SqlUtil.GetColumnReferenceExpr(VariableName, compareKey);
                WScalarExpression secondExpr = SqlUtil.GetValueExpr(value);
                booleanExprList.Add(SqlUtil.GetBooleanComparisonExpr(firstExpr, secondExpr, BooleanComparisonType.Equals));
            }
            WBooleanExpression concatSql = SqlUtil.ConcatBooleanExprWithOr(booleanExprList);
            currentContext.AddPredicate(SqlUtil.GetBooleanParenthesisExpr(concatSql));
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            Populate(GremlinKeyword.Label);
            List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
            foreach (var value in values)
            {
                WScalarExpression firstExpr = SqlUtil.GetColumnReferenceExpr(VariableName, GremlinKeyword.Label);
                WScalarExpression secondExpr = SqlUtil.GetValueExpr(value);
                booleanExprList.Add(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
            }
            WBooleanExpression concatSql = SqlUtil.ConcatBooleanExprWithOr(booleanExprList);
            currentContext.AddPredicate(SqlUtil.GetBooleanParenthesisExpr(concatSql));
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
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            GremlinDropVariable newVariable = null;
            switch (VariableType)
            {
                case GremlinVariableType.Vertex:
                    GremlinVariableProperty variableProperty = GetVariableProperty(GremlinKeyword.NodeID);
                    newVariable = new GremlinDropVertexVariable(variableProperty);
                    break;
                case GremlinVariableType.Edge:
                    var sourceProperty = GetVariableProperty(GremlinKeyword.EdgeSourceV);
                    var edgeProperty = GetVariableProperty(GremlinKeyword.EdgeID);
                    newVariable = new GremlinDropEdgeVariable(sourceProperty, edgeProperty);
                    break;
            }
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }
    }

    internal abstract class GremlinScalarTableVariable : GremlinTableVariable
    {
        public GremlinScalarTableVariable(): base(GremlinVariableType.Scalar) {}
    }

    internal abstract class GremlinVertexTableVariable : GremlinTableVariable
    {
        public GremlinVertexTableVariable(): base(GremlinVariableType.Vertex) {}

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            throw new NotImplementedException();
        }

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            throw new NotImplementedException();
        }

        internal override void BothV(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            throw new NotImplementedException();
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            throw new NotImplementedException();
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            throw new NotImplementedException();
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            throw new NotImplementedException();
        }
    }

    internal abstract class GremlinEdgeTableVariable : GremlinTableVariable
    {
        public GremlinEdgeTableVariable() : base(GremlinVariableType.Edge) {}

        internal override void InV(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }
    }

    internal abstract class GremlinDropVariable : GremlinTableVariable
    {
        public GremlinDropVariable() : base(GremlinVariableType.NULL) {}

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.NULL;
        }
    }
}
