using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinUnfoldVariable : GremlinTableVariable
    {
        public static GremlinUnfoldVariable Create(GremlinVariable inputVariable)
        {
            switch (GetUnfoldVariableType(inputVariable))
            {
                case GremlinVariableType.Vertex:
                    return new GremlinUnfoldVertexVariable(inputVariable);
                case GremlinVariableType.Edge:
                    return new GremlinUnfoldEdgeVariable(inputVariable);
                case GremlinVariableType.Scalar:
                    return new GremlinUnfoldScalarVariable(inputVariable);
                case GremlinVariableType.Property:
                    return new GremlinUnfoldPropertyVariable(inputVariable);
            }
            return new GremlinUnfoldVariable(inputVariable);
        }

        public static GremlinVariableType GetUnfoldVariableType(GremlinVariable inputVariable)
        {
            if (inputVariable is GremlinFoldVariable)
            {
                return (inputVariable as GremlinFoldVariable).FoldVariable.GetVariableType();
            }
            if (inputVariable is GremlinListVariable)
            {
                return (inputVariable as GremlinListVariable).GetVariableType();
            }
            if (inputVariable is GremlinSelectedVariable)
            {
                return GetUnfoldVariableType((inputVariable as GremlinSelectedVariable).RealVariable);
            }
            return inputVariable.GetVariableType();
        }

        public GremlinVariable UnfoldVariable { get; set; }

        public GremlinUnfoldVariable(GremlinVariable unfoldVariable, GremlinVariableType variableType = GremlinVariableType.Table)
            : base(variableType)
        {
            UnfoldVariable = unfoldVariable;
        }

        internal override bool ContainsLabel(string label)
        {
            return false;
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);
            UnfoldVariable.Populate(property);
        }

        public override WTableReference ToTableReference()
        {
            if (UnfoldVariable is GremlinListVariable)
            {
                List<WScalarExpression> parameters = new List<WScalarExpression>();
                parameters.Add((UnfoldVariable as GremlinListVariable).ToScalarExpression());
                foreach (var projectProperty in ProjectedProperties)
                {
                    parameters.Add(SqlUtil.GetValueExpr(projectProperty));
                }
                var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Unfold, parameters, this, VariableName);
                return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
            }
            else
            {
                List<WScalarExpression> parameters = new List<WScalarExpression>();
                parameters.Add(UnfoldVariable.DefaultVariableProperty().ToScalarExpression());
                parameters.Add(SqlUtil.GetValueExpr(UnfoldVariable.DefaultVariableProperty().VariableProperty));
                var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Unfold, parameters, this, VariableName);
                return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
            }
            throw new NotImplementedException();
        }
    }

    internal class GremlinUnfoldVertexVariable : GremlinUnfoldVariable
    {
        public GremlinUnfoldVertexVariable(GremlinVariable unfoldVariable)
            : base(unfoldVariable, GremlinVariableType.Vertex)
        {
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.Both(this, edgeLabels);
        }

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.BothE(this, edgeLabels);
        }

        internal override void BothV(GremlinToSqlContext currentContext)
        {
            currentContext.BothV(this);
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.In(this, edgeLabels);
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.InE(this, edgeLabels);
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.Out(this, edgeLabels);
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.OutE(this, edgeLabels);
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            currentContext.DropVertex(this);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, object value)
        {
            currentContext.Has(this, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, object value)
        {
            currentContext.Has(this, label, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, propertyKey, predicate);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, label, propertyKey, predicate);
        }

        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasId(this, values);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasLabel(this, values);
        }

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Properties(this, propertyKeys);
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Values(this, propertyKeys);
        }
    }

    internal class GremlinUnfoldEdgeVariable : GremlinUnfoldVariable
    {
        public GremlinUnfoldEdgeVariable(GremlinVariable unfoldVariable)
            : base(unfoldVariable, GremlinVariableType.Edge)
        {
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            currentContext.InV(this);
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            currentContext.OutV(this);
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            currentContext.OtherV(this);
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            currentContext.DropEdge(this);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, object value)
        {
            currentContext.Has(this, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, object value)
        {
            currentContext.Has(this, label, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, propertyKey, predicate);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, label, propertyKey, predicate);
        }

        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasId(this, values);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasLabel(this, values);
        }

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Properties(this, propertyKeys);
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Values(this, propertyKeys);
        }
    }

    internal class GremlinUnfoldScalarVariable : GremlinUnfoldVariable
    {
        public GremlinUnfoldScalarVariable(GremlinVariable unfoldVariable)
            : base(unfoldVariable, GremlinVariableType.Scalar)
        {
        }
    }

    internal class GremlinUnfoldPropertyVariable : GremlinUnfoldVariable
    {
        public GremlinUnfoldPropertyVariable(GremlinVariable unfoldVariable)
            : base(unfoldVariable, GremlinVariableType.Property)
        {
        }

        internal override void Key(GremlinToSqlContext currentContext)
        {
            currentContext.Key(this);
        }

        internal override void Value(GremlinToSqlContext currentContext)
        {
            currentContext.Value(this);
        }
    }
}
