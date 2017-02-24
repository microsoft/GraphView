using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinUnionVariable: GremlinTableVariable
    {
        public static GremlinUnionVariable Create(List<GremlinToSqlContext> unionContextList)
        {
            if (unionContextList.Count == 0)
            {
                return new GremlinUnionNullVariable(unionContextList);
            }
            if (GremlinUtil.IsTheSameOutputType(unionContextList))
            {
                switch (unionContextList.First().PivotVariable.GetVariableType())
                {
                    case GremlinVariableType.Vertex:
                        return new GremlinUnionVertexVariable(unionContextList);
                    case GremlinVariableType.Edge:
                        return new GremlinUnionEdgeVariable(unionContextList);
                    case GremlinVariableType.Scalar:
                        return new GremlinUnionScalarVariable(unionContextList);
                    case GremlinVariableType.NULL:
                        return new GremlinUnionNullVariable(unionContextList);
                    case GremlinVariableType.Property:
                        return new GremlinUnionPropertyVariable(unionContextList);
                }
            }
            return new GremlinUnionTableVariable(unionContextList);
        }

        public List<GremlinToSqlContext> UnionContextList { get; set; }

        public GremlinUnionVariable(List<GremlinToSqlContext> unionContextList, GremlinVariableType variableType)
            : base(variableType)
        {
            UnionContextList = unionContextList;
        }

        internal override GremlinVariableType GetUnfoldVariableType()
        {
            throw new NotImplementedException();
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);

            foreach (var context in UnionContextList)
            {
                context.Populate(property);
            }
        }

        internal override void PopulateGremlinPath()
        {
            foreach (var context in UnionContextList)
            {
                context.PopulateGremlinPath();
            }
        }

        internal override GremlinVariableProperty GetPath()
        {
            return new GremlinVariableProperty(this, GremlinKeyword.Path);
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            foreach (var context in UnionContextList)
            {
                var subContextVariableList = context.FetchVarsFromCurrAndChildContext();
                if (subContextVariableList != null)
                {
                    variableList.AddRange(subContextVariableList);
                }
            }
            return variableList;
        }

        internal override List<GremlinVariable> PopulateAllTaggedVariable(string label)
        {
            List<List<GremlinVariable>> branchVariableList = new List<List<GremlinVariable>>();
            foreach (var context in UnionContextList)
            {
                var variableList = context.SelectVarsFromCurrAndChildContext(label);
                branchVariableList.Add(variableList);
            }
            return new List<GremlinVariable>() {GremlinBranchVariable.Create(label, this, branchVariableList)};
        }

        internal override bool ContainsLabel(string label)
        {
            if (base.ContainsLabel(label)) return true;
            foreach (var context in UnionContextList)
            {
                foreach (var variable in context.VariableList)
                {
                    if (variable.ContainsLabel(label))
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            if (UnionContextList.Count == 0)
            {
                foreach (var property in ProjectedProperties)
                {
                    parameters.Add(SqlUtil.GetValueExpr(property));
                }
            }
            foreach (var context in UnionContextList)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(context.ToSelectQueryBlock(ProjectedProperties)));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Union, parameters, this, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinUnionVertexVariable : GremlinUnionVariable
    {
        public GremlinUnionVertexVariable(List<GremlinToSqlContext> unionContextList)
            : base(unionContextList, GremlinVariableType.Vertex)
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

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            currentContext.Has(this, propertyKey);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, GremlinToSqlContext propertyContext)
        {
            currentContext.Has(this, propertyKey, propertyContext);
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

        internal override void HasId(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasId(this, predicate);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasLabel(this, values);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasLabel(this, predicate);
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

    internal class GremlinUnionEdgeVariable : GremlinUnionVariable
    {
        public GremlinUnionEdgeVariable(List<GremlinToSqlContext> unionContextList)
            : base(unionContextList, GremlinVariableType.Edge)
        {
        }

        internal override WEdgeType GetEdgeType()
        {
            if (UnionContextList.Count <= 1) return UnionContextList.First().PivotVariable.GetEdgeType();
            for (var i = 1; i < UnionContextList.Count; i++)
            {
                var isSameType = UnionContextList[i - 1].PivotVariable.GetEdgeType()
                                  == UnionContextList[i].PivotVariable.GetEdgeType();
                if (isSameType == false) throw new NotImplementedException();
            }
            return UnionContextList.First().PivotVariable.GetEdgeType();
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

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            currentContext.Has(this, propertyKey);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, GremlinToSqlContext propertyContext)
        {
            currentContext.Has(this, propertyKey, propertyContext);
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

        internal override void HasId(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasId(this, predicate);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasLabel(this, values);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasLabel(this, predicate);
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

    internal class GremlinUnionScalarVariable : GremlinUnionVariable
    {
        public GremlinUnionScalarVariable(List<GremlinToSqlContext> unionContextList)
            : base(unionContextList, GremlinVariableType.Scalar)
        {
        }
    }

    internal class GremlinUnionPropertyVariable : GremlinUnionVariable
    {
        public GremlinUnionPropertyVariable(List<GremlinToSqlContext> unionContextList)
            : base(unionContextList, GremlinVariableType.Property)
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

    internal class GremlinUnionNullVariable : GremlinUnionVariable
    {
        public GremlinUnionNullVariable(List<GremlinToSqlContext> unionContextList)
            : base(unionContextList, GremlinVariableType.NULL)
        {
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
        }

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
        }

        internal override void BothV(GremlinToSqlContext currentContext)
        {
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, GremlinToSqlContext propertyContext)
        {
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, object value)
        {
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, object value)
        {
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Predicate predicate)
        {
        }

        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
        }

        internal override void HasId(GremlinToSqlContext currentContext, Predicate predicate)
        {
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, Predicate predicate)
        {
        }

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
        }
    }

    internal class GremlinUnionTableVariable : GremlinUnionVariable
    {
        public GremlinUnionTableVariable(List<GremlinToSqlContext> unionContextList)
            : base(unionContextList, GremlinVariableType.Table)
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

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            currentContext.Has(this, propertyKey);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, GremlinToSqlContext propertyContext)
        {
            currentContext.Has(this, propertyKey, propertyContext);
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

        internal override void HasId(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasId(this, predicate);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasLabel(this, values);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasLabel(this, predicate);
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
}
