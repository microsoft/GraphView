using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAddETableVariable: GremlinEdgeTableVariable
    {
        public GremlinVariable InputVariable { get; set; }
        public GremlinToSqlContext FromVertexContext { get; set; }
        public GremlinToSqlContext ToVertexContext { get; set; }
        public List<object> EdgeProperties { get; set; }
        public string EdgeLabel { get; set; }

        private int OtherVIndex;

        public GremlinAddETableVariable(GremlinVariable inputVariable, string edgeLabel)
        {
            EdgeProperties = new List<object>();
            EdgeLabel = edgeLabel;
            InputVariable = inputVariable;
            EdgeType = WEdgeType.OutEdge;
            OtherVIndex = 1;
            ProjectedProperties.Add(GremlinKeyword.Label);
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            EdgeProperties.Add(property);
            EdgeProperties.Add(null);
            base.Populate(property);
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

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(GetSelectQueryBlock(FromVertexContext)));
            parameters.Add(SqlUtil.GetScalarSubquery(GetSelectQueryBlock(ToVertexContext)));

            if (ToVertexContext == null && FromVertexContext != null) OtherVIndex = 0;
            if (ToVertexContext != null && FromVertexContext == null) OtherVIndex = 1;
            if (ToVertexContext != null && FromVertexContext != null) OtherVIndex = 1;

            parameters.Add(SqlUtil.GetValueExpr(OtherVIndex));

            if (EdgeLabel != null)
            {
                parameters.Add(SqlUtil.GetValueExpr(GremlinKeyword.Label));
                parameters.Add(SqlUtil.GetValueExpr(EdgeLabel));
            }
            for (var i = 0; i < EdgeProperties.Count; i += 2)
            {
                parameters.Add(SqlUtil.GetValueExpr(EdgeProperties[i]));
                parameters.Add(SqlUtil.GetValueExpr(EdgeProperties[i+1]));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.AddE, parameters, this, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        private WSelectQueryBlock GetSelectQueryBlock(GremlinToSqlContext context)
        {
            if (context == null)
            {
                return SqlUtil.GetSimpleSelectQueryBlock(InputVariable.DefaultProjection());
            }
            else
            {
                return context.ToSelectQueryBlock();
            } 
        }


        internal override void From(GremlinToSqlContext currentContext, string label)
        {
            List<GremlinVariable> selectVariableList = currentContext.Select(label);
            if (selectVariableList.Count == 0 || selectVariableList.Count > 1)
            {
                throw new Exception("Error: Select variable with label");
            }
            GremlinVariable fromVariable = selectVariableList.First();
            GremlinToSqlContext fromContext = new GremlinToSqlContext();
            fromContext.SetPivotVariable(fromVariable);
            FromVertexContext = fromContext;
        }

        internal override void From(GremlinToSqlContext currentContext, GremlinToSqlContext fromVertexContext)
        {
            FromVertexContext = fromVertexContext;
            FromVertexContext.HomeVariable = this;
        }

        internal override void Property(GremlinToSqlContext currentContext, List<object> properties)
        {
             ProjectedProperties.Add(properties.First() as string);
             EdgeProperties.AddRange(properties);
        }

        internal override void To(GremlinToSqlContext currentContext, string label)
        {
            List<GremlinVariable> selectVariableList = currentContext.Select(label);
            if (selectVariableList.Count == 0 || selectVariableList.Count > 1)
            {
                throw new Exception("Error: Select variable with label");
            }
            GremlinVariable toVariable = selectVariableList.First();
            GremlinToSqlContext toContext = new GremlinToSqlContext();
            toContext.SetPivotVariable(toVariable);
            ToVertexContext = toContext;
        }

        internal override void To(GremlinToSqlContext currentContext, GremlinToSqlContext toVertexContext)
        {
            ToVertexContext = toVertexContext;
            ToVertexContext.HomeVariable = this;
        }
    }
}
