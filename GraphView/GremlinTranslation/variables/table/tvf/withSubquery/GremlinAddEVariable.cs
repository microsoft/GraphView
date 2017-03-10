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
        public List<GremlinProperty> EdgeProperties { get; set; }
        public string EdgeLabel { get; set; }

        private int OtherVIndex;

        public GremlinAddETableVariable(GremlinVariable inputVariable, string edgeLabel)
        {
            EdgeProperties = new List<GremlinProperty>();
            EdgeLabel = edgeLabel;
            InputVariable = inputVariable;
            EdgeType = WEdgeType.OutEdge;
            OtherVIndex = 1;
            ProjectedProperties.Add(GremlinKeyword.Label);
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            EdgeProperties.Add(new GremlinProperty(GremlinKeyword.PropertyCardinality.single, property , null, null));
            base.Populate(property);
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            variableList.AddRange(FromVertexContext == null ? new List<GremlinVariable>() : FromVertexContext.FetchVarsFromCurrAndChildContext());
            variableList.AddRange(ToVertexContext == null ? new List<GremlinVariable>() : ToVertexContext.FetchVarsFromCurrAndChildContext());
            return variableList;
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
            foreach (var property in EdgeProperties)
            {
                parameters.Add(SqlUtil.GetValueExpr(property.Key));
                parameters.Add(SqlUtil.GetValueExpr(property.Value));
            }
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.AddE, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
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

        internal void From(GremlinToSqlContext currentContext, GremlinToSqlContext fromVertexContext)
        {
            FromVertexContext = fromVertexContext;
            FromVertexContext.HomeVariable = this;
        }

        internal override void Property(GremlinToSqlContext currentContext, GremlinProperty property)
        {
            ProjectedProperties.Add(property.Key);
            EdgeProperties.Add(property);
        }

        internal void To(GremlinToSqlContext currentContext, GremlinToSqlContext toVertexContext)
        {
            ToVertexContext = toVertexContext;
            ToVertexContext.HomeVariable = this;
        }
    }
}
