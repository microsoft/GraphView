using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAddETableVariable: GremlinEdgeTableVariable
    {
        public GremlinContextVariable InputVariable { get; set; }
        public GremlinToSqlContext FromVertexContext { get; set; }
        public GremlinToSqlContext ToVertexContext { get; set; }
        public List<GremlinProperty> EdgeProperties { get; set; }
        public string EdgeLabel { get; set; }

        private int OtherVIndex;

        public GremlinAddETableVariable(GremlinVariable inputVariable, string edgeLabel, List<GremlinProperty> edgeProperties, GremlinToSqlContext fromContext, GremlinToSqlContext toContext)
        {
            EdgeProperties = edgeProperties;
            EdgeLabel = edgeLabel;
            InputVariable = new GremlinContextVariable(inputVariable);
            EdgeType = WEdgeType.OutEdge;
            OtherVIndex = 1;
            ProjectedProperties.Add(GremlinKeyword.Label);
            FromVertexContext = fromContext;
            ToVertexContext = toContext;

            foreach (var edgeProperty in EdgeProperties)
            {
                ProjectedProperties.Add(edgeProperty.Key);
            }
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            EdgeProperties.Add(new GremlinProperty(GremlinKeyword.PropertyCardinality.Single, property , null, null));
            base.Populate(property);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() {this};
            variableList.Add(InputVariable);
            if (FromVertexContext != null)
                variableList.AddRange(FromVertexContext.FetchAllVars());
            if (ToVertexContext != null)
                variableList.AddRange(ToVertexContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            if (FromVertexContext != null)
                variableList.AddRange(FromVertexContext.FetchAllTableVars());
            if (ToVertexContext != null)
                variableList.AddRange(ToVertexContext.FetchAllTableVars());
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
                var queryBlock = new WSelectQueryBlock();
                queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(InputVariable.DefaultProjection().ToScalarExpression()));
                return queryBlock;
            }
            else
            {
                return context.ToSelectQueryBlock();
            } 
        }

        internal override void Property(GremlinToSqlContext currentContext, GremlinProperty property)
        {
            ProjectedProperties.Add(property.Key);
            EdgeProperties.Add(property);
        }
    }
}
