using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAddETableVariable : GremlinEdgeTableVariable
    {
        public GremlinToSqlContext InputContext { get; set; }
        public GremlinToSqlContext FromVertexContext { get; set; }
        public GremlinToSqlContext ToVertexContext { get; set; }
        public List<GremlinProperty> EdgeProperties { get; set; }
        public string EdgeLabel { get; set; }
        public bool IsFirstTableReference { get; set; }
        private int OtherVIndex;

        public GremlinAddETableVariable(GremlinToSqlContext inputContext, string edgeLabel,
            List<GremlinProperty> edgeProperties, GremlinToSqlContext fromContext, GremlinToSqlContext toContext,
            bool isFirstTableReference = false)
        {
            this.EdgeProperties = edgeProperties;
            this.EdgeLabel = edgeLabel;
            this.InputContext = inputContext?.Duplicate();
            this.EdgeType = WEdgeType.OutEdge;
            this.OtherVIndex = 1;
            this.ProjectedProperties.Add(GremlinKeyword.Label);
            this.FromVertexContext = fromContext;
            this.ToVertexContext = toContext;
            this.IsFirstTableReference = isFirstTableReference;
            this.ProjectedProperties.Add(GremlinKeyword.EdgeID);
            this.ProjectedProperties.Add(GremlinKeyword.EdgeSourceV);
            this.ProjectedProperties.Add(GremlinKeyword.EdgeSinkV);
            this.ProjectedProperties.Add(GremlinKeyword.EdgeOtherV);
            foreach (var edgeProperty in this.EdgeProperties)
            {
                this.ProjectedProperties.Add(edgeProperty.Key);
            }
        }

        internal override bool Populate(string property, string label = null)
        {
            return this.ProjectedProperties.Contains(property);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            if (this.InputContext != null && InputContext.PivotVariable != null)
            {
                variableList.Add(this.InputContext.PivotVariable);
            }
            if (this.FromVertexContext != null)
            {
                variableList.AddRange(this.FromVertexContext.FetchAllVars());
            }
            if (this.ToVertexContext != null)
            {
                variableList.AddRange(this.ToVertexContext.FetchAllVars());
            }
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            if (this.FromVertexContext != null)
            {
                variableList.AddRange(this.FromVertexContext.FetchAllTableVars());
            }
            if (this.ToVertexContext != null)
            {
                variableList.AddRange(this.ToVertexContext.FetchAllTableVars());
            }
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(GetSelectQueryBlock(this.FromVertexContext)));
            parameters.Add(SqlUtil.GetScalarSubquery(GetSelectQueryBlock(this.ToVertexContext)));

            if (this.ToVertexContext == null && this.FromVertexContext != null)
            {
                this.OtherVIndex = 0;
            }
            if (this.ToVertexContext != null && this.FromVertexContext == null)
            {
                this.OtherVIndex = 1;
            }
            if (this.ToVertexContext != null && this.FromVertexContext != null)
            {
                this.OtherVIndex = 1;
            }

            parameters.Add(SqlUtil.GetValueExpr(this.OtherVIndex));

            parameters.Add(SqlUtil.GetValueExpr(this.EdgeLabel));
            parameters.AddRange(this.EdgeProperties.Select(property => property.ToPropertyExpr()));
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.AddE, parameters, GetVariableName());
            var crossApplyTableRef = SqlUtil.GetCrossApplyTableReference(secondTableRef);
            crossApplyTableRef.FirstTableRef = this.IsFirstTableReference ? SqlUtil.GetDerivedTable(SqlUtil.GetSimpleSelectQueryBlock("1"), "_") : null;

            return SqlUtil.GetCrossApplyTableReference(crossApplyTableRef);
        }

        private WSelectQueryBlock GetSelectQueryBlock(GremlinToSqlContext context)
        {
            if (context == null)
            {
                var queryBlock = new WSelectQueryBlock();
                queryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(this.InputContext.PivotVariable.DefaultProjection().ToScalarExpression()));
                return queryBlock;
            }
            else
            {
                return context.ToSelectQueryBlock();
            }
        }

        internal override void Property(GremlinToSqlContext currentContext, GremlinProperty property)
        {
            this.ProjectedProperties.Add(property.Key);
            this.EdgeProperties.Add(property);
        }
    }
}
