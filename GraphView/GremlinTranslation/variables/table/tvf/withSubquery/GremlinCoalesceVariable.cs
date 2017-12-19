using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinCoalesceVariable : GremlinTableVariable
    {
        public List<GremlinToSqlContext> CoalesceContextList { get; set; }

        public GremlinCoalesceVariable(List<GremlinToSqlContext> coalesceContextList, GremlinVariableType variableType)
            : base(variableType)
        {
            this.CoalesceContextList = new List<GremlinToSqlContext>(coalesceContextList);
        }

        internal override bool Populate(string property, string label = null)
        {
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label))
            {
                foreach (GremlinToSqlContext context in this.CoalesceContextList)
                {
                    populateSuccessfully |= context.Populate(property, null);
                }
            }
            else
            {
                foreach (GremlinToSqlContext context in this.CoalesceContextList)
                {
                    populateSuccessfully |= context.Populate(property, label);
                }
            }
            if (populateSuccessfully && property != null)
            {
                this.ProjectedProperties.Add(property);
            }
            return populateSuccessfully;
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() {this};
            foreach (var context in this.CoalesceContextList)
            {
                variableList.AddRange(context.FetchAllVars());
            }
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            foreach (var context in this.CoalesceContextList)
            {
                variableList.AddRange(context.FetchAllTableVars());
            }
            return variableList;
        }

        public override  WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            foreach (GremlinToSqlContext context in this.CoalesceContextList)
            {
                WSelectQueryBlock selectQueryBlock = context.ToSelectQueryBlock();
                Dictionary<string, WSelectElement> projectionMap = new Dictionary<string, WSelectElement>();
                WSelectElement value = selectQueryBlock.SelectElements[0];
                foreach (WSelectElement selectElement in selectQueryBlock.SelectElements)
                {
                    projectionMap[(selectElement as WSelectScalarExpression).ColumnName] = selectElement;
                }
                selectQueryBlock.SelectElements.Clear();

                selectQueryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr((value as WSelectScalarExpression).SelectExpr, this.DefaultProperty()));
                foreach (string property in this.ProjectedProperties)
                {
                    selectQueryBlock.SelectElements.Add(
                        projectionMap.TryGetValue(property, out value)
                            ? value : SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), property));
                }
                parameters.Add(SqlUtil.GetScalarSubquery(selectQueryBlock));
            }

            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Coalesce, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
