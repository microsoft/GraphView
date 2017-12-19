using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinUnionVariable : GremlinTableVariable
    {
        public List<GremlinToSqlContext> UnionContextList { get; set; }

        public GremlinUnionVariable(List<GremlinToSqlContext> unionContextList, GremlinVariableType variableType) : base(variableType)
        {
            this.UnionContextList = unionContextList;
        }

        internal override bool Populate(string property, string label = null)
        {
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label))
            {
                populateSuccessfully = true;
                foreach (GremlinToSqlContext context in this.UnionContextList)
                {
                    context.Populate(property, null);
                }
            }
            else
            {
                foreach (GremlinToSqlContext context in this.UnionContextList)
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

        internal override bool PopulateStepProperty(string property, string label = null)
        {
            bool populateSuccess = false;
            foreach (var context in this.UnionContextList)
            {
                populateSuccess |= context.ContextLocalPath.PopulateStepProperty(property, label);
            }
            return populateSuccess;
        }

        internal override void PopulateLocalPath()
        {
            if (this.ProjectedProperties.Contains(GremlinKeyword.Path))
            {
                return;
            }
            this.ProjectedProperties.Add(GremlinKeyword.Path);
            this.LocalPathLengthLowerBound = Int32.MaxValue;
            foreach (var context in this.UnionContextList)
            {
                context.PopulateLocalPath();
                this.LocalPathLengthLowerBound = Math.Min(context.MinPathLength, this.LocalPathLengthLowerBound);
            }
        }

        internal override WScalarExpression ToStepScalarExpr(HashSet<string> composedProperties = null)
        {
            return SqlUtil.GetColumnReferenceExpr(GetVariableName(), GremlinKeyword.Path);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            foreach (var context in this.UnionContextList)
            {
                variableList.AddRange(context.FetchAllVars());
            }
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            foreach (var context in this.UnionContextList)
            {
                variableList.AddRange(context.FetchAllTableVars());
            }
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            if (this.UnionContextList.Count == 0)
            {
                parameters.AddRange(this.ProjectedProperties.Select(SqlUtil.GetValueExpr));
            }
            else
            {
                foreach (GremlinToSqlContext context in this.UnionContextList)
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
            }

            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Union, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
