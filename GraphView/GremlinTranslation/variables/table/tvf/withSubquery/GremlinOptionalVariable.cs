using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinOptionalVariable : GremlinTableVariable
    {
        public GremlinToSqlContext OptionalContext { get; set; }
        public GremlinContextVariable InputVariable { get; set; }

        public GremlinOptionalVariable(GremlinVariable inputVariable, GremlinToSqlContext context,
            GremlinVariableType variableType) : base(variableType)
        {
            inputVariable.ProjectedProperties.Clear();
            this.OptionalContext = context;
            this.InputVariable = new GremlinContextVariable(inputVariable);
        }

        internal override bool PopulateStepProperty(string property, string label = null)
        {
            return this.OptionalContext.ContextLocalPath.PopulateStepProperty(property, label);
        }

        internal override void PopulateLocalPath()
        {
            if (ProjectedProperties.Contains(GremlinKeyword.Path))
            {
                return;
            }
            ProjectedProperties.Add(GremlinKeyword.Path);
            this.OptionalContext.PopulateLocalPath();
            this.LocalPathLengthLowerBound = 0;
        }

        internal override bool Populate(string property, string label = null)
        {
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label))
            {
                populateSuccessfully = true;
                this.InputVariable.Populate(property, null);
                this.OptionalContext.Populate(property, null);
            }
            else
            {
                populateSuccessfully |= this.InputVariable.Populate(property, label);
                populateSuccessfully |= this.OptionalContext.Populate(property, label);
            }
           if (populateSuccessfully && property != null)
            {
                this.ProjectedProperties.Add(property);
            }
            return populateSuccessfully;
        }

        internal override WScalarExpression ToStepScalarExpr(HashSet<string> composedProperties = null)
        {
            return SqlUtil.GetColumnReferenceExpr(GetVariableName(), GremlinKeyword.Path);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.Add(this.InputVariable);
            variableList.AddRange(this.OptionalContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            variableList.AddRange(this.OptionalContext.FetchAllTableVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WSelectQueryBlock> selectQueryBlocks = new List<WSelectQueryBlock>();
            selectQueryBlocks.Add(new WSelectQueryBlock());
            selectQueryBlocks.Add(this.OptionalContext.ToSelectQueryBlock());
            
            Dictionary<string, WSelectElement> projectionMap = new Dictionary<string, WSelectElement>();
            WSelectElement value = selectQueryBlocks[1].SelectElements[0];
            foreach (WSelectElement selectElement in selectQueryBlocks[1].SelectElements)
            {
                projectionMap[(selectElement as WSelectScalarExpression).ColumnName] = selectElement;
            }
            selectQueryBlocks[1].SelectElements.Clear();

            selectQueryBlocks[0].SelectElements.Add(SqlUtil.GetSelectScalarExpr(this.InputVariable.DefaultProjection().ToScalarExpression(), this.DefaultProperty()));
            selectQueryBlocks[1].SelectElements.Add(SqlUtil.GetSelectScalarExpr((value as WSelectScalarExpression).SelectExpr, this.DefaultProperty()));
            foreach (string property in this.ProjectedProperties)
            {

                selectQueryBlocks[0].SelectElements.Add(
                    SqlUtil.GetSelectScalarExpr(
                        this.InputVariable.RealVariable.ProjectedProperties.Contains(property)
                            ? this.InputVariable.RealVariable.GetVariableProperty(property).ToScalarExpression()
                            : SqlUtil.GetValueExpr(null), property));
                 
                selectQueryBlocks[1].SelectElements.Add(
                    projectionMap.TryGetValue(property, out value)
                        ? value : SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), property));
            }
            

            WBinaryQueryExpression binaryQueryExpression = SqlUtil.GetBinaryQueryExpr(selectQueryBlocks[0], selectQueryBlocks[1]);

            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetScalarSubquery(binaryQueryExpression));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Optional, parameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
