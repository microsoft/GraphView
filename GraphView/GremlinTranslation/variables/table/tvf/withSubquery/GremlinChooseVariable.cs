using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinChooseVariable : GremlinTableVariable
    {
        public GremlinToSqlContext PredicateContext { get; set; }
        public GremlinToSqlContext TrueChoiceContext { get; set; }
        public GremlinToSqlContext FalseChocieContext { get; set; }
        public GremlinToSqlContext ChoiceContext { get; set; }
        public Dictionary<object, GremlinToSqlContext> Options { get; set; }

        public GremlinChooseVariable(GremlinToSqlContext predicateContext, GremlinToSqlContext trueChoiceContext,
            GremlinToSqlContext falseChocieContext, GremlinVariableType variableType) : base(variableType)
        {
            this.PredicateContext = predicateContext;
            this.TrueChoiceContext = trueChoiceContext;
            this.FalseChocieContext = falseChocieContext;
            this.Options = new Dictionary<object, GremlinToSqlContext>();
        }

        public GremlinChooseVariable(GremlinToSqlContext choiceContext, Dictionary<object, GremlinToSqlContext> options,
            GremlinVariableType variableType) : base(variableType)
        {
            this.ChoiceContext = choiceContext;
            this.Options = options;
        }

        internal override bool Populate(string property, string label = null)
        {
            bool populateSuccessfully = false;
            if (label == null || this.ProjectedProperties.Contains(label))
            {
                populateSuccessfully = true;
                this.TrueChoiceContext?.Populate(property, null);
                this.FalseChocieContext?.Populate(property, null);
                foreach (var option in this.Options)
                {
                    option.Value.Populate(property, null);
                }
            }
            else if (this.PredicateContext != null)
            {
                populateSuccessfully |= this.TrueChoiceContext.Populate(property, label);
                populateSuccessfully |= this.FalseChocieContext.Populate(property, label);
            }
            else
            {
                foreach (var option in this.Options)
                {
                    populateSuccessfully |= option.Value.Populate(property, label);
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
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            if (this.PredicateContext != null)
            {
                variableList.AddRange(this.PredicateContext.FetchAllVars());
                variableList.AddRange(this.TrueChoiceContext.FetchAllVars());
                variableList.AddRange(this.FalseChocieContext.FetchAllVars());
            }
            else
            {
                variableList.AddRange(this.ChoiceContext.FetchAllVars());
                foreach (var option in this.Options)
                {
                    variableList.AddRange(option.Value.FetchAllVars());
                }
            }
            return variableList;
        }

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            if (this.PredicateContext != null)
            {
                variableList.AddRange(this.PredicateContext.FetchAllTableVars());
                variableList.AddRange(this.TrueChoiceContext.FetchAllTableVars());
                variableList.AddRange(this.FalseChocieContext.FetchAllTableVars());
            }
            else
            {
                variableList.AddRange(this.ChoiceContext.FetchAllTableVars());
                foreach (var option in this.Options)
                {
                    variableList.AddRange(option.Value.FetchAllTableVars());
                }
            }
            return variableList;
        }

        internal override bool PopulateStepProperty(string property, string label = null)
        {
            bool populateSuccessfully = false;
            
            if (this.PredicateContext != null)
            {
                populateSuccessfully |= this.TrueChoiceContext.ContextLocalPath.PopulateStepProperty(property, label);
                populateSuccessfully |= this.FalseChocieContext.ContextLocalPath.PopulateStepProperty(property, label);
            }
            else
            {
                foreach (var option in this.Options)
                {
                    populateSuccessfully |= option.Value.ContextLocalPath.PopulateStepProperty(property, label);
                }
            }
            return populateSuccessfully;
        }

        internal override void PopulateLocalPath()
        {
            if (ProjectedProperties.Contains(GremlinKeyword.Path))
            {
                return;
            }
            ProjectedProperties.Add(GremlinKeyword.Path);
            if (this.PredicateContext != null)
            {
                this.TrueChoiceContext.PopulateLocalPath();
                this.FalseChocieContext.PopulateLocalPath();
                this.LocalPathLengthLowerBound = Math.Min(this.TrueChoiceContext.MinPathLength,
                    this.FalseChocieContext.MinPathLength);
            }
            else
            {
                this.LocalPathLengthLowerBound = Int32.MaxValue;
                foreach (var option in this.Options)
                {
                    option.Value.PopulateLocalPath();
                    this.LocalPathLengthLowerBound = Math.Min(this.LocalPathLengthLowerBound, option.Value.MinPathLength);
                }
            }
        }

        internal override WScalarExpression ToStepScalarExpr(HashSet<string> composedProperties = null)
        {
            return SqlUtil.GetColumnReferenceExpr(GetVariableName(), GremlinKeyword.Path);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            WTableReference tableReference;

            if (this.PredicateContext != null)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(this.PredicateContext.ToSelectQueryBlock()));

                // Align
                List<WSelectQueryBlock> selectQueryBlocks = new List<WSelectQueryBlock>();
                selectQueryBlocks.Add(this.TrueChoiceContext.ToSelectQueryBlock());
                selectQueryBlocks.Add(this.FalseChocieContext.ToSelectQueryBlock());

                foreach (WSelectQueryBlock selectQueryBlock in selectQueryBlocks)
                {
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
                parameters.AddRange(selectQueryBlocks.Select(SqlUtil.GetScalarSubquery));

                tableReference = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Choose, parameters, GetVariableName());
            }
            else
            {
                parameters.Add(SqlUtil.GetScalarSubquery(this.ChoiceContext.ToSelectQueryBlock()));

                foreach (var option in this.Options)
                {
                    if (option.Key is GremlinKeyword.Pick && (GremlinKeyword.Pick) option.Key == GremlinKeyword.Pick.None)
                    {
                        parameters.Add(SqlUtil.GetValueExpr(null));
                    }
                    else
                    {
                        parameters.Add(SqlUtil.GetValueExpr(option.Key));
                    }

                    //Align
                    WSelectQueryBlock selectQueryBlock = option.Value.ToSelectQueryBlock();
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
                tableReference = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.ChooseWithOptions, parameters, GetVariableName());
            }
            return SqlUtil.GetCrossApplyTableReference(tableReference);
        }
    }
}
