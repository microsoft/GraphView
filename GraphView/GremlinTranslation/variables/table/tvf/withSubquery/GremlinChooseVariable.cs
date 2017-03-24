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

        public GremlinChooseVariable(GremlinToSqlContext predicateContext, GremlinToSqlContext trueChoiceContext, GremlinToSqlContext falseChocieContext)
            : base(GremlinVariableType.Table)
        {
            PredicateContext = predicateContext;
            TrueChoiceContext = trueChoiceContext;
            FalseChocieContext = falseChocieContext;
            Options = new Dictionary<object, GremlinToSqlContext>();
        }

        public GremlinChooseVariable(GremlinToSqlContext choiceContext, Dictionary<object, GremlinToSqlContext> options)
            : base(GremlinVariableType.Table)
        {
            ChoiceContext = choiceContext;
            Options = options;
        }

        internal override void Populate(string property)
        {
            base.Populate(property);

            TrueChoiceContext?.Populate(property);
            FalseChocieContext?.Populate(property);

            foreach (var option in Options)
            {
                option.Value.Populate(property);
            }
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            if (PredicateContext != null)
            {
                variableList.AddRange(PredicateContext.FetchAllVars());
                variableList.AddRange(TrueChoiceContext.FetchAllVars());
                variableList.AddRange(FalseChocieContext.FetchAllVars());
            }
            else
            {
                variableList.AddRange(ChoiceContext.FetchAllVars());
                foreach (var option in Options)
                {
                    variableList.AddRange(option.Value.FetchAllVars());
                }
            }
            return variableList;
        }

        internal override List<GremlinVariable> FetchAllTableVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            if (PredicateContext != null)
            {
                variableList.AddRange(PredicateContext.FetchAllTableVars());
                variableList.AddRange(TrueChoiceContext.FetchAllTableVars());
                variableList.AddRange(FalseChocieContext.FetchAllTableVars());
            }
            else
            {
                variableList.AddRange(ChoiceContext.FetchAllTableVars());
                foreach (var option in Options)
                {
                    variableList.AddRange(option.Value.FetchAllTableVars());
                }
            }
            return variableList;
        }

        internal override void PopulateStepProperty(string property)
        {
            if (PredicateContext != null)
            {
                TrueChoiceContext.ContextLocalPath.PopulateStepProperty(property);
                FalseChocieContext.ContextLocalPath.PopulateStepProperty(property);
            }
            else
            {
                foreach (var option in Options)
                {
                    option.Value.ContextLocalPath.PopulateStepProperty(property);
                }
            }
        }

        internal override void PopulateLocalPath()
        {
            if (ProjectedProperties.Contains(GremlinKeyword.Path)) return;
            ProjectedProperties.Add(GremlinKeyword.Path);
            if (PredicateContext != null)
            {
                TrueChoiceContext.PopulateLocalPath();
                FalseChocieContext.PopulateLocalPath();
            }
            else
            {
                foreach (var option in Options)
                {
                    option.Value.PopulateLocalPath();
                }
            }
        }

        internal override WScalarExpression ToStepScalarExpr()
        {
            return SqlUtil.GetColumnReferenceExpr(GetVariableName(), GremlinKeyword.Path);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            WTableReference tableReference;

            if (PredicateContext != null)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(PredicateContext.ToSelectQueryBlock()));
                parameters.Add(SqlUtil.GetScalarSubquery(TrueChoiceContext.ToSelectQueryBlock()));
                parameters.Add(SqlUtil.GetScalarSubquery(FalseChocieContext.ToSelectQueryBlock()));
                tableReference = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Choose, parameters, GetVariableName());
            }
            else
            {
                parameters.Add(SqlUtil.GetScalarSubquery(ChoiceContext.ToSelectQueryBlock()));
                foreach (var option in Options)
                {
                    if (option.Key is GremlinKeyword.Pick  && (GremlinKeyword.Pick)option.Key == GremlinKeyword.Pick.None)
                        parameters.Add(SqlUtil.GetValueExpr(null));
                    else
                        parameters.Add(SqlUtil.GetValueExpr(option.Key));

                    parameters.Add(SqlUtil.GetScalarSubquery(option.Value.ToSelectQueryBlock()));
                }
                tableReference = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.ChooseWithOptions, parameters, GetVariableName());
            }

            return SqlUtil.GetCrossApplyTableReference(tableReference);
        }
    }
}
