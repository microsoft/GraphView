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

            TrueChoiceContext.HomeVariable = this;
            FalseChocieContext.HomeVariable = this;
        }

        public GremlinChooseVariable(GremlinToSqlContext choiceContext, Dictionary<object, GremlinToSqlContext> options)
            : base(GremlinVariableType.Table)
        {
            ChoiceContext = choiceContext;
            Options = options;

            foreach (var option in options)
            {
                option.Value.HomeVariable = this;
            }
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);

            TrueChoiceContext?.Populate(property);
            FalseChocieContext?.Populate(property);

            foreach (var option in Options)
            {
                option.Value.Populate(property);
            }
        }

        internal override bool ContainsLabel(string label)
        {
            if (base.ContainsLabel(label)) return true;

            if (PredicateContext != null)
            {
                foreach (var variable in TrueChoiceContext.VariableList)
                {
                    if (variable.ContainsLabel(label))
                    {
                        return true;
                    }
                }
                foreach (var variable in FalseChocieContext.VariableList)
                {
                    if (variable.ContainsLabel(label))
                    {
                        return true;
                    }
                }
            }
            else
            {
                foreach (var option in Options)
                {
                    foreach (var variable in option.Value.VariableList)
                    {
                        if (variable.ContainsLabel(label))
                        {
                            return true;
                        }
                    }
                }
            }

            return false;
        }

        internal override List<GremlinVariable> PopulateAllTaggedVariable(string label)
        {
            //TODO: refactor
            List<List<GremlinVariable>> branchVariableList = new List<List<GremlinVariable>>();

            if (PredicateContext != null)
            {
                branchVariableList.Add(TrueChoiceContext.FetchVarsFromCurrAndChildContext());
                branchVariableList.Add(FalseChocieContext.FetchVarsFromCurrAndChildContext());
            }
            else
            {
                foreach (var option in Options)
                {
                    var variableList = option.Value.SelectVarsFromCurrAndChildContext(label);
                    branchVariableList.Add(variableList);
                }
            }

            return new List<GremlinVariable>() { GremlinBranchVariable.Create(label, this, branchVariableList) };
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();

            if (PredicateContext != null)
            {
                variableList.AddRange(PredicateContext.FetchVarsFromCurrAndChildContext());
                variableList.AddRange(TrueChoiceContext.FetchVarsFromCurrAndChildContext());
                variableList.AddRange(FalseChocieContext.FetchVarsFromCurrAndChildContext());
            }
            else
            {
                variableList.AddRange(ChoiceContext.FetchVarsFromCurrAndChildContext());
                foreach (var option in Options)
                {
                    variableList.AddRange(option.Value.FetchVarsFromCurrAndChildContext());
                }
            }
            return variableList;
        }

        internal override GremlinVariableType GetUnfoldVariableType()
        {
            throw new NotImplementedException();
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            WTableReference tableReference;

            if (PredicateContext != null)
            {
                parameters.Add(SqlUtil.GetScalarSubquery(PredicateContext.ToSelectQueryBlock()));
                parameters.Add(SqlUtil.GetScalarSubquery(TrueChoiceContext.ToSelectQueryBlock(ProjectedProperties)));
                parameters.Add(SqlUtil.GetScalarSubquery(FalseChocieContext.ToSelectQueryBlock(ProjectedProperties)));
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

                    parameters.Add(SqlUtil.GetScalarSubquery(option.Value.ToSelectQueryBlock(ProjectedProperties)));
                }
                tableReference = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.ChooseWithOptions, parameters, GetVariableName());
            }

            return SqlUtil.GetCrossApplyTableReference(tableReference);
        }
    }
}
