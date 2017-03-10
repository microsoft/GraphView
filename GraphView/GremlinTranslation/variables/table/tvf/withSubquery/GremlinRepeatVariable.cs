using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinRepeatVariable : GremlinTableVariable
    {
        public GremlinVariable InputVariable { get; set; }
        public GremlinToSqlContext RepeatContext { get; set; }
        public RepeatCondition RepeatCondition { get; set; }
        public List<Tuple<string, GremlinRepeatSelectedVariable>> SelectedVariableList { get; set; }

        public GremlinRepeatVariable(GremlinVariable inputVariable,
                                    GremlinToSqlContext repeatContext,
                                    RepeatCondition repeatCondition,
                                    GremlinVariableType variableType)
            : base(variableType)
        {
            RepeatContext = repeatContext;
            RepeatContext.HomeVariable = this;
            InputVariable = inputVariable;
            RepeatCondition = repeatCondition;
            SelectedVariableList = new List<Tuple<string, GremlinRepeatSelectedVariable>>();
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);

            InputVariable.Populate(property);
            if (SelectedVariableList.Exists(p => p.Item1 != property))
            {
                RepeatContext.Populate(property);
            }
            else
            {
                RepeatContext.Populate(property);
            }
        }

        internal override GremlinVariableType GetUnfoldVariableType()
        {
            throw new NotImplementedException();
        }

        internal override bool ContainsLabel(string label)
        {
            if (base.ContainsLabel(label)) return true;
            foreach (var variable in RepeatContext.VariableList)
            {
                if (variable.ContainsLabel(label))
                {
                    return true;
                }
            }
            return false;
        }

        internal override GremlinPathStepVariable GetAndPopulatePath()
        {
            GremlinPathVariable pathVariable = RepeatContext.PopulateGremlinPath();
            pathVariable.IsInRepeatContext = true;
            return new GremlinPathStepVariable(pathVariable, this);
        }

        internal override List<GremlinVariable> PopulateAllTaggedVariable(string label)
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            foreach (var variable in RepeatContext.SelectVarsFromCurrAndChildContext(label))
            {
                var repeatSelectedVariable = new GremlinRepeatSelectedVariable(this, variable, label);
                variableList.Add(repeatSelectedVariable);
                if (SelectedVariableList.All(p => p.Item1 != label))
                {
                    SelectedVariableList.Add(new Tuple<string, GremlinRepeatSelectedVariable>(label, repeatSelectedVariable));
                }
            }
            return variableList;
        }

        internal override List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            variableList.AddRange(RepeatContext.FetchVarsFromCurrAndChildContext());
            if (RepeatCondition.EmitContext != null)
                variableList.AddRange(RepeatCondition.EmitContext.FetchVarsFromCurrAndChildContext());
            if (RepeatCondition.TerminationContext != null)
                variableList.AddRange(RepeatCondition.TerminationContext.FetchVarsFromCurrAndChildContext());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            //The following two variables are used for manually creating SelectScalarExpression of repeat
            List<WSelectScalarExpression> repeatFirstSelect = new List<WSelectScalarExpression>();
            List<WSelectScalarExpression> repeatSecondSelect = new List<WSelectScalarExpression>();

            // The following two variables are used for Generating a Map
            // such as N_0.id -> key_0 
            // Then we will use this map to replace ColumnRefernceExpression in the syntax tree which matchs n_0.id to R_0.key_0 
            Dictionary<WColumnReferenceExpression, string> repeatVistorMap = new Dictionary<WColumnReferenceExpression, string>();
            Dictionary<WColumnReferenceExpression, string> conditionVistorMap = new Dictionary<WColumnReferenceExpression, string>();

            //We should generate the syntax tree firstly
            //Some variables will populate ProjectProperty only when we call the ToTableReference function where they appear.
            WRepeatConditionExpression repeatConditionExpr = GetRepeatConditionExpression();
            WSelectQueryBlock repeatQueryBlock = RepeatContext.ToSelectQueryBlock();

            // TODO: explain this step in detail
            var repeatNewToOldSelectedVarMap = GetNewToOldSelectedVarMap(RepeatContext);
            repeatNewToOldSelectedVarMap[RepeatContext.PivotVariable] = RepeatContext.VariableList.First();
            foreach (var pair in repeatNewToOldSelectedVarMap)
            {
                GremlinVariable newVariable = pair.Key;
                GremlinVariable oldVariable = pair.Value;
                foreach (var property in pair.Value.ProjectedProperties)
                {
                    var aliasName = GenerateKey();
                    var firstSelectColumn = oldVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;
                    var secondSelectColumn = newVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;

                    repeatFirstSelect.Add(SqlUtil.GetSelectScalarExpr(firstSelectColumn, aliasName));
                    repeatSecondSelect.Add(SqlUtil.GetSelectScalarExpr(secondSelectColumn, aliasName));
                    repeatVistorMap[firstSelectColumn] = aliasName;
                }
            }

            if (RepeatCondition.TerminationContext != null && RepeatCondition.TerminationContext.VariableList.Count > 0)
            {
                var terminatedNewToOldSelectedVarMap = GetNewToOldSelectedVarMap(RepeatCondition.TerminationContext);
                terminatedNewToOldSelectedVarMap[RepeatContext.PivotVariable] = RepeatCondition.TerminationContext.VariableList.First();
                foreach (var pair in terminatedNewToOldSelectedVarMap)
                {
                    GremlinVariable newVariable = pair.Key;
                    GremlinVariable oldVariable = pair.Value;
                    foreach (var property in pair.Value.ProjectedProperties)
                    {
                        var aliasName = GenerateKey();
                        var firstSelectColumn = RepeatCondition.StartFromContext
                            ? oldVariable.GetVariableProperty(property).ToScalarExpression()
                            : SqlUtil.GetValueExpr(null);
                        var secondSelectColumn = newVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;

                        repeatFirstSelect.Add(SqlUtil.GetSelectScalarExpr(firstSelectColumn, aliasName));
                        repeatSecondSelect.Add(SqlUtil.GetSelectScalarExpr(secondSelectColumn, aliasName));

                        if (RepeatCondition.StartFromContext)
                            conditionVistorMap[firstSelectColumn as WColumnReferenceExpression] = aliasName;
                        else
                            conditionVistorMap[secondSelectColumn] = aliasName;
                    }
                }
            }

            if (RepeatCondition.EmitContext != null && RepeatCondition.EmitContext.VariableList.Count > 0)
            {
                var terminatedNewToOldSelectedVarMap = GetNewToOldSelectedVarMap(RepeatCondition.EmitContext);
                terminatedNewToOldSelectedVarMap[RepeatContext.PivotVariable] = RepeatCondition.EmitContext.VariableList.First();
                foreach (var pair in terminatedNewToOldSelectedVarMap)
                {
                    GremlinVariable newVariable = pair.Key;
                    GremlinVariable oldVariable = pair.Value;
                    foreach (var property in pair.Value.ProjectedProperties)
                    {
                        var aliasName = GenerateKey();
                        var firstSelectColumn = RepeatCondition.IsEmitContext
                            ? oldVariable.GetVariableProperty(property).ToScalarExpression()
                            : SqlUtil.GetValueExpr(null);
                        var secondSelectColumn = newVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;

                        repeatFirstSelect.Add(SqlUtil.GetSelectScalarExpr(firstSelectColumn, aliasName));
                        repeatSecondSelect.Add(SqlUtil.GetSelectScalarExpr(secondSelectColumn, aliasName));

                        if (RepeatCondition.IsEmitContext)
                            conditionVistorMap[firstSelectColumn as WColumnReferenceExpression] = aliasName;
                        else
                            conditionVistorMap[secondSelectColumn] = aliasName;
                    }
                }
            }

            foreach (var property in ProjectedProperties)
            {
                WScalarExpression firstExpr = InputVariable.ProjectedProperties.Contains(property)
                    ? InputVariable.GetVariableProperty(property).ToScalarExpression()
                    : SqlUtil.GetValueExpr(null);

                WScalarExpression secondExpr = RepeatContext.PivotVariable.ProjectedProperties.Contains(property)
                    ? RepeatContext.PivotVariable.GetVariableProperty(property).ToScalarExpression()
                    : SqlUtil.GetValueExpr(null);

                repeatFirstSelect.Add(SqlUtil.GetSelectScalarExpr(firstExpr, property));
                repeatSecondSelect.Add(SqlUtil.GetSelectScalarExpr(secondExpr, property));
            }

            if (SelectedVariableList.Count != 0)
            {
                foreach (var selectedVariableTuple in SelectedVariableList)
                {
                    var columnName = selectedVariableTuple.Item1;
                    var selectedVariable = selectedVariableTuple.Item2;

                    var compose2 = SqlUtil.GetFunctionCall(GremlinKeyword.func.Compose2, new List<WScalarExpression>()
                    {
                        SqlUtil.GetColumnReferenceExpr(GremlinKeyword.RepeatInitalTableName, columnName),
                        selectedVariable.RealVariable.ToCompose1()
                    });

                    repeatFirstSelect.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), columnName));
                    repeatSecondSelect.Add(SqlUtil.GetSelectScalarExpr(compose2, columnName));
                }
            }

            if (RepeatContext.IsPopulateGremlinPath)
            {
                repeatFirstSelect.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), GremlinKeyword.Path));
                repeatSecondSelect.Add(SqlUtil.GetSelectScalarExpr(RepeatContext.CurrentContextPath.DefaultProjection().ToScalarExpression(), GremlinKeyword.Path));
            }

            WSelectQueryBlock firstQueryExpr = new WSelectQueryBlock();
            foreach (var selectColumnExpr in repeatFirstSelect)
            {
                firstQueryExpr.SelectElements.Add(selectColumnExpr);
            }

            repeatQueryBlock = RepeatContext.ToSelectQueryBlock();
            repeatQueryBlock.SelectElements.Clear();
            foreach (var selectColumnExpr in repeatSecondSelect)
            {
                repeatQueryBlock.SelectElements.Add(selectColumnExpr);
            }

            //Replace N_0.id -> R_0.key_0, when N_0 is a outer variable
            new ModifyColumnNameVisitor().Invoke(repeatQueryBlock, repeatVistorMap);
            new ModifyColumnNameVisitor().Invoke(repeatConditionExpr, conditionVistorMap);

            List<WScalarExpression> repeatParameters = new List<WScalarExpression>()
            {
                SqlUtil.GetScalarSubquery(SqlUtil.GetBinaryQueryExpr(firstQueryExpr, repeatQueryBlock)),
                repeatConditionExpr
            };
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Repeat, repeatParameters, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }

        public WRepeatConditionExpression GetRepeatConditionExpression()
        {
            return new WRepeatConditionExpression()
            {
                EmitCondition = RepeatCondition.EmitContext?.ToSqlBoolean(),
                TerminationCondition = RepeatCondition.TerminationContext?.ToSqlBoolean(),
                RepeatTimes = RepeatCondition.RepeatTimes,
                StartFromContext = RepeatCondition.StartFromContext,
                EmitContext = RepeatCondition.IsEmitContext
            };
        }

        public Dictionary<GremlinVariable, GremlinVariable> GetNewToOldSelectedVarMap(GremlinToSqlContext context)
        {
            Dictionary<GremlinVariable, GremlinVariable> newToOldSelectedVarMap = new Dictionary<GremlinVariable, GremlinVariable>();
            if (context == null) return newToOldSelectedVarMap;

            var allVariables = context.FetchVarsFromCurrAndChildContext();
            foreach (var variable in allVariables)
            {
                var oldSelectedVar = variable as GremlinSelectedVariable;
                if (oldSelectedVar != null && oldSelectedVar.IsFromSelect && !allVariables.Contains(oldSelectedVar.RealVariable))
                {
                    List<GremlinVariable> newSelectVariableList = context.Select(oldSelectedVar.SelectKey);
                    GremlinVariable newSelectedVar;
                    switch (oldSelectedVar.Pop)
                    {
                        case GremlinKeyword.Pop.last:
                            newSelectedVar = newSelectVariableList.Last();
                            break;
                        case GremlinKeyword.Pop.first:
                            newSelectedVar = newSelectVariableList.First();
                            break;
                        default:
                            //TODO
                            throw new NotImplementedException("Can't process for now");
                    }
                    newToOldSelectedVarMap[newSelectedVar] = oldSelectedVar;
                }
            }
            return newToOldSelectedVarMap;
        }

        public string GenerateKey()
        {
            return GremlinKeyword.RepeatColumnPrefix + count++;
        }

        private int count;
    }

    internal class ModifyColumnNameVisitor : WSqlFragmentVisitor
    {
        private Dictionary<WColumnReferenceExpression, string> _map;

        public void Invoke(WSqlFragment queryBlock, Dictionary<WColumnReferenceExpression, string> map)
        {
            _map = map;
            queryBlock.Accept(this);
        }

        public override void Visit(WColumnReferenceExpression columnReference)
        {
            var key = columnReference.TableReference;
            var value = columnReference.ColumnName;
            foreach (var item in _map)
            {
                if (item.Key.TableReference.Equals(key) && item.Key.ColumnName.Equals(value))
                {
                    columnReference.MultiPartIdentifier.Identifiers[0].Value = GremlinKeyword.RepeatInitalTableName;
                    columnReference.MultiPartIdentifier.Identifiers[1].Value = item.Value;
                }
            }
        }
    }
}
