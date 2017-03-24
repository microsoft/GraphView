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

        public GremlinRepeatVariable(GremlinVariable inputVariable,
                                    GremlinToSqlContext repeatContext,
                                    RepeatCondition repeatCondition,
                                    GremlinVariableType variableType)
            : base(variableType)
        {
            RepeatContext = repeatContext;
            InputVariable = inputVariable;
            RepeatCondition = repeatCondition;
        }

        internal override void Populate(string property)
        {
            base.Populate(property);

            InputVariable.Populate(property);
            RepeatContext.Populate(property);
        }

        internal override void PopulateStepProperty(string property)
        {
            RepeatContext.ContextLocalPath.PopulateStepProperty(property);
        }

        internal override void PopulateLocalPath()
        {
            if (ProjectedProperties.Contains(GremlinKeyword.Path)) return;
            ProjectedProperties.Add(GremlinKeyword.Path);
            RepeatContext.PopulateLocalPath();
        }

        internal override WScalarExpression ToStepScalarExpr()
        {
            return SqlUtil.GetColumnReferenceExpr(GetVariableName(), GremlinKeyword.Path);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(InputVariable.FetchAllVars());
            variableList.AddRange(RepeatContext.FetchAllVars());
            if (RepeatCondition.EmitContext != null)
                variableList.AddRange(RepeatCondition.EmitContext.FetchAllVars());
            if (RepeatCondition.TerminationContext != null)
                variableList.AddRange(RepeatCondition.TerminationContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinVariable> FetchAllTableVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(RepeatContext.FetchAllTableVars());
            if (RepeatCondition.EmitContext != null)
                variableList.AddRange(RepeatCondition.EmitContext.FetchAllTableVars());
            if (RepeatCondition.TerminationContext != null)
                variableList.AddRange(RepeatCondition.TerminationContext.FetchAllTableVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            if (RepeatContext.ContextLocalPath != null)
            {
                RepeatContext.ContextLocalPath.PathList.Insert(0, null);
                RepeatContext.ContextLocalPath.IsInRepeatContext = true;
            }

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
            if (!repeatNewToOldSelectedVarMap.ContainsKey(RepeatContext.PivotVariable))
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
                    repeatVistorMap[firstSelectColumn.Copy()] = aliasName;
                }
            }

            if (RepeatCondition.TerminationContext != null && RepeatCondition.TerminationContext.VariableList.Count > 0)
            {
                var terminatedNewToOldSelectedVarMap = GetNewToOldSelectedVarMap(RepeatCondition.TerminationContext);
                if (!terminatedNewToOldSelectedVarMap.ContainsKey(RepeatContext.PivotVariable))
                    terminatedNewToOldSelectedVarMap[RepeatContext.PivotVariable] = RepeatCondition.TerminationContext.VariableList.First();
                foreach (var pair in terminatedNewToOldSelectedVarMap)
                {
                    GremlinVariable newVariable = pair.Key;
                    GremlinVariable oldVariable = pair.Value;
                    foreach (var property in oldVariable.ProjectedProperties)
                    {
                        var aliasName = GenerateKey();
                        var firstSelectColumn = RepeatCondition.StartFromContext
                            ? oldVariable.GetVariableProperty(property).ToScalarExpression()
                            : SqlUtil.GetValueExpr(null);
                        var secondSelectColumn = newVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;

                        repeatFirstSelect.Add(SqlUtil.GetSelectScalarExpr(firstSelectColumn, aliasName));
                        repeatSecondSelect.Add(SqlUtil.GetSelectScalarExpr(secondSelectColumn, aliasName));

                        if (RepeatCondition.StartFromContext)
                            conditionVistorMap[(firstSelectColumn as WColumnReferenceExpression).Copy()] = aliasName;
                        else
                            conditionVistorMap[secondSelectColumn.Copy()] = aliasName;
                    }
                }
            }

            if (RepeatCondition.EmitContext != null && RepeatCondition.EmitContext.VariableList.Count > 0)
            {
                var terminatedNewToOldSelectedVarMap = GetNewToOldSelectedVarMap(RepeatCondition.EmitContext);
                if (!terminatedNewToOldSelectedVarMap.ContainsKey(RepeatContext.PivotVariable))
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
                            conditionVistorMap[(firstSelectColumn as WColumnReferenceExpression).Copy()] = aliasName;
                        else
                            conditionVistorMap[secondSelectColumn.Copy()] = aliasName;
                    }
                }
            }

            foreach (var property in ProjectedProperties)
            {
                if (property == GremlinKeyword.Path)
                {
                    repeatFirstSelect.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), GremlinKeyword.Path));
                    repeatSecondSelect.Add(SqlUtil.GetSelectScalarExpr(RepeatContext.ContextLocalPath.DefaultProjection().ToScalarExpression(), GremlinKeyword.Path));
                    continue;
                }
                WScalarExpression firstExpr = InputVariable.ProjectedProperties.Contains(property)
                    ? InputVariable.GetVariableProperty(property).ToScalarExpression()
                    : SqlUtil.GetValueExpr(null);

                WScalarExpression secondExpr = RepeatContext.PivotVariable.ProjectedProperties.Contains(property)
                    ? RepeatContext.PivotVariable.GetVariableProperty(property).ToScalarExpression()
                    : SqlUtil.GetValueExpr(null);

                repeatFirstSelect.Add(SqlUtil.GetSelectScalarExpr(firstExpr, property));
                repeatSecondSelect.Add(SqlUtil.GetSelectScalarExpr(secondExpr, property));
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

            var allVariables = context.FetchAllVars().Distinct().ToList();
            var allTableVariables = context.FetchAllTableVars();
            for (var i = 1; i < allVariables.Count; i++)
            {
                if (!allTableVariables.Select(var => var.GetVariableName()).ToList().Contains(allVariables[i].GetVariableName()))
                {
                    newToOldSelectedVarMap[allVariables[i]] = allVariables[i];
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
        private Dictionary<WColumnReferenceExpression, string> map;

        public void Invoke(WSqlFragment queryBlock, Dictionary<WColumnReferenceExpression, string> map)
        {
            this.map = map;
            queryBlock.Accept(this);
        }

        public override void Visit(WColumnReferenceExpression columnReference)
        {
            var key = columnReference.TableReference;
            var value = columnReference.ColumnName;
            foreach (var item in map)
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
