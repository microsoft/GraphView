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
            this.RepeatContext = repeatContext;
            this.InputVariable = inputVariable;
            this.RepeatCondition = repeatCondition;
        }

        internal override void Populate(string property)
        {
            base.Populate(property);

            this.InputVariable.Populate(property);
            this.RepeatContext.Populate(property);
        }

        internal override void PopulateStepProperty(string property)
        {
            this.RepeatContext.ContextLocalPath.PopulateStepProperty(property);
        }

        internal override void PopulateLocalPath()
        {
            if (this.ProjectedProperties.Contains(GremlinKeyword.Path)) return;
            this.ProjectedProperties.Add(GremlinKeyword.Path);
            this.RepeatContext.PopulateLocalPath();
        }

        internal override WScalarExpression ToStepScalarExpr()
        {
            return SqlUtil.GetColumnReferenceExpr(this.GetVariableName(), GremlinKeyword.Path);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.InputVariable.FetchAllVars());
            variableList.AddRange(this.RepeatContext.FetchAllVars());
            if (this.RepeatCondition.EmitContext != null)
                variableList.AddRange(this.RepeatCondition.EmitContext.FetchAllVars());
            if (this.RepeatCondition.TerminationContext != null)
                variableList.AddRange(this.RepeatCondition.TerminationContext.FetchAllVars());
            return variableList;
        }

        internal override List<GremlinVariable> FetchAllTableVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.RepeatContext.FetchAllTableVars());
            if (this.RepeatCondition.EmitContext != null)
                variableList.AddRange(this.RepeatCondition.EmitContext.FetchAllTableVars());
            if (this.RepeatCondition.TerminationContext != null)
                variableList.AddRange(this.RepeatCondition.TerminationContext.FetchAllTableVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            //The following two variables are used for manually creating SelectScalarExpression of repeat
            List<WSelectScalarExpression> firstSelectList = new List<WSelectScalarExpression>();
            List<WSelectScalarExpression> secondSelectList = new List<WSelectScalarExpression>();

            // The following two variables are used for Generating a Map
            // such as N_0.id -> R.key_0 
            // Then we will use this map to replace ColumnRefernceExpression in the syntax tree which matchs n_0.id to R_0.key_0 
            Dictionary<Tuple<string, string>, Tuple<string, string>> inputVariableVistorMap = new Dictionary<Tuple<string, string>, Tuple<string, string>>();
            Dictionary<Tuple<string, string>, Tuple<string, string>> outerVariablesVistorMap = new Dictionary<Tuple<string, string>, Tuple<string, string>>();

            //We should generate the syntax tree firstly
            //Some variables will populate ProjectProperty only when we call the ToTableReference function where they appear.
            WRepeatConditionExpression repeatConditionExpr = this.GetRepeatConditionExpression();
            WSelectQueryBlock repeatQueryBlock = this.RepeatContext.ToSelectQueryBlock();

            List<GremlinVariable> outerVariables = this.GetOuterVariables(this.RepeatContext);
            outerVariables.AddRange(this.GetOuterVariables(this.RepeatCondition.TerminationContext));
            outerVariables.AddRange(this.GetOuterVariables(this.RepeatCondition.EmitContext));
            foreach (GremlinVariable outerVariable in outerVariables)
            {
                foreach (string property in outerVariable.ProjectedProperties)
                {
                    string aliasName = this.GenerateKey();
                    WScalarExpression firstSelectColumn = outerVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;
                    WScalarExpression secondSelectColumn = SqlUtil.GetColumnReferenceExpr(GremlinKeyword.RepeatInitalTableName, aliasName);

                    firstSelectList.Add(SqlUtil.GetSelectScalarExpr(firstSelectColumn, aliasName));
                    secondSelectList.Add(SqlUtil.GetSelectScalarExpr(secondSelectColumn, aliasName));

                    Tuple<string, string> key = new Tuple<string, string>(outerVariable.GetVariableName(), property);
                    Tuple<string, string> value = new Tuple<string, string>(GremlinKeyword.RepeatInitalTableName, aliasName);
                    outerVariablesVistorMap[key] = value;
                }
            }

            GremlinVariable repeatInputVariable = this.RepeatContext.VariableList.First();
            GremlinVariable repeatPivotVariable = this.RepeatContext.PivotVariable;
            foreach (string property in repeatInputVariable.ProjectedProperties)
            {
                string aliasName = this.GenerateKey();
                WScalarExpression firstSelectColumn = repeatInputVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;
                WScalarExpression secondSelectColumn = repeatPivotVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;

                firstSelectList.Add(SqlUtil.GetSelectScalarExpr(firstSelectColumn, aliasName));
                secondSelectList.Add(SqlUtil.GetSelectScalarExpr(secondSelectColumn, aliasName));

                Tuple<string, string> key = new Tuple<string, string>(repeatInputVariable.GetVariableName(), property);
                Tuple<string, string> value = new Tuple<string, string>(GremlinKeyword.RepeatInitalTableName, aliasName);
                inputVariableVistorMap[key] = value;
            }

            if (this.RepeatCondition.TerminationContext != null && this.RepeatCondition.TerminationContext.VariableList.Count > 0)
            {
                GremlinVariable untilInputVariable = this.RepeatCondition.TerminationContext.VariableList.First();
                foreach (string property in untilInputVariable.ProjectedProperties)
                {
                    string aliasName = this.GenerateKey();
                    WScalarExpression firstSelectColumn = untilInputVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;
                    WScalarExpression secondSelectColumn = repeatPivotVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;

                    firstSelectList.Add(SqlUtil.GetSelectScalarExpr(firstSelectColumn, aliasName));
                    secondSelectList.Add(SqlUtil.GetSelectScalarExpr(secondSelectColumn, aliasName));

                    Tuple<string, string> key = new Tuple<string, string>(untilInputVariable.GetVariableName(), property);
                    Tuple<string, string> value = new Tuple<string, string>(GremlinKeyword.RepeatInitalTableName, aliasName);
                    inputVariableVistorMap[key] = value;
                }
            }

            if (this.RepeatCondition.EmitContext != null && this.RepeatCondition.EmitContext.VariableList.Count > 0)
            {
                GremlinVariable emitInputVariable = this.RepeatCondition.EmitContext.VariableList.First();
                foreach (string property in emitInputVariable.ProjectedProperties)
                {
                    string aliasName = this.GenerateKey();
                    WScalarExpression firstSelectColumn = emitInputVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;
                    WScalarExpression secondSelectColumn = repeatPivotVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;

                    firstSelectList.Add(SqlUtil.GetSelectScalarExpr(firstSelectColumn, aliasName));
                    secondSelectList.Add(SqlUtil.GetSelectScalarExpr(secondSelectColumn, aliasName));

                    Tuple<string, string> key = new Tuple<string, string>(emitInputVariable.GetVariableName(), property);
                    Tuple<string, string> value = new Tuple<string, string>(GremlinKeyword.RepeatInitalTableName, aliasName);
                    inputVariableVistorMap[key] = value;
                }
            }

            foreach (string property in this.ProjectedProperties)
            {
                if (property == GremlinKeyword.Path)
                {
                    firstSelectList.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), GremlinKeyword.Path));
                    secondSelectList.Add(SqlUtil.GetSelectScalarExpr(this.RepeatContext.ContextLocalPath.DefaultProjection().ToScalarExpression(), GremlinKeyword.Path));
                    continue;
                }
                WScalarExpression firstExpr = this.InputVariable.ProjectedProperties.Contains(property)
                    ? this.InputVariable.GetVariableProperty(property).ToScalarExpression()
                    : SqlUtil.GetValueExpr(null);

                WScalarExpression secondExpr = this.RepeatContext.PivotVariable.ProjectedProperties.Contains(property)
                    ? this.RepeatContext.PivotVariable.GetVariableProperty(property).ToScalarExpression()
                    : SqlUtil.GetValueExpr(null);

                firstSelectList.Add(SqlUtil.GetSelectScalarExpr(firstExpr, property));
                secondSelectList.Add(SqlUtil.GetSelectScalarExpr(secondExpr, property));
            }

            WSelectQueryBlock firstQueryExpr = new WSelectQueryBlock();
            foreach (WSelectScalarExpression selectColumnExpr in firstSelectList)
            {
                firstQueryExpr.SelectElements.Add(selectColumnExpr);
            }

            repeatQueryBlock = this.RepeatContext.ToSelectQueryBlock();
            repeatQueryBlock.SelectElements.Clear();
            foreach (WSelectScalarExpression selectColumnExpr in secondSelectList)
            {
                repeatQueryBlock.SelectElements.Add(selectColumnExpr);
            }

            //Replace N_0.id -> R_0.key_0, when N_0 is a outer variable
            new ModifyOuterVariablesVisitor().Invoke(repeatQueryBlock, outerVariablesVistorMap);
            new ModifyOuterVariablesVisitor().Invoke(repeatConditionExpr, outerVariablesVistorMap);
            new ModifyColumnNameVisitor().Invoke(repeatQueryBlock, inputVariableVistorMap);
            new ModifyColumnNameVisitor().Invoke(repeatConditionExpr, inputVariableVistorMap);

            List<WScalarExpression> repeatParameters = new List<WScalarExpression>()
            {
                SqlUtil.GetScalarSubquery(SqlUtil.GetBinaryQueryExpr(firstQueryExpr, repeatQueryBlock)),
                repeatConditionExpr
            };
            WSchemaObjectFunctionTableReference tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Repeat, repeatParameters, this.GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }

        public WRepeatConditionExpression GetRepeatConditionExpression()
        {
            return new WRepeatConditionExpression()
            {
                EmitCondition = this.RepeatCondition.EmitContext?.ToSqlBoolean(),
                TerminationCondition = this.RepeatCondition.TerminationContext?.ToSqlBoolean(),
                RepeatTimes = this.RepeatCondition.RepeatTimes,
                StartFromContext = this.RepeatCondition.StartFromContext,
                EmitContext = this.RepeatCondition.IsEmitContext
            };
        }

        public List<GremlinVariable> GetOuterVariables(GremlinToSqlContext context)
        {
            List<GremlinVariable> outerVariables = new List<GremlinVariable>();
            if (context == null) return outerVariables;

            List<GremlinVariable> allVariables = context.FetchAllVars().Distinct().ToList();
            List<GremlinVariable> allTableVariables = context.FetchAllTableVars();
            for (int i = 1; i < allVariables.Count; i++)
            {
                if (!allTableVariables.Select(var => var.GetVariableName()).ToList().Contains(allVariables[i].GetVariableName()))
                {
                    outerVariables.Add(allVariables[i]);
                }
            }
            return outerVariables;
        }

        public string GenerateKey()
        {
            return GremlinKeyword.RepeatColumnPrefix + this.count++;
        }

        private int count;
    }

    internal class ModifyColumnNameVisitor : WSqlFragmentVisitor
    {
        private Dictionary<Tuple<string, string>, Tuple<string, string>> map;

        public void Invoke(WSqlFragment queryBlock, Dictionary<Tuple<string, string>, Tuple<string, string>> map)
        {
            this.map = map;
            queryBlock.Accept(this);
        }

        public override void Visit(WColumnReferenceExpression columnReference)
        {
            string key = columnReference.TableReference;
            string value = columnReference.ColumnName;
            foreach (KeyValuePair<Tuple<string, string>, Tuple<string, string>> item in this.map)
            {
                if (item.Key.Item1.Equals(key) && item.Key.Item2.Equals(value))
                {
                    columnReference.MultiPartIdentifier.Identifiers[0].Value = item.Value.Item1;
                    columnReference.MultiPartIdentifier.Identifiers[1].Value = item.Value.Item2;
                }
            }
        }
    }

    internal class ModifyOuterVariablesVisitor : WSqlFragmentVisitor
    {
        private Dictionary<Tuple<string, string>, Tuple<string, string>> map;

        public void Invoke(WSqlFragment queryBlock, Dictionary<Tuple<string, string>, Tuple<string, string>> map)
        {
            this.map = map;
            queryBlock.Accept(this);
        }

        public override void Visit(WSchemaObjectFunctionTableReference pathTable)
        {
            if (pathTable is WPathTableReference)
            {
                new ModifyColumnNameVisitor().Invoke(pathTable, this.map);
            }
            else
            {
                pathTable.AcceptChildren(this);
            }
        }
    }
}
