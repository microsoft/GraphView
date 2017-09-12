using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.TSQL_Syntax_Tree;

namespace GraphView
{
    internal class GremlinRepeatVariable : GremlinTableVariable
    {
        public GremlinContextVariable InputVariable { get; set; }
        public GremlinToSqlContext RepeatContext { get; set; }
        public RepeatCondition RepeatCondition { get; set; }

        private int Count;

        public GremlinRepeatVariable(GremlinVariable inputVariable,
            GremlinToSqlContext repeatContext,
            RepeatCondition repeatCondition,
            GremlinVariableType variableType)
            : base(variableType)
        {
            this.InputVariable = new GremlinContextVariable(inputVariable);
            this.RepeatContext = repeatContext;
            this.RepeatCondition = repeatCondition;
            this.Count = 0;
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

        internal override List<GremlinTableVariable> FetchAllTableVars()
        {
            List<GremlinTableVariable> variableList = new List<GremlinTableVariable> { this };
            variableList.AddRange(this.RepeatContext.FetchAllTableVars());
            if (this.RepeatCondition.EmitContext != null)
                variableList.AddRange(this.RepeatCondition.EmitContext.FetchAllTableVars());
            if (this.RepeatCondition.TerminationContext != null)
                variableList.AddRange(this.RepeatCondition.TerminationContext.FetchAllTableVars());
            return variableList;
        }

        // Repeat algorithm
        // Firstly, we generate the repeatQueryBlock in order that we can get the initial query
        // Secondly, we generate the inputVariableVistorMap via repeatInputVariable.ProjectedProperties, 
        // untilInputVariable.ProjectedProperties, emitInputVariable.ProjectedProperties and ProjectedProperties. 
        // Generally, these properties are related to the input every time, but in repeatQueryBlock, these are 
        // just related to the input of the first time. Therefore, we need to replace these after.
        // Thirdly, we need to generate the firstQueryExpr and the selectColumnExpr of repeatQueryBlock. Pay 
        // attention, we need to repeatQueryBlock again because we need more properties about the output of 
        // the last step in repeat-step. These properties are populated in the second step.
        // Fourthly, we use the inputVariableVistorMap to replace the columns in the repeatQueryBlock. But we
        // should not change the columns in path-step. Because if we generate the path in the repeat-step, the 
        // path consists of 
        //  1. the previous steps before the repeat-step
        //  2. the local path(_path) in the repeat-step
        // Keep in mind that the initial _path is null, the _path includes all steps as long as they are in
        // the repeat-step except for the first input. And the _path after the first pass includes the last step
        // in the repeat-step. So the path must include the two part. That means all columns in path-step should 
        // not be replaced. Here, we use the ModifyRepeatInputVariablesVisitor to finish this work. If it visits
        // WPathTableReference, it does nothing, otherwise, it will replace the columns according to the 
        // inputVariableVistorMap.
        public override WTableReference ToTableReference()
        {
            //The following two variables are used for manually creating SelectScalarExpression of repeat
            List<WSelectScalarExpression> firstSelectList = new List<WSelectScalarExpression>();
            List<WSelectScalarExpression> secondSelectList = new List<WSelectScalarExpression>();

            // The following map is used to replace columns of the first input to columns of the repeat input
            // such as N_0.id -> R.key_0 
            Dictionary<Tuple<string, string>, Tuple<string, string>> inputVariableVistorMap = new Dictionary<Tuple<string, string>, Tuple<string, string>>();
            
            // We should generate the syntax tree firstly
            // Some variables will populate ProjectProperty only when we call the ToTableReference function where they appear.
            WRepeatConditionExpression repeatConditionExpr = this.GetRepeatConditionExpression();
            WSelectQueryBlock repeatQueryBlock = this.RepeatContext.ToSelectQueryBlock();

            GremlinVariable repeatInputVariable = this.RepeatContext.VariableList.First();
            GremlinVariable realInputVariable = InputVariable.RealVariable;
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
                WScalarExpression firstExpr = realInputVariable.ProjectedProperties.Contains(property)
                    ? realInputVariable.GetVariableProperty(property).ToScalarExpression()
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

            //Then we will use the inputVariableVistorMap to replace ColumnRefernceExpression in the syntax tree 
            //which matchs n_0.id to R_0.key_0 except for WPathTableReference
            new ModifyRepeatInputVariablesVisitor().Invoke(repeatQueryBlock, inputVariableVistorMap);
            new ModifyRepeatInputVariablesVisitor().Invoke(repeatConditionExpr, inputVariableVistorMap);

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

        public string GenerateKey()
        {
            return GremlinKeyword.RepeatColumnPrefix + this.Count++;
        }
    }
    
    internal class ModifyRepeatInputVariablesVisitor : WSqlFragmentVisitor
    {
        private Dictionary<Tuple<string, string>, Tuple<string, string>> map;

        public void Invoke(WSqlFragment queryBlock, Dictionary<Tuple<string, string>, Tuple<string, string>> map)
        {
            this.map = map;
            queryBlock.Accept(this);
        }

        public override void Visit(WSchemaObjectFunctionTableReference tableRef)
        {
            if (tableRef is WPathTableReference)
            {
                return;
            }
            else
            {
                tableRef.AcceptChildren(this);
            }
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
}
