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
        private int Count { get; set; }

        public GremlinRepeatVariable(
            GremlinVariable inputVariable, 
            GremlinToSqlContext repeatContext, 
            RepeatCondition repeatCondition,
            GremlinVariableType variableType) : base(variableType)
        {
            this.InputVariable = new GremlinContextVariable(inputVariable);
            this.RepeatContext = repeatContext;
            this.RepeatCondition = repeatCondition;
            this.Count = 0;
            GremlinRepeatContextVariable repeatContextVariable = this.RepeatContext.VariableList.First() as GremlinRepeatContextVariable;
            GremlinUntilContextVariable untilContextVariable = null;
            if (this.RepeatCondition.TerminationContext?.VariableList.Count > 0)
            {
                untilContextVariable = this.RepeatCondition.TerminationContext.VariableList.First() as GremlinUntilContextVariable;
            }
            GremlinEmitContextVariable emitContextVariable = null;
            if (this.RepeatCondition.EmitContext?.VariableList.Count > 0)
            {
                emitContextVariable = this.RepeatCondition.EmitContext?.VariableList?.First() as GremlinEmitContextVariable;
            }
            if (repeatContextVariable?.LabelPropertyList.Count > 0 ||
                untilContextVariable?.LabelPropertyList.Count > 0 || emitContextVariable?.LabelPropertyList.Count > 0)
            {
                this.PopulateLocalPath();
                repeatContextVariable?.SetContextLocalPath(this.RepeatContext.ContextLocalPath);
                untilContextVariable?.SetContextLocalPath(this.RepeatContext.ContextLocalPath);
                emitContextVariable?.SetContextLocalPath(this.RepeatContext.ContextLocalPath);
            }
        }

        internal override bool Populate(string property, string label = null)
        {
            bool populateSuccessfully = false;
            if (label == null || this.Labels.Contains(label))
            {
                populateSuccessfully = true;
                this.InputVariable.Populate(property, null);
                this.RepeatContext.Populate(property, null);
            }
            else
            {
                populateSuccessfully |= this.InputVariable.Populate(property, label);
                populateSuccessfully |= this.RepeatContext.Populate(property, label);
            }
            if (populateSuccessfully && property != null)
            {
                this.ProjectedProperties.Add(property);
            }
            return populateSuccessfully;
        }

        internal override bool PopulateStepProperty(string property, string label = null)
        {
            return this.RepeatContext.ContextLocalPath.PopulateStepProperty(property, label);
        }

        internal override void PopulateLocalPath()
        {
            if (this.ProjectedProperties.Contains(GremlinKeyword.Path))
            {
                return;
            }
            this.ProjectedProperties.Add(GremlinKeyword.Path);
            this.RepeatContext.PopulateLocalPath();

            if (this.RepeatCondition.EmitContext != null && this.RepeatCondition.EmitContext.VariableList.Count > 0)
            {
                GremlinVariable repeatInputVariable = this.RepeatContext.VariableList.First();
                GremlinVariable emitInputVariable = this.RepeatCondition.EmitContext.VariableList.First();

                if (repeatInputVariable == emitInputVariable)
                {
                    this.LocalPathLengthLowerBound = 0;
                }
                return;
            }
            this.LocalPathLengthLowerBound = this.RepeatContext.MinPathLength;
        }

        internal override WScalarExpression ToStepScalarExpr(HashSet<string> composedProperties = null)
        {
            return SqlUtil.GetColumnReferenceExpr(this.GetVariableName(), GremlinKeyword.Path);
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.InputVariable.FetchAllVars());
            variableList.AddRange(this.RepeatContext.FetchAllVars());
            if (this.RepeatCondition.EmitContext != null)
            {
                variableList.AddRange(this.RepeatCondition.EmitContext.FetchAllVars());
            }
                
            if (this.RepeatCondition.TerminationContext != null)
            {
                variableList.AddRange(this.RepeatCondition.TerminationContext.FetchAllVars());
            }
                
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
            WScalarExpression firstSelectColumn, secondSelectColumn, firstExpr, secondExpr;

            // The following map is used to replace columns of the first input to columns of the repeat input
            // such as N_0.id -> R.key_0 
            string aliasName;
            Tuple<string, string> key, value;
            Dictionary<Tuple<string, string>, Tuple<string, string>> inputVariableVistorMap = 
                new Dictionary<Tuple<string, string>, Tuple<string, string>>();

            // We should generate the syntax tree firstly
            // Some variables will populate ProjectProperty only when we call the ToTableReference function where they appear.
            WRepeatConditionExpression repeatConditionExpr = this.GetRepeatConditionExpression();
            WSelectQueryBlock repeatQueryBlock = this.RepeatContext.ToSelectQueryBlock();

            GremlinVariable repeatInputVariable = this.RepeatContext.VariableList.First();
            GremlinVariable repeatOutputVariable = this.RepeatContext.PivotVariable;
            GremlinVariable realInputVariable = this.InputVariable.RealVariable;
            GremlinVariable repeatPivotVariable = this.RepeatContext.PivotVariable;
            
            // this.DefaultProperty
            key = new Tuple<string, string>(this.GetVariableName(), this.DefaultProperty());
            value = key;
            inputVariableVistorMap[key] = value;

            key = new Tuple<string, string>(realInputVariable.GetVariableName(), realInputVariable.DefaultProperty());
            value = new Tuple<string, string>(GremlinKeyword.RepeatInitalTableName, this.DefaultProperty());
            inputVariableVistorMap[key] = value;

            firstExpr = realInputVariable.DefaultProjection().ToScalarExpression();
            secondExpr = repeatOutputVariable.DefaultProjection().ToScalarExpression();
            firstSelectList.Add(SqlUtil.GetSelectScalarExpr(firstExpr, this.DefaultProperty()));
            secondSelectList.Add(SqlUtil.GetSelectScalarExpr(secondExpr, this.DefaultProperty()));

            foreach (string property in this.ProjectedProperties)
            {
                if (property == GremlinKeyword.Path)
                {
                    firstSelectList.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), GremlinKeyword.Path));
                    secondSelectList.Add(
                        SqlUtil.GetSelectScalarExpr(
                            this.RepeatContext.ContextLocalPath.DefaultProjection().ToScalarExpression(), 
                            GremlinKeyword.Path));
                }
                else
                {
                    key = new Tuple<string, string>(this.GetVariableName(), property);
                    value = key;
                    inputVariableVistorMap[key] = value;

                    key = new Tuple<string, string>(realInputVariable.GetVariableName(), property);
                    value = new Tuple<string, string>(GremlinKeyword.RepeatInitalTableName, property);
                    inputVariableVistorMap[key] = value;

                    firstExpr = realInputVariable.ProjectedProperties.Contains(property)
                        ? realInputVariable.GetVariableProperty(property).ToScalarExpression()
                        : SqlUtil.GetValueExpr(null);
                    secondExpr = repeatOutputVariable.ProjectedProperties.Contains(property)
                        ? this.RepeatContext.PivotVariable.GetVariableProperty(property).ToScalarExpression()
                        : SqlUtil.GetValueExpr(null);

                    firstSelectList.Add(SqlUtil.GetSelectScalarExpr(firstExpr, property));
                    secondSelectList.Add(SqlUtil.GetSelectScalarExpr(secondExpr, property));
                }
            }

            // repeatInputVariable.DefaultProperty()
            key = new Tuple<string, string>(repeatInputVariable.GetVariableName(), repeatInputVariable.DefaultProperty());
            if (!inputVariableVistorMap.Keys.Contains(key))
            {
                aliasName = this.GenerateKey();
                value = new Tuple<string, string>(GremlinKeyword.RepeatInitalTableName, aliasName);
                inputVariableVistorMap[key] = value;

                firstSelectColumn = repeatInputVariable.DefaultProjection().ToScalarExpression() as WColumnReferenceExpression;
                secondSelectColumn = repeatPivotVariable.DefaultProjection().ToScalarExpression() as WColumnReferenceExpression;
                firstSelectList.Add(SqlUtil.GetSelectScalarExpr(firstSelectColumn, aliasName));
                secondSelectList.Add(SqlUtil.GetSelectScalarExpr(secondSelectColumn, aliasName));
            }
            foreach (string property in repeatInputVariable.ProjectedProperties)
            {
                key = new Tuple<string, string>(repeatInputVariable.GetVariableName(), property);
                if (!inputVariableVistorMap.Keys.Contains(key))
                {
                    aliasName = this.GenerateKey();
                    value = new Tuple<string, string>(GremlinKeyword.RepeatInitalTableName, aliasName);
                    inputVariableVistorMap[key] = value;

                    firstSelectColumn = repeatInputVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;
                    secondSelectColumn = repeatPivotVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;
                    firstSelectList.Add(SqlUtil.GetSelectScalarExpr(firstSelectColumn, aliasName));
                    secondSelectList.Add(SqlUtil.GetSelectScalarExpr(secondSelectColumn, aliasName));
                }
            }

            if (this.RepeatCondition.TerminationContext != null && this.RepeatCondition.TerminationContext.VariableList.Count > 0)
            {
                GremlinVariable untilInputVariable = this.RepeatCondition.TerminationContext.VariableList.First();

                // untilInputVariable.DefaultProperty()
                key = new Tuple<string, string>(untilInputVariable.GetVariableName(), untilInputVariable.DefaultProperty());
                if (!inputVariableVistorMap.Keys.Contains(key))
                {
                    aliasName = this.GenerateKey();
                    value = new Tuple<string, string>(GremlinKeyword.RepeatInitalTableName, aliasName);
                    inputVariableVistorMap[key] = value;

                    firstSelectColumn =
                        untilInputVariable.DefaultProjection().ToScalarExpression() as WColumnReferenceExpression;
                    secondSelectColumn =
                        repeatPivotVariable.DefaultProjection().ToScalarExpression() as WColumnReferenceExpression;
                    firstSelectList.Add(SqlUtil.GetSelectScalarExpr(firstSelectColumn, aliasName));
                    secondSelectList.Add(SqlUtil.GetSelectScalarExpr(secondSelectColumn, aliasName));
                }
                
                foreach (string property in untilInputVariable.ProjectedProperties)
                {
                    key = new Tuple<string, string>(untilInputVariable.GetVariableName(), property);
                    if (!inputVariableVistorMap.Keys.Contains(key))
                    {
                        aliasName = this.GenerateKey();
                        value = new Tuple<string, string>(GremlinKeyword.RepeatInitalTableName, aliasName);
                        inputVariableVistorMap[key] = value;

                        firstSelectColumn = untilInputVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;
                        secondSelectColumn = repeatPivotVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;
                        firstSelectList.Add(SqlUtil.GetSelectScalarExpr(firstSelectColumn, aliasName));
                        secondSelectList.Add(SqlUtil.GetSelectScalarExpr(secondSelectColumn, aliasName));
                    }                    
                }
            }

            if (this.RepeatCondition.EmitContext != null && this.RepeatCondition.EmitContext.VariableList.Count > 0)
            {
                GremlinVariable emitInputVariable = this.RepeatCondition.EmitContext.VariableList.First();

                // emitInputVariable.DefaultProperty()
                key = new Tuple<string, string>(emitInputVariable.GetVariableName(), emitInputVariable.DefaultProperty());
                if (!inputVariableVistorMap.Keys.Contains(key))
                {
                    aliasName = this.GenerateKey();
                    value = new Tuple<string, string>(GremlinKeyword.RepeatInitalTableName, aliasName);
                    inputVariableVistorMap[key] = value;

                    firstSelectColumn = 
                        emitInputVariable.DefaultProjection().ToScalarExpression() as WColumnReferenceExpression;
                    secondSelectColumn = 
                        repeatPivotVariable.DefaultProjection().ToScalarExpression() as WColumnReferenceExpression;
                    firstSelectList.Add(SqlUtil.GetSelectScalarExpr(firstSelectColumn, aliasName));
                    secondSelectList.Add(SqlUtil.GetSelectScalarExpr(secondSelectColumn, aliasName));
                }

                foreach (string property in emitInputVariable.ProjectedProperties)
                {
                    key = new Tuple<string, string>(emitInputVariable.GetVariableName(), property);
                    if (!inputVariableVistorMap.Keys.Contains(key))
                    {
                        aliasName = this.GenerateKey();
                        value = new Tuple<string, string>(GremlinKeyword.RepeatInitalTableName, aliasName);
                        inputVariableVistorMap[key] = value;

                        firstSelectColumn =
                            emitInputVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;
                        secondSelectColumn =
                            repeatPivotVariable.GetVariableProperty(property).ToScalarExpression() as WColumnReferenceExpression;
                        firstSelectList.Add(SqlUtil.GetSelectScalarExpr(firstSelectColumn, aliasName));
                        secondSelectList.Add(SqlUtil.GetSelectScalarExpr(secondSelectColumn, aliasName));
                    }
                }
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

        private WRepeatConditionExpression GetRepeatConditionExpression()
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

        private string GenerateKey()
        {
            return GremlinKeyword.RepeatColumnPrefix + this.Count++;
        }
    }
    
    internal class ModifyRepeatInputVariablesVisitor : WSqlFragmentVisitor
    {
        private Dictionary<Tuple<string, string>, Tuple<string, string>> Map;

        public void Invoke(WSqlFragment queryBlock, Dictionary<Tuple<string, string>, Tuple<string, string>> map)
        {
            this.Map = map;
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
            foreach (KeyValuePair<Tuple<string, string>, Tuple<string, string>> item in this.Map)
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
