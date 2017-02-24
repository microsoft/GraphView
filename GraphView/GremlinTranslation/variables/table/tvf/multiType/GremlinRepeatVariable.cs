using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinRepeatVariable : GremlinTableVariable
    {
        public static GremlinRepeatVariable Create(GremlinVariable inputVariable, GremlinToSqlContext repeatContext,
                                                  RepeatCondition repeatCondition)
        {
            if (repeatContext.PivotVariable.GetVariableType() == inputVariable.GetVariableType())
            {
                switch (inputVariable.GetVariableType())
                {
                    case GremlinVariableType.Vertex:
                        return new GremlinRepeatVertexVariable(inputVariable, repeatContext, repeatCondition);
                    case GremlinVariableType.Edge:
                        return new GremlinRepeatEdgeVariable(inputVariable, repeatContext, repeatCondition);
                    case GremlinVariableType.Scalar:
                        return new GremlinRepeatScalarVariable(inputVariable, repeatContext, repeatCondition);
                    case GremlinVariableType.Property:
                        return new GremlinRepeatPropertyVariable(inputVariable, repeatContext, repeatCondition);
                }
            }
            return new GremlinRepeatTableVariable(inputVariable, repeatContext, repeatCondition);
        }

        public GremlinVariable InputVariable { get; set; }
        public GremlinVariable FirstVariable { get; set; }
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
            FirstVariable = repeatContext.VariableList.First();
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

        internal override void PopulateGremlinPath()
        {
            RepeatContext.PopulateGremlinPath();
            RepeatContext.CurrentContextPath.IsInRepeatContext = true;
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

        internal override GremlinVariableProperty GetPath()
        {
            return new GremlinVariableProperty(this, GremlinKeyword.Path);
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

        public override WTableReference ToTableReference()
        {
            Dictionary<GremlinVariableProperty, string> map = new Dictionary<GremlinVariableProperty, string>();
            Dictionary<GremlinVariableProperty, string> map2 = new Dictionary<GremlinVariableProperty, string>();
            Dictionary<GremlinVariableProperty, string> map3 = new Dictionary<GremlinVariableProperty, string>();
            Dictionary<GremlinVariableProperty, string> map4 = new Dictionary<GremlinVariableProperty, string>();

            WRepeatConditionExpression conditionExpr = GetRepeatConditionExpression();

            List<WSelectScalarExpression> inputSelectList = GetInputSelectList(ref map);
            List<WSelectScalarExpression> outerSelectList = GetOuterSelectList(ref map);
            List<WSelectScalarExpression> terminateSelectList = GetConditionSelectList(ref map2);
            List<WSelectScalarExpression> repeatPathOuterList = GetRepeatPathOuterVariableList(ref map3);
            List<WSelectScalarExpression> conditionPathOuterList = GetConditionPathOuterVariableList(ref map4);

            WSelectQueryBlock selectQueryBlock = RepeatContext.ToSelectQueryBlock();
            selectQueryBlock.SelectElements.Clear();

            foreach (var selectElement in inputSelectList)
            {
                selectQueryBlock.SelectElements.Add(selectElement);
            }
            foreach (var selectElement in outerSelectList)
            {
                selectQueryBlock.SelectElements.Add(selectElement);
            }
            foreach (var selectElement in terminateSelectList)
            {
                selectQueryBlock.SelectElements.Add(selectElement);
            }
            foreach (var selectElement in repeatPathOuterList)
            {
                selectQueryBlock.SelectElements.Add(selectElement);
            }
            foreach (var selectElement in conditionPathOuterList)
            {
                selectQueryBlock.SelectElements.Add(selectElement);
            }

            WSelectQueryBlock firstQueryExpr = new WSelectQueryBlock();
            foreach (var item in map)
            {
                firstQueryExpr.SelectElements.Add(SqlUtil.GetSelectScalarExpr(item.Key.ToScalarExpression(), item.Value));
            }
            foreach (var item in map2)
            {
                firstQueryExpr.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), item.Value));
            }
            foreach (var item in map3)
            {
                firstQueryExpr.SelectElements.Add(SqlUtil.GetSelectScalarExpr(item.Key.ToScalarExpression(), item.Value));
            }
            foreach (var item in map4)
            {
                firstQueryExpr.SelectElements.Add(SqlUtil.GetSelectScalarExpr(item.Key.ToScalarExpression(), item.Value));
            }

            //firstQueryExpr.SelectElements.Add(SqlUtil.GetSelectScalarExpr(FirstVariable.DefaultProjection().ToScalarExpression(), GremlinKeyword.TableDefaultColumnName));
            //selectQueryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(RepeatContext.PivotVariable.DefaultProjection().ToScalarExpression(), GremlinKeyword.TableDefaultColumnName));
            foreach (var property in ProjectedProperties)
            {
                if (InputVariable.ProjectedProperties.Contains(property))
                {
                    firstQueryExpr.SelectElements.Add(
                        SqlUtil.GetSelectScalarExpr(
                            InputVariable.GetVariableProperty(property).ToScalarExpression(), property));
                }
                else
                {
                    firstQueryExpr.SelectElements.Add(
                        SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), property));
                }
                if (RepeatContext.PivotVariable.ProjectedProperties.Contains(property))
                {
                    selectQueryBlock.SelectElements.Add(
                        SqlUtil.GetSelectScalarExpr(
                            RepeatContext.PivotVariable.GetVariableProperty(property).ToScalarExpression(), property));
                }
                else
                {
                    selectQueryBlock.SelectElements.Add(
                        SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), property));
                }
            }

            if (SelectedVariableList.Count != 0)
            {
                foreach (var selectedVariableTuple in SelectedVariableList)
                {
                    var columnName = selectedVariableTuple.Item1;
                    var selectedVariable = selectedVariableTuple.Item2;
                    firstQueryExpr.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), columnName));

                    List<WScalarExpression> compose2Paramters = new List<WScalarExpression>();
                    compose2Paramters.Add(selectedVariable.RealVariable.ToCompose1());
                    compose2Paramters.Add(SqlUtil.GetColumnReferenceExpr("R", columnName));
                    WFunctionCall compose2 = SqlUtil.GetFunctionCall(GremlinKeyword.func.Compose2, compose2Paramters);
                    selectQueryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(compose2, columnName));
                }
            }

            if (RepeatContext.IsPopulateGremlinPath)
            {
                var columnName = GremlinKeyword.Path;
                var pathVariable = RepeatContext.CurrentContextPath;
                firstQueryExpr.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), columnName));
                selectQueryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(pathVariable.DefaultProjection().ToScalarExpression(), columnName));
            }

            var WBinaryQueryExpression = SqlUtil.GetBinaryQueryExpr(firstQueryExpr, selectQueryBlock);


            ModifyColumnNameVisitor newVisitor = new ModifyColumnNameVisitor();
            newVisitor.Invoke(selectQueryBlock, map);
            newVisitor.Invoke(conditionExpr, map2);
            newVisitor.Invoke(selectQueryBlock, map3);
            newVisitor.Invoke(conditionExpr, map4);

            List<WScalarExpression> repeatParameters = new List<WScalarExpression>();
            repeatParameters.Add(SqlUtil.GetScalarSubquery(WBinaryQueryExpression));
            repeatParameters.Add(conditionExpr);
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Repeat, repeatParameters, this, GetVariableName());

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
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

        public List<WSelectScalarExpression> GetInputSelectList(ref Dictionary<GremlinVariableProperty, string> map)
        {
            List<WSelectScalarExpression> inputSelectList = new List<WSelectScalarExpression>();
            foreach (var projectProperty in FirstVariable.ProjectedProperties)
            {
                var aliasName = GenerateKey();
                inputSelectList.Add(SqlUtil.GetSelectScalarExpr(RepeatContext.PivotVariable.GetVariableProperty(projectProperty).ToScalarExpression(), aliasName));
                map[FirstVariable.GetVariableProperty(projectProperty)] = aliasName;
            }

            return inputSelectList;
        }

        public List<WSelectScalarExpression> GetOuterSelectList(ref Dictionary<GremlinVariableProperty, string> map)
        {
            List<WSelectScalarExpression> outerSelectList = new List<WSelectScalarExpression>();
            var allVariablesInRepeatContext = RepeatContext.FetchVarsFromCurrAndChildContext();
            foreach (var variable in allVariablesInRepeatContext)
            {
                if (variable is GremlinSelectedVariable)
                {
                    var repeatInnerVar = (variable as GremlinSelectedVariable);
                    if (repeatInnerVar.IsFromSelect &&
                        !allVariablesInRepeatContext.Contains(repeatInnerVar.RealVariable))
                    {
                        List<GremlinVariable> outerVarList = RepeatContext.Select(repeatInnerVar.SelectKey);
                        GremlinVariable outerVar = null;
                        switch (repeatInnerVar.Pop)
                        {
                            case GremlinKeyword.Pop.last:
                                outerVar = outerVarList.Last();
                                break;
                            case GremlinKeyword.Pop.first:
                                outerVar = outerVarList.First();
                                break;
                        }
                        if (repeatInnerVar != outerVar)
                        {
                            foreach (var property in repeatInnerVar.ProjectedProperties)
                            {
                                var aliasName = GenerateKey();
                                outerSelectList.Add(
                                    SqlUtil.GetSelectScalarExpr(
                                        outerVar.GetVariableProperty(property).ToScalarExpression(), aliasName));
                                map[repeatInnerVar.GetVariableProperty(property)] = aliasName;
                            }
                        }
                    }
                }
            }
            return outerSelectList;
        }

        public List<WSelectScalarExpression> GetConditionSelectList(ref Dictionary<GremlinVariableProperty, string> map)
        {
            List<WSelectScalarExpression> terminateSelectList = new List<WSelectScalarExpression>();
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            List<GremlinVariable> temp = RepeatCondition.TerminationContext?.FetchVarsFromCurrAndChildContext();
            if (temp != null)
            {
                variableList.AddRange(temp);
            }
            temp = RepeatCondition.EmitContext?.FetchVarsFromCurrAndChildContext();
            if (temp != null)
            {
                variableList.AddRange(temp);
            }
            List<Tuple<string, string>> slot = new List<Tuple<string, string>>();
            foreach (var variable in variableList)
            {
                if (variable is GremlinSelectedVariable)
                {
                    var repeatInnerVar = (variable as GremlinSelectedVariable);
                    foreach (var property in repeatInnerVar.ProjectedProperties)
                    {
                        if (slot.FindIndex(p => p.Item1 == variable.GetVariableName() && p.Item2 == property) == -1)
                        {
                            var aliasName = GenerateKey();
                            terminateSelectList.Add(SqlUtil.GetSelectScalarExpr((variable as GremlinSelectedVariable).RealVariable.GetVariableProperty(property).ToScalarExpression(), aliasName));
                            map[repeatInnerVar.GetVariableProperty(property)] = aliasName;
                            slot.Add(new Tuple<string, string>(variable.GetVariableName(), property));
                        }
                    }
                }
            }
            return terminateSelectList;
        }

        public List<WSelectScalarExpression> GetConditionPathOuterVariableList(ref Dictionary<GremlinVariableProperty, string> map)
        {
            List<WSelectScalarExpression> pathOuterVariableList = new List<WSelectScalarExpression>();
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            List<GremlinVariable> temp = RepeatCondition.TerminationContext?.FetchVarsFromCurrAndChildContext();
            if (temp != null)
            {
                variableList.AddRange(temp);
            }
            temp = RepeatCondition.EmitContext?.FetchVarsFromCurrAndChildContext();
            if (temp != null)
            {
                variableList.AddRange(temp);
            }
            foreach (var variable in variableList)
            {
                if (variable is GremlinPathVariable)
                {
                    var pathVariable = (variable as GremlinPathVariable);
                    foreach (var stepVariable in pathVariable.PathList)
                    {
                        if (!variableList.Contains(stepVariable.GremlinVariable))
                        {
                            var aliasName = GenerateKey();
                            pathOuterVariableList.Add(SqlUtil.GetSelectScalarExpr(stepVariable.ToScalarExpression(), aliasName));
                            map[stepVariable] = aliasName;
                        }
                    }
                }
            }
            return pathOuterVariableList;
        }

        public List<WSelectScalarExpression> GetRepeatPathOuterVariableList(ref Dictionary<GremlinVariableProperty, string> map)
        {
            List<WSelectScalarExpression> pathOuterVariableList = new List<WSelectScalarExpression>();
            var allVariablesInRepeatContext = RepeatContext.FetchVarsFromCurrAndChildContext();
            foreach (var variable in allVariablesInRepeatContext)
            {
                if (variable is GremlinPathVariable)
                {
                    var pathVariable = (variable as GremlinPathVariable);
                    foreach (var stepVariable in pathVariable.PathList)
                    {
                        if (!allVariablesInRepeatContext.Contains(stepVariable.GremlinVariable))
                        {
                            var aliasName = GenerateKey();
                            pathOuterVariableList.Add(SqlUtil.GetSelectScalarExpr(stepVariable.ToScalarExpression(), aliasName));
                            map[stepVariable] = aliasName;
                        }
                    }
                }
            }
            return pathOuterVariableList;
        }

        public string GenerateKey()
        {
            return "key_" + count++;
        }

        private int count;
    }

    internal class GremlinRepeatVertexVariable : GremlinRepeatVariable
    {
        public GremlinRepeatVertexVariable(GremlinVariable inputVariable,
                                           GremlinToSqlContext repeatContext,
                                           RepeatCondition repeatCondition)
            : base(inputVariable, repeatContext, repeatCondition, GremlinVariableType.Vertex)
        {
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.Both(this, edgeLabels);
        }

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.BothE(this, edgeLabels);
        }

        internal override void BothV(GremlinToSqlContext currentContext)
        {
            currentContext.BothV(this);
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.In(this, edgeLabels);
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.InE(this, edgeLabels);
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.Out(this, edgeLabels);
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.OutE(this, edgeLabels);
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            currentContext.DropVertex(this);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            currentContext.Has(this, propertyKey);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, GremlinToSqlContext propertyContext)
        {
            currentContext.Has(this, propertyKey, propertyContext);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, object value)
        {
            currentContext.Has(this, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, object value)
        {
            currentContext.Has(this, label, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, propertyKey, predicate);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, label, propertyKey, predicate);
        }

        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasId(this, values);
        }

        internal override void HasId(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasId(this, predicate);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasLabel(this, values);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasLabel(this, predicate);
        }

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Properties(this, propertyKeys);
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Values(this, propertyKeys);
        }
    }

    internal class GremlinRepeatEdgeVariable : GremlinRepeatVariable
    {
        public GremlinRepeatEdgeVariable(GremlinVariable inputVariable,
                                        GremlinToSqlContext repeatContext,
                                        RepeatCondition repeatCondition)
            : base(inputVariable, repeatContext, repeatCondition, GremlinVariableType.Edge)
        {
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            currentContext.InV(this);
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            currentContext.OutV(this);
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            currentContext.OtherV(this);
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            currentContext.DropEdge(this);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            currentContext.Has(this, propertyKey);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, GremlinToSqlContext propertyContext)
        {
            currentContext.Has(this, propertyKey, propertyContext);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, object value)
        {
            currentContext.Has(this, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, object value)
        {
            currentContext.Has(this, label, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, propertyKey, predicate);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, label, propertyKey, predicate);
        }

        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasId(this, values);
        }

        internal override void HasId(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasId(this, predicate);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasLabel(this, values);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasLabel(this, predicate);
        }

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Properties(this, propertyKeys);
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Values(this, propertyKeys);
        }
    }

    internal class GremlinRepeatScalarVariable : GremlinRepeatVariable
    {
        public GremlinRepeatScalarVariable(GremlinVariable inputVariable,
                                           GremlinToSqlContext repeatContext,
                                           RepeatCondition repeatCondition)
            : base(inputVariable, repeatContext, repeatCondition, GremlinVariableType.Scalar)
        {
        }
    }

    internal class GremlinRepeatPropertyVariable : GremlinRepeatVariable
    {
        public GremlinRepeatPropertyVariable(GremlinVariable inputVariable,
                                            GremlinToSqlContext repeatContext,
                                            RepeatCondition repeatCondition)
            : base(inputVariable, repeatContext, repeatCondition, GremlinVariableType.Property)
        {
        }

        internal override void Key(GremlinToSqlContext currentContext)
        {
            currentContext.Key(this);
        }

        internal override void Value(GremlinToSqlContext currentContext)
        {
            currentContext.Value(this);
        }
    }

    internal class GremlinRepeatTableVariable : GremlinRepeatVariable
    {
        public GremlinRepeatTableVariable(GremlinVariable inputVariable,
                                           GremlinToSqlContext repeatContext,
                                           RepeatCondition repeatCondition)
            : base(inputVariable, repeatContext, repeatCondition, GremlinVariableType.Table)
        {
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            currentContext.InV(this);
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            currentContext.OutV(this);
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            currentContext.OtherV(this);
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.Both(this, edgeLabels);
        }

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.BothE(this, edgeLabels);
        }

        internal override void BothV(GremlinToSqlContext currentContext)
        {
            currentContext.BothV(this);
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.In(this, edgeLabels);
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.InE(this, edgeLabels);
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.Out(this, edgeLabels);
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            currentContext.OutE(this, edgeLabels);
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            currentContext.DropVertex(this);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            currentContext.Has(this, propertyKey);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, GremlinToSqlContext propertyContext)
        {
            currentContext.Has(this, propertyKey, propertyContext);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, object value)
        {
            currentContext.Has(this, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, object value)
        {
            currentContext.Has(this, label, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, propertyKey, predicate);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, label, propertyKey, predicate);
        }

        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasId(this, values);
        }

        internal override void HasId(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasId(this, predicate);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasLabel(this, values);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasLabel(this, predicate);
        }

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Properties(this, propertyKeys);
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Values(this, propertyKeys);
        }
    }

    internal class ModifyColumnNameVisitor : WSqlFragmentVisitor
    {
        private Dictionary<GremlinVariableProperty, string> _map;

        public void Invoke(WSqlFragment queryBlock, Dictionary<GremlinVariableProperty, string> map)
        {
            _map = map;
            queryBlock.Accept(this);
        }

        public override void Visit(WColumnReferenceExpression columnReference)
        {
            var key = columnReference.MultiPartIdentifier.Identifiers[0].Value;
            var value = columnReference.MultiPartIdentifier.Identifiers[1].Value;
            foreach (var item in _map)
            {
                if (item.Key.GremlinVariable.GetVariableName().Equals(key) && item.Key.VariableProperty.Equals(value))
                {
                    columnReference.MultiPartIdentifier.Identifiers[0].Value = "R";
                    columnReference.MultiPartIdentifier.Identifiers[1].Value = item.Value;
                }
            }
        }
    }
}
