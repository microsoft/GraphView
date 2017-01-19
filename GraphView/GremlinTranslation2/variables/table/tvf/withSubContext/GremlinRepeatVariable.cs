using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinRepeatVariable : GremlinSqlTableVariable
    {
        public static GremlinTableVariable Create(GremlinVariable inputVariable,
                                                  GremlinToSqlContext repeatContext,
                                                  RepeatCondition repeatCondition)
        {
            var contextVariable = GremlinContextVariable.Create(inputVariable);
            if (repeatContext.PivotVariable.GetVariableType() == inputVariable.GetVariableType())
            {
                switch (repeatContext.PivotVariable.GetVariableType())
                {
                    case GremlinVariableType.Vertex:
                        return new GremlinRepeatVertexVariable(contextVariable, repeatContext, repeatCondition);
                    case GremlinVariableType.Edge:
                        return new GremlinRepeatEdgeVariable(contextVariable, repeatContext, repeatCondition);
                    case GremlinVariableType.Scalar:
                        return new GremlinRepeatScalarVariable(contextVariable, repeatContext, repeatCondition);
                    case GremlinVariableType.Table:
                        return new GremlinRepeatTableVariable(contextVariable, repeatContext, repeatCondition);
                }
            }
            return new GremlinRepeatTableVariable(contextVariable, repeatContext, repeatCondition);

        }

        public GremlinContextVariable InputVariable { get; set; }
        public GremlinToSqlContext RepeatContext { get; set; }
        public RepeatCondition RepeatCondition { get; set; }
        public List<Tuple<string, GremlinRepeatSelectedVariable>> SelectedVariableList { get; set; }

        public GremlinRepeatVariable(GremlinContextVariable inputVariable, GremlinToSqlContext repeatContext,
                                    RepeatCondition repeatCondition)
        {
            RepeatContext = repeatContext;
            InputVariable = inputVariable;
            RepeatCondition = repeatCondition;
            SelectedVariableList = new List<Tuple<string, GremlinRepeatSelectedVariable>>();
        }

        internal override void Populate(string property)
        {
            RepeatContext.Populate(property);
        }

        internal override void PopulateGremlinPath()
        {
            RepeatContext.PopulateGremlinPath();
        }

        internal override bool ContainsLabel(string label)
        {
            foreach (var variable in RepeatContext.VariableList)
            {
                if (variable.ContainsLabel(label))
                {
                    return true;
                }
            }
            return false;
        }

        internal override List<GremlinVariable> PopulateAllTaggedVariable(string label, GremlinVariable parentVariable)
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>();
            foreach (var variable in RepeatContext.SelectCurrentAndChildVariable(label))
            {
                var repeatSelectedVariable = new GremlinRepeatSelectedVariable(parentVariable, variable, label);
                variableList.Add(repeatSelectedVariable);
                if (SelectedVariableList.All(p => p.Item1 != label))
                {
                    SelectedVariableList.Add(new Tuple<string, GremlinRepeatSelectedVariable>(label, repeatSelectedVariable));
                }
            }
            return variableList;
        }

        public override WTableReference ToTableReference(List<string> projectProperties, string tableName, GremlinVariable gremlinVariable)
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            var useProperties = new List<string>();
            InputVariable = RepeatContext.VariableList.First() as GremlinContextVariable;
            foreach (var property in InputVariable.UsedProperties)
            {
                Populate(property);
                useProperties.Add(property);
            }

            //Set the select Elements
            Dictionary<GremlinVariableProperty, string> map = new Dictionary<GremlinVariableProperty, string>();

            List<WSelectScalarExpression> inputSelectList = GetInputSelectList(useProperties, ref map);
            List<WSelectScalarExpression> outerSelectList = GetOuterSelectList(ref map);

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

            WSelectQueryBlock firstQueryExpr = new WSelectQueryBlock();
            foreach (var item in map)
            {
                firstQueryExpr.SelectElements.Add(SqlUtil.GetSelectScalarExpr(item.Key.ToScalarExpression(), item.Value));
            }

            foreach (var property in projectProperties)
            {
                firstQueryExpr.SelectElements.Add(SqlUtil.GetSelectScalarExpr(InputVariable.GetVariableProperty(property).ToScalarExpression(), property));
                selectQueryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(RepeatContext.PivotVariable.GetVariableProperty(property).ToScalarExpression(), property));
            }

            if (SelectedVariableList.Count != 0)
            {
                foreach (var selectedVariableTuple in SelectedVariableList)
                {
                    var columnName = selectedVariableTuple.Item1;
                    var selectedVariable = selectedVariableTuple.Item2;
                    firstQueryExpr.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetValueExpr(null), columnName));

                    List<WScalarExpression> compose1Paramters = new List<WScalarExpression>();
                    foreach (var property in selectedVariable.ProjectedProperties)
                    {
                        compose1Paramters.Add(selectedVariable.RealVariable.GetVariableProperty(property).ToScalarExpression());
                    }
                    WFunctionCall compose1 = SqlUtil.GetFunctionCall(GremlinKeyword.func.Compose1, compose1Paramters);

                    List<WScalarExpression> compose2Paramters = new List<WScalarExpression>();
                    compose2Paramters.Add(compose1);
                    compose2Paramters.Add(SqlUtil.GetColumnReferenceExpr("R", columnName));
                    WFunctionCall compose2 = SqlUtil.GetFunctionCall(GremlinKeyword.func.Compose2, compose2Paramters);
                    selectQueryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(compose2, columnName));
                }
            }

            var WBinaryQueryExpression = SqlUtil.GetBinaryQueryExpr(firstQueryExpr, selectQueryBlock);

            PropertyKeys.Add(SqlUtil.GetScalarSubquery(WBinaryQueryExpression));
            PropertyKeys.Add(GetRepeatConditionExpression());
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Repeat, PropertyKeys, gremlinVariable, tableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        public WRepeatConditionExpression GetRepeatConditionExpression()
        {
            return new WRepeatConditionExpression()
            {
                EmitCondition = RepeatCondition.EmitCondition,
                TerminationCondition = RepeatCondition.TerminationCondition,
                RepeatTimes = RepeatCondition.RepeatTimes,
                StartFromContext = RepeatCondition.StartFromContext,
                EmitContext = RepeatCondition.EmitContext
            };
        }

        public List<WSelectScalarExpression> GetInputSelectList(List<string> projectProperties, ref Dictionary<GremlinVariableProperty, string> map)
        {
            List<WSelectScalarExpression> inputSelectList = new List<WSelectScalarExpression>();
            foreach (var projectProperty in projectProperties)
            {
                var aliasName = GenerateKey();
                inputSelectList.Add(SqlUtil.GetSelectScalarExpr(RepeatContext.PivotVariable.GetVariableProperty(projectProperty).ToScalarExpression(), aliasName));
                map[InputVariable.GetVariableProperty(projectProperty)] = aliasName;
            }

            return inputSelectList;
        }

        public List<WSelectScalarExpression> GetOuterSelectList(ref Dictionary<GremlinVariableProperty, string> map)
        {
            List<WSelectScalarExpression> outerSelectList = new List<WSelectScalarExpression>();
            var allVariablesInRepeatContext = RepeatContext.FetchAllVariablesInCurrAndChildContext();
            foreach (var variable in allVariablesInRepeatContext)
            {
                if (variable is GremlinSelectedVariable)
                {
                    var repeatInnerVar = (variable as GremlinSelectedVariable);
                    if (repeatInnerVar.IsFromSelect && !allVariablesInRepeatContext.Contains(repeatInnerVar.RealVariable))
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
                            foreach (var property in repeatInnerVar.UsedProperties)
                            {
                                outerVar.Populate(property);
                                var aliasName = GenerateKey();
                                outerSelectList.Add(SqlUtil.GetSelectScalarExpr(outerVar.GetVariableProperty(property).ToScalarExpression(), aliasName));
                                map[repeatInnerVar.GetVariableProperty(property)] = aliasName;
                            }
                        }
                    }
                }
            }
            return outerSelectList;
        }

        public string GenerateKey()
        {
            return "key_" + count++;
        }

        private int count;
    }

    internal class GremlinRepeatVertexVariable : GremlinVertexTableVariable
    {
        public GremlinRepeatVertexVariable(GremlinContextVariable inputVariable,
                                           GremlinToSqlContext repeatContext,
                                           RepeatCondition repeatCondition)
        {
            SqlTableVariable = new GremlinRepeatVariable(inputVariable, repeatContext, repeatCondition);
        }
    }

    internal class GremlinRepeatEdgeVariable : GremlinEdgeTableVariable
    {
        public GremlinRepeatEdgeVariable(GremlinContextVariable inputVariable,
                                           GremlinToSqlContext repeatContext,
                                           RepeatCondition repeatCondition)
        {
            SqlTableVariable = new GremlinRepeatVariable(inputVariable, repeatContext, repeatCondition);
        }
    }

    internal class GremlinRepeatScalarVariable : GremlinScalarTableVariable
    {
        public GremlinRepeatScalarVariable(GremlinContextVariable inputVariable,
                                           GremlinToSqlContext repeatContext,
                                           RepeatCondition repeatCondition)
        {
            SqlTableVariable = new GremlinRepeatVariable(inputVariable, repeatContext, repeatCondition);
        }
    }

    internal class GremlinRepeatTableVariable : GremlinTableVariable
    {
        public GremlinRepeatTableVariable(GremlinContextVariable inputVariable,
                                           GremlinToSqlContext repeatContext,
                                           RepeatCondition repeatCondition)
        {
            SqlTableVariable = new GremlinRepeatVariable(inputVariable, repeatContext, repeatCondition);
        }
    }

    internal class ModifyColumnNameVisitor : WSqlFragmentVisitor
    {
        private Dictionary<Tuple<string, string>, Tuple<string, string>> _map;

        public void Invoke(WSqlFragment queryBlock, Dictionary<Tuple<string, string>, Tuple<string, string>> map)
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
                if (item.Key.Item1.Equals(key) && item.Key.Item2.Equals(value))
                {
                    columnReference.MultiPartIdentifier.Identifiers[0].Value = item.Value.Item1;
                    columnReference.MultiPartIdentifier.Identifiers[1].Value = item.Value.Item2;
                }
            }
        }
    }
}
