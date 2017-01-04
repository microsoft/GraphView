using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinRepeatVariable : GremlinTableVariable
    {
        public GremlinVariable2 InputVariable { get; set; }
        public GremlinToSqlContext RepeatContext { get; set; }
        public GremlinToSqlContext ConditionContext { get; set; }
        public RepeatCondition RepeatCondition { get; set; }

        public GremlinRepeatVariable(GremlinVariable2 inputVariable, GremlinToSqlContext repeatContext,
                                       GremlinToSqlContext conditionContext, RepeatCondition repeatCondition)
        {
            VariableName = GenerateTableAlias();
            RepeatContext = repeatContext;
            InputVariable = inputVariable;
            RepeatCondition = repeatCondition;
            ConditionContext = conditionContext;
        }

        internal override void Populate(string property)
        {
            RepeatContext.Populate(property);
            base.Populate(property);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();

            foreach (var property in InputVariable.UsedProperties)
            {
                Populate(property);
            }

            WSelectQueryBlock selectQueryBlock = RepeatContext.ToSelectQueryBlock();

            //Set the select Elements
            selectQueryBlock.SelectElements.Clear();
            List<WSelectScalarExpression> inputSelectList = GetInputSelectList();
            foreach (var selectElement in inputSelectList)
            {
                selectQueryBlock.SelectElements.Add(selectElement);
            }
            List<WSelectScalarExpression> outerSelectList = GetOuterSelectList();
            foreach (var selectElement in outerSelectList)
            {
                selectQueryBlock.SelectElements.Add(selectElement);
            }

            PropertyKeys.Add(GremlinUtil.GetScalarSubquery(selectQueryBlock));
            PropertyKeys.Add(GetRepeatConditionExpression());
            var secondTableRef = GremlinUtil.GetFunctionTableReference("repeat", PropertyKeys, VariableName);

            return GremlinUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        public void SetExtraVariableInSelect(GremlinVariable2 newVar, GremlinVariable2 aliasVar)
        {
            foreach (var property in aliasVar.UsedProperties)
            {
                newVar.Populate(property);
            }
        }

        public WRepeatConditionExpression GetRepeatConditionExpression()
        {
            return new WRepeatConditionExpression()
            {
                ConditionBooleanExpression = ConditionContext.ToSqlBoolean(),
                IsEmitTrue = RepeatCondition.IsEmitTrue,
                IsEmitAfter = RepeatCondition.IsEmitAfter,
                IsEmitBefore = RepeatCondition.IsEmitBefore,
                IsUntilAfter = RepeatCondition.IsUntilAfter,
                IsUntilBefore = RepeatCondition.IsUntilBefore,
                IsTimes = RepeatCondition.IsTimes,
                Times = RepeatCondition.Times
            };
        }

        public List<WSelectScalarExpression> GetInputSelectList()
        {
            List<WSelectScalarExpression> inputSelectList = new List<WSelectScalarExpression>();
            if (RepeatContext.PivotVariable.DefaultProjection() is GremlinVariableProperty)
            {
                var temp = RepeatContext.PivotVariable.DefaultProjection() as GremlinVariableProperty;
                inputSelectList.Add(new WSelectScalarExpression()
                {
                    ColumnName = InputVariable.VariableName + "." + temp.VariableProperty,
                    SelectExpr =
                        GremlinUtil.GetColumnReferenceExpression(RepeatContext.PivotVariable.VariableName, temp.VariableProperty)
                });
            }
            else
            {
                throw new NotImplementedException();
            }

            foreach (var projectProperty in projectedProperties)
            {
                inputSelectList.Add(new WSelectScalarExpression()
                {
                    ColumnName = InputVariable.VariableName + "." + projectProperty,
                    SelectExpr = GremlinUtil.GetColumnReferenceExpression(RepeatContext.PivotVariable.VariableName, projectProperty)
                });
            }

            return inputSelectList;
        }

        public List<WSelectScalarExpression> GetOuterSelectList()
        {
            List<WSelectScalarExpression> outerSelectList = new List<WSelectScalarExpression>();
            foreach (var variable in RepeatContext.VariableList)
            {
                if (variable is GremlinContextEdgeVariable)
                {
                    var temp = (variable as GremlinContextEdgeVariable);
                    if (temp.IsFromSelect)
                    {
                        var selectVar = RepeatContext.SelectVariable(temp.SelectKey, temp.Pop);
                        if (selectVar != temp.ContextVariable)
                        {
                            foreach (var property in temp.ContextVariable.UsedProperties)
                            {
                                selectVar.Populate(property);
                                outerSelectList.Add(new WSelectScalarExpression()
                                {
                                    ColumnName = temp.ContextVariable.VariableName + "." + property,
                                    SelectExpr = GremlinUtil.GetColumnReferenceExpression(selectVar.VariableName, property)
                                });
                            }

                        }
                    }
                }
                else if (variable is GremlinContextVertexVariable)
                {
                    var temp = (variable as GremlinContextVertexVariable);
                    if (temp.IsFromSelect)
                    {
                        var selectVar = RepeatContext.SelectVariable(temp.SelectKey, temp.Pop);
                        if (selectVar != temp.ContextVariable)
                        {
                            foreach (var property in temp.ContextVariable.UsedProperties)
                            {
                                selectVar.Populate(property);
                                outerSelectList.Add(new WSelectScalarExpression()
                                {
                                    ColumnName = temp.ContextVariable.VariableName + "." + property,
                                    SelectExpr = GremlinUtil.GetColumnReferenceExpression(selectVar.VariableName, property)
                                });
                            }
                        }
                    }
                }
            }
            return outerSelectList;
        }
    }
}
