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
        public RepeatCondition RepeatCondition { get; set; }

        public GremlinRepeatVariable(GremlinVariable2 inputVariable, GremlinToSqlContext repeatContext,
                                    RepeatCondition repeatCondition)
        {
            VariableName = GenerateTableAlias();
            RepeatContext = repeatContext;
            InputVariable = inputVariable;
            RepeatCondition = repeatCondition;
        }

        internal override GremlinVariableType GetVariableType()
        {
            return RepeatContext.PivotVariable.GetVariableType();
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

        public WRepeatConditionExpression GetRepeatConditionExpression()
        {
            return new WRepeatConditionExpression()
            {
                //ConditionBooleanExpression = RepeatCondition.ConditionBooleanExpression,
                //IsEmitTrue = RepeatCondition.IsEmitTrue,
                //IsEmitAfter = RepeatCondition.IsEmitAfter,
                //IsEmitBefore = RepeatCondition.IsEmitBefore,
                //IsUntilAfter = RepeatCondition.IsUntilAfter,
                //IsUntilBefore = RepeatCondition.IsUntilBefore,
                //IsTimes = RepeatCondition.IsTimes,
                //Times = RepeatCondition.Times
            };
        }

        public List<WSelectScalarExpression> GetInputSelectList()
        {
            List<WSelectScalarExpression> inputSelectList = new List<WSelectScalarExpression>();
            
            foreach (var projectProperty in ProjectedProperties)
            {
                var projectValue = GremlinUtil.GetColumnReferenceExpr(RepeatContext.PivotVariable.VariableName,
                    projectProperty);
                var alias = InputVariable.VariableName + "." + projectProperty;
                inputSelectList.Add(GremlinUtil.GetSelectScalarExpression(projectValue, alias));
            }

            return inputSelectList;
        }

        public List<WSelectScalarExpression> GetOuterSelectList()
        {
            List<WSelectScalarExpression> outerSelectList = new List<WSelectScalarExpression>();
            foreach (var variable in RepeatContext.VariableList)
            {
                if (variable is GremlinContextVariable)
                {
                    var temp = (variable as GremlinContextVariable);
                    if (temp.IsFromSelect)
                    {
                        var selectVar = RepeatContext.SelectVariable(temp.SelectKey, temp.Pop);
                        if (selectVar != temp.ContextVariable)
                        {
                            foreach (var property in temp.ContextVariable.UsedProperties)
                            {
                                selectVar.Populate(property);
                                var alias = temp.ContextVariable.VariableName + "." + property;
                                var projectValue = GremlinUtil.GetColumnReferenceExpr(selectVar.VariableName,
                                    property);
                                outerSelectList.Add(GremlinUtil.GetSelectScalarExpression(projectValue, alias));
                            }

                        }
                    }
                }
            }
            return outerSelectList;
        }
    }
}
