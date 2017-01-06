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

        public GremlinRepeatVariable(GremlinVariable inputVariable, GremlinToSqlContext repeatContext,
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

            PropertyKeys.Add(SqlUtil.GetScalarSubquery(selectQueryBlock));
            PropertyKeys.Add(GetRepeatConditionExpression());
            var secondTableRef = SqlUtil.GetFunctionTableReference("repeat", PropertyKeys, VariableName);

            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        public WRepeatConditionExpression GetRepeatConditionExpression()
        {
            return new WRepeatConditionExpression()
            {
                EmitCondition = RepeatCondition.EmitCondition,
                TerminationCondition = RepeatCondition.TerminationCondition,
                RepeatTimes = RepeatCondition.RepeatTimes,
                StartFromContext = RepeatCondition.StartFromContext
            };
        }

        public List<WSelectScalarExpression> GetInputSelectList()
        {
            List<WSelectScalarExpression> inputSelectList = new List<WSelectScalarExpression>();
            
            foreach (var projectProperty in ProjectedProperties)
            {
                var projectValue = SqlUtil.GetColumnReferenceExpr(RepeatContext.PivotVariable.VariableName,
                    projectProperty);
                var alias = InputVariable.VariableName + "." + projectProperty;
                inputSelectList.Add(SqlUtil.GetSelectScalarExpr(projectValue, alias));
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
                                var projectValue = SqlUtil.GetColumnReferenceExpr(selectVar.VariableName,
                                    property);
                                outerSelectList.Add(SqlUtil.GetSelectScalarExpr(projectValue, alias));
                            }

                        }
                    }
                }
            }
            return outerSelectList;
        }
    }
}
