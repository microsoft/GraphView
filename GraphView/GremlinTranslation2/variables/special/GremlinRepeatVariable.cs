using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinRepeatVariable : GremlinTableVariable
    {
        private GremlinVariable2 inputVariable;
        private GremlinToSqlContext context;

        public GremlinToSqlContext ConditionContext { get; set; }
        public bool IsEmitTrue { get; set; }
        public bool IsEmitBefore { get; set; }
        public bool IsEmitAfter { get; set; }
        public bool IsUntilBefore { get; set; }
        public bool IsUntilAfter { get; set; }
        public bool IsTimes { get; set; }
        public long Times { get; set; }

        public GremlinRepeatVariable(GremlinVariable2 inputVariable, GremlinToSqlContext context)
        {
            VariableName = GenerateTableAlias();
            this.context = context;
            this.inputVariable = inputVariable;
        }

        internal override void Populate(string property)
        {
            context.Populate(property);
            base.Populate(property);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();

            foreach (var property in inputVariable.UsedProperties)
            {
                Populate(property);
            }

            List<WSelectScalarExpression> outerSelectList = new List<WSelectScalarExpression>();
            foreach (var variable in context.VariableList)
            {
                if (variable is GremlinContextEdgeVariable)
                {
                    var temp = (variable as GremlinContextEdgeVariable);
                    if (temp.IsFromSelect)
                    {
                        var selectVar = context.SelectVariable(temp.Pop, temp.SelectKey);
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
                        var selectVar = context.SelectVariable(temp.Pop, temp.SelectKey);
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

            WSelectQueryBlock selectQueryBlock = context.ToSelectQueryBlock();
            selectQueryBlock.SelectElements.Clear();

            if (context.PivotVariable.DefaultProjection() is GremlinVariableProperty)
            {
                var temp = context.PivotVariable.DefaultProjection() as GremlinVariableProperty;
                selectQueryBlock.SelectElements.Add(new WSelectScalarExpression()
                {
                    ColumnName = inputVariable.VariableName + "." + temp.VariableProperty,
                    SelectExpr =
                        GremlinUtil.GetColumnReferenceExpression(context.PivotVariable.VariableName, temp.VariableProperty)
                });
            }
            else
            {
                throw new NotImplementedException();
            }


            foreach (var projectProperty in projectedProperties)
            {
                selectQueryBlock.SelectElements.Add(new WSelectScalarExpression()
                {
                    ColumnName = inputVariable.VariableName + "." + projectProperty,
                    SelectExpr = GremlinUtil.GetColumnReferenceExpression(context.PivotVariable.VariableName, projectProperty)
                });
            }

            foreach (var selectElement in outerSelectList)
            {
                selectQueryBlock.SelectElements.Add(selectElement);
            }

            PropertyKeys.Add(GremlinUtil.GetScalarSubquery(selectQueryBlock));
            PropertyKeys.Add(GetRepeatConditionExpression());

            var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("repeat", PropertyKeys);
            secondTableRef.Alias = GremlinUtil.GetIdentifier(VariableName);
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
                IsEmitTrue = IsEmitTrue,
                IsEmitAfter = IsEmitAfter,
                IsEmitBefore = IsEmitBefore,
                IsUntilAfter = IsUntilAfter,
                IsUntilBefore = IsUntilBefore,
                IsTimes = IsTimes,
                Times = Times
            };
        }
    }
}
