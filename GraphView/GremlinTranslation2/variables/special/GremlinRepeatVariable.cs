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
        private List<string> projectedProperties;

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
            projectedProperties = new List<string>();
        }

        internal override void Populate(string name, bool isAlias = false)
        {
            context.PivotVariable.Populate(name);
            base.Populate(name);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();

            foreach (var property in inputVariable.UsedProperties)
            {
                //context.PivotVariable.Populate(property, true);
                context.PivotVariable.Populate(property);
            }

            List<Tuple<GremlinVariable2, GremlinVariable2>> aliasList = new List<Tuple<GremlinVariable2, GremlinVariable2>>();
            aliasList.Add(new Tuple<GremlinVariable2, GremlinVariable2>(context.PivotVariable, inputVariable));

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
                            }
                            aliasList.Add(new Tuple<GremlinVariable2, GremlinVariable2>(selectVar, temp.ContextVariable));
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
                            }
                            aliasList.Add(new Tuple<GremlinVariable2, GremlinVariable2>(selectVar, temp.ContextVariable));
                        }
                    }
                }
            }

            WSelectQueryBlock selectQueryBlock = context.ToSelectQueryBlock();

            foreach (var selectElement in selectQueryBlock.SelectElements)
            {
                if (selectElement is WSelectScalarExpression)
                {
                    if ((selectElement as WSelectScalarExpression).SelectExpr is WColumnReferenceExpression)
                    {
                        var temp = ((selectElement as WSelectScalarExpression).SelectExpr as WColumnReferenceExpression);
                        foreach (var pair in aliasList)
                        {
                            if (temp.MultiPartIdentifier.Identifiers.First().Value == pair.Item1.VariableName)
                            {
                                var property = temp.MultiPartIdentifier.Identifiers[1].Value;
                                (selectElement as WSelectScalarExpression).ColumnName = pair.Item2.VariableName + "." + property;
                            }
                        }
                    }
                }
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
