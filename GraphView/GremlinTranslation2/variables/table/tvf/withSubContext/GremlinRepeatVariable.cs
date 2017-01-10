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
            switch (repeatContext.PivotVariable.GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    return new GremlinRepeatVertexVariable(contextVariable, repeatContext, repeatCondition);
                case GremlinVariableType.Edge:
                    return new GremlinRepeatEdgeVariable(contextVariable, repeatContext, repeatCondition);
                case GremlinVariableType.Scalar:
                    return new GremlinRepeatEdgeVariable(contextVariable, repeatContext, repeatCondition);
                case GremlinVariableType.Table:
                    return new GremlinRepeatEdgeVariable(contextVariable, repeatContext, repeatCondition);
            }
            throw new NotImplementedException();
        }

        public GremlinContextVariable InputVariable { get; set; }
        public GremlinToSqlContext RepeatContext { get; set; }
        public RepeatCondition RepeatCondition { get; set; }

        public GremlinRepeatVariable(GremlinContextVariable inputVariable, GremlinToSqlContext repeatContext,
                                    RepeatCondition repeatCondition)
        {
            RepeatContext = repeatContext;
            InputVariable = inputVariable;
            RepeatCondition = repeatCondition;
        }

        internal override void Populate(string property)
        {
            RepeatContext.Populate(property);
        }

        public override WTableReference ToTableReference(List<string> projectProperties, string tableName )
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();

            foreach (var property in InputVariable.UsedProperties)
            {
                Populate(property);
            }

            //Set the select Elements
            List<WSelectScalarExpression> inputSelectList = GetInputSelectList(projectProperties);
            List<WSelectScalarExpression> outerSelectList = GetOuterSelectList();

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

            PropertyKeys.Add(SqlUtil.GetScalarSubquery(selectQueryBlock));
            PropertyKeys.Add(GetRepeatConditionExpression());
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Repeat, PropertyKeys, tableName);

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

        public List<WSelectScalarExpression> GetInputSelectList(List<string> projectProperties)
        {
            List<WSelectScalarExpression> inputSelectList = new List<WSelectScalarExpression>();
            
            foreach (var projectProperty in projectProperties)
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
                            foreach (var property in temp.UsedProperties)
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
}
