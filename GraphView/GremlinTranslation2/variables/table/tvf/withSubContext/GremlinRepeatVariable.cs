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

        public override WTableReference ToTableReference(List<string> projectProperties, string tableName, GremlinVariable gremlinVariable)
        {
            if (projectProperties.Count == 0)
            {
                projectProperties.Add(RepeatContext.PivotVariable.DefaultProjection().VariableProperty);
            }

            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            var useProperties = new List<string>();
            InputVariable = RepeatContext.VariableList.First() as GremlinContextVariable;
            foreach (var property in InputVariable.UsedProperties)
            {
                Populate(property);
                useProperties.Add(property);
            }

            //Set the select Elements
            Dictionary<Tuple<string, string>, Tuple<string, string>> map = new Dictionary<Tuple<string, string>, Tuple<string, string>>();

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

            ModifyColumnNameVisitor newVisitor = new ModifyColumnNameVisitor();
            newVisitor.Invoke(selectQueryBlock, map);

            WSelectQueryBlock firstQueryExpr = new WSelectQueryBlock();
            foreach (var item in map)
            {
                firstQueryExpr.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetColumnReferenceExpr(item.Key.Item1, item.Key.Item2), item.Value.Item2));
            }
            foreach (var temp in projectProperties)
            {
                firstQueryExpr.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetColumnReferenceExpr(InputVariable.VariableName, temp)));
                selectQueryBlock.SelectElements.Add(SqlUtil.GetSelectScalarExpr(SqlUtil.GetColumnReferenceExpr(RepeatContext.PivotVariable.VariableName, temp)));
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

        public List<WSelectScalarExpression> GetInputSelectList(List<string> projectProperties, ref Dictionary<Tuple<string, string>, Tuple<string, string>> map)
        {
            List<WSelectScalarExpression> inputSelectList = new List<WSelectScalarExpression>();
            foreach (var projectProperty in projectProperties)
            {
                var projectValue = SqlUtil.GetColumnReferenceExpr(RepeatContext.PivotVariable.VariableName,
                    projectProperty);
                //var alias = InputVariable.VariableName + "." + projectProperty;
                var tuple = GetMapTuple();
                inputSelectList.Add(SqlUtil.GetSelectScalarExpr(projectValue, tuple.Item2));
                map[new Tuple<string, string>(InputVariable.VariableName, projectProperty)] = tuple;
            }

            return inputSelectList;
        }

        public List<WSelectScalarExpression> GetOuterSelectList(ref Dictionary<Tuple<string, string>, Tuple<string, string>> map)
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
                                //var alias = temp.ContextVariable.VariableName + "." + property;
                                var projectValue = SqlUtil.GetColumnReferenceExpr(selectVar.VariableName, property);
                                var tuple = GetMapTuple();
                                outerSelectList.Add(SqlUtil.GetSelectScalarExpr(projectValue, tuple.Item2));
                                map[new Tuple<string, string>(temp.ContextVariable.VariableName, property)] = tuple;
                            }
                        }
                    }
                }
            }
            return outerSelectList;
        }

        public Tuple<string, string> GetMapTuple()
        {
            var tuple = new Tuple<string, string>("R", "key_" + count.ToString());
            count++;
            return tuple;
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
