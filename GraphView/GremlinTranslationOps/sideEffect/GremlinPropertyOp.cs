using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView.GremlinTranslationOps.sideEffect
{
    internal class GremlinPropertyOp: GremlinTranslationOperator
    {
        public Dictionary<string, object> Properties { get; set; }

        public GremlinPropertyOp(params object[] properties)
        {
            if (properties.Length % 2 != 0) throw new Exception("The parameter of property should be even");
            if (properties.Length < 2) throw new Exception("The number of parameter of property should be larger than 2");
            Properties = new Dictionary<string, object>();
            for (int i = 0; i < properties.Length; i += 2)
            {
                Properties[properties[i] as string] = properties[i + 1];
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            if (inputContext.CurrVariable is GremlinAddEVariable)
            {
                foreach (var property in Properties)
                {
                    if (!(inputContext.CurrVariable as GremlinAddEVariable).Properties.ContainsKey(property.Key))
                    {
                        (inputContext.CurrVariable as GremlinAddEVariable).Properties[property.Key] = new List<object>();
                    }
                    (inputContext.CurrVariable as GremlinAddEVariable).Properties[property.Key] = property.Value;
                }
            }
            else if (inputContext.CurrVariable is GremlinAddVVariable)
            {
                foreach (var property in Properties)
                {
                    if (!(inputContext.CurrVariable as GremlinAddVVariable).Properties.ContainsKey(property.Key))
                    {
                        (inputContext.CurrVariable as GremlinAddVVariable).Properties[property.Key] = new List<object>();
                    }
                    (inputContext.CurrVariable as GremlinAddVVariable).Properties[property.Key] = property.Value;
                }
            }
            else
            {
                string tableName;
                WWhereClause whereClause = null;

                if (inputContext.CurrVariable is GremlinEdgeVariable ||
                    (inputContext.CurrVariable is GremlinVariableReference &&
                     (inputContext.CurrVariable as GremlinVariableReference).GetVariableType() == GremlinVariableType.Edge))
                {
                    throw new NotImplementedException();
                    //tableName = "Edge";
                    //WBooleanExpression inSourceExpr = new WInPredicate()
                    //{
                    //    Expression = GremlinUtil.GetColumnReferenceExpression("Edge", "source"),
                    //    Values =
                    //        new List<WScalarExpression>()
                    //        {
                    //            GremlinUtil.GetColumnReferenceExpression(inputContext.CurrVariable.SetVariableName,
                    //                "source")
                    //        }
                    //};
                    //WBooleanExpression inIdExpr = new WInPredicate()
                    //{
                    //    Expression = GremlinUtil.GetColumnReferenceExpression("Edge", "id"),
                    //    Values =
                    //        new List<WScalarExpression>()
                    //        {
                    //            GremlinUtil.GetColumnReferenceExpression(inputContext.CurrVariable.SetVariableName, "id")
                    //        }
                    //};
                    //WBooleanExpression wherePredicate = new WBooleanBinaryExpression()
                    //{
                    //    BooleanExpressionType = BooleanBinaryExpressionType.And,
                    //    FirstExpr = inSourceExpr,
                    //    SecondExpr = inIdExpr
                    //};
                    //whereClause = new WWhereClause() {SearchCondition = wherePredicate };
                }
                else
                {
                    if (!(inputContext.CurrVariable is GremlinVariableReference))
                        throw new NotImplementedException();
                    tableName = "Node";
                    WBooleanExpression inExpr = new WInPredicate()
                    {
                        Expression = GremlinUtil.GetColumnReferenceExpression("Node", "id"),
                        Values =
                            new List<WScalarExpression>()
                            {
                                new WScalarSubquery()
                                {
                                    SubQueryExpr = GremlinUtil.GetSelectQueryBlockFromVariableReference(inputContext.CurrVariable as GremlinVariableReference)
                                }
                            }
                    };
                    whereClause = new WWhereClause() {SearchCondition = inExpr};
                }
                //Add or update properties
                List<WSetClause> setClause = new List<WSetClause>();
                foreach (var property in Properties)
                {
                    WAssignmentSetClause assignmentSetClause = new WAssignmentSetClause()
                    {
                        Column = GremlinUtil.GetColumnReferenceExpression(property.Key),
                        NewValue =  GremlinUtil.GetValueExpression(property.Value)
                    };
                    setClause.Add(assignmentSetClause);
                }

                WUpdateSpecification updateSpec = new WUpdateSpecification()
                {
                    //FromClause = inputContext.GetFromClause(),
                    WhereClause = whereClause,
                    Target = GremlinUtil.GetNamedTableReference(tableName),
                    SetClauses = setClause
                };

                inputContext.Statements.Add(updateSpec);

            }
            return inputContext;
        }
    }
}
