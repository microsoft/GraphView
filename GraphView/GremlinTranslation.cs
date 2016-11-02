using System;
using System.Collections.Generic;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinVariable
    {
        public string VariableName { get; set; }
        public override int GetHashCode()
        {
            return VariableName.GetHashCode();
        }
    }

    internal enum GremlinEdgeType
    {
        InEdge,
        OutEdge,
        BothEdge
    }

    internal class GremlinVertexVariable : GremlinVariable { }
    internal class GremlinEdgeVariable : GremlinVariable
    {
        public GremlinEdgeType EdgeType { get; set; }
    }

    internal class GremlinRecursiveEdgeVariable : GremlinVariable
    {
        public WSelectQueryBlock StepQuery { get; set; }
        public int iterationCount;
        public WBooleanExpression untilCondition { get; set; }
    }

    internal class GremlinToSqlContext
    {
        public GremlinVariable RootVariable { get; set; }
        public bool fromOuter;

        /// <summary>
        /// A list of Gremlin variables. The variables are expected to 
        /// follow the (vertex)-(edge|path)-(vertex)-... pattern
        /// </summary>
        public List<GremlinVariable> RemainingVariableList { get; set; }

        /// <summary>
        /// A collection of variables and their predicates
        /// </summary>
        public Dictionary<GremlinVariable, WBooleanExpression> VariablePredicates { get; set; }

        /// <summary>
        /// A list of boolean expressions, each of which is on multiple variables
        /// </summary>
        public List<WBooleanExpression> CrossVariableConditions { get; set; }

        /// <summary>
        /// The variable on which the new traversal operates
        /// </summary>
        public GremlinVariable LastVariable
        {
            get
            {
                if (RemainingVariableList == null || RemainingVariableList.Count == 0)
                {
                    return null;
                }
                else
                {
                    return RemainingVariableList[RemainingVariableList.Count - 1];
                }
            }
        }

        /// <summary>
        /// A list of Gremlin variables and their properties the query projects. 
        /// When no property is specified, the variable projects its "ID":
        /// If the variable is a vertex variable, it projects the vertex ID;
        /// If the variable is an edge variable, it projects the (source vertex ID, sink vertex ID, offset) pair. 
        /// 
        /// The projection is updated, as it is passed through every traversal. 
        /// </summary> 
        public List<Tuple<GremlinVariable, string>> Projection { get; set; }

        /// <summary>
        /// The Gremlin variable and its property by which the query groups
        /// </summary>
        public Tuple<GremlinVariable, string> GroupByVariable { get; set; }

        /// <summary>
        /// The Gremlin variable and its property by which the query orders
        /// </summary>
        public Tuple<GremlinVariable, string> OrderByVariable { get; set; }

        public WBooleanExpression ToSqlBoolean()
        {
            return null;
        }

        public WScalarExpression ToSqlScalar()
        {
            return null;
        }

        public WSelectQueryExpression ToSqlQuery()
        {
            return null;
        }
    }

    internal abstract class GremlinTranslationOperator
    {
        public GremlinTranslationOperator InputOperator;
        public virtual GremlinToSqlContext GetContext()
        {
            return null;
        }
    }
    
    internal class GremlinParentContextOp : GremlinTranslationOperator
    {
        public GremlinVariable InheritedVariable { get; set; }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext newContext = new GremlinToSqlContext();
            newContext.RootVariable = InheritedVariable;
            newContext.fromOuter = true;

            return newContext;
        }
    }

    internal class GremlinAndOp : GremlinTranslationOperator
    {
        public IList<GremlinTranslationOperator> ConjunctiveOperators { get; set; }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = InputOperator.GetContext();
            WBooleanExpression andExpression = null;

            foreach (GremlinTranslationOperator predicateOp in ConjunctiveOperators)
            {
                // Traces to the root of the inner translation chain
                var rootOp = predicateOp;
                while (rootOp.InputOperator != null)
                {
                    rootOp = rootOp.InputOperator;
                }

                // Inputs the outer context into the inner translaiton chain, 
                // if the inner translation chain references the outer context
                if (rootOp.GetType() == typeof(GremlinParentContextOp))
                {
                    GremlinParentContextOp rootAsContext = rootOp as GremlinParentContextOp;
                    rootAsContext.InheritedVariable = inputContext.LastVariable;
                }

                GremlinToSqlContext booleanContext = predicateOp.GetContext();
                WBooleanExpression booleanSql = booleanContext.ToSqlBoolean();
                 
                // Constructs a conjunctive boolean expression
                andExpression = andExpression == null ? booleanSql :
                    new WBooleanBinaryExpression()
                    {
                        BooleanExpressionType = BooleanBinaryExpressionType.And,
                        FirstExpr = andExpression,
                        SecondExpr = booleanSql
                    };
            }

            // Puts andExpression into inputContext
            GremlinVariable target = inputContext.LastVariable;
            if (inputContext.VariablePredicates.ContainsKey(target))
            {
                inputContext.VariablePredicates[target] = new WBooleanBinaryExpression()
                {
                    BooleanExpressionType = BooleanBinaryExpressionType.And,
                    FirstExpr = inputContext.VariablePredicates[target],
                    SecondExpr = andExpression
                };
            }
            else
            {
                inputContext.VariablePredicates[target] = andExpression;
            }

            return inputContext;
        }
    }
}
