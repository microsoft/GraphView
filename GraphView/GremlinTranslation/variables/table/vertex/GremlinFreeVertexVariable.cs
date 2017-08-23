using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFreeVertexVariable : GremlinVertexTableVariable
    {
        private bool isTraversalToBound;

        public override WTableReference ToTableReference()
        {
            return new WNamedTableReference()
            {
                Alias = SqlUtil.GetIdentifier(GetVariableName()),
                TableObjectString = "node",
                TableObjectName = SqlUtil.GetSchemaObjectName("node"),
            }; ;
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            if (this.isTraversalToBound)
            {
                base.Both(currentContext, edgeLabels);
                return;
            }
            GremlinFreeEdgeVariable bothEdge = new GremlinFreeEdgeVariable(WEdgeType.BothEdge);
            currentContext.VariableList.Add(bothEdge);
            currentContext.AddLabelPredicateForEdge(bothEdge, edgeLabels);

            GremlinFreeVertexVariable bothVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(bothVertex);

            // In this case, the both-edgeTable variable is not added to the table-reference list. 
            // Instead, we populate a path this_variable-[bothEdge]->bothVertex in the context
            currentContext.TableReferencesInFromClause.Add(bothVertex);
            currentContext.MatchPathList.Add(new GremlinMatchPath(this, bothEdge, bothVertex));
            currentContext.SetPivotVariable(bothVertex);
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            if (this.isTraversalToBound)
            {
                base.In(currentContext, edgeLabels);
                return;
            }
            GremlinFreeEdgeVariable inEdge = new GremlinFreeEdgeVariable(WEdgeType.InEdge);
            currentContext.VariableList.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

            GremlinFreeVertexVariable outVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferencesInFromClause.Add(outVertex);
            currentContext.MatchPathList.Add(new GremlinMatchPath(outVertex, inEdge, this));
            currentContext.SetPivotVariable(outVertex);
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            if (this.isTraversalToBound)
            {
                base.InE(currentContext, edgeLabels);
                return;
            }
            GremlinFreeEdgeVariable inEdge = new GremlinFreeEdgeVariable(WEdgeType.InEdge);
            currentContext.VariableList.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);
            currentContext.MatchPathList.Add(new GremlinMatchPath(null, inEdge, this));
            currentContext.SetPivotVariable(inEdge);
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            if (this.isTraversalToBound)
            {
                base.Out(currentContext, edgeLabels);
                return;
            }
            GremlinFreeEdgeVariable outEdge = new GremlinFreeEdgeVariable(WEdgeType.OutEdge);
            currentContext.VariableList.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            GremlinFreeVertexVariable inVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(inVertex);
            currentContext.TableReferencesInFromClause.Add(inVertex);
            currentContext.MatchPathList.Add(new GremlinMatchPath(this, outEdge, inVertex));
            currentContext.SetPivotVariable(inVertex);
        }
        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            if (this.isTraversalToBound)
            {
                base.OutE(currentContext, edgeLabels);
                return;
            }
            GremlinFreeEdgeVariable outEdgeVar = new GremlinFreeEdgeVariable(WEdgeType.OutEdge);
            currentContext.VariableList.Add(outEdgeVar);
            currentContext.AddLabelPredicateForEdge(outEdgeVar, edgeLabels);
            currentContext.MatchPathList.Add(new GremlinMatchPath(this, outEdgeVar, null));
            currentContext.SetPivotVariable(outEdgeVar);
        }

        internal override void Aggregate(GremlinToSqlContext currentContext, string sideEffectKey, GremlinToSqlContext projectContext)
        {
            this.isTraversalToBound = true;
            base.Aggregate(currentContext, sideEffectKey, projectContext);
        }

        internal override void Barrier(GremlinToSqlContext currentContext)
        {
            this.isTraversalToBound = true;
            base.Barrier(currentContext);
        }

        internal override void Coin(GremlinToSqlContext currentContext, double probability)
        {
            this.isTraversalToBound = true;
            base.Coin(currentContext, probability);
        }

        internal override void CyclicPath(GremlinToSqlContext currentContext, string fromLabel, string toLabel)
        {
            this.isTraversalToBound = true;
            base.CyclicPath(currentContext, fromLabel, toLabel);
        }

        internal override void Dedup(GremlinToSqlContext currentContext, List<string> dedupLabels, GraphTraversal dedupTraversal, GremlinKeyword.Scope scope)
        {
            this.isTraversalToBound = scope == GremlinKeyword.Scope.Global;
            base.Dedup(currentContext, dedupLabels, dedupTraversal, scope);
        }

        internal override void Group(GremlinToSqlContext currentContext, string sideEffectKey, GremlinToSqlContext groupByContext,
            GremlinToSqlContext projectByContext, bool isProjectByString)
        {
            if (sideEffectKey != null) this.isTraversalToBound = true;
            base.Group(currentContext, sideEffectKey, groupByContext, projectByContext, isProjectByString);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            if (this.isTraversalToBound)
            {
                Populate(propertyKey);
                GraphTraversal traversal = GraphTraversal.__().Properties(propertyKey);
                traversal.GetStartOp().InheritedVariableFromParent(currentContext);
                GremlinFilterVariable newVariable = new GremlinFilterVariable(SqlUtil.GetExistPredicate(traversal.GetEndOp().GetContext().ToSelectQueryBlock()));
                currentContext.VariableList.Add(newVariable);
                currentContext.TableReferencesInFromClause.Add(newVariable);
                currentContext.SetPivotVariable(newVariable);
            }
            else
            {
                base.Has(currentContext, propertyKey);
            }
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, object valuesOrPredicate)
        {
            if (this.isTraversalToBound)
            {
                Populate(propertyKey);
                GremlinFilterVariable newVariable = new GremlinFilterVariable(CreateBooleanExpression(GetVariableProperty(propertyKey), valuesOrPredicate));
                currentContext.VariableList.Add(newVariable);
                currentContext.TableReferencesInFromClause.Add(newVariable);
                currentContext.SetPivotVariable(newVariable);
            }
            else
            {
                base.Has(currentContext, propertyKey, valuesOrPredicate);
            }
        }

        internal override void HasIdOrLabel(GremlinToSqlContext currentContext, GremlinHasType hasType, List<object> valuesOrPredicates)
        {
            if (this.isTraversalToBound)
            {
                string propertyKey = hasType == GremlinHasType.HasId ? GremlinKeyword.DefaultId : GremlinKeyword.Label;
                GremlinVariableProperty variableProperty = GetVariableProperty(propertyKey);
                
                Populate(propertyKey);

                List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
                foreach (var valuesOrPredicate in valuesOrPredicates)
                {
                    booleanExprList.Add(CreateBooleanExpression(variableProperty, valuesOrPredicate));
                }
                GremlinFilterVariable newVariable = new GremlinFilterVariable(SqlUtil.ConcatBooleanExprWithOr(booleanExprList));
                currentContext.VariableList.Add(newVariable);
                currentContext.TableReferencesInFromClause.Add(newVariable);
                currentContext.SetPivotVariable(newVariable);
            }
            else
            {
                base.HasIdOrLabel(currentContext, hasType, valuesOrPredicates);
            }
        }

        internal override void HasNot(GremlinToSqlContext currentContext, string propertyKey)
        {
            if (this.isTraversalToBound)
            {
                //Populate(propertyKey);
                GraphTraversal traversal = GraphTraversal.__().Properties(propertyKey);
                traversal.GetStartOp().InheritedVariableFromParent(currentContext);
                GremlinFilterVariable newVariable = new GremlinFilterVariable(SqlUtil.GetNotExistPredicate(traversal.GetEndOp().GetContext().ToSelectQueryBlock()));
                currentContext.VariableList.Add(newVariable);
                currentContext.TableReferencesInFromClause.Add(newVariable);
                currentContext.SetPivotVariable(newVariable);
            }
            else
            {
                base.HasNot(currentContext, propertyKey);
            }
        }

        internal override void Inject(GremlinToSqlContext currentContext, object injection)
        {
            this.isTraversalToBound = true;
            base.Inject(currentContext, injection);
        }

        internal override void Order(GremlinToSqlContext currentContext, List<Tuple<GremlinToSqlContext, IComparer>> byModulatingMap,
            GremlinKeyword.Scope scope)
        {
            this.isTraversalToBound = true;
            base.Order(currentContext, byModulatingMap, scope);
        }

        internal override void Property(GremlinToSqlContext currentContext, GremlinProperty vertexProperty)
        {
            this.isTraversalToBound = true;
            base.Property(currentContext, vertexProperty);
        }

        internal override void Range(GremlinToSqlContext currentContext, int low, int high, GremlinKeyword.Scope scope, bool isReverse)
        {
            this.isTraversalToBound = true;
            base.Range(currentContext, low, high, scope, isReverse);
        }

        internal override void Sample(GremlinToSqlContext currentContext, GremlinKeyword.Scope scope, int amountToSample,
            GremlinToSqlContext probabilityContext)
        {
            this.isTraversalToBound = true;
            base.Sample(currentContext, scope, amountToSample, probabilityContext);
        }

        internal override void SideEffect(GremlinToSqlContext currentContext, GremlinToSqlContext sideEffectContext)
        {
            this.isTraversalToBound = true;
            base.SideEffect(currentContext, sideEffectContext);
        }

        internal override void SimplePath(GremlinToSqlContext currentContext, string fromLabel, string toLabel)
        {
            this.isTraversalToBound = true;
            base.SimplePath(currentContext, fromLabel, toLabel);
        }

        internal override void Store(GremlinToSqlContext currentContext, string sideEffectKey, GremlinToSqlContext projectContext)
        {
            this.isTraversalToBound = true;
            base.Store(currentContext, sideEffectKey, projectContext);
        }


        internal override void Tree(GremlinToSqlContext currentContext, string sideEffectKey, List<GraphTraversal> byList)
        {
            this.isTraversalToBound = true;
            base.Tree(currentContext, sideEffectKey, byList);
        }

        internal override void Where(GremlinToSqlContext currentContext, Predicate predicate, TraversalRing traversalRing)
        {
            if (this.isTraversalToBound)
            {
                GremlinFilterVariable newVariable = new GremlinFilterVariable(GetWherePredicate(currentContext, this, predicate, traversalRing));
                currentContext.VariableList.Add(newVariable);
                currentContext.TableReferencesInFromClause.Add(newVariable);
                currentContext.SetPivotVariable(newVariable);
            }
            else
            {
                base.Where(currentContext, predicate, traversalRing);
            }
        }

        internal override void Where(GremlinToSqlContext currentContext, string startKey, Predicate predicate, TraversalRing traversalRing)
        {
            if (this.isTraversalToBound)
            {
                var selectKey = new List<string> { startKey };
                var selectTraversal = new List<GraphTraversal> { traversalRing.Next() };
                var firstVar = GenerateSelectVariable(currentContext, GremlinKeyword.Pop.Last, selectKey, selectTraversal);
                GremlinFilterVariable newVariable = new GremlinFilterVariable(GetWherePredicate(currentContext, firstVar, predicate, traversalRing));
                currentContext.VariableList.Add(newVariable);
                currentContext.TableReferencesInFromClause.Add(newVariable);
                currentContext.SetPivotVariable(newVariable);
            }
            else
            {
                base.Where(currentContext, startKey, predicate, traversalRing);
            }
        }

        internal override void Where(GremlinToSqlContext currentContext, GremlinToSqlContext whereContext)
        {
            if (this.isTraversalToBound)
            {
                currentContext.AllTableVariablesInWhereClause.AddRange(whereContext.FetchAllTableVars());
                WBooleanExpression wherePredicate = whereContext.ToSqlBoolean();
                GremlinFilterVariable newVariable = new GremlinFilterVariable(wherePredicate);
                currentContext.VariableList.Add(newVariable);
                currentContext.TableReferencesInFromClause.Add(newVariable);
                currentContext.SetPivotVariable(newVariable);
            }
            else
            {
                base.Where(currentContext, whereContext);
            }
        }
    }
}
