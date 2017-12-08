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
        private bool IsTraversalToBound { get; set; }

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
            if (this.IsTraversalToBound)
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
            currentContext.MatchPathList.Add(new GremlinMatchPath(this, bothEdge, bothVertex, false));
            currentContext.SetPivotVariable(bothVertex);
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            if (this.IsTraversalToBound)
            {
                base.In(currentContext, edgeLabels);
                return;
            }
            GremlinFreeEdgeVariable inEdge = new GremlinFreeEdgeVariable(WEdgeType.InEdge);
            currentContext.VariableList.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

            GremlinFreeVertexVariable inVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(inVertex);
            currentContext.TableReferencesInFromClause.Add(inVertex);
            currentContext.MatchPathList.Add(new GremlinMatchPath(inVertex, inEdge, this, false));
            currentContext.SetPivotVariable(inVertex);
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            if (this.IsTraversalToBound)
            {
                base.InE(currentContext, edgeLabels);
                return;
            }
            GremlinFreeEdgeVariable inEdge = new GremlinFreeEdgeVariable(WEdgeType.InEdge);
            currentContext.VariableList.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);
            currentContext.MatchPathList.Add(new GremlinMatchPath(null, inEdge, this, false));
            currentContext.SetPivotVariable(inEdge);
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            if (this.IsTraversalToBound)
            {
                base.Out(currentContext, edgeLabels);
                return;
            }
            GremlinFreeEdgeVariable outEdge = new GremlinFreeEdgeVariable(WEdgeType.OutEdge);
            currentContext.VariableList.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            GremlinFreeVertexVariable outVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferencesInFromClause.Add(outVertex);
            currentContext.MatchPathList.Add(new GremlinMatchPath(this, outEdge, outVertex, false));
            currentContext.SetPivotVariable(outVertex);
        }
        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            if (this.IsTraversalToBound)
            {
                base.OutE(currentContext, edgeLabels);
                return;
            }
            GremlinFreeEdgeVariable outEdgeVar = new GremlinFreeEdgeVariable(WEdgeType.OutEdge);
            currentContext.VariableList.Add(outEdgeVar);
            currentContext.AddLabelPredicateForEdge(outEdgeVar, edgeLabels);
            currentContext.MatchPathList.Add(new GremlinMatchPath(this, outEdgeVar, null, false));
            currentContext.SetPivotVariable(outEdgeVar);
        }

        internal override void Aggregate(GremlinToSqlContext currentContext, string sideEffectKey, GremlinToSqlContext projectContext)
        {
            this.IsTraversalToBound = true;
            base.Aggregate(currentContext, sideEffectKey, projectContext);
        }

        internal override void Barrier(GremlinToSqlContext currentContext)
        {
            this.IsTraversalToBound = true;
            base.Barrier(currentContext);
        }

        internal override void Coin(GremlinToSqlContext currentContext, double probability)
        {
            this.IsTraversalToBound = true;
            base.Coin(currentContext, probability);
        }

        internal override void CyclicPath(GremlinToSqlContext currentContext, string fromLabel = null, string toLabel = null)
        {
            this.IsTraversalToBound = true;
            base.CyclicPath(currentContext, fromLabel, toLabel);
        }

        internal override void Dedup(GremlinToSqlContext currentContext, List<string> dedupLabels, GraphTraversal dedupTraversal, GremlinKeyword.Scope scope)
        {
            this.IsTraversalToBound = scope == GremlinKeyword.Scope.Global;
            base.Dedup(currentContext, dedupLabels, dedupTraversal, scope);
        }

        internal override void Group(GremlinToSqlContext currentContext, string sideEffectKey, GremlinToSqlContext groupByContext,
            GremlinToSqlContext projectByContext, bool isProjectByString)
        {
            if (sideEffectKey != null)
            {
                this.IsTraversalToBound = true;
            }
            base.Group(currentContext, sideEffectKey, groupByContext, projectByContext, isProjectByString);
        }

        internal override void Inject(GremlinToSqlContext currentContext, object injection)
        {
            this.IsTraversalToBound = true;
            base.Inject(currentContext, injection);
        }

        internal override void Order(GremlinToSqlContext currentContext, List<Tuple<GremlinToSqlContext, IComparer>> byModulatingMap,
            GremlinKeyword.Scope scope)
        {
            this.IsTraversalToBound = true;
            base.Order(currentContext, byModulatingMap, scope);
        }

        internal override void Property(GremlinToSqlContext currentContext, GremlinProperty vertexProperty)
        {
            this.IsTraversalToBound = true;
            base.Property(currentContext, vertexProperty);
        }

        internal override void Range(GremlinToSqlContext currentContext, int low, int high, GremlinKeyword.Scope scope, bool isReverse)
        {
            this.IsTraversalToBound = true;
            base.Range(currentContext, low, high, scope, isReverse);
        }

        internal override void Sample(GremlinToSqlContext currentContext, GremlinKeyword.Scope scope, int amountToSample,
            GremlinToSqlContext probabilityContext)
        {
            this.IsTraversalToBound = true;
            base.Sample(currentContext, scope, amountToSample, probabilityContext);
        }

        internal override void SideEffect(GremlinToSqlContext currentContext, GremlinToSqlContext sideEffectContext)
        {
            this.IsTraversalToBound = true;
            base.SideEffect(currentContext, sideEffectContext);
        }

        internal override void SimplePath(GremlinToSqlContext currentContext, string fromLabel, string toLabel)
        {
            this.IsTraversalToBound = true;
            base.SimplePath(currentContext, fromLabel, toLabel);
        }

        internal override void Store(GremlinToSqlContext currentContext, string sideEffectKey, GremlinToSqlContext projectContext)
        {
            this.IsTraversalToBound = true;
            base.Store(currentContext, sideEffectKey, projectContext);
        }

        internal override void Tree(GremlinToSqlContext currentContext, string sideEffectKey, List<GraphTraversal> byList)
        {
            this.IsTraversalToBound = true;
            base.Tree(currentContext, sideEffectKey, byList);
        }
    }
}
