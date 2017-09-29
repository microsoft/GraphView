using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFreeEdgeVariable : GremlinEdgeTableVariable
    {
        private bool IsTraversalToBound;

        public GremlinFreeEdgeVariable(WEdgeType edgeType)
        {
            this.IsTraversalToBound = false;
            this.EdgeType = edgeType;
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            if (this.IsTraversalToBound)
            {
                base.InV(currentContext);
                return;
            }
            GremlinFreeVertexVariable inVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(inVertex);
            currentContext.TableReferencesInFromClause.Add(inVertex);

            GremlinVariableProperty edgeSinkVProperty = this.GetVariableProperty(GremlinKeyword.EdgeSinkV);
            GremlinVariableProperty vNodeIDProperty = inVertex.GetVariableProperty(GremlinKeyword.NodeID);
            WBooleanExpression edgeToSinkVertexExp = SqlUtil.GetEqualBooleanComparisonExpr(edgeSinkVProperty.ToScalarExpression(), vNodeIDProperty.ToScalarExpression());
            currentContext.AddPredicate(edgeToSinkVertexExp);

            currentContext.SetPivotVariable(inVertex);
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            if (this.IsTraversalToBound)
            {
                base.OutV(currentContext);
                return;
            }
            GremlinFreeVertexVariable outVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferencesInFromClause.Add(outVertex);

            GremlinVariableProperty edgeSourceVProperty = this.GetVariableProperty(GremlinKeyword.EdgeSourceV);
            GremlinVariableProperty vNodeIDProperty = outVertex.GetVariableProperty(GremlinKeyword.NodeID);
            WBooleanExpression edgeToSinkVertexExpr = SqlUtil.GetEqualBooleanComparisonExpr(edgeSourceVProperty.ToScalarExpression(), vNodeIDProperty.ToScalarExpression());
            currentContext.AddPredicate(edgeToSinkVertexExpr);

            currentContext.SetPivotVariable(outVertex);
        }

        internal override void BothV(GremlinToSqlContext currentContext)
        {
            if (this.IsTraversalToBound)
            {
                base.BothV(currentContext);
                return;
            }
            GremlinFreeVertexVariable bothSourceVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(bothSourceVertex);
            currentContext.TableReferencesInFromClause.Add(bothSourceVertex);

            GremlinVariableProperty edgeSinkVProperty = this.GetVariableProperty(GremlinKeyword.EdgeSinkV);
            GremlinVariableProperty edgeSourceVProperty = this.GetVariableProperty(GremlinKeyword.EdgeSourceV);
            GremlinVariableProperty vNodeIDProperty = bothSourceVertex.GetVariableProperty(GremlinKeyword.NodeID);

            WBooleanExpression edgeToSinkVertexExpr =
                SqlUtil.GetEqualBooleanComparisonExpr(edgeSinkVProperty.ToScalarExpression(),
                    vNodeIDProperty.ToScalarExpression());
            WBooleanExpression edgeToSourceVertexExpr =
                SqlUtil.GetEqualBooleanComparisonExpr(edgeSourceVProperty.ToScalarExpression(),
                    vNodeIDProperty.ToScalarExpression());
            WBooleanBinaryExpression edgeToBothVertexExpr =
                SqlUtil.GetOrBooleanBinaryExpr(edgeToSinkVertexExpr, edgeToSourceVertexExpr);

            currentContext.AddPredicate(edgeToBothVertexExpr);
            currentContext.SetPivotVariable(bothSourceVertex);
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
