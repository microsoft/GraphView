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
        private bool IsTraversalToBound { get; set; }

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
            
            for (int index = currentContext.MatchPathList.Count - 1; index >= 0; --index)
            {
                if (currentContext.MatchPathList[index].EdgeVariable == this &&
                    currentContext.MatchPathList[index].SinkVariable == null &&
                    !currentContext.MatchPathList[index].IsReversed)
                {
                    GremlinFreeVertexVariable inVertex = new GremlinFreeVertexVariable();
                    currentContext.VariableList.Add(inVertex);
                    currentContext.TableReferencesInFromClause.Add(inVertex);
                    currentContext.SetPivotVariable(inVertex);
                    currentContext.MatchPathList[index].SinkVariable = inVertex;
                    return;
                }
            }

            base.InV(currentContext);
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            if (this.IsTraversalToBound)
            {
                base.OutV(currentContext);
                return;
            }

            for (int index = currentContext.MatchPathList.Count - 1; index >= 0; --index)
            {
                if (currentContext.MatchPathList[index].EdgeVariable == this &&
                    currentContext.MatchPathList[index].SourceVariable == null &&
                    !currentContext.MatchPathList[index].IsReversed)
                {
                    GremlinFreeVertexVariable outVertex = new GremlinFreeVertexVariable();
                    currentContext.VariableList.Add(outVertex);
                    currentContext.TableReferencesInFromClause.Add(outVertex);
                    currentContext.SetPivotVariable(outVertex);
                    currentContext.MatchPathList[index].SourceVariable = outVertex;
                    return;
                }
            }

            base.OutV(currentContext);
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            if (this.IsTraversalToBound)
            {
                base.OtherV(currentContext);
                return;
            }

            if (this.EdgeType == WEdgeType.OutEdge || this.EdgeType == WEdgeType.BothEdge)
            {
                for (int index = currentContext.MatchPathList.Count - 1; index >= 0; --index)
                {
                    if (currentContext.MatchPathList[index].EdgeVariable == this &&
                        currentContext.MatchPathList[index].SinkVariable == null &&
                        !currentContext.MatchPathList[index].IsReversed)
                    {
                        GremlinFreeVertexVariable otherVertex = new GremlinFreeVertexVariable();
                        currentContext.VariableList.Add(otherVertex);
                        currentContext.TableReferencesInFromClause.Add(otherVertex);
                        currentContext.SetPivotVariable(otherVertex);
                        currentContext.MatchPathList[index].SinkVariable = otherVertex;
                        return;
                    }
                }
            }
            else if (this.EdgeType == WEdgeType.InEdge)
            {
                for (int index = currentContext.MatchPathList.Count - 1; index >= 0; --index)
                {
                    if (currentContext.MatchPathList[index].EdgeVariable == this &&
                        currentContext.MatchPathList[index].SourceVariable == null &&
                        !currentContext.MatchPathList[index].IsReversed)
                    {
                        GremlinFreeVertexVariable otherVertex = new GremlinFreeVertexVariable();
                        currentContext.VariableList.Add(otherVertex);
                        currentContext.TableReferencesInFromClause.Add(otherVertex);
                        currentContext.SetPivotVariable(otherVertex);
                        currentContext.MatchPathList[index].SourceVariable = otherVertex;
                        return;
                    }
                }
            }
            base.OtherV(currentContext);
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

        internal override void Subgraph(GremlinToSqlContext currentContext, string sideEffectKey, GremlinToSqlContext dummyContext)
        {
            this.IsTraversalToBound = true;
            base.Subgraph(currentContext, sideEffectKey, dummyContext);
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
