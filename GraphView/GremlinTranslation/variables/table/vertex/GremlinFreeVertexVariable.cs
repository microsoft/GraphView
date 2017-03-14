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
            currentContext.TableReferences.Add(bothVertex);
            currentContext.PathList.Add(new GremlinMatchPath(this, bothEdge, bothVertex));
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
            currentContext.TableReferences.Add(outVertex);
            currentContext.PathList.Add(new GremlinMatchPath(outVertex, inEdge, this));
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
            currentContext.PathList.Add(new GremlinMatchPath(null, inEdge, this));
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
            currentContext.TableReferences.Add(inVertex);
            currentContext.PathList.Add(new GremlinMatchPath(this, outEdge, inVertex));
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
            currentContext.PathList.Add(new GremlinMatchPath(this, outEdgeVar, null));
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

        internal override void CyclicPath(GremlinToSqlContext currentContext)
        {
            this.isTraversalToBound = true;
            base.CyclicPath(currentContext);
        }

        internal override void Dedup(GremlinToSqlContext currentContext, List<string> dedupLabels, GremlinToSqlContext dedupContext, GremlinKeyword.Scope scope)
        {
            this.isTraversalToBound = true;
            base.Dedup(currentContext, dedupLabels, dedupContext, scope);
        }

        internal override void Group(GremlinToSqlContext currentContext, string sideEffectKey, GremlinToSqlContext groupByContext,
            GremlinToSqlContext projectByContext, bool isProjectByString)
        {
            if (sideEffectKey != null) this.isTraversalToBound = true;
            base.Group(currentContext, sideEffectKey, groupByContext, projectByContext, isProjectByString);
        }

        internal override void Inject(GremlinToSqlContext currentContext, List<object> values)
        {
            this.isTraversalToBound = true;
            base.Inject(currentContext, values);
        }

        internal override void Order(GremlinToSqlContext currentContext, List<Tuple<object, IComparer>> byModulatingMap,
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

        internal override void SimplePath(GremlinToSqlContext currentContext)
        {
            this.isTraversalToBound = true;
            base.SimplePath(currentContext);
        }

        internal override void Store(GremlinToSqlContext currentContext, string sideEffectKey, GremlinToSqlContext projectContext)
        {
            this.isTraversalToBound = true;
            base.Store(currentContext, sideEffectKey, projectContext);
        }

       
        internal override void Tree(GremlinToSqlContext currentContext, string sideEffectKey, List<GraphTraversal2> byList)
        {
            this.isTraversalToBound = true;
            base.Tree(currentContext, sideEffectKey, byList);
        }
    }
}
