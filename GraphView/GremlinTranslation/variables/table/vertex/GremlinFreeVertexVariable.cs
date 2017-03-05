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
            GremlinFreeEdgeTableVariable bothEdgeTable = new GremlinFreeEdgeTableVariable(WEdgeType.BothEdge);
            currentContext.VariableList.Add(bothEdgeTable);
            currentContext.AddLabelPredicateForEdge(bothEdgeTable, edgeLabels);

            GremlinFreeVertexVariable bothVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(bothVertex);

            // In this case, the both-edgeTable variable is not added to the table-reference list. 
            // Instead, we populate a path this_variable-[bothEdgeTable]->bothVertex in the context
            currentContext.TableReferences.Add(bothVertex);
            currentContext.AddPath(new GremlinMatchPath(this, bothEdgeTable, bothVertex));
            currentContext.SetPivotVariable(bothVertex);
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            if (this.isTraversalToBound)
            {
                base.In(currentContext, edgeLabels);
                return;
            }
            GremlinFreeEdgeTableVariable inEdgeTable = new GremlinFreeEdgeTableVariable(WEdgeType.InEdge);
            currentContext.VariableList.Add(inEdgeTable);
            currentContext.AddLabelPredicateForEdge(inEdgeTable, edgeLabels);

            GremlinFreeVertexVariable outVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);
            currentContext.AddPath(new GremlinMatchPath(outVertex, inEdgeTable, this));
            currentContext.SetPivotVariable(outVertex);
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            if (this.isTraversalToBound)
            {
                base.InE(currentContext, edgeLabels);
                return;
            }
            GremlinFreeEdgeTableVariable inEdgeTable = new GremlinFreeEdgeTableVariable(WEdgeType.InEdge);
            currentContext.VariableList.Add(inEdgeTable);
            currentContext.AddLabelPredicateForEdge(inEdgeTable, edgeLabels);
            currentContext.AddPath(new GremlinMatchPath(null, inEdgeTable, this));
            currentContext.SetPivotVariable(inEdgeTable);
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            if (this.isTraversalToBound)
            {
                base.Out(currentContext, edgeLabels);
                return;
            }
            GremlinFreeEdgeTableVariable outEdgeTable = new GremlinFreeEdgeTableVariable(WEdgeType.OutEdge);
            currentContext.VariableList.Add(outEdgeTable);
            currentContext.AddLabelPredicateForEdge(outEdgeTable, edgeLabels);

            GremlinFreeVertexVariable inVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(inVertex);
            currentContext.TableReferences.Add(inVertex);
            currentContext.AddPath(new GremlinMatchPath(this, outEdgeTable, inVertex));
            currentContext.SetPivotVariable(inVertex);
        }
        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            if (this.isTraversalToBound)
            {
                base.OutE(currentContext, edgeLabels);
                return;
            }
            GremlinFreeEdgeTableVariable outEdgeTableVar = new GremlinFreeEdgeTableVariable(WEdgeType.OutEdge);
            currentContext.VariableList.Add(outEdgeTableVar);
            currentContext.AddLabelPredicateForEdge(outEdgeTableVar, edgeLabels);
            currentContext.AddPath(new GremlinMatchPath(this, outEdgeTableVar, null));
            currentContext.SetPivotVariable(outEdgeTableVar);
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            currentContext.DropVertex(this);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            currentContext.Has(this, propertyKey);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, GremlinToSqlContext propertyContext)
        {
            currentContext.Has(this, propertyKey, propertyContext);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, object value)
        {
            currentContext.Has(this, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, object value)
        {
            currentContext.Has(this, label, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, propertyKey, predicate);
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Predicate predicate)
        {
            currentContext.Has(this, label, propertyKey, predicate);
        }

        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasId(this, values);
        }

        internal override void HasId(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasId(this, predicate);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            currentContext.HasLabel(this, values);
        }

        internal override void HasLabel(GremlinToSqlContext currentContext, Predicate predicate)
        {
            currentContext.HasLabel(this, predicate);
        }

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Properties(this, propertyKeys);
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            currentContext.Values(this, propertyKeys);
        }

        internal override void Aggregate(GremlinToSqlContext currentContext, string sideEffectKey, GremlinToSqlContext projectContext)
        {
            this.isTraversalToBound = true;
            base.Aggregate(currentContext, sideEffectKey, projectContext);
        }
        
        internal override void Coin(GremlinToSqlContext currentContext, double probability)
        {
            this.isTraversalToBound = true;
            base.Coin(currentContext, probability);
        }
        

        internal override void Dedup(GremlinToSqlContext currentContext, List<string> dedupLabels, GremlinToSqlContext dedupContext, GremlinKeyword.Scope scope)
        {
            this.isTraversalToBound = true;
            base.Dedup(currentContext, dedupLabels, dedupContext, scope);
        }

        internal override void Group(GremlinToSqlContext currentContext, string sideEffectKey, List<object> parameters)
        {
            if (sideEffectKey != null) this.isTraversalToBound = true;
            base.Group(currentContext, sideEffectKey, parameters);
        }

        internal override void Inject(GremlinToSqlContext currentContext, List<object> values)
        {
            this.isTraversalToBound = true;
            base.Inject(currentContext, values);
        }

        internal override void Order(GremlinToSqlContext currentContext, List<object> byList,
            List<IComparer> orderList, GremlinKeyword.Scope scope)
        {
            this.isTraversalToBound = true;
            base.Order(currentContext, byList, orderList, scope);
        }

        internal override void Property(GremlinToSqlContext currentContext, List<object> properties)
        {
            this.isTraversalToBound = true;
            base.Property(currentContext, properties);
        }

        internal override void Range(GremlinToSqlContext currentContext, int low, int high, GremlinKeyword.Scope scope, bool isReverse)
        {
            this.isTraversalToBound = true;
            base.Range(currentContext, low, high, scope, isReverse);
        }

        internal override void SideEffect(GremlinToSqlContext currentContext, GremlinToSqlContext sideEffectContext)
        {
            this.isTraversalToBound = true;
            base.SideEffect(currentContext, sideEffectContext);
        }

        internal override void Store(GremlinToSqlContext currentContext, string sideEffectKey, GremlinToSqlContext projectContext)
        {
            this.isTraversalToBound = true;
            base.Store(currentContext, sideEffectKey, projectContext);
        }

       
        internal override void Tree(GremlinToSqlContext currentContext, string sideEffectKey)
        {
            this.isTraversalToBound = true;
            base.Tree(currentContext, sideEffectKey);
        }
    }
}
