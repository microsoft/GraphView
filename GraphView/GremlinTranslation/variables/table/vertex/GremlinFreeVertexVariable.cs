using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFreeVertexVariable : GremlinVertexTableVariable
    {
        public override WTableReference ToTableReference()
        {
            return SqlUtil.GetNamedTableReference(this);
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
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
            GremlinFreeEdgeTableVariable inEdgeTable = new GremlinFreeEdgeTableVariable(WEdgeType.InEdge);
            currentContext.VariableList.Add(inEdgeTable);
            currentContext.AddLabelPredicateForEdge(inEdgeTable, edgeLabels);
            currentContext.AddPath(new GremlinMatchPath(null, inEdgeTable, this));
            currentContext.SetPivotVariable(inEdgeTable);
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
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
    }
}
