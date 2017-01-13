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
            GremlinFreeEdgeVariable bothEdge = new GremlinFreeEdgeVariable(WEdgeType.BothEdge);
            currentContext.VariableList.Add(bothEdge);
            currentContext.AddLabelPredicateForEdge(bothEdge, edgeLabels);

            GremlinFreeVertexVariable bothVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(bothVertex);

            // In this case, the both-edge variable is not added to the table-reference list. 
            // Instead, we populate a path this_variable-[bothEdge]->bothVertex in the context
            currentContext.TableReferences.Add(bothVertex);
            currentContext.AddPath(new GremlinMatchPath(this, bothEdge, bothVertex));
            currentContext.SetPivotVariable(bothVertex);
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinFreeEdgeVariable inEdge = new GremlinFreeEdgeVariable(WEdgeType.InEdge);
            currentContext.VariableList.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

            GremlinFreeVertexVariable outVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);
            currentContext.AddPath(new GremlinMatchPath(this, inEdge, outVertex));
            currentContext.SetPivotVariable(outVertex);
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinFreeEdgeVariable inEdge = new GremlinFreeEdgeVariable(WEdgeType.InEdge);
            currentContext.VariableList.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);
            currentContext.AddPath(new GremlinMatchPath(this, inEdge, null));
            currentContext.SetPivotVariable(inEdge);
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinFreeEdgeVariable outEdge = new GremlinFreeEdgeVariable(WEdgeType.OutEdge);
            currentContext.VariableList.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            GremlinFreeVertexVariable inVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(inVertex);
            currentContext.TableReferences.Add(inVertex);
            currentContext.AddPath(new GremlinMatchPath(this, outEdge, inVertex));
            currentContext.SetPivotVariable(inVertex);
        }
        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinFreeEdgeVariable outEdgeVar = new GremlinFreeEdgeVariable(WEdgeType.OutEdge);
            currentContext.VariableList.Add(outEdgeVar);
            currentContext.AddLabelPredicateForEdge(outEdgeVar, edgeLabels);
            currentContext.AddPath(new GremlinMatchPath(this, outEdgeVar, null));
            currentContext.SetPivotVariable(outEdgeVar);
        }
    }
}
