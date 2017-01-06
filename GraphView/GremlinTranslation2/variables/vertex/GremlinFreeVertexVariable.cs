using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFreeVertexVariable : GremlinVertexVariable
    {
        public GremlinFreeVertexVariable()
        {
            VariableName = GenerateTableAlias();
        }

        public override WTableReference ToTableReference()
        {
            return SqlUtil.GetNamedTableReference(this);
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinEdgeVariable bothEdgeVar = new GremlinBoundEdgeVariable(this, new GremlinVariableProperty(this, "BothAdjacencyList"), WEdgeType.BothEdge);
            currentContext.VariableList.Add(bothEdgeVar);
            GremlinFreeVertexVariable bothVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(bothVertex);

            // In this case, the both-edge variable is not added to the table-reference list. 
            // Instead, we populate a path this_variable-[bothEdge]->bothVertex in the context
            currentContext.TableReferences.Add(bothVertex);
            currentContext.Paths.Add(new GremlinMatchPath(this, bothEdgeVar, bothVertex));
            //add Predicate to edge
            foreach (var edgeLabel in edgeLabels)
            {
                var firstExpr = SqlUtil.GetColumnReferenceExpr(bothEdgeVar.VariableName, "label");
                var secondExpr = SqlUtil.GetValueExpr(edgeLabel);
                currentContext.AddPredicate(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
            }

            currentContext.PivotVariable = bothVertex;
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinEdgeVariable inEdgeVar = new GremlinBoundEdgeVariable(this, new GremlinVariableProperty(this, "BothAdjacencyList"));
            currentContext.VariableList.Add(inEdgeVar);
            GremlinFreeVertexVariable inVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(inVertex);

            currentContext.TableReferences.Add(inVertex);
            currentContext.Paths.Add(new GremlinMatchPath(inVertex, inEdgeVar, this));
            //add Predicate to edge
            foreach (var edgeLabel in edgeLabels)
            {
                var firstExpr = SqlUtil.GetColumnReferenceExpr(inEdgeVar.VariableName, "label");
                var secondExpr = SqlUtil.GetValueExpr(edgeLabel);
                currentContext.AddPredicate(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
            }

            currentContext.PivotVariable = inVertex;
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinEdgeVariable inEdgeVar = new GremlinBoundEdgeVariable(this, new GremlinVariableProperty(this, "BothAdjacencyList"));
            currentContext.VariableList.Add(inEdgeVar);

            currentContext.Paths.Add(new GremlinMatchPath(null, inEdgeVar, this));
            //add Predicate to edge
            foreach (var edgeLabel in edgeLabels)
            {
                var firstExpr = SqlUtil.GetColumnReferenceExpr(inEdgeVar.VariableName, "label");
                var secondExpr = SqlUtil.GetValueExpr(edgeLabel);
                currentContext.AddPredicate(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
            }

            currentContext.PivotVariable = inEdgeVar;
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinEdgeVariable outEdgeVar = new GremlinBoundEdgeVariable(this, new GremlinVariableProperty(this, "BothAdjacencyList"));
            currentContext.VariableList.Add(outEdgeVar);
            GremlinFreeVertexVariable outVertex = new GremlinFreeVertexVariable();
            currentContext.VariableList.Add(outVertex);

            currentContext.TableReferences.Add(outVertex);
            currentContext.Paths.Add(new GremlinMatchPath(this, outEdgeVar, outVertex));
            //add Predicate to edge
            foreach (var edgeLabel in edgeLabels)
            {
                var firstExpr = SqlUtil.GetColumnReferenceExpr(outEdgeVar.VariableName, "label");
                var secondExpr = SqlUtil.GetValueExpr(edgeLabel);
                currentContext.AddPredicate(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
            }

            currentContext.PivotVariable = outVertex;
        }
        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinEdgeVariable outEdgeVar = new GremlinBoundEdgeVariable(this, new GremlinVariableProperty(this, "BothAdjacencyList"));
            currentContext.VariableList.Add(outEdgeVar);

            currentContext.Paths.Add(new GremlinMatchPath(this, outEdgeVar, null));
            //add Predicate to edge
            foreach (var edgeLabel in edgeLabels)
            {
                var firstExpr = SqlUtil.GetColumnReferenceExpr(outEdgeVar.VariableName, "label");
                var secondExpr = SqlUtil.GetValueExpr(edgeLabel);
                currentContext.AddPredicate(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
            }

            currentContext.PivotVariable = outEdgeVar;
        }

        //internal override void Where(GremlinToSqlContext currentContext, Predicate predicate)
        //{
        //    WScalarExpression key = SqlUtil.GetColumnReferenceExpression(VariableName, "id");
        //    WBooleanExpression booleanExpr = SqlUtil.GetBooleanComparisonExpr(key, predicate);
        //    currentContext.AddPredicate(booleanExpr);
        //}
    }
}
