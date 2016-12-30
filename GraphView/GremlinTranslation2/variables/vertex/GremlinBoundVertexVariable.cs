using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    /// <summary>
    /// A free vertex variable is translated to a node table reference in 
    /// the FROM clause, whereas a bound vertex variable is translated into
    /// a table-valued function following a prior table-valued function producing vertex references. 
    /// </summary>
    internal class GremlinBoundVertexVariable : GremlinVertexVariable2
    {
        private GremlinVariableProperty vertexId;
        private List<string> projectedProperties;

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            PropertyKeys.Add(vertexId.ToScalarExpression());
            var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("V", PropertyKeys);
            secondTableRef.Alias = GremlinUtil.GetIdentifier(VariableName);
            return GremlinUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        public GremlinBoundVertexVariable(GremlinVariableProperty vertexId)
        {
            VariableName = GenerateTableAlias();
            this.vertexId = vertexId;
            projectedProperties = new List<string>();
        }

        internal override GremlinScalarVariable DefaultProjection()
        {
            return new GremlinVariableProperty(this, "id");
        }

        internal override void Populate(string name)
        {
            if (!projectedProperties.Contains(name))
            {
                projectedProperties.Add(name);
            }
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("BothAdjacencyList");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "BothAdjacencyList");
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(adjacencyList);
            bothEdge.Populate("_sink");
            currentContext.VariableList.Add(bothEdge);

            GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(bothEdge, "_sink"));
            currentContext.VariableList.Add(bothVertex);

            currentContext.TableReferences.Add(bothEdge);
            currentContext.TableReferences.Add(bothVertex);

            //add Predicate to edge
            foreach (var edgeLabel in edgeLabels)
            {
                var firstExpr = GremlinUtil.GetColumnReferenceExpression(bothEdge.VariableName, "label");
                var secondExpr = GremlinUtil.GetValueExpression(edgeLabel);
                currentContext.AddEqualPredicate(firstExpr, secondExpr);
            }

            currentContext.PivotVariable = bothVertex;
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "_edge");
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(adjacencyList);
            currentContext.VariableList.Add(outEdge);

            currentContext.TableReferences.Add(outEdge);

            //add Predicate to edge
            foreach (var edgeLabel in edgeLabels)
            {
                var firstExpr = GremlinUtil.GetColumnReferenceExpression(outEdge.VariableName, "label");
                var secondExpr = GremlinUtil.GetValueExpression(edgeLabel);
                currentContext.AddEqualPredicate(firstExpr, secondExpr);
            }

            currentContext.PivotVariable = outEdge;
        }
    }
}
