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

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            PropertyKeys.Add(vertexId.ToScalarExpression());
            PropertyKeys.Add(GremlinUtil.GetValueExpression("id"));
            foreach (var property in projectedProperties)
            {
                PropertyKeys.Add(GremlinUtil.GetValueExpression(property));
            }
            var secondTableRef = GremlinUtil.GetSchemaObjectFunctionTableReference("V", PropertyKeys);
            secondTableRef.Alias = GremlinUtil.GetIdentifier(VariableName);
            return GremlinUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        public GremlinBoundVertexVariable(GremlinVariableProperty vertexId)
        {
            VariableName = GenerateTableAlias();
            this.vertexId = vertexId;
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("BothAdjacencyList");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "BothAdjacencyList");
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(this, adjacencyList, WEdgeType.BothEdge);
            bothEdge.Populate("_sink");
            currentContext.VariableList.Add(bothEdge);

            GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(bothEdge, "_sink"));
            currentContext.VariableList.Add(bothVertex);

            currentContext.TableReferences.Add(bothEdge);
            currentContext.TableReferences.Add(bothVertex);

            //add Predicate to edge
            AddLabelPredicateToEdge(currentContext, bothEdge, edgeLabels);

            currentContext.PivotVariable = bothVertex;
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "_edge");
            GremlinEdgeVariable2 inEdge = new GremlinBoundEdgeVariable(this, adjacencyList);
            inEdge.Populate("_sink");
            currentContext.VariableList.Add(inEdge);

            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(inEdge, "_sink"));
            currentContext.VariableList.Add(outVertex);

            currentContext.TableReferences.Add(inEdge);
            currentContext.TableReferences.Add(outVertex);

            //add Predicate to edge
            AddLabelPredicateToEdge(currentContext, inEdge, edgeLabels);

            currentContext.PivotVariable = outVertex;
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("_reverse_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "_reverse_edge");
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(this, adjacencyList);
            currentContext.VariableList.Add(outEdge);

            currentContext.TableReferences.Add(outEdge);

            //add Predicate to edge
            AddLabelPredicateToEdge(currentContext, outEdge, edgeLabels);

            currentContext.PivotVariable = outEdge;
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "_edge");
            GremlinEdgeVariable2 outEdge = new GremlinBoundEdgeVariable(this, adjacencyList);
            outEdge.Populate("_sink");
            currentContext.VariableList.Add(outEdge);

            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(outEdge, "_sink"));
            currentContext.VariableList.Add(outVertex);

            currentContext.TableReferences.Add(outEdge);
            currentContext.TableReferences.Add(outVertex);

            //add Predicate to edge
            AddLabelPredicateToEdge(currentContext, outEdge, edgeLabels);

            currentContext.PivotVariable = outVertex;
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "_edge");
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(this, adjacencyList);
            currentContext.VariableList.Add(outEdge);

            currentContext.TableReferences.Add(outEdge);

            //add Predicate to edge
            AddLabelPredicateToEdge(currentContext, outEdge, edgeLabels);

            currentContext.PivotVariable = outEdge;
        }
    }
}
