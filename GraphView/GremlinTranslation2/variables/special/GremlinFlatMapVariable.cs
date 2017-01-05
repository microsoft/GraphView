using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinFlatMapVariable: GremlinTableVariable
    {
        public GremlinToSqlContext FlatMapContext;

        public static GremlinFlatMapVariable Create(GremlinToSqlContext flatMapContext)
        {
            switch (flatMapContext.PivotVariable.GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    return new GremlinFlatMapVertexVariable(flatMapContext);
                case GremlinVariableType.Edge:
                    return new GremlinFlatMapEdgeVariable(flatMapContext);
                case GremlinVariableType.Scalar:
                    throw new NotImplementedException();
                case GremlinVariableType.Table:
                    throw new NotImplementedException();
            }
            throw new NotImplementedException();
        }

        public GremlinFlatMapVariable(GremlinToSqlContext flatMapContext)
        {
            FlatMapContext = flatMapContext;
            VariableName = GenerateTableAlias();
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            PropertyKeys.Add(GremlinUtil.GetScalarSubquery(FlatMapContext.ToSelectQueryBlock(ProjectedProperties)));
            var secondTableRef = GremlinUtil.GetFunctionTableReference("flatMap", PropertyKeys, VariableName);

            return GremlinUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinFlatMapVertexVariable : GremlinFlatMapVariable
    {
        public GremlinFlatMapVertexVariable(GremlinToSqlContext flatMapContext) : base(flatMapContext) { }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Vertex;
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
    }

    internal class GremlinFlatMapEdgeVariable : GremlinFlatMapVariable
    {
        public GremlinFlatMapEdgeVariable(GremlinToSqlContext flatMapContext): base(flatMapContext) { }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }

    }
}
