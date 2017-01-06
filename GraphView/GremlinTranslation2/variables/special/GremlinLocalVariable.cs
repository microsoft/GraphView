using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinLocalVariable : GremlinTableVariable
    {
        public GremlinToSqlContext LocalContext;

        public static GremlinLocalVariable Create(GremlinToSqlContext localContext)
        {
            switch (localContext.PivotVariable.GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    return new GremlinLocalVertexVariable(localContext);
                case GremlinVariableType.Edge:
                    return new GremlinLocalEdgeVariable(localContext);
                case GremlinVariableType.Scalar:
                    throw new NotImplementedException();
                case GremlinVariableType.Table:
                    throw new NotImplementedException();
            }
            throw new NotImplementedException();
        }

        public GremlinLocalVariable(GremlinToSqlContext localContext)
        {
            LocalContext = localContext;
            VariableName = GenerateTableAlias();
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> PropertyKeys = new List<WScalarExpression>();
            PropertyKeys.Add(GremlinUtil.GetScalarSubquery(LocalContext.ToSelectQueryBlock(ProjectedProperties)));
            var secondTableRef = GremlinUtil.GetFunctionTableReference("local", PropertyKeys, VariableName);

            return GremlinUtil.GetCrossApplyTableReference(null, secondTableRef);
        }
    }

    internal class GremlinLocalVertexVariable : GremlinLocalVariable
    {
        public GremlinLocalVertexVariable(GremlinToSqlContext localContext) : base(localContext) { }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Vertex;
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "_edge");
            GremlinEdgeVariable outEdge = new GremlinBoundEdgeVariable(this, adjacencyList);
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

    internal class GremlinLocalEdgeVariable : GremlinLocalVariable
    {
        public GremlinLocalEdgeVariable(GremlinToSqlContext localContext) : base(localContext) { }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }

    }
}
