using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.GremlinTranslation.DEMO;

namespace GraphView
{
    internal abstract class GremlinTableVariable : GremlinVariable
    {
        protected static int _count = 0;

        internal virtual string GenerateTableAlias()
        {
            return "R_" + _count++;
        }

        public List<string> ProjectedProperties { get; set; }
        public GremlinSqlTableVariable SqlTableVariable { get; set; }

        public GremlinTableVariable()
        {
            ProjectedProperties = new List<string>();
            VariableName = GenerateTableAlias();
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }

        public virtual WTableReference ToTableReference()
        {
            return SqlTableVariable?.ToTableReference(ProjectedProperties, VariableName);
        }

        internal override void Populate(string property)
        {
            if (!ProjectedProperties.Contains(property))
            {
                ProjectedProperties.Add(property);
            }
            if (!UsedProperties.Contains(property))
            {
                UsedProperties.Add(property);
            }
            SqlTableVariable?.Populate(property);
        }

        internal override void Range(GremlinToSqlContext currentContext, int low, int high)
        {
            Low = low;
            High = high;
        }

        internal override void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("_reverse_edge");
            Populate("_edge");

            GremlinVariableProperty adjReverseEdge = new GremlinVariableProperty(this, "_reverse_edge");
            GremlinVariableProperty adjEdge = new GremlinVariableProperty(this, "_edge");
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(this, adjEdge, adjReverseEdge, WEdgeType.BothEdge);
            bothEdge.Populate("_sink");
            currentContext.VariableList.Add(bothEdge);
            currentContext.TableReferences.Add(bothEdge);
            currentContext.AddLabelPredicateForEdge(bothEdge, edgeLabels);

            GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(bothEdge, "_sink"));
            currentContext.VariableList.Add(bothVertex);
            currentContext.TableReferences.Add(bothVertex);
            currentContext.PivotVariable = bothVertex;
        }

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("_reverse_edge");
            Populate("_edge");

            GremlinVariableProperty adjReverseEdge = new GremlinVariableProperty(this, "_reverse_edge");
            GremlinVariableProperty adjEdge = new GremlinVariableProperty(this, "_edge");
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(this, adjEdge, adjReverseEdge, WEdgeType.BothEdge);
            bothEdge.Populate("_sink");
            currentContext.VariableList.Add(bothEdge);
            currentContext.TableReferences.Add(bothEdge);
            currentContext.AddLabelPredicateForEdge(bothEdge, edgeLabels);
            currentContext.PivotVariable = bothEdge;
        }


        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "_edge");
            GremlinEdgeVariable inEdge = new GremlinBoundEdgeVariable(this, adjacencyList);
            inEdge.Populate("_sink");
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(inEdge, "_sink"));
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);
            currentContext.PivotVariable = outVertex;
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("_reverse_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "_reverse_edge");
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(this, adjacencyList);
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);
            currentContext.PivotVariable = outEdge;
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            GremlinVariable inVertex = currentContext.GetSinkVertex(this);
            if (inVertex == null)
            {
                //var path = currentContext.Paths.Find(p => p.EdgeVariable == this);
                //if (path != null)
                //{
                //    GremlinFreeVertexVariable newVertex = new GremlinFreeVertexVariable();
                //    path.SinkVariable = newVertex;
                //    currentContext.TableReferences.Add(newVertex);
                //    currentContext.VariableList.Add(newVertex);
                //    currentContext.PivotVariable = newVertex;
                //}
                //else
                //{
                    Populate("_sink");
                    GremlinBoundVertexVariable newVertex =
                        new GremlinBoundVertexVariable(new GremlinVariableProperty(this, "_sink"));
                    currentContext.VariableList.Add(newVertex);
                    currentContext.TableReferences.Add(newVertex);
                    currentContext.PivotVariable = newVertex;
                //}
            }
            else
            {
                currentContext.PivotVariable = inVertex;
            }
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "_edge");
            GremlinEdgeVariable outEdge = new GremlinBoundEdgeVariable(this, adjacencyList);
            outEdge.Populate("_sink");
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(outEdge, "_sink"));
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);
            currentContext.PivotVariable = outVertex;
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "_edge");
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(this, adjacencyList);
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);
            currentContext.PivotVariable = outEdge;
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            // A naive implementation would be: add a new bound/free vertex to the variable list. 
            // A better implementation should reason the status of the edge variable and only reset
            // the pivot variable if possible, thereby avoiding adding a new vertex variable  
            // and reducing one join.
            GremlinVariable outVertex = currentContext.GetSourceVertex(this);
            if (outVertex == null)
            {
                Populate("_sink");
                GremlinBoundVertexVariable newVertex =
                    new GremlinBoundVertexVariable(new GremlinVariableProperty(this, "_sink"));
                currentContext.VariableList.Add(newVertex);
                currentContext.TableReferences.Add(newVertex);
                currentContext.PivotVariable = newVertex;
            }
            else
            {
                currentContext.PivotVariable = outVertex;
            }
        }
    }

    internal abstract class GremlinScalarTableVariable : GremlinTableVariable
    {
        internal override GremlinScalarVariable DefaultProjection()
        {
            return new GremlinVariableProperty(this, "_value");
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }
    }

    internal abstract class GremlinVertexTableVariable : GremlinTableVariable
    {
        internal override GremlinScalarVariable DefaultProjection()
        {
            return new GremlinVariableProperty(this, "id");
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Vertex;
        }
    }

    internal abstract class GremlinEdgeTableVariable : GremlinTableVariable
    {
        internal override GremlinScalarVariable DefaultProjection()
        {
            return new GremlinVariableProperty(this, "id");
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }
    }
}
