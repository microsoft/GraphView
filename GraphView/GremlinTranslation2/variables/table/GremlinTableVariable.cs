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
        public GremlinUpdatePropertiesVariable UpdateVariable { get; set; }

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

        internal override GremlinVariableProperty DefaultProjection()
        {
            //TODO
            return new GremlinVariableProperty(this, "id");
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Table;
        }

        public virtual WTableReference ToTableReference()
        {
            if (SqlTableVariable != null)
            {
                //if (ProjectedProperties.Count == 0)
                //{
                //    Populate(DefaultProjection().VariableProperty);
                //}
                return SqlTableVariable.ToTableReference(ProjectedProperties, VariableName, this);
            }
            else
            {
                throw  new NotImplementedException();
            }
        }

        internal override void Populate(string property)
        {
            if (!ProjectedProperties.Contains(property))
            {
                ProjectedProperties.Add(property);
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
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(adjEdge, adjReverseEdge, WEdgeType.BothEdge);
            bothEdge.Populate("_sink");
            currentContext.VariableList.Add(bothEdge);
            currentContext.TableReferences.Add(bothEdge);
            currentContext.AddLabelPredicateForEdge(bothEdge, edgeLabels);

            GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(new GremlinVariableProperty(bothEdge, "_sink"));
            currentContext.VariableList.Add(bothVertex);
            currentContext.TableReferences.Add(bothVertex);

            currentContext.AddPath(new GremlinMatchPath(this, bothEdge, bothVertex));

            currentContext.PivotVariable = bothVertex;
        }

        internal override void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate(GremlinKeyword.NodeID);
            Populate("_reverse_edge");
            Populate("_edge");

            GremlinVariableProperty sourceNode = new GremlinVariableProperty(this, GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = new GremlinVariableProperty(this, "_reverse_edge");
            GremlinVariableProperty adjEdge = new GremlinVariableProperty(this, "_edge");
            GremlinBoundEdgeVariable bothEdge = new GremlinBoundEdgeVariable(sourceNode, adjEdge, adjReverseEdge, WEdgeType.BothEdge);
            bothEdge.Populate("_source");
            bothEdge.Populate("_sink");
            currentContext.VariableList.Add(bothEdge);
            currentContext.TableReferences.Add(bothEdge);
            currentContext.AddLabelPredicateForEdge(bothEdge, edgeLabels);

            //currentContext.AddPath(new GremlinMatchPath(this, bothEdge, null));

            currentContext.PivotVariable = bothEdge;
        }

        internal override void BothV(GremlinToSqlContext currentContext)
        {
            if ((this as GremlinEdgeTableVariable).EdgeType == WEdgeType.BothEdge)
            {
                Populate("_sink");
                Populate("_source");
                GremlinVariableProperty sinkProperty = new GremlinVariableProperty(this, "_sink");
                GremlinVariableProperty sourceProperty = new GremlinVariableProperty(this, "_source");
                GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(sinkProperty, sourceProperty);
                currentContext.VariableList.Add(bothVertex);
                currentContext.TableReferences.Add(bothVertex);

                currentContext.PivotVariable = bothVertex;
            }
            else
            {
                Populate("_sink");
                var path = currentContext.GetPathFromPathList(this);

                GremlinVariableProperty sourceProperty = new GremlinVariableProperty(this, "_sink");
                GremlinVariableProperty sinkProperty = new GremlinVariableProperty(path.SourceVariable, "id");
                GremlinBoundVertexVariable newVertex = new GremlinBoundVertexVariable(sourceProperty, sinkProperty);

                currentContext.VariableList.Add(newVertex);
                currentContext.TableReferences.Add(newVertex);
                currentContext.PivotVariable = newVertex;
            }
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, object value)
        {
            Populate(propertyKey);
            WScalarExpression firstExpr = SqlUtil.GetColumnReferenceExpr(VariableName, propertyKey);
            WScalarExpression secondExpr = SqlUtil.GetValueExpr(value);
            currentContext.AddPredicate(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, object value)
        {
            Has(currentContext, GremlinKeyword.Label, label);
            Has(currentContext, propertyKey, value);
        }

        internal override void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
            Populate(propertyKey);
            WScalarExpression firstExpr = SqlUtil.GetColumnReferenceExpr(VariableName, propertyKey);
            currentContext.AddPredicate(SqlUtil.GetBooleanComparisonExpr(firstExpr, null, predicate));
        }

        internal override void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Predicate predicate)
        {
            Has(currentContext, GremlinKeyword.Label, label);
            Has(currentContext, propertyKey, predicate);
        }

        internal override void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("_reverse_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "_reverse_edge");
            GremlinBoundEdgeVariable inEdge = new GremlinBoundEdgeVariable(adjacencyList, WEdgeType.InEdge);
            inEdge.Populate("_sink");
            currentContext.VariableList.Add(inEdge);
            currentContext.TableReferences.Add(inEdge);
            currentContext.AddLabelPredicateForEdge(inEdge, edgeLabels);

            GremlinVariableProperty inEdgeProperty = new GremlinVariableProperty(inEdge, "_sink");
            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(inEdgeProperty);
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);

            currentContext.AddPath(new GremlinMatchPath(this, inEdge, outVertex));

            currentContext.PivotVariable = outVertex;
        }

        internal override void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("_reverse_edge");

            GremlinVariableProperty sourceProperty = new GremlinVariableProperty(this, GremlinKeyword.NodeID);
            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "_reverse_edge");
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjacencyList, WEdgeType.InEdge);
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            currentContext.AddPath(new GremlinMatchPath(this, outEdge, null));

            currentContext.PivotVariable = outEdge;
        }

        internal override void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("_edge");

            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "_edge");
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(adjacencyList, WEdgeType.OutEdge);
            outEdge.Populate("_sink");
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            GremlinVariableProperty outEdgeSink = new GremlinVariableProperty(outEdge, "_sink");
            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(outEdgeSink);
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);

            currentContext.AddPath(new GremlinMatchPath(this, outEdge, outVertex));

            currentContext.PivotVariable = outVertex;
        }

        internal override void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            Populate("_edge");

            GremlinVariableProperty sourceProperty = new GremlinVariableProperty(this, GremlinKeyword.NodeID);
            GremlinVariableProperty adjacencyList = new GremlinVariableProperty(this, "_edge");
            GremlinBoundEdgeVariable outEdge = new GremlinBoundEdgeVariable(sourceProperty, adjacencyList, WEdgeType.OutEdge);
            currentContext.VariableList.Add(outEdge);
            currentContext.TableReferences.Add(outEdge);
            currentContext.AddLabelPredicateForEdge(outEdge, edgeLabels);

            currentContext.AddPath(new GremlinMatchPath(this, outEdge, null));

            currentContext.PivotVariable = outEdge;
        }

        internal override void InV(GremlinToSqlContext currentContext)
        {
            if ((this as GremlinEdgeTableVariable).EdgeType == WEdgeType.BothEdge)
            {
                Populate("_sink");

                GremlinVariableProperty sinkProperty = new GremlinVariableProperty(this, "_sink");
                GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(sinkProperty);
                currentContext.VariableList.Add(outVertex);
                currentContext.TableReferences.Add(outVertex);

                //currentContext.AddPath(new GremlinMatchPath(this, outEdge, outVertex));

                currentContext.PivotVariable = outVertex;
            }
            else
            {
                GremlinTableVariable inVertex = currentContext.GetSinkVertex(this);
                if (inVertex == null)
                {
                    //It's a forward edge, so the _sink points to the sink vertex
                    // n_0->[edge as e_0]
                    Populate("_sink");
                    GremlinVariableProperty sinkProperty = new GremlinVariableProperty(this, "_sink");
                    GremlinBoundVertexVariable newVertex = new GremlinBoundVertexVariable(sinkProperty);

                    var path = currentContext.GetPathFromPathList(this);
                    if (path != null) path.SetSinkVariable(newVertex);

                    currentContext.VariableList.Add(newVertex);
                    currentContext.TableReferences.Add(newVertex);
                    currentContext.PivotVariable = newVertex;
                }
                else
                {   //It's a reversed edge, so the _sink points to the sink vertex
                    // n_0<-[edge as e_0]
                    if (currentContext.IsVariableInCurrentContext(inVertex))
                    {
                        currentContext.PivotVariable = inVertex;
                    }
                    else
                    {
                        GremlinContextVertexVariable newVariable = new GremlinContextVertexVariable(inVertex);
                        currentContext.VariableList.Add(newVariable);
                        currentContext.PivotVariable = newVariable;
                    }
                }
            }
        }

        internal override void OutV(GremlinToSqlContext currentContext)
        {
            // A naive implementation would be: add a new bound/free vertex to the variable list. 
            // A better implementation should reason the status of the edge variable and only reset
            // the pivot variable if possible, thereby avoiding adding a new vertex variable  
            // and reducing one join.
            if ((this as GremlinEdgeTableVariable).EdgeType == WEdgeType.BothEdge)
            {
                Populate("_source");
                GremlinVariableProperty sourceProperty = new GremlinVariableProperty(this, "_source");
                GremlinBoundVertexVariable outVertex =  new GremlinBoundVertexVariable(sourceProperty);
                currentContext.VariableList.Add(outVertex);
                currentContext.TableReferences.Add(outVertex);

                //currentContext.AddPath(new GremlinMatchPath(this, outEdge, outVertex));

                currentContext.PivotVariable = outVertex;
            }
            else
            {
                GremlinTableVariable outVertex = currentContext.GetSourceVertex(this);
                if (outVertex == null)
                {
                    //It's a reversed edge, so the _sink points to the source vertex
                    // n_1<-[edge as e_0]
                    Populate("_sink");
                    GremlinVariableProperty sinkProperty = new GremlinVariableProperty(this, "_sink");
                    GremlinBoundVertexVariable newVertex = new GremlinBoundVertexVariable(sinkProperty);

                    var path = currentContext.GetPathFromPathList(this);
                    if (path != null) path.SetSourceVariable(newVertex);

                    currentContext.VariableList.Add(newVertex);
                    currentContext.TableReferences.Add(newVertex);
                    currentContext.PivotVariable = newVertex;
                }
                else
                {
                    //It's a forward edge, so the _sink points to the sink vertex
                    // n_0->[edge as e_0]
                    if (currentContext.IsVariableInCurrentContext(outVertex))
                    {
                        currentContext.PivotVariable = outVertex;
                    }
                    else
                    {
                        GremlinContextVertexVariable newVariable = new GremlinContextVertexVariable(outVertex);
                        currentContext.VariableList.Add(newVariable);
                        currentContext.PivotVariable = newVariable;
                    }
                }
            }
        }

        internal override void OtherV(GremlinToSqlContext currentContext)
        {
            if ((this as GremlinEdgeTableVariable).EdgeType == WEdgeType.BothEdge)
            {
                Populate("_other");

                GremlinVariableProperty otherProperty = new GremlinVariableProperty(this, "_other");
                GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(otherProperty);
                currentContext.VariableList.Add(outVertex);
                currentContext.TableReferences.Add(outVertex);

                currentContext.PivotVariable = outVertex;
            }
            else
            {
                var path = currentContext.GetPathFromPathList(this);
                var edge = (path.EdgeVariable as GremlinEdgeTableVariable).EdgeType;

                if (path == null)
                {
                    throw new QueryCompilationException("Can't find a path");
                }

                if (edge == WEdgeType.InEdge)
                {
                    OutV(currentContext);
                }
                else if (edge == WEdgeType.OutEdge)
                {
                    InV(currentContext);
                }
                else if (edge == WEdgeType.BothEdge)
                {
                    throw new QueryCompilationException();
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
        }

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            foreach (var property in propertyKeys)
            {
                Populate(property);
            }
            GremlinPropertiesVariable newVariable = new GremlinPropertiesVariable(this, propertyKeys);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }

        internal override void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            if (propertyKeys.Count == 1)
            {
                Populate(propertyKeys.First());
                GremlinVariableProperty newVariableProperty = new GremlinVariableProperty(this, propertyKeys.First());
                currentContext.VariableList.Add(newVariableProperty);
                currentContext.PivotVariable = newVariableProperty;
            }
            else
            {
                foreach (var property in propertyKeys)
                {
                    Populate(property);
                }
                GremlinValuesVariable newVariable = new GremlinValuesVariable(this, propertyKeys);
                currentContext.VariableList.Add(newVariable);
                currentContext.TableReferences.Add(newVariable);
                currentContext.PivotVariable = newVariable;
            }
        }
    }

    internal abstract class GremlinScalarTableVariable : GremlinTableVariable
    {
        internal override GremlinVariableProperty DefaultProjection()
        {
            return new GremlinVariableProperty(this, GremlinKeyword.TableValue);
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }

        internal override void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            throw new QueryCompilationException("The OutV() step can only be applied to edges or vertex.");
        }
    }

    internal abstract class GremlinVertexTableVariable : GremlinTableVariable
    {
        protected static int _count = 0;

        internal override string GenerateTableAlias()
        {
            return "N_" + _count++;
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            return new GremlinVariableProperty(this, GremlinKeyword.NodeID);
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Vertex;
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            GremlinVariableProperty variableProperty = new GremlinVariableProperty(this, GremlinKeyword.NodeID);
            GremlinDropVertexVariable newVariable = new GremlinDropVertexVariable(variableProperty);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            if (UpdateVariable == null)
            {
                GremlinVariableProperty variableProperty = new GremlinVariableProperty(this, GremlinKeyword.NodeID);
                UpdateVariable = new GremlinUpdateNodePropertiesVariable(variableProperty, properties);
                currentContext.VariableList.Add(UpdateVariable);
                currentContext.TableReferences.Add(UpdateVariable);
            }
            else
            {
                UpdateVariable.Property(currentContext, properties);
            }
        }
        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            foreach (var value in values)
            {
                Has(currentContext, GremlinKeyword.NodeID, value);
            }
        }
    }

    internal abstract class GremlinEdgeTableVariable : GremlinTableVariable
    {
        protected static int _count = 0;

        internal override string GenerateTableAlias()
        {
            return "E_" + _count++;
        }

        public WEdgeType EdgeType { get; set; }

        internal override GremlinVariableProperty DefaultProjection()
        {
            return new GremlinVariableProperty(this, GremlinKeyword.EdgeID);
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            var variableProperty = currentContext.GetSourceVariableProperty(this);
            var edgeProperty = currentContext.GetEdgeVariableProperty(this);
            GremlinDropEdgeVariable newVariable = new GremlinDropEdgeVariable(variableProperty, edgeProperty);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }

        internal override void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            if (UpdateVariable == null)
            {
                var nodeProperty = currentContext.GetSourceVariableProperty(this);
                var edgeProperty = currentContext.GetEdgeVariableProperty(this);
                UpdateVariable = new GremlinUpdateEdgePropertiesVariable(nodeProperty, edgeProperty, properties);
                currentContext.VariableList.Add(UpdateVariable);
                currentContext.TableReferences.Add(UpdateVariable);
            }
            else
            {
                UpdateVariable.Property(currentContext, properties);
            }
        }

        internal override void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            foreach (var value in values)
            {
                Has(currentContext, GremlinKeyword.EdgeID, value);
            }
        }
    }
}
