using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using static GraphView.DocumentDBKeywords;

namespace GraphView
{
    partial class WCommitTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewCommand command)
        {
            CommitOperator commitOp = new CommitOperator(command, context.CurrentExecutionOperator);
            context.CurrentExecutionOperator = commitOp;

            return commitOp;
        }
    }
    
    partial class WAddVTableReference
    {
        public JObject ConstructNodeJsonDocument(GraphViewCommand command, string vertexLabel)
        {
            JObject vertexObject = new JObject
            {
                [KW_VERTEX_LABEL] = vertexLabel,
            };

            if (command.Connection.EdgeSpillThreshold == 1) {
                vertexObject[KW_VERTEX_EDGE] = new JArray { KW_VERTEX_DUMMY_EDGE };
                vertexObject[KW_VERTEX_REV_EDGE] = new JArray { KW_VERTEX_DUMMY_EDGE };
                vertexObject[KW_VERTEX_EDGE_SPILLED] = true;
                vertexObject[KW_VERTEX_REVEDGE_SPILLED] = true;
            }
            else {
                vertexObject[KW_VERTEX_EDGE] = new JArray();
                vertexObject[KW_VERTEX_REV_EDGE] = new JArray();
                vertexObject[KW_VERTEX_EDGE_SPILLED] = false;
                vertexObject[KW_VERTEX_REVEDGE_SPILLED] = false;
            }

            return vertexObject;
        }

        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewCommand command)
        {
            //
            // Parameters:
            //   #1 <WValueExpression>: Vertex label
            //   ... <WPropertyExpression>: The initial properties on vertex
            //

            WValueExpression labelValue = (WValueExpression)this.Parameters[0];
            Debug.Assert(labelValue.Value != null, "[WAddVTableReference.Compile] Vertex label should not be null");

            List<PropertyTuple> vertexProperties = new List<PropertyTuple>();

            List<string> projectedField = new List<string>(GraphViewReservedProperties.InitialPopulateNodeProperties);
            projectedField.Add(GremlinKeyword.Star);
            projectedField.Add(GremlinKeyword.Label);
            

            for (int i = 1; i < this.Parameters.Count; i++) {
                WPropertyExpression property = (WPropertyExpression)this.Parameters[i];
                Debug.Assert(property != null, "[WAddVTableReference.Compile] Vertex property should not be null");
                Debug.Assert(property.Cardinality == GremlinKeyword.PropertyCardinality.List, "[WAddVTableReference.Compile] Vertex property should be append-mode");
                Debug.Assert(property.Value != null);

                if (!projectedField.Contains(property.Key.Value))
                {
                    projectedField.Add(property.Key.Value);
                }

                if (property.Value is WValueExpression)
                {
                    WValueExpression value = property.Value as WValueExpression;
                    Dictionary<string, Tuple<StringField, ScalarSubqueryFunction>> meta = new Dictionary<string, Tuple<StringField, ScalarSubqueryFunction>>();
                    foreach (KeyValuePair<WValueExpression, WScalarExpression> pair in property.MetaProperties)
                    {
                        string name = pair.Key.Value;
                        if (pair.Value is WValueExpression)
                        {
                            WValueExpression metaValue = pair.Value as WValueExpression;
                            meta.Add(name, new Tuple<StringField, ScalarSubqueryFunction>(metaValue.ToStringField(), null));
                        }
                        else
                        {
                            WScalarSubquery metaScalarSubquery = pair.Value as WScalarSubquery;
                            ScalarSubqueryFunction metaValueFunction = (ScalarSubqueryFunction)metaScalarSubquery.CompileToFunction(context, command);
                            meta.Add(name, new Tuple<StringField, ScalarSubqueryFunction>(null, metaValueFunction));
                        }
                    }

                    PropertyTuple valueProperty = new PropertyTuple(property.Cardinality, property.Key.Value, value.ToStringField(), meta);
                    vertexProperties.Add(valueProperty);
                }
                else
                {
                    WScalarSubquery scalarSubquery = property.Value as WScalarSubquery;
                    ScalarSubqueryFunction valueFunction = (ScalarSubqueryFunction)scalarSubquery.CompileToFunction(context, command);

                    Dictionary<string, Tuple<StringField, ScalarSubqueryFunction>> meta = new Dictionary<string, Tuple<StringField, ScalarSubqueryFunction>>();
                    foreach (KeyValuePair<WValueExpression, WScalarExpression> pair in property.MetaProperties)
                    {
                        string name = pair.Key.Value;
                        if (pair.Value is WValueExpression)
                        {
                            WValueExpression metaValue = pair.Value as WValueExpression;
                            meta.Add(name, new Tuple<StringField, ScalarSubqueryFunction>(metaValue.ToStringField(), null));
                        }
                        else
                        {
                            WScalarSubquery metaScalarSubquery = pair.Value as WScalarSubquery;
                            ScalarSubqueryFunction metaValueFunction = (ScalarSubqueryFunction)metaScalarSubquery.CompileToFunction(context, command);
                            meta.Add(name, new Tuple<StringField, ScalarSubqueryFunction>(null, metaValueFunction));
                        }
                    }

                    PropertyTuple valueProperty = new PropertyTuple(property.Cardinality, property.Key.Value, valueFunction, meta);
                    vertexProperties.Add(valueProperty);
                }
            }

            JObject nodeJsonDocument = ConstructNodeJsonDocument(command, labelValue.Value);

            AddVOperator addVOp = new AddVOperator(
                context.CurrentExecutionOperator,
                command,
                nodeJsonDocument,
                projectedField,
                vertexProperties);
            context.CurrentExecutionOperator = addVOp;

            for (int i = 0; i < projectedField.Count; i++)
            {
                string propertyName = projectedField[i];
                ColumnGraphType columnGraphType = GraphViewReservedProperties.IsNodeReservedProperty(propertyName)
                    ? GraphViewReservedProperties.ReservedNodePropertiesColumnGraphTypes[propertyName]
                    : ColumnGraphType.Value;
                context.AddField(Alias.Value, propertyName, columnGraphType);
            }

            // Convert the connection to Hybrid if necessary
            if (command.Connection.GraphType != GraphType.GraphAPIOnly) {
                command.Connection.GraphType = GraphType.Hybrid;
            }

            return addVOp;
        }
    }
    

    partial class WAddETableReference
    {
        /// <summary>
        /// </summary>
        /// <param name="command"></param>
        /// <param name="edgeLabel"></param>
        /// <param name="edgeProperties">All propertyValue is WValueExpression!</param>
        /// <returns></returns>
        public JObject ConstructEdgeJsonObject(GraphViewCommand command, string edgeLabel, List<WPropertyExpression> edgeProperties)
        {
            JObject edgeObject = new JObject
            {
                [KW_EDGE_LABEL] = edgeLabel
            };

            // Skip edgeSourceScalarFunction, edgeSinkScalarFunction, otherVTag
            foreach (WPropertyExpression edgeProperty in edgeProperties)
            {
                WValueExpression propertyValue = edgeProperty.Value as WValueExpression;
                GraphViewJsonCommand.UpdateProperty(edgeObject, edgeProperty.Key, propertyValue);
            }

            return edgeObject;
        }

        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewCommand command)
        {
            WScalarSubquery srcSubQuery = Parameters[0] as WScalarSubquery;
            WScalarSubquery sinkSubQuery = Parameters[1] as WScalarSubquery;
            if (srcSubQuery == null || sinkSubQuery == null)
                throw new SyntaxErrorException("The first two parameters of AddE can only be WScalarSubquery.");

            Container container = new Container();
            QueryCompilationContext srcSubContext = new QueryCompilationContext(context);
            srcSubContext.OuterContextOp.SetContainer(container);
            GraphViewExecutionOperator srcSubQueryOp = srcSubQuery.SubQueryExpr.Compile(srcSubContext, command);

            QueryCompilationContext sinkSubContext = new QueryCompilationContext(context);
            sinkSubContext.OuterContextOp.SetContainer(container);
            GraphViewExecutionOperator sinkSubQueryOp = sinkSubQuery.SubQueryExpr.Compile(sinkSubContext, command);

            WValueExpression otherVTagParameter = Parameters[2] as WValueExpression;
            Debug.Assert(otherVTagParameter != null, "otherVTagParameter != null");
            //
            // if otherVTag == 0, this newly added edge's otherV() is the src vertex.
            // Otherwise, it's the sink vertex
            //
            int otherVTag = int.Parse(otherVTagParameter.Value);

            WValueExpression labelValue = (WValueExpression)this.Parameters[3];

            List<WPropertyExpression> edgeProperties = new List<WPropertyExpression>();
            List<PropertyTuple> subtraversalProperties = new List<PropertyTuple>();

            List<string> projectedField = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties);
            projectedField.Add(GremlinKeyword.Label);

            for (int i = 4; i < this.Parameters.Count; i++)
            {
                WPropertyExpression property = (WPropertyExpression)this.Parameters[i];
                Debug.Assert(property != null, "[WAddETableReference.Compile] Edge property should not be null");
                Debug.Assert(property.Cardinality == GremlinKeyword.PropertyCardinality.Single, "[WAddETableReference.Compile] Edge property should not be append-mode");
                Debug.Assert(property.Value != null);

                if (!projectedField.Contains(property.Key.Value))
                {
                    projectedField.Add(property.Key.Value);
                }

                if (property.Value is WValueExpression)
                {
                    edgeProperties.Add(property);
                }
                else
                {
                    WScalarSubquery scalarSubquery = property.Value as WScalarSubquery;
                    ScalarSubqueryFunction valueFunction = (ScalarSubqueryFunction)scalarSubquery.CompileToFunction(context, command);
                    subtraversalProperties.Add(new PropertyTuple(property.Cardinality, property.Key.Value, valueFunction));
                }
            }

            JObject edgeJsonObject = ConstructEdgeJsonObject(command, labelValue.Value, edgeProperties);  // metadata remains missing

            GraphViewExecutionOperator addEOp = new AddEOperator(context.CurrentExecutionOperator, command, container,
                srcSubQueryOp, sinkSubQueryOp, otherVTag, edgeJsonObject, projectedField, subtraversalProperties);
            context.CurrentExecutionOperator = addEOp;

            // Update context's record layout
            context.AddField(Alias.Value, GremlinKeyword.EdgeSourceV, ColumnGraphType.EdgeSource);
            context.AddField(Alias.Value, GremlinKeyword.EdgeSinkV, ColumnGraphType.EdgeSink);
            context.AddField(Alias.Value, GremlinKeyword.EdgeOtherV, ColumnGraphType.Value);
            context.AddField(Alias.Value, GremlinKeyword.EdgeID, ColumnGraphType.EdgeId);
            context.AddField(Alias.Value, GremlinKeyword.Star, ColumnGraphType.EdgeObject);
            for (var i = GraphViewReservedProperties.ReservedEdgeProperties.Count; i < projectedField.Count; i++)
            {
                context.AddField(Alias.Value, projectedField[i], ColumnGraphType.Value);
            }

            return addEOp;
        }
    }


    partial class WDropTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewCommand command)
        {
            var dropTargetParameter = Parameters[0] as WColumnReferenceExpression;
            var dropTargetIndex = context.LocateColumnReference(dropTargetParameter);

            List<string> populateColumns = new List<string>() { GremlinKeyword.TableDefaultColumnName };

            for (int i = 1; i < this.Parameters.Count; i++)
            {
                WValueExpression populateColumn = this.Parameters[i] as WValueExpression;
                Debug.Assert(populateColumn != null, "populateColumn != null");
                populateColumns.Add(populateColumn.Value);
            }

            var dropOp = new DropOperator(context.CurrentExecutionOperator, command, dropTargetIndex);
            context.CurrentExecutionOperator = dropOp;
            foreach (string columnName in populateColumns)
            {
                context.AddField(Alias.Value, columnName, ColumnGraphType.Value);
            }

            return dropOp;
        }
    }


    partial class WUpdatePropertiesTableReference
    {
        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewCommand command)
        {
            WColumnReferenceExpression updateParameter = this.Parameters[0] as WColumnReferenceExpression;
            int updateIndex = context.LocateColumnReference(updateParameter);
            List<PropertyTuple> propertiesList = new List<PropertyTuple>();

            for (int i = 1; i < this.Parameters.Count; ++i)
            {
                WPropertyExpression property = this.Parameters[i] as WPropertyExpression;
                if (property.Value is WValueExpression)
                {
                    WValueExpression value = property.Value as WValueExpression;
                    Dictionary<string, Tuple<StringField, ScalarSubqueryFunction>> meta = new Dictionary<string, Tuple<StringField, ScalarSubqueryFunction>>();

                    foreach (KeyValuePair<WValueExpression, WScalarExpression> pair in property.MetaProperties)
                    {
                        string name = pair.Key.Value;
                        if (pair.Value is WValueExpression)
                        {
                            WValueExpression metaValue = pair.Value as WValueExpression;
                            meta.Add(name, new Tuple<StringField, ScalarSubqueryFunction>(metaValue.ToStringField(), null));
                        }
                        else
                        {
                            WScalarSubquery metaScalarSubquery = pair.Value as WScalarSubquery;
                            ScalarSubqueryFunction metaValueFunction = (ScalarSubqueryFunction)metaScalarSubquery.CompileToFunction(context, command);
                            meta.Add(name, new Tuple<StringField, ScalarSubqueryFunction>(null, metaValueFunction));
                        }
                    }

                    PropertyTuple valueProperty = new PropertyTuple(property.Cardinality, property.Key.Value, value.ToStringField(), meta);
                    propertiesList.Add(valueProperty);
                }
                else
                {
                    WScalarSubquery scalarSubquery = property.Value as WScalarSubquery;
                    ScalarSubqueryFunction valueFunction = (ScalarSubqueryFunction)scalarSubquery.CompileToFunction(context, command);

                    Dictionary<string, Tuple<StringField, ScalarSubqueryFunction>> meta = new Dictionary<string, Tuple<StringField, ScalarSubqueryFunction>>();
                    foreach (KeyValuePair<WValueExpression, WScalarExpression> pair in property.MetaProperties)
                    {
                        string name = pair.Key.Value;
                        if (pair.Value is WValueExpression)
                        {
                            WValueExpression metaValue = pair.Value as WValueExpression;
                            meta.Add(name, new Tuple<StringField, ScalarSubqueryFunction>(metaValue.ToStringField(), null));
                        }
                        else
                        {
                            WScalarSubquery metaScalarSubquery = pair.Value as WScalarSubquery;
                            ScalarSubqueryFunction metaValueFunction = (ScalarSubqueryFunction)metaScalarSubquery.CompileToFunction(context, command);
                            meta.Add(name, new Tuple<StringField, ScalarSubqueryFunction>(null, metaValueFunction));
                        }
                    }

                    PropertyTuple valueProperty = new PropertyTuple(property.Cardinality, property.Key.Value, valueFunction, meta);
                    propertiesList.Add(valueProperty);
                }
            }

            UpdatePropertiesOperator updateOp = new UpdatePropertiesOperator(
                context.CurrentExecutionOperator,
                command,
                updateIndex,
                propertiesList);
            context.CurrentExecutionOperator = updateOp;

            return updateOp;
        }
    }

}
