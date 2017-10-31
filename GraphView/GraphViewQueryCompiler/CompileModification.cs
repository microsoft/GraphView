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
        public JObject ConstructNodeJsonDocument(GraphViewCommand command, string vertexLabel, List<WPropertyExpression> vertexProperties, out List<string> projectedFieldList)
        {
            JObject vertexObject = new JObject
            {
                [KW_VERTEX_LABEL] = vertexLabel,
            };

            projectedFieldList = new List<string>(GraphViewReservedProperties.InitialPopulateNodeProperties);
            projectedFieldList.Add(GremlinKeyword.Label);

            foreach (WPropertyExpression vertexProperty in vertexProperties)
            {
                Debug.Assert(vertexProperty.Cardinality == GremlinKeyword.PropertyCardinality.List);

                if (!projectedFieldList.Contains(vertexProperty.Key.Value))
                    projectedFieldList.Add(vertexProperty.Key.Value);

                if (vertexProperty.Value.ToJValue() == null)
                {
                    continue;
                }

                // Special treat the partition key
                if (command.Connection.CollectionType == CollectionType.PARTITIONED)
                {
                    Debug.Assert(command.Connection.RealPartitionKey != null);
                    if (vertexProperty.Key.Value == command.Connection.RealPartitionKey)
                    {
                        if (vertexProperty.MetaProperties.Count > 0)
                        {
                            throw new GraphViewException("Partition value must not have meta properties");
                        }

                        if (vertexObject[command.Connection.RealPartitionKey] == null)
                        {
                            JValue value = vertexProperty.Value.ToJValue();
                            vertexObject[command.Connection.RealPartitionKey] = value;
                        }
                        else
                        {
                            throw new GraphViewException("Partition value must not be a list");
                        }
                        continue;
                    }
                }

                // Special treat the "id" property
                if (vertexProperty.Key.Value == KW_DOC_ID)
                {
                    if (vertexObject[KW_DOC_ID] == null)
                    {
                        JValue value = vertexProperty.Value.ToJValue();
                        if (value.Type != JTokenType.String)
                        {
                            throw new GraphViewException("Vertex's ID must be a string");
                        }
                        if (string.IsNullOrEmpty((string)value))
                        {
                            throw new GraphViewException("Vertex's ID must not be null or empty");
                        }
                        vertexObject[KW_DOC_ID] = (string)value;
                    }
                    else
                    {
                        throw new GraphViewException("Vertex's ID must not be specified more than once");
                    }
                    continue;
                }

                JObject meta = new JObject();
                foreach (KeyValuePair<WValueExpression, WValueExpression> pair in vertexProperty.MetaProperties)
                {
                    WValueExpression metaName = pair.Key;
                    WValueExpression metaValue = pair.Value;
                    meta[metaName.Value] = metaValue.ToJValue();
                }

                string name = vertexProperty.Key.Value;
                JArray propArray = (JArray)vertexObject[name];
                if (propArray == null)
                {
                    propArray = new JArray();
                    vertexObject[name] = propArray;
                }

                JObject prop = new JObject
                {
                    [KW_PROPERTY_VALUE] = vertexProperty.Value.ToJValue(),
                    [KW_PROPERTY_ID] = GraphViewConnection.GenerateDocumentId(),
                };
                if (meta.Count > 0)
                {
                    prop[KW_PROPERTY_META] = meta;
                }
                propArray.Add(prop);
            }

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

            List<WPropertyExpression> vertexProperties = new List<WPropertyExpression>()
                {
                    (WPropertyExpression) (new GremlinProperty(GremlinKeyword.PropertyCardinality.List,
                        GremlinKeyword.Star, null, null).ToPropertyExpr())
                };

            for (int i = 1; i < this.Parameters.Count; i++) {
                WPropertyExpression property = (WPropertyExpression)this.Parameters[i];
                Debug.Assert(property != null, "[WAddVTableReference.Compile] Vertex property should not be null");
                Debug.Assert(property.Cardinality == GremlinKeyword.PropertyCardinality.List, "[WAddVTableReference.Compile] Vertex property should be append-mode");
                Debug.Assert(property.Value != null);

                vertexProperties.Add(property);
            }

            List<string> projectedField;

            JObject nodeJsonDocument = ConstructNodeJsonDocument(command, labelValue.Value, vertexProperties, out projectedField);

            AddVOperator addVOp = new AddVOperator(
                context.CurrentExecutionOperator,
                command,
                nodeJsonDocument,
                projectedField);
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
        public JObject ConstructEdgeJsonObject(GraphViewCommand command, string edgeLabel, List<WPropertyExpression> edgeProperties, out List<string> projectedFieldList)
        {
            JObject edgeObject = new JObject
            {
                [KW_EDGE_LABEL] = edgeLabel
            };

            projectedFieldList = new List<string>(GraphViewReservedProperties.ReservedEdgeProperties);
            projectedFieldList.Add(GremlinKeyword.Label);

            // Skip edgeSourceScalarFunction, edgeSinkScalarFunction, otherVTag
            foreach (WPropertyExpression edgeProperty in edgeProperties)
            {
                GraphViewJsonCommand.UpdateProperty(edgeObject, edgeProperty.Key, edgeProperty.Value);
                if (!projectedFieldList.Contains(edgeProperty.Key.Value))
                    projectedFieldList.Add(edgeProperty.Key.Value);
            }

            return edgeObject;
        }

        internal override GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewCommand command)
        {
            WScalarSubquery srcSubQuery = Parameters[0] as WScalarSubquery;
            WScalarSubquery sinkSubQuery = Parameters[1] as WScalarSubquery;
            if (srcSubQuery == null || sinkSubQuery == null)
                throw new SyntaxErrorException("The first two parameters of AddE can only be WScalarSubquery.");
            QueryCompilationContext srcSubContext = new QueryCompilationContext(context);
            GraphViewExecutionOperator srcSubQueryOp = srcSubQuery.SubQueryExpr.Compile(srcSubContext, command);

            QueryCompilationContext sinkSubContext = new QueryCompilationContext(context);
            GraphViewExecutionOperator sinkSubQueryOp = sinkSubQuery.SubQueryExpr.Compile(sinkSubContext, command);

            WValueExpression otherVTagParameter = Parameters[2] as WValueExpression;
            Debug.Assert(otherVTagParameter != null, "otherVTagParameter != null");
            //
            // if otherVTag == 0, this newly added edge's otherV() is the src vertex.
            // Otherwise, it's the sink vertex
            //
            int otherVTag = int.Parse(otherVTagParameter.Value);

            WValueExpression labelValue = (WValueExpression)this.Parameters[3];

            List<WPropertyExpression> edgeProperties = new List<WPropertyExpression>()
            {
                (WPropertyExpression) (new GremlinProperty(GremlinKeyword.PropertyCardinality.Single,
                    GremlinKeyword.Star, null, null).ToPropertyExpr())
            };

            for (int i = 4; i < this.Parameters.Count; i++)
            {
                WPropertyExpression property = (WPropertyExpression)this.Parameters[i];
                Debug.Assert(property != null, "[WAddETableReference.Compile] Edge property should not be null");
                Debug.Assert(property.Cardinality == GremlinKeyword.PropertyCardinality.Single, "[WAddETableReference.Compile] Edge property should not be append-mode");
                Debug.Assert(property.Value != null);

                edgeProperties.Add(property);
            }

            List<string> projectedField;
            JObject edgeJsonObject = ConstructEdgeJsonObject(command, labelValue.Value, edgeProperties, out projectedField);  // metadata remains missing
            
            GraphViewExecutionOperator addEOp = new AddEOperator(context.CurrentExecutionOperator, command,
                srcSubContext.OuterContextOp, srcSubQueryOp, sinkSubContext.OuterContextOp, sinkSubQueryOp, 
                otherVTag, edgeJsonObject, projectedField);
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
            // var propertiesList = new List<WPropertyExpression>();

            for (int i = 1; i < this.Parameters.Count; ++i) {
                WPropertyExpression property = this.Parameters[i] as WPropertyExpression;
                if (property.Value is WValueExpression)
                {
                    // GraphViewExecutionOperator valueOperator = new GraphViewExecutionOperator();
                }
                else
                {
                    WScalarSubquery scalarSubquery = property.Value as WScalarSubquery;
                    ContainerEnumerator sourceEnumerator = new ContainerEnumerator();
                    QueryCompilationContext subcontext = new QueryCompilationContext(context);
                    subcontext.OuterContextOp.SourceEnumerator = sourceEnumerator;
                    subcontext.InBatchMode = context.InBatchMode;
                    subcontext.CarryOn = true;
                    GraphViewExecutionOperator valueOperator = scalarSubquery.SubQueryExpr.Compile(subcontext, command);

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
