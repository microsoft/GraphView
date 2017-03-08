using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal enum GremlinVariableType
    {
        Vertex,
        Edge,
        Scalar,
        Table,
        VertexProperty,
        Property,
        NULL,
        Undefined
    }
     
    internal abstract class GremlinVariable
    {
        protected string variableName;
        public List<string> Labels { get; set; }
        public GremlinToSqlContext HomeContext { get; set; }
        public List<string> ProjectedProperties { get; set; }

        public GremlinVariable()
        {
            Labels = new List<string>();
            ProjectedProperties = new List<string>();
        }

        internal virtual GremlinVariableType GetUnfoldVariableType()
        {
            return GetVariableType();
        }

        internal virtual GremlinVariableType GetVariableType()
        {
            throw new NotImplementedException();
        }

        //internal virtual WEdgeType GetEdgeType()
        //{
        //    throw new NotImplementedException();
        //}

        internal virtual bool ContainsLabel(string label)
        {
            return Labels.Contains(label);
        }

        internal virtual void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            ProjectedProperties.Add(property);
        }

        internal virtual GremlinVariableProperty GetVariableProperty(string property)
        {
            Populate(property);
            return new GremlinVariableProperty(this, property);
        }

        internal virtual string GetVariableName()
        {
            if (variableName == null) throw new Exception("_variable can't be null");
            return variableName;
        }

        internal virtual void BottomUpPopulate(GremlinVariable terminateVariable, string property, string columnName)
        {
            if (terminateVariable == this) return ;
            if (HomeContext == null) throw new Exception();

            HomeContext.PopulateColumn(GetVariableProperty(property), columnName);
            if (!(HomeContext.HomeVariable is GremlinRepeatVariable) && !HomeContext.HomeVariable.ProjectedProperties.Contains(columnName))
            {
                HomeContext.HomeVariable.ProjectedProperties.Add(columnName);
            }

            if (HomeContext.HomeVariable == null) throw new Exception();
            HomeContext.HomeVariable.BottomUpPopulate(terminateVariable, columnName, columnName);
        }

        internal virtual List<GremlinVariable> PopulateAllTaggedVariable(string label)
        {
            if (Labels.Contains(label)) return new List<GremlinVariable>() {this};
            return new List<GremlinVariable>();
        }

        internal virtual List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            return null;
        }

        internal virtual GremlinPathStepVariable GetAndPopulatePath()
        {
            return new GremlinPathStepVariable(this);
        }

        internal virtual GremlinVariableProperty DefaultVariableProperty()
        {
            switch (GetVariableType())
            {
                case GremlinVariableType.Edge:
                    return GetVariableProperty(GremlinKeyword.EdgeID);
                case GremlinVariableType.Vertex:
                    return GetVariableProperty(GremlinKeyword.NodeID);
                default:
                    return GetVariableProperty(GremlinKeyword.TableDefaultColumnName);
            }
        }

        internal virtual string GetProjectKey()
        {
            throw new NotImplementedException();
        }

        internal virtual GremlinVariableProperty DefaultProjection()
        {
            switch (GetVariableType())
            {
                case GremlinVariableType.Edge:
                case GremlinVariableType.Vertex:
                    return GetVariableProperty(GremlinKeyword.Star);
                default:
                    return GetVariableProperty(GremlinKeyword.TableDefaultColumnName);
            }
        }

        internal virtual WFunctionCall ToCompose1()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(GetProjectKey()));
            foreach (var projectProperty in ProjectedProperties)
            {
                if (projectProperty == GremlinKeyword.TableDefaultColumnName)
                {
                    parameters.Add(DefaultProjection().ToScalarExpression());
                    parameters.Add(SqlUtil.GetValueExpr(GremlinKeyword.TableDefaultColumnName));
                }
                else
                {
                    parameters.Add(GetVariableProperty(projectProperty).ToScalarExpression());
                    parameters.Add(SqlUtil.GetValueExpr(projectProperty));
                }
            }
            return SqlUtil.GetFunctionCall(GremlinKeyword.func.Compose1, parameters);
        }

        /// <summary>
        /// Step Funtions
        /// </summary>

        internal virtual void AddE(GremlinToSqlContext currentContext, string edgeLabel)
        {
            GremlinAddETableVariable newTableVariable = new GremlinAddETableVariable(this, edgeLabel);
            currentContext.VariableList.Add(newTableVariable);
            currentContext.TableReferences.Add(newTableVariable);
            currentContext.SetPivotVariable(newTableVariable);
        }

        internal virtual void AddV(GremlinToSqlContext currentContext, string vertexLabel, List<GremlinProperty> propertyKeyValues)
        {
            GremlinAddVVariable newVariable = new GremlinAddVVariable(vertexLabel, propertyKeyValues);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Aggregate(GremlinToSqlContext currentContext, string sideEffectKey, GremlinToSqlContext projectContext)
        {
            GremlinAggregateVariable newVariable = new GremlinAggregateVariable(projectContext, sideEffectKey);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
        }

        internal virtual void And(GremlinToSqlContext currentContext, List<GremlinToSqlContext> andContexts)
        {
            List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
            foreach (var context in andContexts)
            {
                booleanExprList.Add(context.ToSqlBoolean());
            }
            currentContext.AddPredicate(SqlUtil.ConcatBooleanExprWithAnd(booleanExprList));
        }

        internal virtual void As(GremlinToSqlContext currentContext, List<string> labels)
        {
            foreach (var label in labels)
            {
                currentContext.PivotVariable.Labels.Add(label);
            }
        }

        internal virtual void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty adjEdge = GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinVariableProperty labelProperty = GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeTableVariable bothEdgeTable = new GremlinBoundEdgeTableVariable(sourceProperty,
                                                                             adjEdge,
                                                                             adjReverseEdge,
                                                                             labelProperty,
                                                                             WEdgeType.BothEdge);
            currentContext.VariableList.Add(bothEdgeTable);
            currentContext.TableReferences.Add(bothEdgeTable);
            currentContext.AddLabelPredicateForEdge(bothEdgeTable, edgeLabels);

            GremlinVariableProperty otherProperty = bothEdgeTable.GetVariableProperty(GremlinKeyword.EdgeOtherV);
            GremlinBoundVertexVariable otherVertex = new GremlinBoundVertexVariable(otherProperty);
            currentContext.VariableList.Add(otherVertex);
            currentContext.TableReferences.Add(otherVertex);
            currentContext.SetPivotVariable(otherVertex);
        }

        internal virtual void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty adjEdge = GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinVariableProperty labelProperty = GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeTableVariable bothEdgeTable = new GremlinBoundEdgeTableVariable(sourceProperty, adjEdge, adjReverseEdge, labelProperty,
                WEdgeType.BothEdge);
            currentContext.VariableList.Add(bothEdgeTable);
            currentContext.TableReferences.Add(bothEdgeTable);
            currentContext.AddLabelPredicateForEdge(bothEdgeTable, edgeLabels);

            currentContext.SetPivotVariable(bothEdgeTable);
        }

        internal virtual void BothV(GremlinToSqlContext currentContext)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.EdgeSourceV);
            GremlinVariableProperty sinkProperty = GetVariableProperty(GremlinKeyword.EdgeSinkV);
            GremlinBoundVertexVariable bothVertex = new GremlinBoundVertexVariable(sourceProperty, sinkProperty);

            currentContext.VariableList.Add(bothVertex);
            currentContext.TableReferences.Add(bothVertex);
            currentContext.SetPivotVariable(bothVertex);
        }

        internal virtual void Cap(GremlinToSqlContext currentContext, List<string> sideEffectKeys)
        {
            GremlinCapVariable newVariable = new GremlinCapVariable(currentContext.Duplicate(), sideEffectKeys);
            currentContext.Reset();
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Choose(GremlinToSqlContext currentContext, Predicate choosePredicate, GremlinToSqlContext trueChoice, GremlinToSqlContext falseChoice)
        {
            throw new NotImplementedException();
        }

        internal virtual void Choose(GremlinToSqlContext currentContext, GremlinToSqlContext predicateContext, GremlinToSqlContext trueChoice, GremlinToSqlContext falseChoice)
        {
            throw new NotImplementedException();
        }

        internal virtual void Choose(GremlinToSqlContext currentContext, GremlinToSqlContext choiceContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Coalesce(GremlinToSqlContext currentContext, List<GremlinToSqlContext> coalesceContextList)
        {
            GremlinCoalesceVariable newVariable = new GremlinCoalesceVariable(coalesceContextList, GremlinUtil.GetContextListType(coalesceContextList));
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Coin(GremlinToSqlContext currentContext, double probability)
        {
            GremlinCoinVariable newVariable = new GremlinCoinVariable(probability);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
        }

        internal virtual void Constant(GremlinToSqlContext currentContext, object value)
        {
            GremlinConstantVariable newVariable = new GremlinConstantVariable(value);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Count(GremlinToSqlContext currentContext)
        {
            GremlinCountVariable newVariable = new GremlinCountVariable(currentContext.Duplicate());
            currentContext.Reset();
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void CountLocal(GremlinToSqlContext currentContext)
        {
            GremlinCountLocalVariable newVariable = new GremlinCountLocalVariable(this);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void CyclicPath(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Dedup(GremlinToSqlContext currentContext, List<string> dedupLabels, GremlinToSqlContext dedupContext, GremlinKeyword.Scope scope)
        {
            List<GremlinVariable> dedupVariables = new List<GremlinVariable>();
            foreach (var dedupLabel in dedupLabels)
            {
                dedupVariables.Add(currentContext.Select(dedupLabel).Last());
            }

            GremlinDedupVariable newVariable = new GremlinDedupVariable(this, dedupVariables, dedupContext, scope);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
        }

        internal virtual void Drop(GremlinToSqlContext currentContext)
        {
            GremlinDropVariable dropVariable = new GremlinDropVariable(this);
            currentContext.VariableList.Add(dropVariable);
            currentContext.TableReferences.Add(dropVariable);
            currentContext.SetPivotVariable(dropVariable);
        }

        internal virtual void FlatMap(GremlinToSqlContext currentContext, GremlinToSqlContext flatMapContext)
        {
            GremlinFlatMapVariable flatMapVariable = new GremlinFlatMapVariable(flatMapContext, flatMapContext.PivotVariable.GetVariableType());
            currentContext.VariableList.Add(flatMapVariable);
            currentContext.TableReferences.Add(flatMapVariable);
            currentContext.SetPivotVariable(flatMapVariable);
        }

        internal virtual void Fold(GremlinToSqlContext currentContext)
        {
            GremlinFoldVariable newVariable  = new GremlinFoldVariable(currentContext.Duplicate());
            currentContext.Reset();
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Group(GremlinToSqlContext currentContext, string sideEffectKey, List<object> parameters)
        {
            GremlinGroupVariable newVariable = new GremlinGroupVariable(this, sideEffectKey, parameters);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            if (sideEffectKey == null)
            {
                currentContext.SetPivotVariable(newVariable);
            }
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(DefaultProjection().ToScalarExpression());
            parameters.Add(SqlUtil.GetValueExpr(propertyKey));
            currentContext.AddPredicate(SqlUtil.GetFunctionBooleanExpression("hasProperty", parameters));
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string propertyKey, object value)
        {
            WScalarExpression firstExpr = GetVariableProperty(propertyKey).ToScalarExpression();
            WScalarExpression secondExpr = SqlUtil.GetValueExpr(value);
            currentContext.AddPredicate(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string label, string propertyKey, object value)
        {
            Has(currentContext, GremlinKeyword.Label, label);
            Has(currentContext, propertyKey, value);
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
            WScalarExpression firstExpr = GetVariableProperty(propertyKey).ToScalarExpression();
            WScalarExpression secondExpr = SqlUtil.GetValueExpr(predicate.Value);
            currentContext.AddPredicate(SqlUtil.GetBooleanComparisonExpr(firstExpr, secondExpr, predicate));
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Predicate predicate)
        {
            Has(currentContext, GremlinKeyword.Label, label);
            Has(currentContext, propertyKey, predicate);
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string propertyKey, GremlinToSqlContext propertyContext)
        {
            Has(currentContext, propertyKey);
            currentContext.AddPredicate(propertyContext.ToSqlBoolean());
        }

        internal virtual void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
            foreach (var value in values)
            {
                WScalarExpression firstExpr = DefaultVariableProperty().ToScalarExpression();
                WScalarExpression secondExpr = SqlUtil.GetValueExpr(value);
                booleanExprList.Add(SqlUtil.GetBooleanComparisonExpr(firstExpr, secondExpr, BooleanComparisonType.Equals));
            }
            WBooleanExpression concatSql = SqlUtil.ConcatBooleanExprWithOr(booleanExprList);
            currentContext.AddPredicate(SqlUtil.GetBooleanParenthesisExpr(concatSql));
        }

        internal virtual void HasId(GremlinToSqlContext currentContext, Predicate predicate)
        {
            WScalarExpression firstExpr = DefaultVariableProperty().ToScalarExpression();
            WScalarExpression secondExpr = SqlUtil.GetValueExpr(predicate.Value);
            currentContext.AddPredicate(SqlUtil.GetBooleanComparisonExpr(firstExpr, secondExpr, BooleanComparisonType.Equals));
        }

        internal virtual void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
            foreach (var value in values)
            {
                WScalarExpression firstExpr = GetVariableProperty(GremlinKeyword.Label).ToScalarExpression();
                WScalarExpression secondExpr = SqlUtil.GetValueExpr(value);
                booleanExprList.Add(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
            }
            WBooleanExpression concatSql = SqlUtil.ConcatBooleanExprWithOr(booleanExprList);
            currentContext.AddPredicate(SqlUtil.GetBooleanParenthesisExpr(concatSql));
        }

        internal virtual void HasLabel(GremlinToSqlContext currentContext, Predicate predicate)
        {
            throw new QueryCompilationException("The Has(key, predicate) step only applies to vertices and edges.");
        }

        internal virtual void HasKey(GremlinToSqlContext currentContext, List<string> values)
        {
            throw new QueryCompilationException("The Has(key, predicate) step only applies to properties.");
        }

        internal virtual void HasKey(GremlinToSqlContext currentContext, Predicate predicate)
        {
            throw new QueryCompilationException("The Has(key, predicate) step only applies to properties.");
        }

        internal virtual void HasValue(GremlinToSqlContext currentContext, List<object> values)
        {
            throw new QueryCompilationException("The Has(key, predicate) step only applies to properties.");
        }

        internal virtual void HasValue(GremlinToSqlContext currentContext, Predicate predicate)
        {
            throw new QueryCompilationException("The Has(key, predicate) step only applies to properties.");
        }

        internal virtual void HasNot(GremlinToSqlContext currentContext, string propertyKey)
        {
            throw new QueryCompilationException("The HasNot(propertyKey) step only applies to vertices and edges.");
        }

        internal virtual void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty labelProperty = GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeTableVariable inEdgeTable = new GremlinBoundEdgeTableVariable(sourceProperty, adjReverseEdge,
                labelProperty, WEdgeType.InEdge);
            currentContext.VariableList.Add(inEdgeTable);
            currentContext.TableReferences.Add(inEdgeTable);
            currentContext.AddLabelPredicateForEdge(inEdgeTable, edgeLabels);

            GremlinVariableProperty edgeProperty = inEdgeTable.GetVariableProperty(GremlinKeyword.EdgeSourceV);
            GremlinBoundVertexVariable outVertex = new GremlinBoundVertexVariable(edgeProperty);
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);

            //currentContext.PathList.Add(new GremlinMatchPath(outVertex, inEdgeTable, this));

            currentContext.SetPivotVariable(outVertex);
        }

        internal virtual void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjReverseEdge = GetVariableProperty(GremlinKeyword.ReverseEdgeAdj);
            GremlinVariableProperty labelProperty = GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeTableVariable inEdgeTable = new GremlinBoundEdgeTableVariable(sourceProperty, adjReverseEdge, labelProperty, WEdgeType.InEdge);
            currentContext.VariableList.Add(inEdgeTable);
            currentContext.TableReferences.Add(inEdgeTable);
            currentContext.AddLabelPredicateForEdge(inEdgeTable, edgeLabels);

            //currentContext.PathList.Add(new GremlinMatchPath(null, inEdgeTable, this));
            currentContext.SetPivotVariable(inEdgeTable);
        }

        internal virtual void Inject(GremlinToSqlContext currentContext, List<object> values)
        {
            GremlinInjectVariable injectVar = new GremlinInjectVariable(values);
            currentContext.VariableList.Add(injectVar);
            currentContext.TableReferences.Add(injectVar);
        }

        internal virtual void InV(GremlinToSqlContext currentContext)
        {
            GremlinVariableProperty sinkProperty = GetVariableProperty(GremlinKeyword.EdgeSinkV);
            GremlinTableVariable inVertex = new GremlinBoundVertexVariable(sinkProperty);
            currentContext.VariableList.Add(inVertex);
            currentContext.TableReferences.Add(inVertex);
            currentContext.SetPivotVariable(inVertex);
        }

        internal virtual void Is(GremlinToSqlContext currentContext, object value)
        {
            WScalarExpression firstExpr = DefaultVariableProperty().ToScalarExpression();
            WScalarExpression secondExpr = SqlUtil.GetValueExpr(value);
            currentContext.AddPredicate(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
        }

        internal virtual void Is(GremlinToSqlContext currentContext, Predicate predicate)
        {
            WScalarExpression secondExpr = SqlUtil.GetValueExpr(predicate.Value);
            var firstExpr = DefaultVariableProperty().ToScalarExpression();
            var booleanExpr = SqlUtil.GetBooleanComparisonExpr(firstExpr, secondExpr, predicate);
            currentContext.AddPredicate(booleanExpr);
        }

        internal virtual void Key(GremlinToSqlContext currentContext)
        {
            GremlinKeyVariable newVariable = new GremlinKeyVariable(DefaultVariableProperty());
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Local(GremlinToSqlContext currentContext, GremlinToSqlContext localContext)
        {
            GremlinLocalVariable localMapVariable = new GremlinLocalVariable(localContext, localContext.PivotVariable.GetVariableType());
            currentContext.VariableList.Add(localMapVariable);
            currentContext.VariableList.AddRange(localContext.VariableList);

            currentContext.TableReferences.Add(localMapVariable);
            currentContext.SetPivotVariable(localMapVariable);
        }

        //internal virtual void Loops(GremlinToSqlContext currentContext, )
        //internal virtual void MapKeys() //Deprecated
        //internal virtual void Mapvalues(GremlinToSqlContext currentContext, ) //Deprecated

        internal virtual void Match(GremlinToSqlContext currentContext, List<GremlinToSqlContext> matchContexts)
        {
            throw new NotImplementedException();
        }

        internal virtual void Map(GremlinToSqlContext currentContext, GremlinToSqlContext mapContext)
        {

            GremlinMapVariable mapVariable = new GremlinMapVariable(mapContext, mapContext.PivotVariable.GetVariableType());
            currentContext.VariableList.Add(mapVariable);
            currentContext.TableReferences.Add(mapVariable);
            currentContext.SetPivotVariable(mapVariable);
        }

        internal virtual void Max(GremlinToSqlContext currentContext)
        {
            GremlinMaxVariable newVariable = new GremlinMaxVariable(currentContext.Duplicate());
            currentContext.Reset();
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void MaxLocal(GremlinToSqlContext currentContext)
        {
            GremlinMaxLocalVariable newVariable = new GremlinMaxLocalVariable(this);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Mean(GremlinToSqlContext currentContext)
        {
            GremlinMeanVariable newVariable = new GremlinMeanVariable(currentContext.Duplicate());
            currentContext.Reset();
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void MeanLocal(GremlinToSqlContext currentContext)
        {
            GremlinMeanLocalVariable newVariable = new GremlinMeanLocalVariable(this);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Min(GremlinToSqlContext currentContext)
        {
            GremlinMinVariable newVariable = new GremlinMinVariable(currentContext.Duplicate());
            currentContext.Reset();
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void MinLocal(GremlinToSqlContext currentContext)
        {
            GremlinMinLocalVariable newVariable = new GremlinMinLocalVariable(this);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Not(GremlinToSqlContext currentContext, GremlinToSqlContext notContext)
        {
            WBooleanExpression booleanExpr = SqlUtil.GetNotExistPredicate(notContext.ToSelectQueryBlock());
            currentContext.AddPredicate(booleanExpr);
        }

        internal virtual void Optional(GremlinToSqlContext currentContext, GremlinToSqlContext optionalContext)
        {
            GremlinVariableType variableType = GetVariableType() == optionalContext.PivotVariable.GetVariableType()
                ? GetVariableType()
                : GremlinVariableType.Table;
            GremlinOptionalVariable newVariable = new GremlinOptionalVariable(this, optionalContext, variableType);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Or(GremlinToSqlContext currentContext, List<GremlinToSqlContext> orContexts)
        {
            List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
            foreach (var context in orContexts)
            {
                booleanExprList.Add(context.ToSqlBoolean());
            }
            currentContext.AddPredicate(SqlUtil.ConcatBooleanExprWithOr(booleanExprList));
        }

        internal virtual void Order(GremlinToSqlContext currentContext, List<object> byList,
            List<IComparer> orderList, GremlinKeyword.Scope scope)
        {
            GremlinOrderVariable newVariable = new GremlinOrderVariable(this, byList, orderList, scope);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
        }

        internal virtual void OtherV(GremlinToSqlContext currentContext)
        {
            GremlinVariableProperty otherProperty = GetVariableProperty(GremlinKeyword.EdgeOtherV);
            GremlinBoundVertexVariable otherVertex = new GremlinBoundVertexVariable(otherProperty);
            currentContext.VariableList.Add(otherVertex);
            currentContext.TableReferences.Add(otherVertex);
            currentContext.SetPivotVariable(otherVertex);
        }

        internal virtual void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjEdge = GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinVariableProperty labelProperty = GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeTableVariable outEdgeTable = new GremlinBoundEdgeTableVariable(sourceProperty, adjEdge, labelProperty, WEdgeType.OutEdge);
            currentContext.VariableList.Add(outEdgeTable);
            currentContext.TableReferences.Add(outEdgeTable);
            currentContext.AddLabelPredicateForEdge(outEdgeTable, edgeLabels);

            GremlinVariableProperty sinkProperty = outEdgeTable.GetVariableProperty(GremlinKeyword.EdgeSinkV);
            GremlinBoundVertexVariable inVertex = new GremlinBoundVertexVariable(sinkProperty);
            currentContext.VariableList.Add(inVertex);
            currentContext.TableReferences.Add(inVertex);

            //currentContext.PathList.Add(new GremlinMatchPath(this, outEdgeTable, inVertex));

            currentContext.SetPivotVariable(inVertex);
        }

        internal virtual void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.NodeID);
            GremlinVariableProperty adjEdge = GetVariableProperty(GremlinKeyword.EdgeAdj);
            GremlinVariableProperty labelProperty = GetVariableProperty(GremlinKeyword.Label);
            GremlinBoundEdgeTableVariable outEdgeTable = new GremlinBoundEdgeTableVariable(sourceProperty, adjEdge, labelProperty, WEdgeType.OutEdge);
            currentContext.VariableList.Add(outEdgeTable);
            currentContext.TableReferences.Add(outEdgeTable);
            currentContext.AddLabelPredicateForEdge(outEdgeTable, edgeLabels);

            //currentContext.PathList.Add(new GremlinMatchPath(this, outEdgeTable, null));
            currentContext.SetPivotVariable(outEdgeTable);
        }

        internal virtual void OutV(GremlinToSqlContext currentContext)
        {
            GremlinVariableProperty sourceProperty = GetVariableProperty(GremlinKeyword.EdgeSourceV);
            GremlinTableVariable outVertex = new GremlinBoundVertexVariable(sourceProperty);
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);
            currentContext.SetPivotVariable(outVertex);
        }

        //internal virtual void Path(GremlinToSqlContext currentContext)
        //{
        //    GremlinPathVariable newVariable = new GremlinPathVariable(currentContext.GetGremlinStepList());
        //    currentContext.VariableList.Add(newVariable);
        //    currentContext.TableReferences.Add(newVariable);
        //    currentContext.SetPivotVariable(newVariable);
        //}

        internal virtual void Path2(GremlinToSqlContext currentContext, List<object> byList)
        {
            List<GremlinToSqlContext> byContexts = new List<GremlinToSqlContext>();
            List<GremlinPathStepVariable> steps = currentContext.GetGremlinStepList();
            foreach (var by in byList)
            {
                GremlinToSqlContext newContext = new GremlinToSqlContext();
                GremlinDecompose1Variable decompose1 = new GremlinDecompose1Variable(steps);
                newContext.VariableList.Add(decompose1);
                newContext.TableReferences.Add(decompose1);
                newContext.SetPivotVariable(decompose1);

                if (by is string)
                {
                    decompose1.DefaultProjectionKey = by as string;
                    byContexts.Add(newContext);
                }
                else if (by is GraphTraversal2)
                {
                    var traversal = by as GraphTraversal2;
                    if (traversal.GetStartOp() is GremlinParentContextOp)
                    {
                        traversal.GetStartOp().InheritedContextFromParent(newContext);
                    }
                    byContexts.Add(traversal.GetEndOp().GetContext());
                }
                else
                {
                    throw new QueryCompilationException($"Can't process this type {by.GetType()}.");
                }
            }

            GremlinPathVariable newVariable = new GremlinPathVariable(steps, byContexts);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Project(GremlinToSqlContext currentContext, List<string> projectKeys, List<GremlinToSqlContext> byContexts)
        {
            GremlinProjectVariable newVariable = new GremlinProjectVariable(projectKeys, byContexts);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            GremlinPropertiesVariable newVariable = new GremlinPropertiesVariable(this, propertyKeys);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Property(GremlinToSqlContext currentContext, GremlinProperty vertexProperty)
        {
            GremlinUpdatePropertiesVariable updateVariable =
                currentContext.VariableList.Find(
                    p =>
                        (p is GremlinUpdatePropertiesVariable) &&
                        (p as GremlinUpdatePropertiesVariable).UpdateVariable == this) as GremlinUpdatePropertiesVariable;
            if (updateVariable == null)
            {
                updateVariable = new GremlinUpdatePropertiesVariable(this, vertexProperty);
                currentContext.VariableList.Add(updateVariable);
                currentContext.TableReferences.Add(updateVariable);
            }
            else
            {
                updateVariable.PropertyList.Add(vertexProperty);
            }
        }

        internal virtual void PropertyMap(GremlinToSqlContext currentContext, params string[] propertyKeys)
        {
            throw new NotImplementedException();
        }

        internal virtual void Range(GremlinToSqlContext currentContext, int low, int high, GremlinKeyword.Scope scope, bool isReverse)
        {
            GremlinRangeVariable newVariable = new GremlinRangeVariable(this, low, high, scope, isReverse);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
        }

        internal virtual void Repeat(GremlinToSqlContext currentContext, GremlinToSqlContext repeatContext,
                                     RepeatCondition repeatCondition)
        {
            GremlinVariableType variableType = repeatContext.PivotVariable.GetVariableType() == GetVariableType()
                ? GetVariableType()
                : GremlinVariableType.Table;
            GremlinRepeatVariable newVariable = new GremlinRepeatVariable(this, repeatContext, repeatCondition, variableType);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        //internal virtual void Sack() //Deprecated
        //internal virtual void Sack(BiFunction<V, U, V>) sackOperator) //Deprecated
        //internal virtual void Sack(BiFunction<V, U, V>) sackOperator, string, elementPropertyKey) //Deprecated

        internal virtual void Sample(GremlinToSqlContext currentContext, int amountToSample)
        {
            throw new NotImplementedException();
        }

        internal virtual void sample(GremlinToSqlContext currentContext, GremlinKeyword.Scope scope, int amountToSample)
        {
            throw new NotImplementedException();
        }

        internal GremlinVariable GetTheFirstVariable(List<GremlinVariable> taggedVariableList)
        {
            var firstVariable = taggedVariableList.First();
            if (firstVariable is GremlinBranchVariable) throw new NotImplementedException();
            return firstVariable;
        }

        internal GremlinVariable GetTheLastVariable(List<GremlinVariable> taggedVariableList)
        {
            var lastVariable = taggedVariableList.Last();
            if (lastVariable is GremlinBranchVariable) throw new NotImplementedException();
            return lastVariable;
        }

        internal virtual void Select(GremlinToSqlContext currentContext, GremlinKeyword.Pop pop, string selectKey)
        {
            List<GremlinVariable> taggedVariableList = currentContext.Select(selectKey);
            GremlinVariable selectedVariable;

            if (taggedVariableList.Count == 0)
            {
                throw new QueryCompilationException(string.Format("The specified tag \"{0}\" is not defined.", selectKey));
            }
            else if (taggedVariableList.Count == 1)
            {
                taggedVariableList[0].HomeContext = currentContext;
                selectedVariable = taggedVariableList.First();
                currentContext.VariableList.Add(selectedVariable);
                currentContext.SetPivotVariable(selectedVariable);
            }
            else
            {
                switch (pop)
                {
                    case GremlinKeyword.Pop.first:
                        selectedVariable = GetTheFirstVariable(taggedVariableList);
                        break;
                    case GremlinKeyword.Pop.last:
                        selectedVariable = GetTheLastVariable(taggedVariableList);
                        break;
                    default:
                        throw new NotImplementedException();
                }
                //if (selectedVariable is GremlinGhostVariable) throw new NotImplementedException();

                selectedVariable.HomeContext = currentContext;
                currentContext.VariableList.Add(selectedVariable);
                currentContext.SetPivotVariable(selectedVariable);
            }

            if (selectedVariable is GremlinSelectedVariable)
            {
                (selectedVariable as GremlinSelectedVariable).IsFromSelect = true;
                (selectedVariable as GremlinSelectedVariable).Pop = pop;
                (selectedVariable as GremlinSelectedVariable).SelectKey = selectKey;
            }
        }

        internal virtual void Select(GremlinToSqlContext currentContext, string label)
        {
            List<GremlinVariable> taggedVariableList = currentContext.Select(label);

            GremlinVariable selectedVariable = null;
            if (taggedVariableList.Count == 0)
            {
                throw new QueryCompilationException(string.Format("The specified tag \"{0}\" is not defined.", label));
            }
            else if (taggedVariableList.Count == 1)
            {
                selectedVariable = taggedVariableList[0];
                selectedVariable.HomeContext = currentContext;
                currentContext.VariableList.Add(selectedVariable);
                currentContext.SetPivotVariable(selectedVariable);
            }
            else
            {
                selectedVariable = new GremlinListVariable(taggedVariableList);
                selectedVariable.HomeContext = currentContext;
                currentContext.VariableList.Add(selectedVariable);
                currentContext.SetPivotVariable(selectedVariable);
            }

            if (selectedVariable is GremlinSelectedVariable)
            {
                (selectedVariable as GremlinSelectedVariable).IsFromSelect = true;
                (selectedVariable as GremlinSelectedVariable).SelectKey = label;
            }

        }

        internal virtual void Select(GremlinToSqlContext currentContext, List<string> selectKeys)
        {
            throw new NotImplementedException();
        }

        internal virtual void Select(GremlinToSqlContext currentContext, GremlinKeyword.Pop pop, List<string> selectKeys)
        {
            throw new NotImplementedException();
        }
        
        internal virtual void SideEffect(GremlinToSqlContext currentContext, GremlinToSqlContext sideEffectContext)
        {
            GremlinSideEffectVariable newVariable = new GremlinSideEffectVariable(sideEffectContext);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
        }

        //internal virtual void SimplePath()

        internal virtual void Store(GremlinToSqlContext currentContext, string sideEffectKey, GremlinToSqlContext projectContext)
        {
            GremlinStoreVariable newVariable = new GremlinStoreVariable(projectContext, sideEffectKey);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
        }

        //internal virtual void Subgraph(string sideEffectKey)

        internal virtual void Sum(GremlinToSqlContext currentContext)
        {
            GremlinSumVariable newVariable = new GremlinSumVariable(currentContext.Duplicate());
            currentContext.Reset();
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void SumLocal(GremlinToSqlContext currentContext)
        {
            GremlinSumLocalVariable newVariable = new GremlinSumLocalVariable(this);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void TimeLimit(GremlinToSqlContext currentContext, long timeLimit)
        {
            throw new NotImplementedException();
        }

        internal virtual void Times(GremlinToSqlContext currentContext, int maxLoops)
        {
            throw new NotImplementedException();
        }

        internal virtual void Tree(GremlinToSqlContext currentContext)
        {
            GremlinPathVariable pathVariable = new GremlinPathVariable(currentContext.GetGremlinStepList());
            currentContext.VariableList.Add(pathVariable);
            currentContext.TableReferences.Add(pathVariable);

            GremlinTreeVariable newVariable = new GremlinTreeVariable(currentContext.Duplicate(), pathVariable);
            currentContext.Reset();
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Tree(GremlinToSqlContext currentContext, string sideEffectKey)
        {
            throw new NotImplementedException();
        }

        internal virtual void Unfold(GremlinToSqlContext currentContext)
        {
            GremlinUnfoldVariable newVariable = new GremlinUnfoldVariable(this, GetUnfoldVariableType());
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Union(ref GremlinToSqlContext currentContext, List<GremlinToSqlContext> unionContexts)
        {
            GremlinUnionVariable newVariable = new GremlinUnionVariable(unionContexts, GremlinUtil.GetContextListType(unionContexts));
            foreach (var unionContext in unionContexts)
            {
                unionContext.HomeVariable = newVariable;
            }
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Value(GremlinToSqlContext currentContext)
        {
            GremlinValueVariable newVariable = new GremlinValueVariable(DefaultVariableProperty());
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void ValueMap(GremlinToSqlContext currentContext, Boolean includeTokens, params string[] propertyKeys)
        {
            throw new NotImplementedException();
        }

        internal virtual void ValueMap(GremlinToSqlContext currentContext, params string[] propertyKeys)
        {
            throw new NotImplementedException();
        }

        internal virtual void Values(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            if (propertyKeys.Count == 0)
            {
                Populate(GremlinKeyword.Star);
            }
            else
            {
                foreach (var property in propertyKeys)
                {
                    Populate(property);
                }
            }
            GremlinValuesVariable newVariable = new GremlinValuesVariable(this, propertyKeys);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Where(GremlinToSqlContext currentContext, Predicate predicate)
        {
            var compareVar = currentContext.Select(predicate.Value as string);
            if (compareVar.Count > 1) throw new Exception();

            var firstExpr = DefaultProjection().ToScalarExpression();
            var secondExpr = compareVar.First().DefaultProjection().ToScalarExpression();
            var booleanExpr = SqlUtil.GetBooleanComparisonExpr(firstExpr, secondExpr, predicate);
            currentContext.AddPredicate(booleanExpr);
        }

        internal virtual void Where(GremlinToSqlContext currentContext, string startKey, Predicate predicate)
        {
            predicate.IsTag = true;
            throw new NotImplementedException();
        }

        internal virtual void Where(GremlinToSqlContext currentContext, GremlinToSqlContext whereContext)
        {
            WBooleanExpression wherePredicate = whereContext.ToSqlBoolean();
            currentContext.AddPredicate(wherePredicate);
        }

    }
}

