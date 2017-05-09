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
        protected string VariableName;
        public List<string> Labels { get; set; }
        public List<string> ProjectedProperties { get; set; }

        public GremlinVariable()
        {
            Labels = new List<string>();
            ProjectedProperties = new List<string>();
        }

        internal virtual GremlinVariableType GetVariableType()
        {
            throw new NotImplementedException();
        }

        internal virtual void Populate(string property)
        {
            if (!ProjectedProperties.Contains(property))
            {
                ProjectedProperties.Add(property);
            }
        }

        /// <summary>
        /// This function is used for populate local path in a subquery,
        /// so it should be overrided by any GremlinVariable which has Subquery.
        /// In the base case, it will do nothing.
        /// These variables are: Local/Optional/Union/Choose/Repeat.
        /// Note: Although Coalesce/FlatMap/Map/Project have subquery, these steps are treated as one step, so we needn't override this function
        /// </summary>
        internal virtual void PopulateLocalPath() {}

        /// <summary>
        /// This function is used for populate property for each step in a path
        /// If a step has a subquery, then this funcion will populate the property for each step in the subquery,
        /// so it should be overrided by any GremlinVariable which has Subquery.
        /// In the base case, it will populate the property for itself. 
        /// This function should be overrided by any GremlinVariable which has Subquery.
        /// These variables are: Local/Optional/Union/Choose/Repeat.
        /// Note: Although Coalesce/FlatMap/Map/Project have subquery, these steps are treated as one step, so we needn't override this function
        /// </summary>
        internal virtual void PopulateStepProperty(string property)
        {
            Populate(property);
        }

        internal virtual GremlinVariableProperty GetVariableProperty(string property)
        {
            if (property != GremlinKeyword.Path)
                Populate(property);
            return new GremlinVariableProperty(this, property);
        }

        internal virtual string GetVariableName()
        {
            if (VariableName == null) throw new QueryCompilationException("VariableName can't be null");
            return VariableName;
        }

        /// <summary>
        /// //This function is used for the algorithm of Repeat Step 
        /// </summary>
        /// <returns></returns>
        internal virtual List<GremlinVariable> FetchAllVars()
        {
            return new List<GremlinVariable> { this };
        }

        internal virtual List<GremlinVariable> FetchAllTableVars()
        {
            return new List<GremlinVariable> { this };
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

        internal virtual WScalarExpression ToStepScalarExpr()
        {
            return ToCompose1();
        }

        internal virtual WFunctionCall ToCompose1()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(SqlUtil.GetValueExpr(GremlinKeyword.TableDefaultColumnName));
            parameters.Add(DefaultProjection().ToScalarExpression());
            parameters.Add(SqlUtil.GetValueExpr(GremlinKeyword.TableDefaultColumnName));
            foreach (var projectProperty in ProjectedProperties)
            {
                if (projectProperty == GremlinKeyword.TableDefaultColumnName)
                {
                    continue;
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
        internal virtual void AddE(GremlinToSqlContext currentContext, string edgeLabel, List<GremlinProperty> edgeProperties, GremlinToSqlContext fromContext, GremlinToSqlContext toContext)
        {
            GremlinAddETableVariable newTableVariable = new GremlinAddETableVariable(this, edgeLabel, edgeProperties, fromContext, toContext);
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
                Labels.Add(label);
            }
        }

        internal virtual void Barrier(GremlinToSqlContext currentContext)
        {
            GremlinBarrierVariable newVariable = new GremlinBarrierVariable();
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
        }

        internal virtual void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVertexToBothEdgeVariable bothEdgeTable = new GremlinVertexToBothEdgeVariable(this);
            currentContext.VariableList.Add(bothEdgeTable);
            currentContext.TableReferences.Add(bothEdgeTable);
            currentContext.AddLabelPredicateForEdge(bothEdgeTable, edgeLabels);

            GremlinEdgeToOtherVertexVariable otherSourceVertex = new GremlinEdgeToOtherVertexVariable(bothEdgeTable);
            currentContext.VariableList.Add(otherSourceVertex);
            currentContext.TableReferences.Add(otherSourceVertex);
            currentContext.SetPivotVariable(otherSourceVertex);
        }

        internal virtual void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVertexToBothEdgeVariable bothEdgeTable = new GremlinVertexToBothEdgeVariable(this);
            currentContext.VariableList.Add(bothEdgeTable);
            currentContext.TableReferences.Add(bothEdgeTable);
            currentContext.AddLabelPredicateForEdge(bothEdgeTable, edgeLabels);

            currentContext.SetPivotVariable(bothEdgeTable);
        }

        internal virtual void BothV(GremlinToSqlContext currentContext)
        {
            GremlinEdgeToBothVertexVariable bothSourceVertex = new GremlinEdgeToBothVertexVariable(this);

            currentContext.VariableList.Add(bothSourceVertex);
            currentContext.TableReferences.Add(bothSourceVertex);
            currentContext.SetPivotVariable(bothSourceVertex);
        }

        internal virtual void Cap(GremlinToSqlContext currentContext, List<string> sideEffectKeys)
        {
            GremlinCapVariable newVariable = new GremlinCapVariable(currentContext.Duplicate(), sideEffectKeys);
            currentContext.Reset();
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Choose(GremlinToSqlContext currentContext, GremlinToSqlContext predicateContext, GremlinToSqlContext trueChoiceContext, GremlinToSqlContext falseChoiceContext)
        {
            GremlinChooseVariable newVariable = new GremlinChooseVariable(predicateContext, trueChoiceContext, falseChoiceContext);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Choose(GremlinToSqlContext currentContext, GremlinToSqlContext choiceContext, Dictionary<object, GremlinToSqlContext> options)
        {
            GremlinChooseVariable newVariable = new GremlinChooseVariable(choiceContext, options);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
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
            GremlinCyclicPathVariable newVariable = new GremlinCyclicPathVariable(GeneratePath(currentContext));
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
        }

        internal virtual void Dedup(GremlinToSqlContext currentContext, List<string> dedupLabels, GraphTraversal2 dedupTraversal, GremlinKeyword.Scope scope)
        {
            List<GremlinVariable> dedupVariables = new List<GremlinVariable>();
            foreach (var dedupLabel in dedupLabels)
            {
                dedupVariables.Add(GenerateSelectVariable(currentContext, GremlinKeyword.Pop.Last, new List<string> { dedupLabel}, new List<GraphTraversal2>() {dedupTraversal.Copy()}));
            }

            dedupTraversal.GetStartOp().InheritedVariableFromParent(currentContext);
            GremlinToSqlContext dedupContext = dedupTraversal.GetEndOp().GetContext();

            GremlinDedupVariable newVariable = new GremlinDedupVariable(this, dedupVariables, dedupContext, scope);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            if (scope == GremlinKeyword.Scope.Local)
            {
                currentContext.SetPivotVariable(newVariable);
            }
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

        internal virtual void Group(GremlinToSqlContext currentContext, string sideEffectKey, GremlinToSqlContext groupByContext,
            GremlinToSqlContext projectByContext, bool isProjectingACollection)
        {
            //TODO: clear history of path
            GremlinGroupVariable newVariable = new GremlinGroupVariable(this, sideEffectKey, groupByContext, projectByContext, isProjectingACollection);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            if (sideEffectKey == null)
            {
                currentContext.SetPivotVariable(newVariable);
            }
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            GraphTraversal2 traversal2 = GraphTraversal2.__().Properties(propertyKey);
            traversal2.GetStartOp().InheritedVariableFromParent(currentContext);
            currentContext.AddPredicate(SqlUtil.GetExistPredicate(traversal2.GetEndOp().GetContext().ToSelectQueryBlock()));
        }

        internal virtual void HasNot(GremlinToSqlContext currentContext, string propertyKey)
        {
            GraphTraversal2 traversal2 = GraphTraversal2.__().Properties(propertyKey);
            traversal2.GetStartOp().InheritedVariableFromParent(currentContext);
            currentContext.AddPredicate(SqlUtil.GetNotExistPredicate(traversal2.GetEndOp().GetContext().ToSelectQueryBlock()));
        }

        private WBooleanExpression CreateBooleanExpression(GremlinVariableProperty variableProperty, object valuesOrPredicate)
        {
            if (valuesOrPredicate is string || GremlinUtil.IsNumber(valuesOrPredicate) || valuesOrPredicate is bool)
            {
                WScalarExpression firstExpr = variableProperty.ToScalarExpression();
                WScalarExpression secondExpr = SqlUtil.GetValueExpr(valuesOrPredicate);
                return SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr);
            }
            if (valuesOrPredicate is Predicate)
            {
                WScalarExpression firstExpr = variableProperty.ToScalarExpression();
                WScalarExpression secondExpr = SqlUtil.GetValueExpr((valuesOrPredicate as Predicate).Value);
                return SqlUtil.GetBooleanComparisonExpr(firstExpr, secondExpr, valuesOrPredicate as Predicate);
            }
            throw new ArgumentException();
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string propertyKey, object valuesOrPredicate)
        {
            currentContext.AddPredicate(CreateBooleanExpression(GetVariableProperty(propertyKey), valuesOrPredicate));
        }

        internal virtual void HasIdOrLabel(GremlinToSqlContext currentContext, GremlinHasType hasType, List<object> valuesOrPredicates)
        {
            GremlinVariableProperty variableProperty = hasType == GremlinHasType.HasId
                ? GetVariableProperty(GremlinKeyword.DefaultId)
                : GetVariableProperty(GremlinKeyword.Label);
            List <WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
            foreach (var valuesOrPredicate in valuesOrPredicates)
            {
                booleanExprList.Add(CreateBooleanExpression(variableProperty, valuesOrPredicate));
            }
            currentContext.AddPredicate(SqlUtil.ConcatBooleanExprWithOr(booleanExprList));
        }

        /// <summary>
        /// Only valid for VertexProperty
        /// </summary>
        internal virtual void HasKeyOrValue(GremlinToSqlContext currentContext, GremlinHasType hasType, List<object> valuesOrPredicates)
        {
            GraphTraversal2 traversal2 = hasType == GremlinHasType.HasKey ? GraphTraversal2.__().Key() : GraphTraversal2.__().Value();
            traversal2.GetStartOp().InheritedVariableFromParent(currentContext);
            GremlinToSqlContext existContext = traversal2.GetEndOp().GetContext();

            List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
            GremlinVariableProperty defaultVariableProperty = existContext.PivotVariable.DefaultProjection();
            foreach (var valuesOrPredicate in valuesOrPredicates)
            {
                booleanExprList.Add(CreateBooleanExpression(defaultVariableProperty, valuesOrPredicate));
            }
            existContext.AddPredicate(SqlUtil.ConcatBooleanExprWithOr(booleanExprList));

            currentContext.AddPredicate(SqlUtil.GetExistPredicate(existContext.ToSelectQueryBlock()));
        }

        internal virtual void Id(GremlinToSqlContext currentContext)
        {
            GremlinIdVariable newVariable = new GremlinIdVariable(this);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void In(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVertexToBackwardEdgeVariable inEdgeTable = new GremlinVertexToBackwardEdgeVariable(this);
            currentContext.VariableList.Add(inEdgeTable);
            currentContext.TableReferences.Add(inEdgeTable);
            currentContext.AddLabelPredicateForEdge(inEdgeTable, edgeLabels);

            GremlinEdgeToSourceVertexVariable outSourceVertex = new GremlinEdgeToSourceVertexVariable(inEdgeTable);
            currentContext.VariableList.Add(outSourceVertex);
            currentContext.TableReferences.Add(outSourceVertex);

            currentContext.SetPivotVariable(outSourceVertex);
        }

        internal virtual void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVertexToBackwardEdgeVariable inEdgeTable = new GremlinVertexToBackwardEdgeVariable(this);
            currentContext.VariableList.Add(inEdgeTable);
            currentContext.TableReferences.Add(inEdgeTable);
            currentContext.AddLabelPredicateForEdge(inEdgeTable, edgeLabels);

            currentContext.SetPivotVariable(inEdgeTable);
        }

        internal virtual void Inject(GremlinToSqlContext currentContext, object injection)
        {
            GremlinInjectVariable injectVar = new GremlinInjectVariable(this, injection);
            currentContext.VariableList.Add(injectVar);
            currentContext.TableReferences.Add(injectVar);
        }

        internal virtual void InV(GremlinToSqlContext currentContext)
        {
            GremlinEdgeToSinkVertexVariable inVertex = new GremlinEdgeToSinkVertexVariable(this);
            currentContext.VariableList.Add(inVertex);
            currentContext.TableReferences.Add(inVertex);
            currentContext.SetPivotVariable(inVertex);
        }

        internal virtual void Is(GremlinToSqlContext currentContext, object value)
        {
            WScalarExpression firstExpr = DefaultProjection().ToScalarExpression();
            WScalarExpression secondExpr = SqlUtil.GetValueExpr(value);
            currentContext.AddPredicate(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
        }

        internal virtual void Is(GremlinToSqlContext currentContext, Predicate predicate)
        {
            WScalarExpression firstExpr = DefaultProjection().ToScalarExpression();
            WScalarExpression secondExpr = SqlUtil.GetValueExpr(predicate.Value);
            currentContext.AddPredicate(SqlUtil.GetBooleanComparisonExpr(firstExpr, secondExpr, predicate));
        }

        internal virtual void Key(GremlinToSqlContext currentContext)
        {
            GremlinKeyVariable newVariable = new GremlinKeyVariable(this);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Label(GremlinToSqlContext currentContext)
        {
            GremlinLabelVariable newVariable = new GremlinLabelVariable(this);
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

        internal virtual void Order(GremlinToSqlContext currentContext, List<Tuple<GremlinToSqlContext, IComparer>> byModulatingMap, GremlinKeyword.Scope scope)
        {
            GremlinOrderVariable newVariable = new GremlinOrderVariable(this, byModulatingMap, scope);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            if (scope == GremlinKeyword.Scope.Local)
            {
                currentContext.SetPivotVariable(newVariable);
            }
        }

        internal virtual void OtherV(GremlinToSqlContext currentContext)
        {
            GremlinEdgeToOtherVertexVariable otherSourceVertex = new GremlinEdgeToOtherVertexVariable(this);
            currentContext.VariableList.Add(otherSourceVertex);
            currentContext.TableReferences.Add(otherSourceVertex);
            currentContext.SetPivotVariable(otherSourceVertex);
        }

        internal virtual void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVertexToForwardEdgeVariable outEdgeTable = new GremlinVertexToForwardEdgeVariable(this);
            currentContext.VariableList.Add(outEdgeTable);
            currentContext.TableReferences.Add(outEdgeTable);
            currentContext.AddLabelPredicateForEdge(outEdgeTable, edgeLabels);

            GremlinEdgeToSinkVertexVariable inSourceVertex = new GremlinEdgeToSinkVertexVariable(outEdgeTable);
            currentContext.VariableList.Add(inSourceVertex);
            currentContext.TableReferences.Add(inSourceVertex);

            currentContext.SetPivotVariable(inSourceVertex);
        }

        internal virtual void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            GremlinVertexToForwardEdgeVariable outEdgeTable = new GremlinVertexToForwardEdgeVariable(this);
            currentContext.VariableList.Add(outEdgeTable);
            currentContext.TableReferences.Add(outEdgeTable);
            currentContext.AddLabelPredicateForEdge(outEdgeTable, edgeLabels);

            currentContext.SetPivotVariable(outEdgeTable);
        }

        internal virtual void OutV(GremlinToSqlContext currentContext)
        {
            GremlinEdgeToSourceVertexVariable outVertex = new GremlinEdgeToSourceVertexVariable(this);
            currentContext.VariableList.Add(outVertex);
            currentContext.TableReferences.Add(outVertex);
            currentContext.SetPivotVariable(outVertex);
        }

        private GremlinPathVariable GeneratePath(GremlinToSqlContext currentContext, List<GraphTraversal2> byList = null)
        {
            //TODO: refactor
            List<GremlinToSqlContext> byContexts = new List<GremlinToSqlContext>();
            List<GremlinVariable> steps = currentContext.GetGlobalPathStepList();
            if (byList == null)
            {
                byList = new List<GraphTraversal2> {GraphTraversal2.__()};
            }


            GremlinGlobalPathVariable newVariable = new GremlinGlobalPathVariable(steps, byContexts);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);

            foreach (var by in byList)
            {
                GremlinToSqlContext newContext = new GremlinToSqlContext();
                GremlinDecompose1Variable decompose1 = new GremlinDecompose1Variable(newVariable);
                newContext.VariableList.Add(decompose1);
                newContext.TableReferences.Add(decompose1);
                newContext.SetPivotVariable(decompose1);

                by.GetStartOp().InheritedContextFromParent(newContext);
                byContexts.Add(by.GetEndOp().GetContext());
            }
            newVariable.ByContexts = byContexts;

            return newVariable;
        }

        internal virtual void Path(GremlinToSqlContext currentContext, List<GraphTraversal2> byList)
        {
            currentContext.SetPivotVariable(GeneratePath(currentContext, byList));
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

        internal virtual void PropertyMap(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            GremlinPropertyMapVariable newVariable = new GremlinPropertyMapVariable(this, propertyKeys);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Range(GremlinToSqlContext currentContext, int low, int high, GremlinKeyword.Scope scope, bool isReverse)
        {
            GremlinRangeVariable newVariable = new GremlinRangeVariable(this, low, high, scope, isReverse);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            if (scope == GremlinKeyword.Scope.Local)
            {
                currentContext.SetPivotVariable(newVariable);
            }
        }

        internal virtual void Repeat(GremlinToSqlContext currentContext, GremlinToSqlContext repeatContext,
                                     RepeatCondition repeatCondition)
        {
            GremlinVariableType variableType = repeatContext.PivotVariable.GetVariableType() == GetVariableType()
                ? GetVariableType()
                : GremlinVariableType.Table;

            GremlinRepeatVariable repeatVariable = new GremlinRepeatVariable(this, repeatContext, repeatCondition, variableType);
            currentContext.VariableList.Add(repeatVariable);
            currentContext.TableReferences.Add(repeatVariable);
            currentContext.SetPivotVariable(repeatVariable);

            //TODO: refactor
            List<GremlinVariable> allTableVars = repeatVariable.FetchAllTableVars();
            foreach (var variable in allTableVars)
            {
                var pathVariable = variable as GremlinGlobalPathVariable;
                if (pathVariable != null)
                {
                    repeatVariable.PopulateLocalPath();
                    foreach (var property in pathVariable.ProjectedProperties)
                    {
                        repeatContext.ContextLocalPath.Populate(property);
                    }
                    pathVariable.IsInRepeatContext = true;
                    pathVariable.PathList.Insert(pathVariable.PathList.FindLastIndex(p => p.GetVariableName() == this.GetVariableName()), null);
                }
            }
        }

        internal virtual void Sample(GremlinToSqlContext currentContext, GremlinKeyword.Scope scope, int amountToSample, GremlinToSqlContext probabilityContext)
        {
            GremlinSampleVariable newVariable = new GremlinSampleVariable(scope, amountToSample, probabilityContext);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            //TODO: set pivotVariable when scope is local, need to sync with compilation and physical operator
        }

        internal virtual void SelectColumn(GremlinToSqlContext currentContext, GremlinKeyword.Column column)
        {
            GremlinSelectColumnVariable newVariable = new GremlinSelectColumnVariable(this, column);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual GremlinSelectVariable GenerateSelectVariable(GremlinToSqlContext currentContext, GremlinKeyword.Pop pop, List<string> selectKeys, List<GraphTraversal2> byList=null)
        {
            //TODO: refactor
            if (byList == null)
            {
                byList = new List<GraphTraversal2>() {GraphTraversal2.__()};
            }
            List<GremlinToSqlContext> byContexts = new List<GremlinToSqlContext>();
            List<GremlinVariable> steps = currentContext.GetGlobalPathStepList();
            List<GremlinVariable> sideEffectVariables = currentContext.GetSideEffectVariables();

            GremlinGlobalPathVariable pathVariable = new GremlinGlobalPathVariable(steps);
            currentContext.VariableList.Add(pathVariable);
            currentContext.TableReferences.Add(pathVariable);

            foreach (var by in byList)
            {
                GremlinToSqlContext newContext = new GremlinToSqlContext();
                GremlinDecompose1Variable decompose1 = new GremlinDecompose1Variable(pathVariable);
                newContext.VariableList.Add(decompose1);
                newContext.TableReferences.Add(decompose1);
                newContext.SetPivotVariable(decompose1);

                by.GetStartOp().InheritedContextFromParent(newContext);
                byContexts.Add(by.GetEndOp().GetContext());
            }

            GremlinSelectVariable newVariable = new GremlinSelectVariable(this, pathVariable, sideEffectVariables, pop, selectKeys, byContexts);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);

            return newVariable;
        }

        internal virtual void Select(GremlinToSqlContext currentContext, GremlinKeyword.Pop pop, List<string> selectKeys, List<GraphTraversal2> byList)
        {
            GremlinSelectVariable newVariable = GenerateSelectVariable(currentContext, pop, selectKeys, byList);
            currentContext.SetPivotVariable(newVariable);
        }
        
        internal virtual void SideEffect(GremlinToSqlContext currentContext, GremlinToSqlContext sideEffectContext)
        {
            GremlinSideEffectVariable newVariable = new GremlinSideEffectVariable(sideEffectContext);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
        }

        internal virtual void SimplePath(GremlinToSqlContext currentContext)
        {
            GremlinSimplePathVariable newVariable = new GremlinSimplePathVariable(GeneratePath(currentContext));
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
        }

        internal virtual void Store(GremlinToSqlContext currentContext, string sideEffectKey, GremlinToSqlContext projectContext)
        {
            GremlinStoreVariable newVariable = new GremlinStoreVariable(projectContext, sideEffectKey);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
        }

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

        internal virtual void Tree(GremlinToSqlContext currentContext, List<GraphTraversal2> byList)
        {
            GremlinPathVariable pathVariable = GeneratePath(currentContext, byList);
            GremlinTreeVariable newVariable = new GremlinTreeVariable(currentContext.Duplicate(), pathVariable);
            currentContext.Reset();
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Tree(GremlinToSqlContext currentContext, string sideEffectKey, List<GraphTraversal2> byList)
        {
            GremlinPathVariable pathVariable = GeneratePath(currentContext, byList);
            GremlinTreeSideEffectVariable newVariable = new GremlinTreeSideEffectVariable(sideEffectKey, pathVariable);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
        }

        internal virtual void Unfold(GremlinToSqlContext currentContext)
        {
            GremlinUnfoldVariable newVariable = new GremlinUnfoldVariable(this);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Union(ref GremlinToSqlContext currentContext, List<GremlinToSqlContext> unionContexts)
        {
            GremlinUnionVariable newVariable = new GremlinUnionVariable(unionContexts, GremlinUtil.GetContextListType(unionContexts));
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Value(GremlinToSqlContext currentContext)
        {
            GremlinValueVariable newVariable = new GremlinValueVariable(this);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void ValueMap(GremlinToSqlContext currentContext, bool isIncludeTokens, List<string> propertyKeys)
        {
            GremlinValueMapVariable newVariable = new GremlinValueMapVariable(this, isIncludeTokens, propertyKeys);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
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
            if (predicate is AndPredicate || predicate is OrPredicate)
                throw new NotImplementedException($"Can't supported {predicate.GetType()} in where()-step for now");
            List<string> selectKeys = new List<string>() { predicate.Value as string };
            var compareVar = GenerateSelectVariable(currentContext, GremlinKeyword.Pop.All, selectKeys);

            var firstExpr = DefaultProjection().ToScalarExpression();
            var secondExpr = compareVar.DefaultProjection().ToScalarExpression();
            var booleanExpr = SqlUtil.GetBooleanComparisonExpr(firstExpr, secondExpr, predicate);
            currentContext.AddPredicate(booleanExpr);
        }

        internal virtual void Where(GremlinToSqlContext currentContext, string startKey, Predicate predicate)
        {
            if (predicate is AndPredicate || predicate is OrPredicate)
                throw new NotImplementedException($"Can't supported {predicate.GetType()} in where()-step for now");
            GremlinVariable firstVar = GenerateSelectVariable(currentContext, GremlinKeyword.Pop.All, new List<string> { startKey });
            GremlinVariable secondVar = GenerateSelectVariable(currentContext, GremlinKeyword.Pop.All, new List<string> { predicate.Value as string});

            var firstExpr = firstVar.DefaultProjection().ToScalarExpression();
            var secondExpr = secondVar.DefaultProjection().ToScalarExpression();

            var booleanExpr = SqlUtil.GetBooleanComparisonExpr(firstExpr, secondExpr, predicate);
            currentContext.AddPredicate(booleanExpr);
        }

        internal virtual void Where(GremlinToSqlContext currentContext, GremlinToSqlContext whereContext)
        {
            WBooleanExpression wherePredicate = whereContext.ToSqlBoolean();
            currentContext.AddPredicate(wherePredicate);
        }
    }
}

