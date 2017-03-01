using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal interface ISqlStatement
    {
        List<WSqlStatement> ToSetVariableStatements();
    }
    internal interface ISqlTable
    {
        WTableReference ToTableReference(List<string> projectProperties, string tableName, GremlinVariable gremlinVariable);
    }

    internal interface ISqlScalar
    {
        WScalarExpression ToScalarExpression();
    }

    internal interface ISqlBoolean { }

    internal enum GremlinVariableType
    {
        Vertex,
        Edge,
        Scalar,
        Table,
        Property,
        NULL,
        Undefined
    }
     
    internal abstract class GremlinVariable
    {
        protected string _variableName;
        public int Low { get; set; }
        public int High { get; set; }
        public bool IsReverse { get; set; }
        public bool IsLocal { get; set; }
        public List<string> Labels { get; set; }
        public GremlinToSqlContext HomeContext { get; set; }
        public List<string> ProjectedProperties { get; set; }

        public GremlinVariable()
        {
            Low = Int32.MinValue;
            High = Int32.MaxValue;
            IsReverse = false;
            IsLocal = false;
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

        internal virtual WEdgeType GetEdgeType()
        {
            throw new NotImplementedException();
        }

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

        internal virtual GremlinTableVariable CreateAdjVertex(GremlinVariableProperty propertyVariable)
        {
            return new GremlinBoundVertexVariable(GetEdgeType(), propertyVariable);
        }

        internal virtual string GetVariableName()
        {
            if (_variableName == null) throw new Exception("_variable can't be null");
            return _variableName;
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

        internal virtual void PopulateGremlinPath() {}

        internal virtual List<GremlinVariable> PopulateAllTaggedVariable(string label)
        {
            if (Labels.Contains(label)) return new List<GremlinVariable>() {this};
            return new List<GremlinVariable>();
        }

        internal virtual List<GremlinVariable> FetchVarsFromCurrAndChildContext()
        {
            return null;
        }

        internal virtual GremlinVariableProperty GetPath()
        {
            return DefaultProjection();
        }

        internal virtual GremlinVariableProperty DefaultVariableProperty()
        {
            throw new NotImplementedException();
        }

        internal virtual string GetPrimaryKey()
        {
            throw new NotImplementedException();
        }

        internal virtual string GetProjectKey()
        {
            throw new NotImplementedException();
        }

        internal virtual GremlinVariableProperty DefaultProjection()
        {
            throw new NotImplementedException();
        }

        internal virtual WFunctionCall ToCompose1()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            var projectKey = GetProjectKey();
            parameters.Add(SqlUtil.GetValueExpr(projectKey));
            foreach (var projectProperty in ProjectedProperties)
            {
                parameters.Add(GetVariableProperty(projectProperty).ToScalarExpression());
                parameters.Add(SqlUtil.GetValueExpr(projectProperty));
            }
            return SqlUtil.GetFunctionCall(GremlinKeyword.func.Compose1, parameters);
        }

        internal virtual void AddE(GremlinToSqlContext currentContext, string edgeLabel)
        {
            GremlinAddETableVariable newTableVariable = new GremlinAddETableVariable(this, edgeLabel);
            currentContext.VariableList.Add(newTableVariable);
            currentContext.TableReferences.Add(newTableVariable);
            currentContext.SetPivotVariable(newTableVariable);
        }

        internal virtual void AddV(GremlinToSqlContext currentContext, string vertexLabel, List<object> propertyKeyValues)
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
        //internal virtual void barrier()

        internal virtual void Both(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            throw new QueryCompilationException("The Both() step only applies to vertices.");
        }


        internal virtual void BothE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            throw new NotImplementedException();
        }


        internal virtual void BothV(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        //internal virtual void By(GremlinToSqlContext currentContext)
        //internal virtual void By(GremlinToSqlContext currentContext, string name)
        //internal virtual void by(GremlinToSqlContext currentContext, Comparator<E> comparator)
        //internal virtual void by(GremlinToSqlContext currentContext, Function<U, Object> function, Comparator comparator)
        //internal virtual void by(GremlinToSqlContext currentContext, Function<V, Object> function)
        //internal virtual void By(GremlinToSqlContext currentContext, GremlinKeyword.Order order)
        //internal virtual void by(GremlinToSqlContext currentContext, string key, Comparator<V> comparator)
        //internal virtual void by(GremlinToSqlContext currentContext, T token)
        //internal virtual void By(GremlinToSqlContext currentContext, GraphTraversal2 byContext)
        //internal virtual void by(GremlinToSqlContext currentContext, GremlinToSqlContext<?, ?> byContext, Comparator comparator)

        internal virtual void Cap(GremlinToSqlContext currentContext, List<string> sideEffectKeys)
        {
            GremlinCapVariable newVariable = new GremlinCapVariable(currentContext.Duplicate(), sideEffectKeys);
            currentContext.Reset();
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        //internal virtual void Choose(Function<E, M> choiceFunction)

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
            GremlinTableVariable newVariable = GremlinCoalesceVariable.Create(coalesceContextList);
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


        //internal virtual void cyclicPath(GremlinToSqlContext currentContext)
        //internal virtual void dedup(GremlinToSqlContext currentContext, Scope scope, params string[] dedupLabels)

        internal virtual void Dedup(GremlinToSqlContext currentContext, List<string> dedupLabels)
        {
            GremlinDedupVariable newVariable = new GremlinDedupVariable(this, dedupLabels);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
        }

        internal virtual void Drop(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void E(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void FlatMap(GremlinToSqlContext currentContext, GremlinToSqlContext flatMapContext)
        {
            GremlinTableVariable flatMapVariable = GremlinFlatMapVariable.Create(flatMapContext);
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

        //internal virtual void fold(E2 seed, BiFuntion<E2, E, E2> foldFunction)

        internal virtual void From(GremlinToSqlContext currentContext, string fromGremlinTranslationOperatorLabel)
        {
            throw new NotImplementedException();
        }

        internal virtual void From(GremlinToSqlContext currentContext, GremlinToSqlContext fromVertexContext)
        {
            throw new NotImplementedException();
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

        //internal virtual void groupCount()
        //internal virtual void groupCount(string sideEffectKey)

        internal virtual void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            throw new QueryCompilationException("The Has(propertyKey) step only applies to vertices and edges.");
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string propertyKey, object value)
        {
            throw new QueryCompilationException("The Has(key,value) step only applies to vertices and edges.");
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string label, string propertyKey, object value)
        {
            throw new QueryCompilationException("The Has(label, key,value) step only applies to vertices and edges.");
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
            throw new QueryCompilationException("The Has(key, predicate) step only applies to vertices and edges.");
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Predicate predicate)
        {
            throw new QueryCompilationException("The Has(key, predicate) step only applies to vertices and edges.");
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string propertyKey, GremlinToSqlContext propertyContext)
        {
            throw new QueryCompilationException("The Has(propertyKey, traversal) step only applies to vertices and edges.");
        }

        internal virtual void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            throw new QueryCompilationException("The Has(key, predicate) step only applies to vertices and edges.");
        }

        internal virtual void HasId(GremlinToSqlContext currentContext, Predicate predicate)
        {
            throw new QueryCompilationException("The Has(key, predicate) step only applies to vertices and edges.");
        }

        internal virtual void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            throw new QueryCompilationException("The Has(key, predicate) step only applies to vertices and edges.");
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
            throw new NotImplementedException();
        }

        internal virtual void InE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            throw new NotImplementedException();
        }

        internal virtual void Inject(GremlinToSqlContext currentContext, List<object> values)
        {
            GremlinInjectVariable injectVar = new GremlinInjectVariable(values);
            currentContext.VariableList.Add(injectVar);
            currentContext.TableReferences.Add(injectVar);
        }

        internal virtual void InV(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
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

        internal virtual void Iterate(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Key(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Local(GremlinToSqlContext currentContext, GremlinToSqlContext localContext)
        {
            GremlinTableVariable localMapVariable = GremlinLocalVariable.Create(localContext);
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

            GremlinTableVariable mapVariable = GremlinMapVariable.Create(mapContext);
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

        internal virtual void Option(GremlinToSqlContext currentContext, object pickToken, GremlinToSqlContext optionContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Optional(GremlinToSqlContext currentContext, GremlinToSqlContext optionalContext)
        {
            GremlinTableVariable newVariable = GremlinOptionalVariable.Create(this, optionalContext);
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

        internal virtual void Order(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }
        //internal virtual void order(Scope scope)

        internal virtual void OtherV(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Out(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            throw new QueryCompilationException("The OutV() step can only be applied to vertex.");
        }

        internal virtual void OutE(GremlinToSqlContext currentContext, List<string> edgeLabels)
        {
            throw new NotImplementedException();
        }

        internal virtual void OutV(GremlinToSqlContext currentContext)
        {
            throw new QueryCompilationException("The OutV() step can only be applied to edges.");
        }

        //internal virtual void PageRank()
        //internal virtual void PageRank(double alpha)
        internal virtual void Path(GremlinToSqlContext currentContext)
        {
            GremlinPathVariable newVariable = new GremlinPathVariable(currentContext.GetGremlinStepList());
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        //internal virtual void PeerPressure()
        //internal virtual void Profile()
        //internal virtual void Profile(string sideEffectKey)
        //internal virtual void Program(VertexProgram<?> vertexProgram)

        internal virtual void Project(GremlinToSqlContext currentContext, List<string> projectKeys, List<GremlinToSqlContext> byContexts)
        {
            GremlinProjectVariable newVariable = new GremlinProjectVariable(projectKeys, byContexts);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            throw new QueryCompilationException("The OutV() step can only be applied to edges or vertex.");
        }

        internal virtual void Property(GremlinToSqlContext currentContext, List<object> properties)
        {
            GremlinUpdatePropertiesVariable updateVariable;
            switch (GetVariableType())
            {
                case GremlinVariableType.Vertex:
                    updateVariable =
                        currentContext.VariableList.Find(
                            p =>
                                (p is GremlinUpdateVertexPropertiesVariable) &&
                                (p as GremlinUpdateVertexPropertiesVariable).VertexVariable == this) as GremlinUpdateVertexPropertiesVariable;
                    if (updateVariable == null)
                    {
                        updateVariable = new GremlinUpdateVertexPropertiesVariable(this, properties);
                        currentContext.VariableList.Add(updateVariable);
                        currentContext.TableReferences.Add(updateVariable);
                    }
                    else
                    {
                        updateVariable.Property(currentContext, properties);
                    }
                    break;
                case GremlinVariableType.Edge:
                    updateVariable =
                        currentContext.VariableList.Find(
                            p =>
                                (p is GremlinUpdateEdgePropertiesVariable) &&
                                (p as GremlinUpdateEdgePropertiesVariable).EdgeVariable == this) as GremlinUpdateEdgePropertiesVariable;
                    if (updateVariable == null)
                    {
                        updateVariable = new GremlinUpdateEdgePropertiesVariable(this, properties);
                        currentContext.VariableList.Add(updateVariable);
                        currentContext.TableReferences.Add(updateVariable);
                    }
                    else
                    {
                        updateVariable.Property(currentContext, properties);
                    }
                    break;
                default:
                    throw new Exception();
            }
        }

        //internal virtual void Property(GremlinToSqlContext currentContext, VertexProperty.Cardinality cardinality, string key, string value, params string[] keyValues)

        internal virtual void PropertyMap(GremlinToSqlContext currentContext, params string[] propertyKeys)
        {
            throw new NotImplementedException();
        }

        internal virtual void Range(GremlinToSqlContext currentContext, int low, int high, GremlinKeyword.Scope scope, bool isReverse)
        {
            Low = low;
            High = high;
            IsLocal = scope == GremlinKeyword.Scope.local;
            IsReverse = isReverse;
        }

        internal virtual void Repeat(GremlinToSqlContext currentContext, GremlinToSqlContext repeatContext,
                                     RepeatCondition repeatCondition)
        {
            GremlinTableVariable newVariable = GremlinRepeatVariable.Create(this, repeatContext, repeatCondition);
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

        //internal virtual void To(GremlinToSqlContext currentContext, Direction direction, params string[] edgeLabels)

        internal virtual void To(GremlinToSqlContext currentContext, string toGremlinTranslationOperatorLabel)
        {
            throw new NotImplementedException();
        }

        internal virtual void To(GremlinToSqlContext currentContext, GremlinToSqlContext toVertex)
        {
            throw new NotImplementedException();
        }
        //internal virtual void ToE(GremlinToSqlContext currentContext, Direction direction, params string[] edgeLabels)
        //internal virtual void ToV(GremlinToSqlContext currentContext, Direction direction)
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
        //internal virtual void tree(GremlinToSqlContext currentContext, string sideEffectKey)

        internal virtual void Unfold(GremlinToSqlContext currentContext)
        {
            GremlinTableVariable newVariable = GremlinUnfoldVariable.Create(this);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Union(ref GremlinToSqlContext currentContext, List<GremlinToSqlContext> unionContexts)
        {
            GremlinTableVariable newVariable = GremlinUnionVariable.Create(unionContexts);
            foreach (var unionContext in unionContexts)
            {
                unionContext.HomeVariable = newVariable;
            }
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Until(GremlinToSqlContext currentContext, Predicate untilPredicate)
        {
            throw new NotImplementedException();
        }

        internal virtual void Until(GremlinToSqlContext currentContext, GremlinToSqlContext untilContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void V(GremlinToSqlContext currentContext, params object[] vertexIdsOrElements)
        {
            throw new NotImplementedException();
        }

        internal virtual void V(GremlinToSqlContext currentContext, List<object> vertexIdsOrElements)
        {
            throw new NotImplementedException();
        }

        internal virtual void Value(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
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
            throw new QueryCompilationException("The Values() step can only be applied to edges or vertex.");
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
