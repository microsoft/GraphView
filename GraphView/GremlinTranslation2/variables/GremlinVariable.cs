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
        WTableReference ToTableReference();
    }

    internal interface ISqlScalar
    {
        WSelectElement ToSelectElement();
        WScalarExpression ToScalarExpression();
    }

    internal interface ISqlBoolean { }

    internal enum GremlinVariableType
    {
        Vertex,
        Edge,
        Scalar,
        Table,
        Undefined
    }
     
    internal abstract class GremlinVariable2
    {
        public string VariableName { get; set; }
        public List<string> UsedProperties = new List<string>();
        public long Low = Int64.MinValue;
        public long High = Int64.MaxValue;

        internal virtual GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Undefined;
        }

        internal virtual void Populate(string property)
        {
            if (!UsedProperties.Contains(property))
            {
                UsedProperties.Add(property);
            }
        }

        internal virtual GremlinScalarVariable DefaultProjection()
        {
            return new GremlinVariableProperty(this, "id");
        }

        internal void AddLabelPredicateToEdge(GremlinToSqlContext currentContext, GremlinEdgeVariable2 edge, List<string> edgeLabels)
        {
            edge.Populate("label");
            List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
            foreach (var edgeLabel in edgeLabels)
            {
                var firstExpr = GremlinUtil.GetColumnReferenceExpr(edge.VariableName, "label");
                var secondExpr = GremlinUtil.GetValueExpression(edgeLabel);
                booleanExprList.Add(GremlinUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
            }
            currentContext.AddPredicate(GremlinUtil.ConcatBooleanExprWithOr(booleanExprList));
        }

        internal virtual void AddE(GremlinToSqlContext currentContext, string edgeLabel)
        {
            GremlinToSqlContext copyContext = currentContext.Duplicate();
            //GremlinVariable2 pivotVariable = currentContext.PivotVariable;
            currentContext.Reset();

            GremlinAddEVariable newVariable = null;
            if (this is GremlinAddVVariable)
            {
                newVariable = new GremlinAddEVariable(edgeLabel, this as GremlinAddVVariable);
            }
            else
            {
                var variableRef = new GremlinVariableReference(copyContext);
                currentContext.VariableList.Add(variableRef);
                currentContext.SetVariables.Add(variableRef);
                newVariable = new GremlinAddEVariable(edgeLabel, variableRef);
            }
            
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetVariables.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }

        //internal virtual void addInE(GremlinToSqlContext currentContext, string firstVertexKeyOrEdgeLabel, string edgeLabelOrSecondVertexKey, params Object[] propertyKeyValues)
        //internal virtual void addOutE(GremlinToSqlContext currentContext, string firstVertexKeyOrEdgeLabel, string edgeLabelOrSecondVertexKey, params Object[] propertyKeyValues)

        internal virtual void AddV(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void AddV(GremlinToSqlContext currentContext, params Object[] propertyKeyValues)
        {
            throw new NotImplementedException();
        }

        internal virtual void AddV(GremlinToSqlContext currentContext, string vertexLabel)
        {
            //TODO
            currentContext.Reset();
            GremlinAddVVariable newVariable = new GremlinAddVVariable(vertexLabel);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetVariables.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }

        internal virtual void Aggregate(GremlinToSqlContext currentContext, string sideEffectKey)
        {
            throw new NotImplementedException();
        }

        internal virtual void And(
            GremlinToSqlContext currentContext,
            GremlinToSqlContext subContext1,
            GremlinToSqlContext subContext2)
        {
        }

        internal virtual void As(GremlinToSqlContext currentContext, List<string> labels)
        {
            foreach (var label in labels)
            {
                if (!currentContext.TaggedVariables.ContainsKey(label))
                {
                    currentContext.TaggedVariables[label] = new List<Tuple<GremlinVariable2, GremlinToSqlContext>>();
                }
                currentContext.TaggedVariables[label].Add(new Tuple<GremlinVariable2, GremlinToSqlContext>(this, currentContext));
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

        internal virtual void By(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void By(GremlinToSqlContext currentContext, string name)
        {
            throw new NotImplementedException();
        }


        //internal virtual void by(GremlinToSqlContext currentContext, Comparator<E> comparator)
        //internal virtual void by(GremlinToSqlContext currentContext, Function<U, Object> function, Comparator comparator)
        //internal virtual void by(GremlinToSqlContext currentContext, Function<V, Object> function)

        internal virtual void By(GremlinToSqlContext currentContext, GremlinKeyword.Order order)
        {
            throw new NotImplementedException();
        }

        //internal virtual void by(GremlinToSqlContext currentContext, string key, Comparator<V> comparator)
        //internal virtual void by(GremlinToSqlContext currentContext, T token)

        
        internal virtual void By(GremlinToSqlContext currentContext, GraphTraversal2 byContext)
        {
            // use GraphTraversal2 instead of GremlinToSqlContext
            //because it should inherite from last context rather than current Context 
            throw new NotImplementedException();
        }

        //internal virtual void by(GremlinToSqlContext currentContext, GremlinToSqlContext<?, ?> byContext, Comparator comparator)
        internal virtual void Cap(GremlinToSqlContext currentContext, params string[] keys)
        {
            //currentContext.ProjectedVariables.Clear();

            //foreach (string key in keys)
            //{
            //    if (!currentContext.TaggedVariables.ContainsKey(key))
            //    {
            //        throw new QueryCompilationException(string.Format("The specified tag \"{0}\" is not defined.", key));
            //    }

            //    GremlinVariable2 var = currentContext.TaggedVariables[key].Item1;
            //    currentContext.ProjectedVariables.Add(var.DefaultProjection());
            //}
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
            GremlinCoalesceVariable newVariable = GremlinCoalesceVariable.Create(coalesceContextList);
            foreach (var context in coalesceContextList)
            {
                currentContext.SetVariables.AddRange(context.SetVariables);
            }
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }

        internal virtual void Coin(GremlinToSqlContext currentContext, double probability)
        {
            throw new NotImplementedException();
        }

        internal virtual void Constant(GremlinToSqlContext currentContext, object value)
        {
            GremlinConstantVariable newVariable = new GremlinConstantVariable(value);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }
        internal virtual void Count(GremlinToSqlContext currentContext)
        {
            GremlinCountVariable newVariable = new GremlinCountVariable(currentContext.Duplicate());
            currentContext.Reset();
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }

        //internal virtual void count(GremlinToSqlContext currentContext, Scope scope)
        //internal virtual void cyclicPath(GremlinToSqlContext currentContext)
        //internal virtual void dedup(GremlinToSqlContext currentContext, Scope scope, params string[] dedupLabels)
        internal virtual void Dedup(GremlinToSqlContext currentContext, List<string> dedupLabels)
        {
            GremlinDedupVariable newVariable = new GremlinDedupVariable(currentContext.Duplicate(), dedupLabels);
            currentContext.Reset();
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }

        internal virtual void Drop(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void E(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        //internal virtual void emit(GremlinToSqlContext currentContext)
        //{
        //    throw new NotImplementedException();
        //}

        //internal virtual void emit(Predicate emitPredicate)
        //{
        //    throw new NotImplementedException();
        //}

        //internal virtual void emit(GremlinToSqlContext emitContext)
        //{
        //    throw new NotImplementedException();
        //}

        internal virtual void FlatMap(GremlinToSqlContext currentContext, GremlinToSqlContext flatMapContext)
        { 
            GremlinFlatMapVariable flatMapVariable = GremlinFlatMapVariable.Create(flatMapContext);
            currentContext.VariableList.Add(flatMapVariable);
            
            //It's used for repeat step, we should propagate all the variable to the main context
            //Then we can check the variableList to know if the sub context used the main context variable when
            //the variable is GremlinContextVariable and the value of IsFromSelect is True
            //
            currentContext.VariableList.AddRange(flatMapContext.VariableList);

            currentContext.TableReferences.Add(flatMapVariable);
            currentContext.PivotVariable = flatMapVariable;
        }

        internal virtual void Fold(GremlinToSqlContext currentContext)
        {
            GremlinFoldVariable newVariable  = new GremlinFoldVariable(currentContext.Duplicate());
            currentContext.Reset();
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.PivotVariable = newVariable;
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

        internal virtual void Group(GremlinToSqlContext currentContext)
        {
            GremlinGroupVariable groupVariable = new GremlinGroupVariable();
            currentContext.VariableList.Add(groupVariable);
        }

        //internal virtual void groupCount()
        //internal virtual void groupCount(string sideEffectKey)

        internal virtual void Has(GremlinToSqlContext currentContext, string propertyKey)
        {
            throw new NotImplementedException();
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string propertyKey, Object value)
        {
            WScalarExpression firstExpr = GremlinUtil.GetColumnReferenceExpr(VariableName, propertyKey);
            WScalarExpression secondExpr = GremlinUtil.GetValueExpression(value);
            currentContext.AddEqualPredicate(firstExpr, secondExpr);
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Object value)
        {
            throw new NotImplementedException();
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string propertyKey, Predicate predicate)
        {
            throw new NotImplementedException();
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string label, string propertyKey, Predicate predicate)
        {
            throw new NotImplementedException();
        }

        internal virtual void Has(GremlinToSqlContext currentContext, string propertyKey, GremlinToSqlContext propertyContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            throw new NotImplementedException();
        }

        internal virtual void HasKey(GremlinToSqlContext currentContext, List<object> values)
        {
            throw new NotImplementedException();
        }

        internal virtual void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
            foreach (var value in values)
            {
                WScalarExpression firstExpr = GremlinUtil.GetColumnReferenceExpr(VariableName, GremlinKeyword.Label);
                WScalarExpression secondExpr = GremlinUtil.GetValueExpression(value);
                booleanExprList.Add(GremlinUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
            }
            WBooleanExpression concatSql = GremlinUtil.ConcatBooleanExprWithOr(booleanExprList);
            currentContext.AddPredicate(concatSql);
        }

        internal virtual void HasValue(GremlinToSqlContext currentContext, string value, params string[] values)
        {
            throw new NotImplementedException();
        }
        internal virtual void HasNot(GremlinToSqlContext currentContext, string propertyKey)
        {
            throw new NotImplementedException();
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
            GremlinToSqlContext priorContext = currentContext.Duplicate();
            currentContext.Reset();
            GremlinInjectVariable injectVar = new GremlinInjectVariable(priorContext, values);
            currentContext.VariableList.Add(injectVar);
            currentContext.TableReferences.Add(injectVar);
            currentContext.PivotVariable = injectVar;
        }

        internal virtual void InV(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Is(GremlinToSqlContext currentContext, object value)
        {
            WScalarExpression firstExpr = DefaultProjection().ToScalarExpression();
            WScalarExpression secondExpr = GremlinUtil.GetValueExpression(value);
            currentContext.AddEqualPredicate(firstExpr, secondExpr);
        }

        internal virtual void Is(GremlinToSqlContext currentContext, Predicate predicate)
        {
            WScalarExpression secondExpr = null;
            if (predicate.Label != null)
            {
                var compareVar = currentContext.TaggedVariables[predicate.Label].Last().Item1;
                secondExpr = compareVar.DefaultProjection().ToScalarExpression();
            }
            var firstExpr = DefaultProjection().ToScalarExpression();
            var booleanExpr = GremlinUtil.GetBooleanComparisonExpr(firstExpr, secondExpr, predicate);
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

        internal virtual void Limit(GremlinToSqlContext currentContext, long limit)
        {
            throw new NotImplementedException();
        }

        //internal virtual void Limit(Scope scope, long limit)

        internal virtual void Local(GremlinToSqlContext currentContext, GremlinToSqlContext localContext)
        {
            GremlinLocalVariable localMapVariable = GremlinLocalVariable.Create(localContext);
            currentContext.VariableList.Add(localMapVariable);
            currentContext.VariableList.AddRange(localContext.VariableList);

            currentContext.TableReferences.Add(localMapVariable);
            currentContext.PivotVariable = localMapVariable;
        }
        //internal virtual void Loops(GremlinToSqlContext currentContext, )
        //internal virtual void MapKeys() //Deprecated
        //internal virtual void Mapvalues(GremlinToSqlContext currentContext, ) //Deprecated

        internal virtual void Match(GremlinToSqlContext currentContext, List<GremlinToSqlContext> matchContexts)
        {
            throw new NotImplementedException();
        }

        internal virtual void Max(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Max(GremlinToSqlContext currentContext, GremlinKeyword.Scope scope)
        {
            throw new NotImplementedException();
        }

        internal virtual void Mean(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Mean(GremlinToSqlContext currentContext, GremlinKeyword.Scope scope)
        {
            throw new NotImplementedException();
        }

        internal virtual void Min(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Min(GremlinToSqlContext currentContext, GremlinKeyword.Scope scope)
        {
            throw new NotImplementedException();
        }

        internal virtual void Not(GremlinToSqlContext currentContext, GremlinToSqlContext notContext)
        {
            WBooleanExpression booleanExpr = GremlinUtil.GetNotExistPredicate(notContext.ToSelectQueryBlock());
            currentContext.AddPredicate(booleanExpr);
        }

        internal virtual void Option(GremlinToSqlContext currentContext, object pickToken, GremlinToSqlContext optionContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Optional(GremlinToSqlContext currentContext, GremlinToSqlContext optionalContext)
        {
            GremlinOptionalVariable newVariable = GremlinOptionalVariable.Create(this, optionalContext);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetVariables.AddRange(optionalContext.SetVariables);
            currentContext.PivotVariable = newVariable;
        }

        internal virtual void Or(GremlinToSqlContext currentContext, List<GremlinToSqlContext> orContexts)
        {
            throw new NotImplementedException();
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
        //internal virtual void Path()
        //internal virtual void PeerPressure()
        //internal virtual void Profile()
        //internal virtual void Profile(string sideEffectKey)
        //internal virtual void Program(VertexProgram<?> vertexProgram)
        internal virtual void Project(GremlinToSqlContext currentContext, List<string> projectKeys)
        {
            GremlinProjectVariable newVariable = new GremlinProjectVariable(currentContext.Duplicate(), projectKeys);
            currentContext.Reset();
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }

        internal virtual void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
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

        internal virtual void Property(GremlinToSqlContext currentContext, Dictionary<string, object> properties)
        {
            throw new NotImplementedException();
        }

        //internal virtual void Property(GremlinToSqlContext currentContext, VertexProperty.Cardinality cardinality, string key, string value, params string[] keyValues)

        internal virtual void PropertyMap(GremlinToSqlContext currentContext, params string[] propertyKeys)
        {
            throw new NotImplementedException();
        }

        internal virtual void Range(GremlinToSqlContext currentContext, long low, long high)
        {
            throw new NotImplementedException();
        }

        internal virtual void Repeat(GremlinToSqlContext currentContext, GremlinToSqlContext repeatContext,
                                     RepeatCondition repeatCondition)
        {
            GremlinRepeatVariable newVariable = new GremlinRepeatVariable(this, repeatContext, repeatCondition);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }

        //internal virtual void Sack() //Deprecated
        //internal virtual void Sack(BiFunction<V, U, V>) sackOperator) //Deprecated
        //internal virtual void Sack(BiFunction<V, U, V>) sackOperator, string, elementPropertyKey) //Deprecated

        internal virtual void Sample(GremlinToSqlContext currentContext, int amountToSample)
        {
            throw new NotImplementedException();
        }

        //internal virtual void sample(Scope scope, int amountToSample)

        internal virtual void Select(GremlinToSqlContext currentContext, GremlinKeyword.Pop pop, string selectKey)
        {
            if (!currentContext.TaggedVariables.ContainsKey(selectKey))
            {
                throw new QueryCompilationException(string.Format("The specified tag \"{0}\" is not defined.", selectKey));
            }

            Tuple<GremlinVariable2, GremlinToSqlContext> pair;
            switch (pop) 
            {
                case GremlinKeyword.Pop.first:
                    pair = currentContext.TaggedVariables[selectKey].First();
                    break;
                case GremlinKeyword.Pop.last:
                    pair = currentContext.TaggedVariables[selectKey].Last();
                    break;
                default:
                    throw new NotImplementedException();
            }

            if (pair.Item2 == currentContext || pair.Item1 is GremlinContextVariable)
            {
                currentContext.PivotVariable = pair.Item1;
            }
            else {
                switch (pair.Item1.GetVariableType())
                {
                    case GremlinVariableType.Vertex:
                        GremlinContextVertexVariable contextVertex = new GremlinContextVertexVariable(pair.Item1);
                        contextVertex.IsFromSelect = true;
                        contextVertex.Pop = pop;
                        contextVertex.SelectKey = selectKey;
                        currentContext.VariableList.Add(contextVertex);
                        currentContext.PivotVariable = contextVertex;
                        break;
                    case GremlinVariableType.Edge:
                        GremlinContextEdgeVariable contextEdge = new GremlinContextEdgeVariable(pair.Item1);
                        contextEdge.IsFromSelect = true;
                        contextEdge.Pop = pop;
                        contextEdge.SelectKey = selectKey;
                        currentContext.VariableList.Add(contextEdge);
                        currentContext.PivotVariable = contextEdge;
                        break;
                    case GremlinVariableType.Table:
                        throw new NotImplementedException();
                    case GremlinVariableType.Scalar:
                        throw new NotImplementedException();
                }
            }
        }
        internal virtual void Select(GremlinToSqlContext currentContext, string tagName)
        {
            Select(currentContext, GremlinKeyword.Pop.last, tagName);
        }

        internal virtual void Select(GremlinToSqlContext currentContext, List<string> selectKeys)
        {
            throw new NotImplementedException();
        }

        internal virtual void Select(GremlinToSqlContext currentContext, GremlinKeyword.Pop pop, List<string> selectKeys)
        {
            throw new NotImplementedException();
        }

        //internal virtual void SideEffect(Consumer<Traverser<E>> consumer)
        internal virtual void SideEffect(GremlinToSqlContext currentContext, GremlinToSqlContext sideEffectContext)
        {
            GremlinSideEffectVariable newVariable = new GremlinSideEffectVariable(sideEffectContext);
            currentContext.VariableList.Add(newVariable);
            currentContext.SetVariables.AddRange(sideEffectContext.SetVariables);
            currentContext.SetVariables.Add(newVariable);
        }

        //internal virtual void SimplePath()
        //internal virtual void Store(string sideEffectKey)
        //internal virtual void Subgraph(string sideEffectKey)

        internal virtual void Sum(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        //internal virtual void Sum(Scope scope)


        internal virtual void Tail(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Tail(GremlinToSqlContext currentContext, long limit)
        {
            throw new NotImplementedException();
        }

        //internal virtual void Tail(GremlinToSqlContext currentContext, Scope scope)


        //internal virtual void Tail(GremlinToSqlContext currentContext, Scope scope, long limit)

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
            GremlinTreeVariable newVariable = new GremlinTreeVariable();

            currentContext.VariableList.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }
        //internal virtual void tree(GremlinToSqlContext currentContext, string sideEffectKey)

        internal virtual void Unfold(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Union(ref GremlinToSqlContext currentContext, List<GremlinToSqlContext> unionContexts)
        {
            if (unionContexts.Count == 0)
            {
                throw new NotImplementedException();
            }
            if (unionContexts.Count == 1)
            {
                currentContext = unionContexts.First();
                return;
            }
            GremlinUnionVariable newVariable = new GremlinUnionVariable(unionContexts);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetVariables.Add(newVariable);
            currentContext.PivotVariable = newVariable;
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
            if (propertyKeys.Count == 1)
            {
                Populate(propertyKeys.First());
                GremlinVariableProperty newVariableProperty = new GremlinVariableProperty(this, propertyKeys.First());
                currentContext.VariableList.Add(newVariableProperty);
                currentContext.PivotVariable = newVariableProperty;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        internal virtual void Where(GremlinToSqlContext currentContext, Predicate predicate)
        {
            WScalarExpression secondExpr = null;
            if (predicate.Label != null)
            {
                var compareVar = currentContext.TaggedVariables[predicate.Label].Last().Item1;
                secondExpr = compareVar.DefaultProjection().ToScalarExpression();
            }
            var firstExpr = DefaultProjection().ToScalarExpression();
            var booleanExpr = GremlinUtil.GetBooleanComparisonExpr(firstExpr, secondExpr, predicate);
            currentContext.AddPredicate(booleanExpr);
        }

        internal virtual void Where(GremlinToSqlContext currentContext, string startKey, Predicate predicate)
        {
            throw new NotImplementedException();
        }

        internal virtual void Where(GremlinToSqlContext currentContext, GremlinToSqlContext whereContext)
        {
            WBooleanExpression wherePredicate = whereContext.ToSqlBoolean();
            currentContext.SetVariables.AddRange(whereContext.SetVariables);
            currentContext.AddPredicate(wherePredicate);
        }

    }
}
