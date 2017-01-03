using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.GremlinTranslation2.variables.special;
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
        List<WSelectElement> ToSelectElementList();
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
        public virtual GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Undefined;
        }

        internal virtual void Populate(string name, bool isAlias = false)
        {
            if (!UsedProperties.Contains(name))
            {
                UsedProperties.Add(name);
            }
        }

        internal virtual GremlinVariableProperty DefaultProjection()
        {
            return new GremlinVariableProperty(this, "id");
        }

        internal virtual void AddE(GremlinToSqlContext currentContext, string edgeLabel)
        {
            GremlinAddEVariable newVariable = null;
            if (currentContext.PivotVariable is GremlinAddVVariable)
            {
                newVariable = new GremlinAddEVariable(edgeLabel, currentContext.PivotVariable as GremlinAddVVariable);
            }
            else
            {
                newVariable = new GremlinAddEVariable(edgeLabel, new GremlinVariableReference(currentContext));
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
        //internal virtual void barrier(Comsumer<org.apache.tinkerpop.gremlin.process.traversal.traverser.util,.TraverserSet<Object>> barrierConsumer)

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

        //internal virtual void branch(GremlinToSqlContext currentContext, Function<Traversal<E>, M> function)
        //internal virtual void branch(GremlinToSqlContext currentContext, Traversal<?, M> branchTraversal)

        internal virtual void By(GremlinToSqlContext currentContext, GremlinToSqlContext byContext)
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
        internal virtual void By(GremlinToSqlContext currentContext, GraphTraversal2 traversal)
        {
            throw new NotImplementedException();
        }
        //internal virtual void by(GremlinToSqlContext currentContext, Traversal<?, ?> traversal, Comparator comparator)
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

        internal virtual void Choose(GremlinToSqlContext currentContext, Predicate choosePredicate, GraphTraversal2 trueChoice, GraphTraversal2 falseChoice)
        {
            throw new NotImplementedException();
        }
        internal virtual void Choose(GremlinToSqlContext currentContext, GraphTraversal2 traversalPredicate, GraphTraversal2 trueChoice, GraphTraversal2 falseChoice)
        {
            throw new NotImplementedException();
        }

        internal virtual void Choose(GremlinToSqlContext currentContext, GraphTraversal2 choiceTraversal)
        {
            throw new NotImplementedException();
        }

        internal virtual void Coalesce(
                    GremlinToSqlContext currentContext,
                    GremlinToSqlContext traversal1,
                    GremlinToSqlContext traversal2)
        {
            GremlinVariableType type1 = traversal1.PivotVariable.GetVariableType();
            GremlinVariableType type2 = traversal2.PivotVariable.GetVariableType();

            if (type1 == type2)
            {
                switch (type1)
                {
                    case GremlinVariableType.Vertex:
                        GremlinCoalesceVertexVariable vertexVariable = new GremlinCoalesceVertexVariable(traversal1, traversal2);
                        currentContext.VariableList.Add(vertexVariable);
                        currentContext.TableReferences.Add(vertexVariable);
                        currentContext.PivotVariable = vertexVariable;
                        break;
                    case GremlinVariableType.Edge:
                        GremlinCoalesceEdgeVariable edgeVariable = new GremlinCoalesceEdgeVariable(traversal1, traversal2);
                        currentContext.VariableList.Add(edgeVariable);
                        currentContext.TableReferences.Add(edgeVariable);
                        currentContext.PivotVariable = edgeVariable;
                        break;
                    case GremlinVariableType.Table:
                        GremlinCoalesceTableVariable tabledValue = new GremlinCoalesceTableVariable(traversal1, traversal2);
                        currentContext.VariableList.Add(tabledValue);
                        currentContext.TableReferences.Add(tabledValue);
                        currentContext.PivotVariable = tabledValue;
                        break;
                    case GremlinVariableType.Scalar:
                        currentContext.PivotVariable = new GremlinCoalesceValueVariable(traversal1, traversal2);
                        break;
                    default:
                        break;
                }
            }
            else
            {
                GremlinCoalesceTableVariable tabledValue = new GremlinCoalesceTableVariable(traversal1, traversal2);
                currentContext.VariableList.Add(tabledValue);
                currentContext.TableReferences.Add(tabledValue);
                currentContext.PivotVariable = tabledValue;
            }
        }

        internal virtual void Coin(GremlinToSqlContext currentContext, double probability)
        {
            throw new NotImplementedException();
        }

        internal virtual void Constant(GremlinToSqlContext currentContext, object value)
        {
            throw new NotImplementedException();
        }
        internal virtual void Count(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        //internal virtual void count(GremlinToSqlContext currentContext, Scope scope)
        //internal virtual void cyclicPath(GremlinToSqlContext currentContext)
        //internal virtual void dedup(GremlinToSqlContext currentContext, Scope scope, params string[] dedupLabels)
        internal virtual void Dedup(GremlinToSqlContext currentContext, params string[] dedupLabels)
        {
            throw new NotImplementedException();
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

        //internal virtual void emit(GraphTraversal2 emitTraversal)
        //{
        //    throw new NotImplementedException();
        //}

        //internal virtual void filter(Predicate<Traversal<E>> predicate)
        //internal virtual void filter(Traversal<?, ?> filterTraversal)
        //internal virtual void flatMap(Funtion<Traversal<E>, Iterator<E>> funtion)
        internal virtual void FlatMap(GremlinToSqlContext currentContext, GraphTraversal2 flatMapTraversal)
        {
            throw new NotImplementedException();
        }
        internal virtual void Fold(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        //internal virtual void fold(E2 seed, BiFuntion<E2, E, E2> foldFunction)

        internal virtual void From(GremlinToSqlContext currentContext, string fromGremlinTranslationOperatorLabel)
        {
            throw new NotImplementedException();
        }

        internal virtual void From(GremlinToSqlContext currentContext, GraphTraversal2 fromVertexTraversal)
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
            throw new NotImplementedException();
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

        internal virtual void Has(GremlinToSqlContext currentContext, string propertyKey, GraphTraversal2 propertyTraversal)
        {
            throw new NotImplementedException();
        }

        internal virtual void HasId(GremlinToSqlContext currentContext, params object[] values)
        {
            throw new NotImplementedException();
        }

        internal virtual void HasKey(GremlinToSqlContext currentContext, params object[] values)
        {
            throw new NotImplementedException();
        }

        internal virtual void HasLabel(GremlinToSqlContext currentContext, params object[] values)
        {
            throw new NotImplementedException();
        }

        internal virtual void HasValue(GremlinToSqlContext currentContext, string value, params string[] values)
        {
            throw new NotImplementedException();
        }
        internal virtual void HasNot(GremlinToSqlContext currentContext, string propertyKey)
        {
            throw new NotImplementedException();
        }

        internal virtual void Id(GremlinToSqlContext currentContext)
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

        internal virtual void Inject(GremlinToSqlContext currentContext, params string[] values)
        {
            if (currentContext.VariableList.Count == 0)
            {
                GremlinInjectVariable injectVar = new GremlinInjectVariable(null, values);
                currentContext.VariableList.Add(injectVar);
                currentContext.TableReferences.Add(injectVar);
                currentContext.PivotVariable = injectVar;
            }
            else
            {
                GremlinToSqlContext priorContext = currentContext.Duplicate();
                currentContext.Reset();
                GremlinInjectVariable injectVar = new GremlinInjectVariable(priorContext, values);
                currentContext.VariableList.Add(injectVar);
                currentContext.TableReferences.Add(injectVar);
                currentContext.PivotVariable = injectVar;
            }
        }

        internal virtual void InV(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Is(GremlinToSqlContext currentContext, object value)
        {
            throw new NotImplementedException();
        }

        internal virtual void Is(GremlinToSqlContext currentContext, Predicate predicate)
        {
            throw new NotImplementedException();
        }

        internal virtual void Iterate(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Key(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Label(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Limit(GremlinToSqlContext currentContext, long limit)
        {
            throw new NotImplementedException();
        }

        //internal virtual void Limit(Scope scope, long limit)

        internal virtual void Local(GremlinToSqlContext currentContext, GraphTraversal2 localTraversal)
        {
            throw new NotImplementedException();
        }
        //internal virtual void Loops(GremlinToSqlContext currentContext, )
        //internal virtual void Map(Function<Traversal<?, E2>> function)
        //internal virtual void Map(GremlinToSqlContext currentContext, Traversal<?, E2> mapTraversal)
        //internal virtual void MapKeys() //Deprecated
        //internal virtual void Mapvalues(GremlinToSqlContext currentContext, ) //Deprecated

        internal virtual void Match(GremlinToSqlContext currentContext, params GraphTraversal2[] matchTraversals)
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

        internal virtual void Not(GremlinToSqlContext currentContext, GraphTraversal2 notTraversal)
        {
            throw new NotImplementedException();
        }

        internal virtual void Option(GremlinToSqlContext currentContext, object pickToken, GraphTraversal2 traversalOption)
        {
            throw new NotImplementedException();
        }
        //internal virtual void Option(GremlinToSqlContext currentContext, Traversal<E, E2 tarversalOption>

        internal virtual void Optional(GremlinToSqlContext currentContext, GremlinToSqlContext optionalContext)
        {
            GremlinOptionalVariable newVariable = null;
            //To do more reasoning
            if (optionalContext.PivotVariable.GetVariableType() == GremlinVariableType.Edge)
            {
                newVariable = new GremlinOptionalEdgeVariable(optionalContext);
            }
            else
            {
                throw new NotImplementedException();
            }
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }

        internal virtual void Or(GremlinToSqlContext currentContext, params GraphTraversal2[] orTraversals)
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
            throw new NotImplementedException();
        }

        internal virtual void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            throw new NotImplementedException();
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

        internal virtual void Repeat(GremlinToSqlContext currentContext, GremlinRepeatOp op)
        {
            GremlinUtil.InheritedVariableFromParent(op.RepeatTraversal, currentContext);
            GremlinToSqlContext context = op.RepeatTraversal.GetEndOp().GetContext();

            GremlinToSqlContext conditionContext = null;
            if (op.ConditionTraversal != null)
            {
                GremlinUtil.InheritedVariableFromParent(op.ConditionTraversal, context);
                conditionContext = op.ConditionTraversal.GetEndOp().GetContext();
            }
            else if (op.ConditionPredicate != null)
            {
                throw new NotImplementedException();
            }
            
            GremlinRepeatVariable newVariable = new GremlinRepeatVariable(currentContext.PivotVariable, context)
            {
                     ConditionContext = conditionContext,
                     IsEmitTrue = op.IsEmitTrue,
                     IsEmitAfter = op.IsEmitAfter,
                     IsEmitBefore = op.IsEmitBefore,
                     IsUntilAfter = op.IsUntilAfter,
                     IsUntilBefore = op.IsUntilBefore,
                     IsTimes = op.IsTimes,
                     Times = op.Times
            };
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
        //internal virtual void Select(Column column)

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

            if (pair.Item2 == currentContext)
            {
                currentContext.PivotVariable = pair.Item1;
            }
            else
            {
                if (pair.Item1 is GremlinVertexVariable2)
                {
                    GremlinContextVertexVariable contextVertex = new GremlinContextVertexVariable(pair.Item1 as GremlinVertexVariable2);
                    contextVertex.IsFromSelect = true;
                    contextVertex.Pop = pop;
                    contextVertex.SelectKey = selectKey;
                    currentContext.VariableList.Add(contextVertex);
                    currentContext.PivotVariable = contextVertex;
                }
                else if (pair.Item1 is GremlinEdgeVariable2)
                {
                    GremlinContextEdgeVariable contextEdge = new GremlinContextEdgeVariable(pair.Item1 as GremlinContextEdgeVariable);
                    contextEdge.IsFromSelect = true;
                    contextEdge.Pop = pop;
                    contextEdge.SelectKey = selectKey;
                    currentContext.VariableList.Add(contextEdge);
                    currentContext.PivotVariable = contextEdge;
                }
            }
        }
        internal virtual void Select(GremlinToSqlContext currentContext, string tagName)
        {
            //if (!currentContext.TaggedVariables.ContainsKey(tagName))
            //{
            //    throw new QueryCompilationException(string.Format("The specified tag \"{0}\" is not defined.", tagName));
            //}

            //var pair = currentContext.TaggedVariables[tagName];

            //if (pair.Item2 == currentContext)
            //{
            //    currentContext.PivotVariable = pair.Item1;
            //}
            //else
            //{
            //    if (pair.Item1 is GremlinVertexVariable2)
            //    {
            //        GremlinContextVertexVariable contextVertex = new GremlinContextVertexVariable(pair.Item1 as GremlinVertexVariable2);
            //        currentContext.VariableList.Add(contextVertex);
            //        currentContext.PivotVariable = contextVertex;
            //    }
            //    else if (pair.Item1 is GremlinEdgeVariable2)
            //    {
            //        GremlinContextEdgeVariable contextEdge = new GremlinContextEdgeVariable(pair.Item1 as GremlinContextEdgeVariable);
            //        currentContext.VariableList.Add(contextEdge);
            //        currentContext.PivotVariable = contextEdge;
            //    }
            //}
        }

        //internal virtual void select(string selectKey)
        internal virtual void Select(GremlinToSqlContext currentContext, params string[] selectKeys)
        {
            throw new NotImplementedException();
        }


        //internal virtual void SideEffect(Consumer<Traverser<E>> consumer)
        internal virtual void SideEffect(GremlinToSqlContext currentContext, GraphTraversal2 sideEffectTraversal)
        {
            GremlinUtil.InheritedContextFromParent(sideEffectTraversal, currentContext);
            GremlinToSqlContext context = sideEffectTraversal.GetEndOp().GetContext();
            GremlinSideEffectVariable newVariable = new GremlinSideEffectVariable(context);
            currentContext.VariableList.Add(newVariable);
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

        internal virtual void To(GremlinToSqlContext currentContext, GraphTraversal2 toVertex)
        {
            throw new NotImplementedException();
        }
        //internal virtual void ToE(GremlinToSqlContext currentContext, Direction direction, params string[] edgeLabels)
        //internal virtual void ToV(GremlinToSqlContext currentContext, Direction direction)
        internal virtual void Tree(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }
        //internal virtual void tree(GremlinToSqlContext currentContext, string sideEffectKey)

        internal virtual void Unfold(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void Union(GremlinToSqlContext currentContext, List<GraphTraversal2> unionTraversals)
        {
            if (unionTraversals.Count == 0)
            {
                throw new NotImplementedException();
            }
            if (unionTraversals.Count == 1)
            {
                throw new NotImplementedException();
            }

            List<GremlinToSqlContext> unionContextList = new List<GremlinToSqlContext>();
            foreach (var traversal in unionTraversals)
            {
                GremlinUtil.InheritedContextFromParent(traversal, currentContext);
                unionContextList.Add(traversal.GetEndOp().GetContext());
            }
            GremlinUnionVariable newVariable = new GremlinUnionVariable(unionContextList);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetVariables.Add(newVariable);
            currentContext.PivotVariable = newVariable;
        }

        internal virtual void Until(GremlinToSqlContext currentContext, Predicate untilPredicate)
        {
            throw new NotImplementedException();
        }

        internal virtual void Until(GremlinToSqlContext currentContext, GraphTraversal2 untilTraversal)
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

        internal virtual void Where(GremlinToSqlContext currentContext, GraphTraversal2 whereTraversal)
        {
            GremlinUtil.InheritedVariableFromParent(whereTraversal, currentContext);

            GremlinToSqlContext subQueryContext = whereTraversal.GetEndOp().GetContext();
            WBooleanExpression existPredicate = subQueryContext.ToSqlBoolean();
            currentContext.AddPredicate(existPredicate);
        }

    }
}
