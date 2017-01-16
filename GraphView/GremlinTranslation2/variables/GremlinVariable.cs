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
        Undefined
    }
     
    internal abstract class GremlinVariable
    {
        public string VariableName { get; set; }
        public int Low { get; set; }
        public int High { get; set; }
        public List<string> Labels { get; set; }
        public GremlinToSqlContext ParentContext;

        public GremlinVariable()
        {
            Low = Int32.MinValue;
            High = Int32.MaxValue;
            Labels = new List<string>();
        }

        internal virtual GremlinVariableType GetVariableType()
        {
            throw new NotImplementedException();
        }

        internal virtual bool ContainsLabel(string label)
        {
            return Labels.Contains(label);
        }

        internal virtual void Populate(string property) {}

        internal virtual string BottomUpPopulate(string property, GremlinVariable terminateVariable, string alias, string columnName = null)
        {
            if (this is GremlinBranchVariable)
            {
                foreach (var variableList in (this as GremlinBranchVariable).BrachVariableList)
                {
                    foreach (var variable in variableList)
                    {
                        variable.BottomUpPopulate(property, terminateVariable, alias, columnName);
                    }
                } 
                return alias + "_" + property;
            }
            else
            {
                if (terminateVariable == this) return property;
                if (ParentContext == null) throw new Exception();
                if (columnName == null)
                {
                    columnName = alias + "_" + property;
                }
                ParentContext.AddProjectVariablePropertiesList(new GremlinVariableProperty(this, property), columnName);
                if (ParentContext.ParentVariable == null) throw new Exception();
                return ParentContext.ParentVariable.BottomUpPopulate(columnName, terminateVariable, alias, columnName);
            }
        }

        internal virtual void PopulateGremlinPath() {}

        internal virtual List<GremlinVariable> PopulateAllTaggedVariable(string label)
        {
            if (Labels.Contains(label)) return new List<GremlinVariable>() {this};
            return null;
        }

        //internal virtual GremlinVariable PopulateLastTaggedVariable(string label)
        //{
        //    if (Labels.Contains(label)) return this;
        //    return null;
        //}

        //internal virtual GremlinVariable PopulateFirstTaggedVariable(string label)
        //{
        //    if (Labels.Contains(label)) return this;
        //    return null;
        //}

        internal virtual GremlinVariableProperty GetPath()
        {
            return DefaultProjection();
        }

        internal virtual GremlinVariableProperty DefaultProjection()
        {
            throw new NotImplementedException();
        }

        internal virtual void AddE(GremlinToSqlContext currentContext, string edgeLabel)
        {
            GremlinAddEVariable newVariable = new GremlinAddEVariable(this, edgeLabel);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        //internal virtual void addInE(GremlinToSqlContext currentContext, string firstVertexKeyOrEdgeLabel, string edgeLabelOrSecondVertexKey, params Object[] propertyKeyValues)
        //internal virtual void addOutE(GremlinToSqlContext currentContext, string firstVertexKeyOrEdgeLabel, string edgeLabelOrSecondVertexKey, params Object[] propertyKeyValues)

        internal virtual void AddV(GremlinToSqlContext currentContext)
        {
            throw new NotImplementedException();
        }

        internal virtual void AddV(GremlinToSqlContext currentContext, params object[] propertyKeyValues)
        {
            throw new NotImplementedException();
        }

        internal virtual void AddV(GremlinToSqlContext currentContext, string vertexLabel)
        {
            GremlinAddVVariable newVariable = new GremlinAddVVariable(vertexLabel);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Aggregate(GremlinToSqlContext currentContext, string sideEffectKey)
        {
            throw new NotImplementedException();
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
                if (!currentContext.TaggedVariables.ContainsKey(label))
                {
                    currentContext.TaggedVariables[label] = new List<GremlinVariable>();
                }
                currentContext.TaggedVariables[label].Add(this);
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

            //    GremlinVariable var = currentContext.TaggedVariables[key].Item1;
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
            GremlinTableVariable newVariable = GremlinCoalesceVariable.Create(coalesceContextList);
            foreach (var context in coalesceContextList)
            {
                context.ParentVariable = newVariable;
            }
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
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

        //internal virtual void count(GremlinToSqlContext currentContext, Scope scope)
        //internal virtual void cyclicPath(GremlinToSqlContext currentContext)
        //internal virtual void dedup(GremlinToSqlContext currentContext, Scope scope, params string[] dedupLabels)
        internal virtual void Dedup(GremlinToSqlContext currentContext, List<string> dedupLabels)
        {
            //GremlinTableVariable newVariable = GremlinDedupVariable.Create(this, dedupLabels);
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
            GremlinTableVariable flatMapVariable = GremlinFlatMapVariable.Create(flatMapContext);
            currentContext.VariableList.Add(flatMapVariable);
            
            //It's used for repeat step, we should propagate all the variable to the main context
            //Then we can check the variableList to know if the sub context used the main context variable when
            //the variable is GremlinContextVariable and the value of IsFromSelect is True
            //
            currentContext.VariableList.AddRange(flatMapContext.VariableList);

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
            throw new NotImplementedException();
        }

        internal virtual void HasId(GremlinToSqlContext currentContext, List<object> values)
        {
            throw new QueryCompilationException("The Has(key, predicate) step only applies to vertices and edges.");
        }

        internal virtual void HasKey(GremlinToSqlContext currentContext, List<string> values)
        {
            throw new QueryCompilationException("The Has(key, predicate) step only applies to properties.");
        }

        internal virtual void HasLabel(GremlinToSqlContext currentContext, List<object> values)
        {
            Populate(GremlinKeyword.Label);
            List<WBooleanExpression> booleanExprList = new List<WBooleanExpression>();
            foreach (var value in values)
            {
                WScalarExpression firstExpr = SqlUtil.GetColumnReferenceExpr(VariableName, GremlinKeyword.Label);
                WScalarExpression secondExpr = SqlUtil.GetValueExpr(value);
                booleanExprList.Add(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
            }
            WBooleanExpression concatSql = SqlUtil.ConcatBooleanExprWithOr(booleanExprList);
            currentContext.AddPredicate(concatSql);
        }

        internal virtual void HasValue(GremlinToSqlContext currentContext, List<object> values)
        {
            throw new QueryCompilationException("The Has(key, predicate) step only applies to properties.");
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
            WScalarExpression firstExpr = DefaultProjection().ToScalarExpression();
            WScalarExpression secondExpr = SqlUtil.GetValueExpr(value);
            currentContext.AddPredicate(SqlUtil.GetEqualBooleanComparisonExpr(firstExpr, secondExpr));
        }

        internal virtual void Is(GremlinToSqlContext currentContext, Predicate predicate)
        {
            WScalarExpression secondExpr = null;
            if (predicate.Label != null)
            {
                var compareVar = currentContext.TaggedVariables[predicate.Label].Last();
                secondExpr = compareVar.DefaultProjection().ToScalarExpression();
            }
            else if (predicate.Number != null)
            {
                secondExpr = SqlUtil.GetValueExpr(predicate.Number);
            }
            else
            {
                throw new Exception();
            }
            var firstExpr = DefaultProjection().ToScalarExpression();
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

        internal virtual void Limit(GremlinToSqlContext currentContext, long limit)
        {
            throw new NotImplementedException();
        }

        //internal virtual void Limit(Scope scope, long limit)

        internal virtual void Local(GremlinToSqlContext currentContext, GremlinToSqlContext localContext)
        {
            GremlinTableVariable localMapVariable = GremlinLocalVariable.Create(localContext);
            localContext.ParentVariable = localMapVariable;
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
            
            //It's used for repeat step, we should propagate all the variable to the main context
            //Then we can check the variableList to know if the sub context used the main context variable when
            //the variable is GremlinContextVariable and the value of IsFromSelect is True
            //
            currentContext.VariableList.AddRange(mapContext.VariableList);

            currentContext.TableReferences.Add(mapVariable);
            currentContext.SetPivotVariable(mapVariable);
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
            optionalContext.ParentVariable = newVariable;
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
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

        internal virtual void Project(GremlinToSqlContext currentContext, List<string> projectKeys)
        {
            GremlinProjectVariable newVariable = new GremlinProjectVariable(currentContext.Duplicate(), projectKeys);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal virtual void Properties(GremlinToSqlContext currentContext, List<string> propertyKeys)
        {
            throw new QueryCompilationException("The OutV() step can only be applied to edges or vertex.");
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

        internal virtual void Range(GremlinToSqlContext currentContext, int low, int high)
        {
            throw new NotImplementedException();
        }

        internal virtual void Repeat(GremlinToSqlContext currentContext, GremlinToSqlContext repeatContext,
                                     RepeatCondition repeatCondition)
        {
            GremlinTableVariable newVariable = GremlinRepeatVariable.Create(this, repeatContext, repeatCondition);
            repeatContext.ParentVariable = newVariable;
            if (repeatContext.PivotVariable.GetVariableType() == GremlinVariableType.Edge)
                newVariable.Populate(GremlinKeyword.EdgeID);
            if (repeatContext.PivotVariable.GetVariableType() == GremlinVariableType.Vertex)
                newVariable.Populate(GremlinKeyword.NodeID);
            if (repeatContext.PivotVariable.GetVariableType() == GremlinVariableType.Scalar)
                newVariable.Populate(GremlinKeyword.TableValue);
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

        internal virtual void Select(GremlinToSqlContext currentContext, GremlinKeyword.Pop pop, string selectKey)
        {
            //GremlinVariable selectVariable;
            //switch (pop)
            //{
            //    case GremlinKeyword.Pop.first:
            //        selectVariable = currentContext.SelectFirstTaggedVariable(selectKey);
            //        break;
            //    case GremlinKeyword.Pop.last:
            //        selectVariable = currentContext.SelectLastTaggedVariable(selectKey);
            //        break;
            //    default:
            //        throw new NotImplementedException();
            //}

            //if (selectVariable == null)
            //{
            //    throw new QueryCompilationException(string.Format("The specified tag \"{0}\" is not defined.", selectKey));
            //}
            //else if (selectVariable is GremlinListVariable)
            //{
            //    currentContext.VariableList.Add(selectVariable);
            //    currentContext.PivotVariable = selectVariable;
            //} else if (selectVariable is GremlinWrapVariable)
            //{
            //    GremlinExpandVariable expandVariable = new GremlinExpandVariable(selectVariable);
            //    currentContext.VariableList.Add(expandVariable);
            //    currentContext.TableReferences.Add(expandVariable);
            //    currentContext.PivotVariable = expandVariable;
            //}
            //else
            //{
            //    switch (selectVariable.GetVariableType())
            //    {
            //        case GremlinVariableType.Vertex:
            //            GremlinContextVertexVariable contextVertex = new GremlinContextVertexVariable(selectVariable);
            //            contextVertex.IsFromSelect = true;
            //            contextVertex.Pop = pop;
            //            contextVertex.SelectKey = selectKey;
            //            currentContext.VariableList.Add(contextVertex);
            //            currentContext.SetPivotVariable(contextVertex);
            //            break;
            //        case GremlinVariableType.Edge:
            //            GremlinContextEdgeVariable contextEdge = new GremlinContextEdgeVariable(selectVariable);
            //            contextEdge.IsFromSelect = true;
            //            contextEdge.Pop = pop;
            //            contextEdge.SelectKey = selectKey;
            //            currentContext.VariableList.Add(contextEdge);
            //            currentContext.SetPivotVariable(contextEdge);
            //            break;
            //        case GremlinVariableType.Table:
            //            throw new NotImplementedException();
            //        case GremlinVariableType.Scalar:
            //            throw new NotImplementedException();
            //    }
            //}

                //List<GremlinVariable> taggedVariable = new List<GremlinVariable>();

                //foreach (var variable in currentContext.InheritedVariableList)
                //{
                //    var  
                //}

                //GremlinVariable variable;

                //if (currentContext.TaggedVariables.ContainsKey(selectKey))
                //{
                //    switch (pop)
                //    {
                //        case GremlinKeyword.Pop.first:
                //            variable = currentContext.TaggedVariables[selectKey].First();
                //            break;
                //        case GremlinKeyword.Pop.last:
                //            variable = currentContext.TaggedVariables[selectKey].Last();
                //            break;
                //        default:
                //            throw new NotImplementedException();
                //    }
                //    currentContext.SetPivotVariable(variable);
                //}
                //else if (currentContext.InheritedTaggedVariables.ContainsKey(selectKey))
                //{
                //    switch (pop)
                //    {
                //        case GremlinKeyword.Pop.first:
                //            variable = currentContext.InheritedTaggedVariables[selectKey].First();
                //            break;
                //        case GremlinKeyword.Pop.last:
                //            variable = currentContext.InheritedTaggedVariables[selectKey].Last();
                //            break;
                //        default:
                //            throw new NotImplementedException();
                //    }
                //    switch (variable.GetVariableType())
                //    {
                //        case GremlinVariableType.Vertex:
                //            GremlinContextVertexVariable contextVertex = new GremlinContextVertexVariable(variable);
                //            contextVertex.IsFromSelect = true;
                //            contextVertex.Pop = pop;
                //            contextVertex.SelectKey = selectKey;
                //            currentContext.VariableList.Add(contextVertex);
                //            currentContext.SetPivotVariable(contextVertex);
                //            break;
                //        case GremlinVariableType.Edge:
                //            GremlinContextEdgeVariable contextEdge = new GremlinContextEdgeVariable(variable);
                //            contextEdge.IsFromSelect = true;
                //            contextEdge.Pop = pop;
                //            contextEdge.SelectKey = selectKey;
                //            currentContext.VariableList.Add(contextEdge);
                //            currentContext.SetPivotVariable(contextEdge);
                //            break;
                //        case GremlinVariableType.Table:
                //            throw new NotImplementedException();
                //        case GremlinVariableType.Scalar:
                //            throw new NotImplementedException();
                //    }
                //}
                //else
                //{
                //    throw new QueryCompilationException(string.Format("The specified tag \"{0}\" is not defined.", selectKey));
                //}
            }

        internal virtual void Select(GremlinToSqlContext currentContext, string label)
        {
            List<GremlinVariable> taggedVariableList = currentContext.Select(label);

            if (taggedVariableList.Count == 0)
            {
                throw new QueryCompilationException(string.Format("The specified tag \"{0}\" is not defined.", label));
            } else if (taggedVariableList.Count == 1)
            {
                currentContext.VariableList.Add(taggedVariableList.First());
                currentContext.PivotVariable = taggedVariableList.First();
            }
            else
            {
                GremlinListVariable newVariableList = new GremlinListVariable(taggedVariableList);
                currentContext.VariableList.Add(newVariableList);
                currentContext.PivotVariable = newVariableList;
            }

            
            //if (selectVariable is GremlinListVariable)
            //{
            //    currentContext.VariableList.Add(selectVariable);
            //    currentContext.PivotVariable = selectVariable;
            //} else if (selectVariable is GremlinWrapVariable)
            //{
            //    GremlinExpandVariable newExpandVariable = new GremlinExpandVariable(selectVariable);
            //    currentContext.VariableList.Add(newExpandVariable);
            //    currentContext.TableReferences.Add(newExpandVariable);
            //    currentContext.PivotVariable = newExpandVariable;
            //}
            //else
            //{
            //    switch (selectVariable.GetVariableType())
            //    {
            //        case GremlinVariableType.Vertex:
            //            GremlinContextVertexVariable contextVertex = new GremlinContextVertexVariable(selectVariable);
            //            contextVertex.IsFromSelect = true;
            //            contextVertex.SelectKey = label;
            //            currentContext.VariableList.Add(contextVertex);
            //            currentContext.SetPivotVariable(contextVertex);
            //            break;
            //        case GremlinVariableType.Edge:
            //            GremlinContextEdgeVariable contextEdge = new GremlinContextEdgeVariable(selectVariable);
            //            contextEdge.IsFromSelect = true;
            //            contextEdge.SelectKey = label;
            //            currentContext.VariableList.Add(contextEdge);
            //            currentContext.SetPivotVariable(contextEdge);
            //            break;
            //        case GremlinVariableType.Table:
            //            throw new NotImplementedException();
            //        case GremlinVariableType.Scalar:
            //            throw new NotImplementedException();
            //    }
            //}
        }

        internal virtual void Select(GremlinToSqlContext currentContext, List<string> selectKeys)
        {
            //TODO: select all the variable as a map
            Select(currentContext, GremlinKeyword.Pop.last, selectKeys);
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
            currentContext.TableReferences.Add(newVariable);
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
            currentContext.PopulateGremlinPath();
            GremlinVariableProperty pathVariableProperty = currentContext.CurrentContextPath.DefaultProjection();
            GremlinTreeVariable newVariable = new GremlinTreeVariable(currentContext.Duplicate(), pathVariableProperty);
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
                unionContext.ParentVariable = newVariable;
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
            WScalarExpression secondExpr = null;
            if (predicate.Label != null)
            {
                //TODO
                var compareVar = currentContext.TaggedVariables[predicate.Label].Last();
                Populate(compareVar.DefaultProjection().VariableProperty);
                secondExpr = compareVar.DefaultProjection().ToScalarExpression();
            }
            else
            {
                throw new Exception("Predicate.Label can't be null");
            }
            var firstExpr = DefaultProjection().ToScalarExpression();
            Populate(DefaultProjection().VariableProperty);
            var booleanExpr = SqlUtil.GetBooleanComparisonExpr(firstExpr, secondExpr, predicate);
            currentContext.AddPredicate(booleanExpr);
        }

        internal virtual void Where(GremlinToSqlContext currentContext, string startKey, Predicate predicate)
        {
            throw new NotImplementedException();
        }

        internal virtual void Where(GremlinToSqlContext currentContext, GremlinToSqlContext whereContext)
        {
            WBooleanExpression wherePredicate = whereContext.ToSqlBoolean();
            currentContext.AddPredicate(wherePredicate);
        }

    }
}
