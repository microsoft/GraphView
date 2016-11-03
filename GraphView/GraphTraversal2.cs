using GraphView.GramlinTranslationOperator;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    public class GraphTraversal2
    {
        internal List<GremlinTranslationOperator> _GremlinTranslationOpList = new List<GremlinTranslationOperator>();
        internal GremlinTranslationOperator _lastGremlinTranslationOp { set; get }

        public GraphTraversal2() {

        }

        public GraphTraversal2(GraphTraversal rhs)
        {

        }

        public GraphTraversal2(GraphViewConnection pConnection)
        {
        }

        public void next()
        {
            GraphViewConnection connection = new GraphViewConnection("https://graphview.documents.azure.com:443/",
                "MqQnw4xFu7zEiPSD+4lLKRBQEaQHZcKsjlHxXn2b96pE/XlJ8oePGhjnOofj1eLpUdsfYgEhzhejk2rjH/+EKA==",
                "GroupMatch", "MarvelTest"); ;
            var sqlQuery = _lastGremlinTranslationOp.GetContext().ToSqlQuery().Generate(connection);
        }

        internal void addGremlinOperator(GremlinTranslationOperator newGremlinTranslationOp)
        {
            _GremlinTranslationOpList.Add(newGremlinTranslationOp);
            if (_lastGremlinTranslationOp == null)
            {
                _lastGremlinTranslationOp = newGremlinTranslationOp;
            } else
            {
                newGremlinTranslationOp.InputOperator = _lastGremlinTranslationOp;
                _lastGremlinTranslationOp = newGremlinTranslationOp;
            }
        }

        //GremlinTranslationOperator

        //public GraphTraversal addE(Direction direction, string firstVertexKeyOrEdgeLabel, string edgeLabelOrSecondVertexKey, params Object[] propertyKeyValues)

        public GraphTraversal2 addE(string edgeLabel)
        {
            return this;
        }

        public GraphTraversal2 addInE(string firstVertexKeyOrEdgeLabel, string edgeLabelOrSecondVertexKey, params Object[] propertyKeyValues)
        {
            return this;
        }

        public GraphTraversal2 addOutE(string firstVertexKeyOrEdgeLabel, string edgeLabelOrSecondVertexKey, params Object[] propertyKeyValues)
        {
            return this;
        }

        public GraphTraversal2 addV()
        {
            return this;
        }

        public GraphTraversal2 addV(params Object[] propertyKeyValues)
        {
            return this;
        }

        public GraphTraversal2 addV(string vertexLabel)
        {
            return this;
        }

        public GraphTraversal2 aggregate(string sideEffectKey)
        {
            return this;
        }

        public GraphTraversal2 and(params GraphTraversal2[] andTraversals)
        {
            GremlinAndOp AndOp = new GremlinAndOp();
            foreach (var traversal in andTraversals)
            {
                AndOp.ConjunctiveOperators.Add(traversal._lastGremlinTranslationOp);
            }
            this.addGremlinOperator(AndOp);
            return this;
        }
        
            
        //public GraphTraversal2 as(string GremlinTranslationOperatorLabel, params string[] GremlinTranslationOperatorLabels)
        //public GraphTraversal2 barrier()
        //public GraphTraversal2 barrier(Comsumer<org.apache.tinkerpop.gremlin.process.traversal.traverser.util,.TraverserSet<Object>> barrierConsumer)
        //public GraphTraversal2 both(params string[] edgeLabels)
        //public GraphTraversal2 bothE(params string[] edgeLabels)
        //public GraphTraversal2 branch(Function<Traversal<E>, M> function)
        //public GraphTraversal2 branch(Traversal<?, M> branchTraversal)
        //public GraphTraversal2 by()
        //public GraphTraversal2 by(Comparator<E> comparator)
        //public GraphTraversal2 by(Function<U, Object> function, Comparator comparator)
        //public GraphTraversal2 by(Function<V, Object> function)
        //public GraphTraversal2 by(Order order)
        //public GraphTraversal2 by(string key)
        //public GraphTraversal2 by(string key, Comparator<V> comparator)
        //public GraphTraversal2 by(T token)
        //public GraphTraversal2 by(Traversal<?, ?> traversal)
        //public GraphTraversal2 by(Traversal<?, ?> traversal, Comparator comparator)
        //public GraphTraversal2 cap(string sideEffectKey, params string[] sideEffectKeys)
        //public GraphTraversal2 choose(Function<E, M> choiceFunction)
        //public GraphTraversal2 choose(Predicate<E> choosePredicate, Traversal<?, E2> trueChoice, Traversal<?, E2> falseChoice)
        //public GraphTraversal2 choose(Traversal<?, ?>t traversalPredicate, Travaersal<?, E2> trueChoice, Traversal<?, E2> falseChoice)
        //public GraphTraversal2 choose(Traversal<?, M> choiceTraversal)
        //public GraphTraversal2 coalesce(Traversal<?, E2> ..coalesceTraversals)
        //public GraphTraversal2 coin(double probability)
        //public GraphTraversal2 constant(E2 e)
        //public GraphTraversal2 count()
        //public GraphTraversal2 count(Scope scope)
        //public GraphTraversal2 cyclicPath()
        //public GraphTraversal2 dedup(Scope scope, params string[] dedupLabels)
        //public GraphTraversal2 dedup(params string[] dedupLabels)
        //public GraphTraversal2 drop()

        public GraphTraversal2 E()
        {
            this.addGremlinOperator(new GremlinEOp());
            return this;
        }


        //public GraphTraversal2 emit()
        //public GraphTraversal2 emit(Predicate<Traversal<E>> emitPredicate)
        //public GraphTraversal2 emit(Traversal<?, ?> emitTraversal)
        //public GraphTraversal2 filter(Predicate<Traversal<E>> predicate)
        //public GraphTraversal2 filter(Traversal<?, ?> filterTraversal)
        //public GraphTraversal2 flatMap(Funtion<Traversal<E>, Iterator<E>> funtion)
        //public GraphTraversal2 flatMap(Traversal<?, E2> flatMapTraversal)
        //public GraphTraversal2 fold()
        //public GraphTraversal2 fold(E2 seed, BiFuntion<E2, E, E2> foldFunction)
        //public GraphTraversal2 from(string fromGremlinTranslationOperatorLabel)
        //public GraphTraversal2 from(Traversal<E, Vertex> fromVertex)
        //public GraphTraversal2 group()
        //public GraphTraversal2 group(string sideEffectKey)
        //public GraphTraversal2 groupCount()
        //public GraphTraversal2 groupCount(string sideEffectKey)
        //public GraphTraversal2 groupV3d0() //Deprecated
        //public GraphTraversal2 groupV3d0(string sideEffectKey) //Deprecated

        public GraphTraversal2 has(string propertyKey)
        {
            return this;
        }

        public GraphTraversal2 has(string propertyKey, Object value)
        {
            return this;
        }

        //public GraphTraversal2 has(string propertyKey, P<?> predicate)

        public GraphTraversal2 has(string label, string propertyKey, Object value)
        {
            return this;
        }

        //public GraphTraversal2 has(string label, string propertyKey, Predicate<?> predicate)
        //public GraphTraversal2 has(string propertyKey, Traversal<?, ?> propertyTraversal)
        //public GraphTraversal2 has(T accessor, Object value)
        //public GraphTraversal2 has(T accessor, Object value, Object ...value)
        //public GraphTraversal2 has(T accessor, Traversal<?, ?> propertyTraversal)

        public GraphTraversal2 hasId(string value, params string[] values)
        {
            return this;
        }

        public GraphTraversal2 hasKey(string value, params string[] values)
        {
            return this;
        }

        public GraphTraversal2 hasLabel(string value, params string[] values)
        {
            return this;
        }

        public GraphTraversal2 hasNot(string propertyKey)
        {
            return this;
        }

        public GraphTraversal2 hasValue(string value, params string[] values)
        {
            return this;
        }

        public GraphTraversal2 id()
        {
            return this;
        }

        public GraphTraversal2 In(params string[] edgeLabels)
        {
            return this;
        }

        public GraphTraversal2 inE(params string[] edgeLabels)
        {
            return this;
        }

        //public GraphTraversal2 inject()

        public GraphTraversal2 inV()
        {
            return this;
        }

        public GraphTraversal2 Is(string value)
        {
            return this;
        }

        //public GraphTraversal2 Is(P<E> predicate)

        public GraphTraversal2 iterate()
        {
            return this;
        }

        public GraphTraversal2 key()
        {
            return this;
        }

        public GraphTraversal2 label()
        {
            return this;
        }

        public GraphTraversal2 limit(long limit)
        {
            return this;
        }

        //public GraphTraversal2 limit(Scope scope, long limit)

        //public GraphTraversal2 local(Traversal<?, E2> localTraversal)
        //public GraphTraversal2 loops()
        //public GraphTraversal2 map(Function<Traversal<?, E2>> function)
        //public GraphTraversal2 map(Traversal<?, E2> mapTraversal)
        //public GraphTraversal2 mapKeys() //Deprecated
        //public GraphTraversal2 mapvalues() //Deprecated
        //public GraphTraversal2 match(Traversal<?, ?>..matchTraversals)

        public GraphTraversal2 max()
        {
            return this;
        }

        //public GraphTraversal2 max(Scope scope)

        public GraphTraversal2 mean()
        {
            return this;
        }

        //public GraphTraversal2 mean(Scope scope)

        public GraphTraversal2 min()
        {
            return this;
        }

        //public GraphTraversal2 min(Scope scope)


        //public GraphTraversal2 not(Traversal<?, ?> notTraversal)
        //public GraphTraversal2 option(M pickToken, Traversal<E, E2> traversalOption)
        //public GraphTraversal2 option(Traversal<E, E2 tarversalOption>
        //public GraphTraversal2 optional(Traversal<E, E2> traversalOption)
        //public GraphTraversal2 or(Traversal<?, ?> ...orTraversals)

        public GraphTraversal2 order()
        {
            return this;
        }

        //public GraphTraversal2 order(Scope scope)


        public GraphTraversal2 otherV()
        {
            return this;
        }

        public GraphTraversal2 Out(params string[] edgeLabels)
        {
            this.addGremlinOperator(new GremlinOutOp(edgeLabels));
            return this;
        }

        public GraphTraversal2 outE(params string[] edgeLabels)
        {
            return this;
        }

        public GraphTraversal2 outV()
        {
            return this;
        }

        //public GraphTraversal2 pageRank()
        //public GraphTraversal2 pageRank(double alpha)
        //public GraphTraversal2 path()
        //public GraphTraversal2 peerPressure()
        //public GraphTraversal2 profile()
        //public GraphTraversal2 profile(string sideEffectKey)
        //public GraphTraversal2 program(VertexProgram<?> vertexProgram)
        //public GraphTraversal2 project(string projectKey, params string[] otherProjectKeys)

        public GraphTraversal2 properties(params string[] propertyKeys)
        {
            return this;
        }

        public GraphTraversal2 property(string key, string value, params string[] keyValues)
        {
            return this;
        }

        //public GraphTraversal2 property(VertexProperty.Cardinality cardinality, string key, string value, params string[] keyValues)

        public GraphTraversal2 propertyMap(params string[] propertyKeys)
        {
            return this;
        }

        public GraphTraversal2 range(long low, long high)
        {
            return this;
        }

        //public GraphTraversal2 repeat(Traversal<?, E> repeatTraversal)

        //public GraphTraversal2 sack() //Deprecated
        //public GraphTraversal2 sack(BiFunction<V, U, V>) sackOperator) //Deprecated
        //public GraphTraversal2 sack(BiFunction<V, U, V>) sackOperator, string, elementPropertyKey) //Deprecated

        public GraphTraversal2 sample(int amountToSample)
        {
            return this;
        }

        //public GraphTraversal2 sample(Scope scope, int amountToSample)
        //public GraphTraversal2 select(Column column)
        //public GraphTraversal2 select(Pop pop, string selectKey)
        //public GraphTraversal2 select(Pop pop, string selectKey1, string selectKey2, params string[] otherSelectKeys)
        //public GraphTraversal2 select(string selectKey)
        //public GraphTraversal2 select(string selectKey1, string selectKey2, params string[] otherSelectKeys)
        //public GraphTraversal2 sideEffect(Consumer<Traverser<E>> consumer)
        //public GraphTraversal2 sideEffect(Traversal<?, ?> sideEffectTraversal)
        //public GraphTraversal2 simplePath()
        //public GraphTraversal2 store(string sideEffectKey)
        //public GraphTraversal2 subgraph(string sideEffectKey)

        public GraphTraversal2 sum()
        {
            return this;
        }

        //public GraphTraversal2 sum(Scope scope)


        public GraphTraversal2 tail()
        {
            return this;
        }

        public GraphTraversal2 tail(long limit)
        {
            return this;
        }

        //public GraphTraversal2 tail(Scope scope)


        //public GraphTraversal2 tail(Scope scope, long limit)

        public GraphTraversal2 timeLimit(long timeLimit)
        {
            return this;
        }

        public GraphTraversal2 times(int maxLoops)
        {
            return this;
        }

        //public GraphTraversal2 to(Direction direction, params string[] edgeLabels)

        public GraphTraversal2 to(string toGremlinTranslationOperatorLabel)
        {
            return this;
        }

        //public GraphTraversal2 to(Traversal<E, Vertex> toVertex)
        //public GraphTraversal2 toE(Direction direction, params string[] edgeLabels)
        //public GraphTraversal2 toV(Direction direction)
        //public GraphTraversal2 tree()
        //public GraphTraversal2 tree(string sideEffectKey)

        public GraphTraversal2 unfold()
        {
            return this;
        }

        //public GraphTraversal2 union(params Traversal<?, E2>[] unionTraversals)
        //public GraphTraversal2 until(Predicate<Traverser<E>> untilPredicate)
        //public GraphTraversal2 unitl(Traversal<?, ?> untilTraversal)

        public GraphTraversal2 V()
        {
            this.addGremlinOperator(new GremlinVOp());
            return this;
        }

        public GraphTraversal2 value()
        {
            return this;
        }

        public GraphTraversal2 valueMap(Boolean includeTokens, params string[] propertyKeys)
        {
            return this;
        }

        public GraphTraversal2 valueMap(params string[] propertyKeys)
        {
            return this;
        }

        public GraphTraversal2 values(params string[] propertyKeys)
        {
            return this;
        }

        //public GraphTraversal2 where(P<string> predicate)
        //public GraphTraversal2 where(string startKey, P<string> predicate)
        //public GraphTraversal2 where(Traversal<?, ?> whereTraversal)
    }
}
