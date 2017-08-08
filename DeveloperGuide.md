# GraphView

## Introduction
GraphView is a middleware of database.

> A graph is a structure that's composed of vertices and edges. Both vertices and edges can have an arbitrary number of properties. Vertices denote discrete objects such as a person, a place, or an event. Edges denote relationships between vertices. 

GraphView could let you store, manipulate, and retrieve a graph on relational database or NoSQL database by SQL-like language or [Gremlin][7] language.

We implement GraphView in C#, which is an open source project published on [Github][8].

We divide a Gremlin query into three parts, including translation, compilation, execution. And if SQL-like language is used,  the latter two parts will enough.

## Main processes

### Translation

GraphView provide two ways to store, manipulate and retrieve a graph. One way is to use C# API, the other is to use a string variable to maintain the command. If we use the latter way, GraphView will replace the gremlin-groovy statement with the C# codes by regular expressions and generate a complete C# codes. And then compile and execute them in GraphView runtime dynamicly. So it is more vital to understand the former way.

Let's start from an easy example. 

``` C#
GraphViewCommand command = new GraphViewCommand(graphConnection);
var traversal = command.g().V().Has("name").Out("created");
var result = traversal.Next();
foreach (var r in result)
{
    Console.WriteLine(r);
}
```

First of all, an instance of GraphViewConnection is required. In this case, `graphConnection` is the instance.

Then, an instance of GraphViewCommand is created, which needs a GraphViewConnection as a parameter.

In gremlin, `g()`, `V()`, `Has("name")` and `Out("created")` are called *steps*. In C#, they are method callings. It is obvious that the execution order of this method chaining is  `g()`, `V()`, `Has("name")` and `Out("created")`.

`g()` creates a GraphTraversal instance.  
`V()` adds a GremlinVOp in a GremlinTranslationOpList included in the GraphTraversal instance.  
`Has("name")` adds a GremlinHasOp to the same GraphTraversal.  
`Out("created")` adds a GremlinOutOp to the same GraphTraversal.  

Therefore, GraphView gets a traversal object with a GremlinTranslationOpList, which maintains information about all steps. That is, the traversal object contains all the information about this query.

When `Next()` of the traversal is called, GraphView gets an instance of WSqlScript through a series of complicated procedures. In fact, this instance is a SQL syntax tree which we will explain later. The result of WSqlScript_Instance.ToString() is like the following

``` SQL
SELECT ALL N_19.* AS value$3087edd8
FROM node AS [N_18], node AS [N_19]
MATCH N_18-[Edge AS E_6]->N_19
WHERE 
 EXISTS (
   SELECT ALL R_0.value$3087edd8 AS value$3087edd8
   FROM CROSS APPLY  Properties(N_18.name) AS [R_0]
 ) AND E_6.label = 'created'
```

As you can see, there is some differences between this SQL-like script and the standard SQL query. But now we just ignore these differences for the moment.

### Compilation

After the translation part finishing, GraphView compiles this SQL syntax tree and gets a linked list of GraphViewExecutionOperator. As you know, GraphView just needs the last operator in this list as it needs the head of one linked list. 

### Execution

For the GraphViewExecutionOperator list, it calls `Next()` until the result is not `null`. Then the execution of this Gremlin query is done.

## Project file structure
Now (8/1/2017) we are working on branch [clean-up][9]. Generally, we have two subprojects, `GraphView` and `GraphViewUnitTest` .

### Main Subproject: GraphView
This is our core codes. 

#### GraphTraversal.cs
Include the C# API about [Gremlin][7]. 

> Gremlin is written in Java 8. There are various language variants of Gremlin such as Gremlin-Groovy, Gremlin-Python, Gremlin-Scala, Gremlin-JavaScript, Gremlin-Clojure, etc. It is best to think of Gremlin as a style of graph traversing that is not bound to a particular programming language per se.

And what we do in this file is implementing the Gremlin-API in C#.

``` C#
GraphViewCommand command = new GraphViewCommand(graphConnection);
var traversal = command.g().V().Out("created").Has("name", "lop");
var result = traversal.Next();
```
  
#### GraphViewConnection
It provides the ability of communication between the GraphView and datebase (i.e. CosmosDB, MariaDB). 

#### GraphViewDBPortal
It's the main member of a GraphViewConnection object, which sends specific queries to database to execute. You can derive a new DBPortal to communicate another database if necessary.

#### GremlinTraslation
The implementation of translation from almost all [Gremlin steps][10] to our defined SQL-like language script (maybe named GraphViewQuery?) .

``` SQL
--command.g().V().Has("name", "josh")
SELECT ALL N_18.* AS value$ea54b4b6
FROM node AS [N_18]
WHERE N_18.name = 'josh'
```

#### TSQL Syntax Tree
The definition of SQL-like syntax tree. In fact, we store the translation result as a SQL-like syntax tree.

#### GraphViewQueryCompiler
Compile the SQL-like syntax tree to an GraphViewExecutionOperator list.

#### GraphViewExecutionRuntime
Accept the result of GraphViewQueryCompiler (the operator list) and execute the operators via GraphViewConnection.

#### GraphViewUtils
Just as its name.

#### DocDBScript.cs
Something about DocumentDB Identifier Normalization. Just ignore it.

#### GraphViewCommand
It defines a GraphView query or command. It would have a VertexObjectCache for each instance.

#### VertexObjectCache.cs
When GraphView loads data from the database, it stores data in the VertexObjectCache via hash. If we update some VertexFields, we need update the related cache immediately. And if some data we need has been loaded before, we can get them in memory instead of the database to reduce the time.

#### GraphViewException
Just as its name.

### Test Subproject: GraphViewUnitTest
There are some unit tests.

#### Gremlin/GraphDataLoader.cs 
There are two different graph data we can use directly, GraphData.MODERN and GraphDataCLASSIC. We can choose either through the `LoadGraphData` function.

#### Gremlin/CustomText.cs
This is a test file provided for any developer to run some queries and debug. Pay attention, this file doesn't need to be committed.

#### Gremlin/AbstractGremlinTest.cs
It defines an abstract class `AbstractGremlinTest`, which is used to identify classes that contain test methods and not abled to be inherited.

## More details in translation

### The SQL-like language format

In fact, the translation part is the most amazing part in GraphView. A gremlin-groove query is a sequencial steps ([Imperative programmming][11]). However, it is hard to optimize. We want to translate the query to a descriptive format ([Declarative programming][12]), here that it is the SQL-like format. A SQL query consists of `SELECT`, `FROM`, `WHERE` clauses, but the SQL-like format adds the `MATCH` clause, which is used to find the neighbours of vectrices or the edges of vectrices.

```  SQL
MATCH N_1-[Edge E_1]->N_2
```

This clause can find the outgoing edge E_1 of vertices N_1, and the neighbours N_2 through E_1.

```  SQL
MATCH N_1<-[Edge E_1]-N_2
```

This clause can find the incoming edge E_1 of vertices N_1, and the neighbours N_2 through E_1.

### How to describe the behaviours of Gremlin steps? 

`g.V("1")` can get one vertex whose id is 1, `g.V("1").out()` can find the neighbours of `g.V("1")`...  
We can use a [finite-state machine][13] to simulate this process.  
Consider the Gremlin query `g.V("1").out().outE()`, the initial state is the whole graph, the vertex set N_1 = {v| v is a vertex whose label is "1"} is the next state, which is determined by `g.V("1")`. The next state is the vertex set N_2 = {v | There exists an edge from a vertex in N_1 to v}. The last state is the edge set E_1 = {The outgoing edges of any vertex in N_2}.

Pay attention, not all steps can be represented by states. For example, the FSM of `g.V().hasLabel("person")` has only one state, the vertex set N_1 = {v| v is a vertex who have the "person" label}.

### How to build the [FSM][15]?

#### Build a GremlinTranslationOpList

We do not build a [FSM][15] directly from the a Gremlin query due to some historical reasons. We build a GremlinTranslationOpList firstly. Generally, every step in gremlin can correspond to a  GremlinTranslationOperator, such as `V()` and `GremlinVOp`, `has("...")` and `GremlinHasOp`. Every GremlinTranslationOperator can maintain the information of the corresponding step so that the GremlinTranslationOpList can maintain the information of the gremlin query. This GremlinTranslationOpList belongs to a traversal object. The GremlinTranslationOpList of `g.V("1").out().outE()` is [`GrenlinVOp`, `GremlinOutOp`, `GremlinOutEOp`]

####  Build a FSM

Our model is a lazy model, that is to say we generate something only when we need it. If we want to get the last state of the FSM, we need the previous state, then previous... until the first state.

After we get a traversal object, we call the method `Next()`. This methodis very complex, but only two main processes are related to the translation, `GetContext()` and `ToSqlScript()`. `GetContext()` is the method to build the FSM.

Due to the lasy property, we just need to call `GetContext()` on the last `GremlinTranslationOperator` of the GremlinTranslationOpList. It calls `GetContext()` on the previous `GremlinTranslationOperator`... The first `GremlinTranslationOperator` is an object of `GrenlinVOp`. We will create an instance of `GremlinFreeVertexVariable`. Because we want the vertex whose id is "1" rather all vertices, we need to add a predicate in order to ensure {v| v is a vertex whose id is "1"}. We need to add the instance to a `VariableList`, which maintains all `GremlinTranslationOperator`, add the it to a `TableReferencesInFromClause`, which is used to generate the `FROM` clause, and set it as the `PivotVariable`, which means the current state in FSM, and add it to `StepList`, which maintains all states.  

The first state is generated, then how to transfor it to the next state?  

We maintain all information about FSM in an instance of `GremlinToSqlContext`. Because C# pass objects by reference, we can add new information on the previous object. Finally, we can use an object of `GremlinToSqlContext` to represent the FSM. Therefore, we can return an object of `GremlinToSqlContext` to the next state.  

The next `GremlinTranslationOperator` is an object of `GremlinOutOp`. We need to get an object of `GremlinFreeEdgeVariable` and then an object of `GremlinFreeVertexVariable`. Then add the two to `VariableList` and `TableReferencesInFromClause`, but only set the latter as the `PivotVariable` and add it to `StepList`. Because the second state of the FSM is {v | There exists an edge from a vertex in N_1 to v}. If you still remember, `MATCH` can be used to find edges. Therefore, we add the object of `GremlinFreeVertexVariable` to `MatchPathList`.

`GremlinOutEOp` is the next and the last one. Similar to the `GremlinTranslationOperator`, add an object of `GremlinVertexToForwardEdgeVariable` to `VariableList` and `MatchPathList` and set the object as the `PivotVariable` and add it to `StepList`.  

So far, the construction of FSM is finished.

### How to get the SQL-like query from a FSM?

During constructing the FSM, we created `VariableList`, `TableReferencesInFromClause`, `Predicates`, `StepList` and `PivotVariable`. We will use them to get the SQL-like query.

#### SELECT

Because our ultimate goal is to get the final state so that the 'SELECT' of SQL-like query is determined by the `PivotVariable`.  
As we know, in some cases, not all the columns are needed. If `PivotVariable` records the columns (or projectedProperties) we need, we will explicitly stated. But if we do not records any columns, we will state the `DefaultProjection`. For example, the `PivotVariable` of `g.V()` is an object of `GremlinFreeVertexVariable` without any column given so that the `SELECT` will get the `DefaultProjection``*` of `GremlinFreeVertexVariable`, like `SELECT ALL N_1.* AS value$2c25dcb6`. The`DefaultProjection` is `*` if and only if the type of `GremlinVariableType` is Edge or Vertex. Now the `PivotVariable` of `g.V("1").out().outE()` is an object of  `GremlinFreeEdgeVariable` without any column given so that the `SELECT` clause is "SELECT ALL E_2.* AS value$821a7846"

#### FROM

This part is the most complicated. Keep in mind that GraphView is a lasy and pull system. Assume that `TableReferencesInFromClause` is [T_1, T_2, ..., T_i, ..., T_n]. T_i may depends on T_1, T_2, ..., T_i-1.

Therefore, if we traverse `TableReferencesInFromClause` from 1 to n, we do not know the properties the latter obnject of `GremlinTableVariable` needs. The wiser way to traverse in the reverse order. If one object of `GremlinTableVariable` needs some properties, it will "tell" previous objects. `g.V("1").out().outE()` is so simple that it does not use this information. 

Now, `TableReferencesInFromClause` is [`GremlinFreeVertexVariable`, `GremlinFreeVertexVariable`]. The first one is created due to `g.V("1")`, the second one is created due to `.out()`. We firstly call `ToTableReference` on the second `GremlinFreeVertexVariable`. The `ToTableReference` method is designed to translate `GremlinTableVariable` to `WTableReference`. `GremlinFreeVertexVariable` is so simple that the instance is just like "node AS [N_2]". So similarly， the first instance is "node AS [N_1]"

Finally, we put them into a list, ["node AS [N_1]", "node AS [N_2]"]. And the `FROM` clause is "FROM node AS [N_1], node AS [N_2]"

#### MATCH

Maybe you never hear the `MATCH` clause before, but it is easy to understand. The result of MatchClause is ["N_1-[Edge AS E_1]->N_2", "N_2-[Edge AS E_2]"]. Every element is an object of `WMatchPath` with three parts, `SourceVariable`, `EdgeVariable` and `SinkVariable`. Take "N_1-[Edge AS E_1]->N_2" for example, "N_1" is the `SourceVariable`, "E_1" is the `EdgeVariable` and "N_2" is the `SinkVariable`. Repeat it again, the `MATCH` clause is "MATCH N_1-[Edge AS E_1]->N_2
  N_2-[Edge AS E_2]"

#### WHERE

`Predicates` is generated during building the FSM. But how to compose many predicates? Every predicate is an instance of `WBooleanExpression`, which has two instances of `WBooleanExpression`. You can image it as one node in a binary tree. If one predicate is added, just need to create an object of `WBooleanExpression` to store the new predicate, create an instance of `WBooleanExpression` to merge the old object and the new object with `AND`, `OR`. We can use the result to represent all predicates. Finally, the `WHERE` clause is "WHERE N_1.id = '1'"

#### SQL-like query

``` SQL
SELECT ALL E_2.* AS value$821a7846
FROM node AS [N_1], node AS [N_2]
MATCH N_1-[Edge AS E_1]->N_2
  N_2-[Edge AS E_2]
WHERE N_1.id = '1'
```

Then the translation part is finished. However, it is the simplest example. We will show you more.

## Something about the translation part implementation of [path-step][14] in Gremlin

### Semantic
> A traverser is transformed as it moves through a series of steps within a traversal. The history of the traverser is realized by examining its path with path()-step (map).

### Parameters
Let's see three translation results as below:

``` SQL
-- The SQL-like script translated from 'g.V().path()'
--                                  or 'g.V().path().by()'
--                                  or 'g.V().path().by(__.identity())'

SELECT ALL R_0.value$172110a0 AS value$172110a0
FROM node AS [N_18], 
    CROSS APPLY 
    Path(
      Compose1('value$172110a0', N_18.*, 'value$172110a0', N_18.*, '*'),
      (
        SELECT ALL Compose1('value$172110a0', R_1.value$172110a0, 'value$172110a0') AS value$172110a0
        FROM CROSS APPLY  Decompose1(C.value$172110a0, 'value$172110a0') AS [R_1]
      )
    ) AS [R_0]
```

``` SQL
-- The SQL-like script translated from 'g.V().out().path()' or ...

SELECT ALL R_0.value$77083d6d AS value$77083d6d
FROM node AS [N_18], node AS [N_19], 
    CROSS APPLY 
    Path(
      Compose1('value$77083d6d', N_18.*, 'value$77083d6d', N_18.*, '*'),
      Compose1('value$77083d6d', N_19.*, 'value$77083d6d', N_19.*, '*'),
      (
        SELECT ALL Compose1('value$77083d6d', R_1.value$77083d6d, 'value$77083d6d') AS value$77083d6d
        FROM CROSS APPLY  Decompose1(C.value$77083d6d, 'value$77083d6d') AS [R_1]
      )
    ) AS [R_0]
MATCH N_18-[Edge AS E_6]->N_19
```

``` SQL
-- The SQL-like script translated from 'g.V().out().path().by("name")'
--                                  or 'g.V().out().path().by(__.values("name"))'

SELECT ALL R_0.value$51636abd AS value$51636abd
FROM node AS [N_18], node AS [N_19], 
    CROSS APPLY 
    Path(
      Compose1('value$51636abd', N_18.*, 'value$51636abd', N_18.name, 'name', N_18.*, '*'),
      Compose1('value$51636abd', N_19.*, 'value$51636abd', N_19.name, 'name', N_19.*, '*'),
      (
        SELECT ALL Compose1('value$51636abd', R_2.value$51636abd, 'value$51636abd') AS value$51636abd
        FROM CROSS APPLY  Decompose1(C.value$51636abd, 'name') AS [R_1], CROSS APPLY  Values(R_1.name) AS [R_2]
      )
    ) AS [R_0]
MATCH N_18-[Edge AS E_6]->N_19
```

Here, `V()` creates a FreeVariable N_18 and `out()` creates a FreeVariable N_19. So in this traversal, each traverser will take along a path (history) including two elements, N_18 and N_19. And we can see that the TVF `Path` in script has n + 1 parameters, the information of n steps and the `By` projection sub query.

For each Compose1 in parameter list, the `By` projection sub query will decompose it and do some projections, then compose it as last, which is one element of the path-step result.

By the way, the `C` in Decompose1 argument is a bound variable of the input, pointing to each Compose1 in Path argument list.

### Parameters including `As` labels

``` SQL
-- g.V().as("a").out().path()

SELECT ALL R_0.value$1a57e96e AS value$1a57e96e
FROM node AS [N_18], node AS [N_19], 
    CROSS APPLY 
    Path(
      Compose1('value$1a57e96e', N_18.*, 'value$1a57e96e', N_18.*, '*'),
      'a',
      Compose1('value$1a57e96e', N_19.*, 'value$1a57e96e', N_19.*, '*'),
      (
        SELECT ALL Compose1('value$1a57e96e', R_1.value$1a57e96e, 'value$1a57e96e') AS value$1a57e96e
        FROM CROSS APPLY  Decompose1(C.value$1a57e96e, 'value$1a57e96e') AS [R_1]
      )
    ) AS [R_0]
MATCH N_18-[Edge AS E_6]->N_19
```

``` SQL
-- g.V().as("a").out().as("b").path()

SELECT ALL R_0.value$9416a28c AS value$9416a28c
FROM node AS [N_18], node AS [N_19], 
    CROSS APPLY 
    Path(
      Compose1('value$9416a28c', N_18.*, 'value$9416a28c', N_18.*, '*'),
      'a',
      Compose1('value$9416a28c', N_19.*, 'value$9416a28c', N_19.*, '*'),
      'b',
      (
        SELECT ALL Compose1('value$9416a28c', R_1.value$9416a28c, 'value$9416a28c') AS value$9416a28c
        FROM CROSS APPLY  Decompose1(C.value$9416a28c, 'value$9416a28c') AS [R_1]
      )
    ) AS [R_0]
MATCH N_18-[Edge AS E_6]->N_19
```

As you can see, the label will be a parameter after its related Variable in Path parameter list.

### Path performance with steps which have sub traversals
``` SQL
-- g.V().union(__.inE().outV(), __.outE().inV()).both().path()

SELECT ALL R_2.value$424a9cdf AS value$424a9cdf
FROM node AS [N_18], 
    CROSS APPLY 
    Union(
      (
        SELECT ALL R_0.value$424a9cdf AS _path, N_19.* AS *
        FROM CROSS APPLY  VertexToBackwardEdge(N_18.*, 'value$424a9cdf', '*') AS [E_6], CROSS APPLY  EdgeToSourceVertex(E_6.*, 'value$424a9cdf', '*') AS [N_19], 
            CROSS APPLY 
            Path(
              Compose1('value$424a9cdf', E_6.*, 'value$424a9cdf', E_6.*, '*'),
              Compose1('value$424a9cdf', N_19.*, 'value$424a9cdf', N_19.*, '*')
            ) AS [R_0]
      ),
      (
        SELECT ALL R_1.value$424a9cdf AS _path, N_20.* AS *
        FROM CROSS APPLY  VertexToForwardEdge(N_18.*, 'value$424a9cdf', '*') AS [E_7], CROSS APPLY  EdgeToSinkVertex(E_7.*, 'value$424a9cdf', '*') AS [N_20], 
            CROSS APPLY 
            Path(
              Compose1('value$424a9cdf', E_7.*, 'value$424a9cdf', E_7.*, '*'),
              Compose1('value$424a9cdf', N_20.*, 'value$424a9cdf', N_20.*, '*')
            ) AS [R_1]
      )
    ) AS [N_21], CROSS APPLY  VertexToBothEdge(N_21.*, '*') AS [E_8], CROSS APPLY  EdgeToOtherVertex(E_8.*, 'value$424a9cdf', '*') AS [N_22], 
    CROSS APPLY 
    Path(
      Compose1('value$424a9cdf', N_18.*, 'value$424a9cdf', N_18.*, '*'), 
      N_21._path,
      Compose1('value$424a9cdf', N_22.*, 'value$424a9cdf', N_22.*, '*'),
      (
        SELECT ALL Compose1('value$424a9cdf', R_3.value$424a9cdf, 'value$424a9cdf') AS value$424a9cdf
        FROM CROSS APPLY  Decompose1(C.value$424a9cdf, 'value$424a9cdf') AS [R_3]
      )
    ) AS [R_2]
```

In this case, `g.V().union(__.inE().outV(), __.outE().inV()).both().path()`, if one traverser which has finished this traversal has walked through the path in order `V` -> `inE` -> `outV` -> `both`, the GraphView got `V` -> `union` -> `both` at first and `N_21._path` pointed to the path in sub traversal of `union` (called local path). Then `N_21._path` would be **flattened** and inserted into the global path. So after replacing the `N_21._path` with `inE` -> `outV` (for this traverser), we get the final path result `V` -> `inE` -> `outV` -> `both`.


## Something about the implementation of [select-step](http://tinkerpop.apache.org/docs/current/reference/#select-step) in Gremlin

### Usage

> There are three ways to use select()-step.  
1. Select labeled steps within a path (as defined by as() in a traversal).  
2. Select objects out of a Map&lt;String,Object&gt; flow (i.e. a sub-map).  
3. When the set of keys or values (i.e. columns) of a path or map are needed, use select(keys) and select(values), respectively. 

### Use select with one label

``` SQL
-- The SQL-like script translated from g.V().as("a").out().select("a").values("name", "age")
--                                  or g.V().as("a").out().select("a").by().values("name", "age")
--                                  or g.V().as("a").out().select("a").by(__.identity()).values("name", "age")

SELECT ALL R_3.value$018cf619 AS value$018cf619
FROM node AS [N_18], node AS [N_19], 
    CROSS APPLY 
    Path(
      Compose1('value$018cf619', N_18.*, 'value$018cf619', N_18.name, 'name', N_18.age, 'age', N_18.*, '*'),
      'a',
      Compose1('value$018cf619', N_19.*, 'value$018cf619', N_19.name, 'name', N_19.age, 'age', N_19.*, '*')
    ) AS [R_0], 
    CROSS APPLY 
    SelectOne(
      N_19.*,
      R_0.value$018cf619,
      'All',
      'a',
      (
        SELECT ALL Compose1('value$018cf619', R_1.value$018cf619, 'value$018cf619', R_1.name, 'name', R_1.age, 'age') AS value$018cf619
        FROM CROSS APPLY  Decompose1(C.value$018cf619, 'name', 'age', 'value$018cf619') AS [R_1]
      ),
      'name',
      'age'
    ) AS [R_2], CROSS APPLY  Values(R_2.name, R_2.age) AS [R_3]
MATCH N_18-[Edge AS E_6]->N_19
```

When using select-step with only one label, we will get the SQL-like script which includes a TVF `SelectOne`.

First three arguments in `SelectOne`:

- `N_19.*`:  The previous step of select-step, that is, the `out` in `g.V().as("a").out().select("a")`. If this step returns a dict(map), the `SelectOne` will select the value in this dict with key "a" (in this case, the `out` variable would not return a dict obviously). For instance, `g.V().valueMap().select("name")`, in which the `valueMap` yields a dict(map) representation of the properties of an element. And then `select` will select the value with key "name".

- `R_0.value$018cf619`: The path of `g.V().as("a").out()` in this case, which contains all the steps the traverser goes through (some steps are labeled, such as `V` with label "a"). So `select` will select the step `V`.

- `'All'`: The option of the "pop" operation. It could be `'All'`, `'First'` and `'Last'`.

	> There is also an option to supply a Pop operation to select() to manipulate List objects in the Traverser:
	>
	> gremlin> g.V(1).as("a").repeat(out().as("a")).times(2).select(**first**, "a")
	> ==>v[1]
	> ==>v[1]
	> gremlin> g.V(1).as("a").repeat(out().as("a")).times(2).select(**last**, "a")
	> ==>v[5]
	> ==>v[3]
	> gremlin> g.V(1).as("a").repeat(out().as("a")).times(2).select(**all**, "a")
	> ==>[v[1],v[4],v[5]]
	> ==>[v[1],v[4],v[3]]

The argument as label in `SelectOne`:

- `'a'`: `g.V().as("a").out().select("a")` so ... but pay attention:

	> If the selection is one step, no map is returned. 
	> When there is only one label selected, then a single object is returned. 

The argument that a sub SQL-like select query in `SelectOne`:

- `(SELECT ... FROM ... WHERE ... )`: The sub traversal in `by` after `select`. It will do some projections for results of  select-step.

The last two arguments in `SelectOne`:

- `'name'` and `'age'`: The populated columns' name of the `SelectOne` result.

### Use select with multiple labels

``` SQL
-- g.V().as("a").out().as("b").in().select("a", "b").by("name").by(__.valueMap())

SELECT ALL R_5.value$9bf2a502 AS value$9bf2a502
FROM node AS [N_18], node AS [N_19], node AS [N_20], 
    CROSS APPLY 
    Path(
      Compose1('value$9bf2a502', N_18.*, 'value$9bf2a502', N_18.name, 'name', N_18.*, '*'),
      'a',
      Compose1('value$9bf2a502', N_19.*, 'value$9bf2a502', N_19.name, 'name', N_19.*, '*'),
      'b',
      Compose1('value$9bf2a502', N_20.*, 'value$9bf2a502', N_20.name, 'name', N_20.*, '*')
    ) AS [R_0], 
    CROSS APPLY 
    Select(
      N_20.*,
      R_0.value$9bf2a502,
      'All',
      'a',
      'b',
      (
        SELECT ALL Compose1('value$9bf2a502', R_2.value$9bf2a502, 'value$9bf2a502') AS value$9bf2a502
        FROM CROSS APPLY  Decompose1(C.value$9bf2a502, 'name') AS [R_1], CROSS APPLY  Values(R_1.name) AS [R_2]
      ),
      (
        SELECT ALL Compose1('value$9bf2a502', R_4.value$9bf2a502, 'value$9bf2a502') AS value$9bf2a502
        FROM CROSS APPLY  Decompose1(C.value$9bf2a502, 'value$9bf2a502') AS [R_3], CROSS APPLY  ValueMap(R_3.value$9bf2a502, -1) AS [R_4]
      )
    ) AS [R_5]
MATCH N_18-[Edge AS E_6]->N_19
  N_19<-[Edge AS E_7]-N_20
```

The situation of `Select` argument list is almost same as the `SelectOne`. Note that the first sub query translated from `by("name")` will apply to the selected value with label "a", and the second one will apply to the selected value with "b", and so on.

### Some detials of our Implementation
We will insert a GremlinGlobalPathVariable before each select-step because the select-step depends on path.


## Something about the implementation of [repeat-step](http://tinkerpop.apache.org/docs/current/reference/#repeat-step) in Gremlin

### Repeat with emit/until/times

We assume that you have know these usage cases such as 
- `times(1).repeat(...)`
- `repeat(...).until(...)`
- `until(...).repeat(...)`
- `until(...).repeat(...).emit(...)`
- `emit(...).until(...).repeat(...)`
- etc

### The parameters of Repeat

``` SQL
-- g.V().repeat(out()).times(2).path().by('name')

SELECT ALL R_1.value$be711b03 AS value$be711b03
FROM node AS [N_18], 
    CROSS APPLY 
    Repeat(
      (
        SELECT ALL N_18.name AS key_0, N_18.* AS key_1, null AS _path
        UNION ALL
        SELECT ALL N_19.name AS key_0, N_19.* AS key_1, R_0.value$be711b03 AS _path
        FROM CROSS APPLY  VertexToForwardEdge(R.key_1, '*') AS [E_6], CROSS APPLY  EdgeToSinkVertex(E_6.*, 'name', '*') AS [N_19], 
            CROSS APPLY 
            Path(
              R._path,
              Compose1('value$be711b03', N_19.*, 'value$be711b03', N_19.name, 'name', N_19.*, '*')
            ) AS [R_0]
      ),
      RepeatCondition(2)
    ) AS [N_20], 
    CROSS APPLY 
    Path(
      Compose1('value$be711b03', N_18.*, 'value$be711b03', N_18.name, 'name', N_18.*, '*'),
      N_20._path,
      (
        SELECT ALL Compose1('value$be711b03', R_3.value$be711b03, 'value$be711b03') AS value$be711b03
        FROM CROSS APPLY  Decompose1(C.value$be711b03, 'name') AS [R_2], CROSS APPLY  Values(R_2.name) AS [R_3]
      )
    ) AS [R_1]
```

#### RepeatCondition
A number of repeat rounds from argument of `times`, or an EXISTS clause translated from sub traversal in `until`.

#### Sub Traversal of Repeat
``` SQL
...
	(
	   SELECT ALL N_18.name AS key_0, N_18.* AS key_1, null AS _path
	   
	   UNION ALL
	   
	   SELECT ALL N_19.name AS key_0, N_19.* AS key_1, R_0.value$be711b03 AS _path
	   
	   FROM CROSS APPLY  VertexToForwardEdge(R.key_1, '*') AS [E_6], CROSS APPLY  EdgeToSinkVertex(E_6.*, 'name', '*') AS [N_19], 
	       CROSS APPLY 
	       Path(
	         R._path,
	         Compose1('value$be711b03', N_19.*, 'value$be711b03', N_19.name, 'name', N_19.*, '*')
	       ) AS [R_0]
	 )
 ...
```

The first SELECT clause would be run only one time to select the specific columns in RawRecord for align when the traverser comes in this repeat step at the first time. And `null As _path` is only the dummy padding.

The second time and later, the repeat step receives the result of previous repeat as the input, applies the second SELECT clause in the final stage of process. And in fact, `R` refers to it. So `R._path` means the local path in previous repeat.


## Something about the implementation of [match-step][1] in Gremlin
### Semantic
The match-step in Gremlin is a map step, which maps the traverser to some object for the next step to process. That is, one traverser in, some object out. And match-step does the same thing as it.

> With match(), the user provides a collection of "traversal fragments," called patterns, that have variables defined that must hold **true** throughout the duration of the match(). 

In official implementation of Gremlin, we found that when a traverser is in match(), a registered *MatchAlgorithm* (i.e. CountMatchAlgorithm or GreedyMatchAlgorithm) analyzes the current state of the traverser, <u>the runtime statistics of the traversal patterns, and returns a traversal-pattern that the traverser should try next</u>.

  "Who created a project named 'lop' that was also created by someone who is 29 years old? Return the two creators."

In Gremlin, we can do as below:

``` Groovy
// the gremlin query
g.V().match(
     __.as('a').out('created').as('b'),  // 1
     __.as('b').has('name', 'lop'),      // 2
     __.as('b').in('created').as('c'),   // 3
     __.as('c').has('age', 29)).         // 4
   select('a','c').by('name')
```

That means for each traverser processed in this match step will try to traverse all of the match-traversals, and if it completes four traversals (order-independent), it is matched.

### Our Implementation

#### Because
We consider the modern graph in gremlin:

![modern graph](http://tinkerpop.apache.org/docs/current/images/match-step.png)

Now a traverser which current traversed vertex 1 comes in this match-step. If it is matched, a groovy-dict will be return, then select-step will takes the values' names by keys 'a' and 'c'.

We know that it may fails to finish all traversals in some orders. For example, we process this traverser in order 4-3-2-1. We consider vertex 1 (which this traverser current traversed) is 'c' because there is no label 'c' in this traverser's path, and it did have a property 'age' valued 29, so it success to pass through the match-traversal 4. And then in next traversal(match-traversal 3), we cannot find what is the 'b' pointing to, so it fails, or is not matched.

So in order to make it success to complete all traversals (if it can), we must choose a right order for it. 

And some rules we should follow:
* The start/end label in `__.as('a').out('created').as('b')` is 'a'/'b'; while `__.as('c').has('age', 29)` has no end label.
* The first traversal it comes to, we will tag the traverser by the start label if the start label is not in its path.
* For each end label (if it exists), we will tag it on its path when it is processed by this traversal.
* For each start label in traversal, if we can not recognize it (the label not in the traverser's path), it fails. 
* The result of match-step is a map(dict) including all the keys on all match-traversals (which are on the traverser's path without doubt).

--- 

First of all, we should find the proper order. 

``` plain
 a -> b       // 1
 b -> null    // 2
 b -> c       // 3
 c -> null    // 4
```

We can get a graph:

``` plain
 a -> b -> c
```

Well it is obvious that 1-2-3-4 is a right order, 1-3-4-2 as well. Ok, you may consider it just is a [Partially ordered set][2], and I did a [Topological sorting][3]. But if it is not a [DAG(directed acyclic graph)][4], how should I do?

``` plain
a -> b
b -> c
c -> a
a -> d
e -> f
```

We could ignore the `x -> null` (the match-traversal that just has start label) because we can put it behind of `x -> y` (the match-traversal that has start label and end label), which is a valid "edge".

so we will get the graph
``` plain
  +---------------+
  |               |
  v               +
  a +---> b +---> c
  +
  |
  v
  d
```

If we start at 'a', we can go to 'b' and 'd' at the same time. Because the traverser can "split" another one to do the next traversal. So in this graph, starting at 'a', 'b', 'c' are the right ways, could complete all traversals. 

For this, we say `a > b`, `b > c`, `c > a`, `a > d`, it is not satisfied to the partially order.

And of cause, [Topological sorting][3] can not be applied.

---

If there is a graph as below: 

``` plain
  +---------------+
  |               |
  v               +
  a +---> b +---> c

  e +-----------> f
```

Obviously, if it is not a connectivity, let A = {a, b, c}, B = {e, f}, where anyone on the front does not matter.

#### Therefore
For this graph:
``` plain
g +------> e +--------> f
+          ^
|          |
|          |
|          |
v          +
h +------> a +--------> b <--+
           ^            +    |
           |            |    |
           |            |    |
           +            |    |
           d <----------+    |
           +                 |
           |                 |
           |                 |
           +----------> c +--+

sorted result: []
```
Our solution is:

- **Step 1**: For every vertex, do the [Breadth-first search (BFS)][5] to find a longest path starting from it:
``` plain
5 +------> 1 +--------> 0
+          ^
|          |
|          |
|          |
v          +
4 +------> 3 +--------> 4 <--+
           ^            +    |
           |            |    |
           |            |    |
           +            |    |
           3 <----------+    |
           +                 |
           |                 |
           |                 |
           +----------> 5 +--+

sorted result: []
```
- **Step 2**: Compare their longest paths, find the "global longest path" (if not only one, choose one arbitrarily):
``` plain
g          e +--------> f
           ^
           |
           |
           |
           +
h          a            b <--+
           ^            +    |
           |            |    |
           |            |    |
           +            |    |
           d <----------+    |
                             |
                             |
                             |
                        c +--+

sorted result: []
```
- **Step 3**: Move it out from the graph, remove all the related edges:
``` plain
g
+
|
|
|
v
h  








sorted result: [ c, b, d, a, e, f ]
```
- **Step 4**: Put this path in the result list, then loop the step 1, 2, 3 for the rest graph. Because of `g -> e`, which means {g, h} > {c, b, d, a, e, f}, so we should put {g, h} on the front of the part {c, b, d, a, e, f}
``` plain


















sorted result: [ g, h, c, b, d, a, e, f ]
```

In Our GraphView Code, we [**polyfill**][6] the `Match` by `FlatMap`, `Choose`, `Where`, `Select` and so on, which means it is none of the compilation or execution parts' business, but only translation part.

``` Groovy
g.V().match(
  __.as('a').out('created').as('b'),
  __.as('b').has('name', 'lop'),
  __.as('b').in('created').as('c'),
  __.as('c').has('age', 29),
  )
  
// is equal to

g.V().flatMap(__.choose(__.select('a'), __.identity(), __.as('a')).
  select(last, 'a').flatMap(__.out('created')).choose(__.select('b'), __.where(eq('b'))).as('b').
  select(last, 'b').flatMap(__.has('name', 'lop')).
  select(last, 'b').flatMap(__.in('created')).choose(__.select('c'), __.where(eq('c'))).as('c').
  select(last, 'c').flatMap(__.has('age', 29)).
  select('a', 'b', 'c'))
```

We will **string the match-traversals into one flatmap-traversal**. (and will add some steps in it)

[1]: http://tinkerpop.apache.org/docs/current/reference/#match-step
[2]: https://en.wikipedia.org/wiki/Partially_ordered_set
[3]: https://en.wikipedia.org/wiki/Topological_sorting
[4]: https://en.wikipedia.org/wiki/Directed_acyclic_graph
[5]: https://en.wikipedia.org/wiki/Breadth-first_search
[6]: https://en.wikipedia.org/wiki/Polyfill
[7]: http://tinkerpop.apache.org/docs/current/reference/
[8]: https://github.com/Microsoft/GraphView
[9]: https://github.com/Microsoft/GraphView/tree/clean-up
[10]: http://tinkerpop.apache.org/docs/current/reference/#graph-traversal-steps
[11]: https://en.wikipedia.org/wiki/Imperative_programming
[12]: https://en.wikipedia.org/wiki/Declarative_programming
[13]: https://en.wikipedia.org/wiki/Finite-state_machine
[14]: http://tinkerpop.apache.org/docs/current/reference/#path-step
[15]: https://en.wikipedia.org/wiki/Finite-state_machine
