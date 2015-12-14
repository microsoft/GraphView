GraphView
=========
GraphView is a DLL library that enables users to use SQL Server or Azure SQL Database to manage graphs. It connects to a SQL database locally or in the cloud, stores graph data in tables and queries graphs through an SQL-extended language. It is not an independent database, but a middleware that accepts graph operations and translates them to T-SQL executed in SQL Server or Azure SQL Database. As such, GraphView can be viewed as a special connector to SQL Server/Azure SQL Database. Developers will experience no differences than the default SQL connector provided by the .NET framework (i.e., SqlConnection), only except that this new connector accepts graph-oriented statements.


Design Philosophy
-----------------

Graph data is becoming ubiquitous. Today’s solutions for managing graphs have been mostly centered on the concept of NoSQL. A common argument against SQL databases in graph processing is that graph traversals are implemented as a sequence of table joins and join operations are expensive. This statement, however, is inaccurate. First, not all join operations are expensive; depending on input sizes and available data structures such as indexes and views, a join operation can be efficient. More importantly, a graph traversal in a native graph database is not join-free. To understand this, consider a native graph database in which every node is physically represented as a record that contains the node’s properties, as well as one or more adjacency lists of its neighbors. In this graph database, a traversal from a node to its neighbors involves three steps: (1) retrieve the node’s physical record from the data store, (2) iterate through its adjacency list and (3) for each neighbor, retrieve its physical record using its ID (or reference). The last retrieval step is essentially a join; it is logically a nested-loop join that uses a record’s key—a logical address or reference—to locate a record. The nested-loop join in SQL databases is not always expensive, when there is an index on nodes’ IDs. It can even be fairly efficient when data resides in main memory and indexes accesses involve no random disk reads.

We identify that the second step—iterating through neighbors—in a graph traversal is the key gap between native graph stores and SQL databases. In conventional SQL databases, data is usually normalized to avoid redundancy and update anomalies. Entities in a graph are highly connected and mainly present many-to-many relationships. By data normalization, a many-to-many relationship between two entities yields a junction table, with two columns referencing the two entities respectively. Such an organization means that an entity’s properties are separated from graph topology. To traverse from an entity to its neighbors, the query engine needs an additional join to look up the junction table to obtain the topology information of the entity, yielding poorer cache locality than native graph databases.

The goal of GraphView is to bridge the gap between native graph stores and SQL databases, making SQL Server (and Azure SQL Database) behave in the same way as a native graph store. By using SQL functions appropriately, the physical data representation and runtime behavior of GraphView closely resemble those of native graph databases. In fact, thanks to years of research and development of SQL Server, GraphView inherits many more sophisticated optimizations that have been neglected by native graph databases, such as column store indexes and vectorized query execution, providing unparalleled performance advantages.

Features
---------

GraphView is a DLL library through which you manage graph data in SQL Server (version 2008 and onward) and Azure SQL Database (v12 and onward). It provides features a standard graph database is expected to have. In addition, since GraphView relies on SQL databases, it inherits many features in the relational world that are often missing in native graph databases.

GraphView offers the following major features:

- **Graph database** A graph database in GraphView is a conventional SQL database. The graph database consists of one or more types of nodes and edges, each of which may have one or more properties.

- **Data manipulations** GraphView provides an SQL-extended language for graph manipulation, including inserting/deleting nodes and edges. The syntax is similar to INSERT/DELETE statements in SQL, but is extended to accommodate graph semantics.

- **Queries** GraphView’s query language allows users to match graph patterns against the graph in a graph database. The query language extends the SQL SELECT statement with a MATCH clause, in which the graph pattern is specified. Coupled with loop/iteration statements from T-SQL, the language also allows users to perform iterative computations over the graph.

- **Indexes** To accelerate query processing, GraphView also allows users to create indexes. All indexes supported by SQL Server and Azure SQL Database are available, including not only conventional B-tree indexes but also new indexing technologies such as columnstore indexes.

- **Transactions** All operations in GraphView are transaction-safe. What is more, there is no limit on a transaction’s scope; a transaction can span nodes, edges or even graphs.

- **SQL-related features** GraphView inherits many administration features from SQL Server and Azure SQL Database. Below is a short list of features that are crucial to administration tasks:
  1.  `Access control.` GraphView uses the authentication mechanism of SQL Server to control accesses to graph databases. A user can access a graph database if SQL Server says so.
  2.  `Replication.` GraphView stores graph data in a SQL Server database. A replication of the database will result in a replication of all graph data.
  3.  `Backup.` GraphView maintains SQL Server databases that are visible to SQL Server administrators. Administrators can apply backup operations to the database explicitly.

Getting Started
----------------
GraphView is a DLL library. You reference the library in your application and open a graph database by instantiating a GraphViewConnection object with the connection string of a SQL database.
```C#
using GraphView;
......
string connectionString = "Data Source= (local); Initial Catalog=GraphTesting; Integrated Security=true;";
GraphViewConnection gdb = new GraphViewConnection(connectionString);
try {
  // Connects to a database. 
  gdb.Open(true);
}
catch(DatabaseException e) {
  // Exception handling goes here
}
```
When the connection string points to an Azure SQL Database instance, you open a graph database in Azure:
```C#
using GraphView;
......
var sqlConnectionBuilder = new SqlConnectionStringBuilder();
sqlConnectionBuilder["Server"]="tcp:graphview.database.windows.net,1433";
sqlConnectionBuilder["User ID"]="xxx";
sqlConnectionBuilder[“Password”] = "xxx";
sqlConnectionBuilder[“Database”] = "GraphTesting";
GraphViewConnection gdb = new GraphViewConnection(connectionString);
try {
  gdb.Open(true);
}
catch(DatabaseException e) {
  // Exception handling goes here
}
```
Once you open a database, you send graph queries through the connector and retrieve results using the .NET standard data reader DataReader if needed.
```C#
try {
  gdb.Open(true);
  string q1 = "......";
  gdb.ExecuteNonReader(q1);
  string q2 = "......";
  DataReader dataReader = gdb.ExecuteReader(q2);
  While (dataReader.Read()) {
    // Retrieve results through DataReader
  }
  dataReader.Close();
  gdb.Close();
}
```
Please read the user manual for the full language specification, functionality and programming API's. 

Get Help
-----------

[`User manual`][manual] GraphView's user manual is the first place to get help. It introduces the full query language, functionality and programming API's. It also includes many code samples. 

`GitHub`  The GitHub repository contains a short introduction and a FAQ section. You can use Github's issue tracker to report bugs, suggest features and ask questions.

[`Email`][Email]If you prefer to talk to us in private, write to graphview@microsoft.com


License
--------------
GraphView is under the [MIT license][MIT].
© 2015 Microsoft Corporation

[manual]:manual_link
[Email]:mailto:graphview@microsoft.com
[MIT]:http://opensource.org/licenses/MIT

