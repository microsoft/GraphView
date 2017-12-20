using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using GraphView.GraphViewDBPortal;

namespace GraphView
{
    internal abstract class CompileNode
    {
        public string NodeAlias { get; set; }
        public int Position { get; set; }

        // TODO: get cardinality from database and experience
        public virtual double GetCardinality()
        {
            return 1.0;
        }

        // TODO: estimate Cost according experience
        public virtual double ComputationalCost()
        {
            return 1.0;
        }

        public virtual ExecutionOrder GetLocalExecutionOrder(ExecutionOrder parentExecutionOrder)
        {
            ExecutionOrder executionOrder = new ExecutionOrder();
            executionOrder.Order.Add(new Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>>(
                null, null, null, null, new List<ExecutionOrder>()));
            return executionOrder;
        }

    }

    internal abstract class CompileLink
    {
        public string LinkAlias { get; set; }

        // TODO: get selectivity from database
        public virtual double GetSelectivity()
        {
            return 1.0;
        }

        // TODO: estimate Cost according experience
        public virtual double GetComputationalCost()
        {
            return 1.0;
        }
    }

    internal class MatchNode : CompileNode
    {
        public WSchemaObjectName NodeTableObjectName { get; set; }
        public List<MatchEdge> Neighbors { get; set; }
        public List<MatchEdge> ReverseNeighbors { get; set; }
        public List<MatchEdge> DanglingEdges { get; set; }
        public double EstimatedRows { get; set; }
        public int TableRowCount { get; set; }
        internal JsonQuery AttachedJsonQuery { get; set; }
        public HashSet<string> Properties { get; set; }
        public bool IsDummyNode { get; set; }

        /// <summary>
        /// The density value of the GlobalNodeId Column of the corresponding node table.
        /// This value is used to estimate the join selectivity of A-->B. 
        /// </summary>
        public double GlobalNodeIdDensity { get; set; }

        /// <summary>
        /// Conjunctive predicates from the WHERE clause that 
        /// can be associated with this node variable. 
        /// </summary>
        public List<WBooleanExpression> Predicates { get; set; }

        public MatchNode() { }

        public MatchNode(MatchNode rhs)
        {
            this.NodeAlias = rhs.NodeAlias;
            this.NodeTableObjectName = rhs.NodeTableObjectName;
            this.Neighbors = rhs.Neighbors;
            this.ReverseNeighbors = rhs.ReverseNeighbors;
            this.DanglingEdges = rhs.DanglingEdges;
            this.EstimatedRows = rhs.EstimatedRows;
            this.TableRowCount = rhs.TableRowCount;
            this.AttachedJsonQuery = rhs.AttachedJsonQuery;
            this.Properties = new HashSet<string>(rhs.Properties);
            this.GlobalNodeIdDensity = rhs.GlobalNodeIdDensity;
            this.Predicates = rhs.Predicates;
            this.IsDummyNode = false;
        }

        public override bool Equals(object obj)
        {
            if (Object.ReferenceEquals(this, obj))
            {
                return true;
            }

            MatchNode rhs = obj as MatchNode;
            if (rhs == null)
            {
                return false;
            }

            return this.NodeAlias.Equals(rhs.NodeAlias);
        }

        public override double GetCardinality()
        {
            return this.EstimatedRows;
        }
    }

    internal class NonFreeTable : CompileNode
    {
        public WTableReferenceWithAlias TableReference { get; set; }
        public double Cardinality { get; set; }

        public NonFreeTable()
        {
            this.NodeAlias = "dummy";
            this.TableReference = null;
            this.Cardinality = 0.0;
        }

        public NonFreeTable(WTableReferenceWithAlias tableReference)
        {
            this.NodeAlias = tableReference.Alias.Value;
            this.TableReference = tableReference;
            this.Cardinality = 1.0;
        }

        public GraphViewExecutionOperator Compile(QueryCompilationContext context, GraphViewCommand command)
        {
            return this.TableReference.Compile(context, command);
        }

        // TODO: get cardinality from database and experience
        // Maybe every TVF needs to override this method
        public override double GetCardinality()
        {
            return this.Cardinality;
        }

        public override ExecutionOrder GetLocalExecutionOrder(ExecutionOrder parentExecutionOrder)
        {
            if (this.TableReference != null)
            {
                return this.TableReference.GetLocalExecutionOrder(parentExecutionOrder);
            }
            else
            {
                ExecutionOrder executionOrder = new ExecutionOrder();
                executionOrder.Order.Add(new Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>>(
                    null, null, null, null, new List<ExecutionOrder>()));
                return executionOrder;
            }
        }

    }

    internal class PredicateLink : CompileLink
    {
        internal WBooleanExpression BooleanExpression { get; set; }

        public PredicateLink(WBooleanExpression booleanExpression)
        {
            this.BooleanExpression = booleanExpression;
            this.LinkAlias = booleanExpression.ToString();
        }

        // TODO: get selectivity from database and experience
        public override double GetSelectivity()
        {
            return 0.5;
        }
        
        // TODO: get Cost from database and experience
        public override double GetComputationalCost()
        {
            return 0.5;
        }
    }

    internal class MatchEdge : CompileLink
    {
        public MatchNode SourceNode { get; set; }
        public MatchNode SinkNode { get; set; }
        public bool IsReversed { get; set; }
        public WEdgeType EdgeType { get; set; }
        public bool IsDanglingEdge { get; set; }
        internal JsonQuery AttachedJsonQuery { get; set; }

        /// <summary>
        /// Schema Object of the node table/node view which the edge is bound to.
        /// It is an instance in the syntax tree.
        /// </summary>
        public WSchemaObjectName BindNodeTableObjName { get; set; }
        public double AverageDegree { get; set; }
        public IList<WBooleanExpression> Predicates { get; set; }
        public List<string> Properties { get; set; }
        public int Low { get; set; }
        public int High { get; set; }
        public Statistics Statistics { get; set; }

        /// <summary>
        /// Converts edge attribute predicates into a boolean expression, which is used for
        /// constructing queries for retrieving edge statistics
        /// </summary>
        /// <returns></returns>
        public virtual WBooleanExpression RetrievePredicatesExpression()
        {
            if (Predicates != null)
            {
                WBooleanExpression res = null;
                foreach (var expression in Predicates)
                {
                    res = WBooleanBinaryExpression.Conjunction(res, expression);
                }
                return res;
            }
            return null;
        }
    }

    internal class ParentLink : CompileLink
    {
        internal ExecutionOrder ParentExecutionOrder { get; set; }

        public ParentLink(ExecutionOrder parentExecutionOrder)
        {
            this.LinkAlias = parentExecutionOrder.Order.Last().Item1.NodeAlias + "->";
            this.ParentExecutionOrder = parentExecutionOrder.Duplicate();
        }

        // TODO: get selectivity from database and experience
        public override double GetSelectivity()
        {
            return 0;
        }

        // TODO: get Cost from database and experience
        public override double GetComputationalCost()
        {
            return 0;
        }
    }

    internal class MatchPath : MatchEdge
    {
        // The minimal length constraint for the path
        public int MinLength { get; set; }
        // The maximal length constraint for the path. Represents max when the value is set to -1.
        public int MaxLength { get; set; }
        /// <summary>
        /// True, the path is referenced in the SELECT clause and path information should be displayed
        /// False, path information can be neglected
        /// </summary>
        public bool ReferencePathInfo { get; set; }

        // Predicates associated with the path constructs in the current context. 
        // Note that path predicates are defined as a part of path constructs, rather than
        // defined in the WHERE clause. The current supported predicates are only equality comparison,
        // and a predicate is in a pair of <edge_attribute, attribute_value>.
        public Dictionary<string, string> AttributeValueDict { get; set; }

        /// <summary>
        /// Converts edge attribute predicates into a boolean expression, which is used for
        /// constructing queries for retrieving edge statistics
        /// </summary>
        /// <returns></returns>
        public override WBooleanExpression RetrievePredicatesExpression()
        {
            if (AttributeValueDict != null)
            {
                WBooleanExpression res = null;
                foreach (var tuple in AttributeValueDict)
                {
                    res = WBooleanBinaryExpression.Conjunction(res, new WBooleanComparisonExpression
                    {
                        ComparisonType = BooleanComparisonType.Equals,
                        FirstExpr =
                            new WColumnReferenceExpression
                            {
                                MultiPartIdentifier =
                                    new WMultiPartIdentifier(new Identifier { Value = LinkAlias },
                                        new Identifier { Value = tuple.Key })
                            },
                        SecondExpr = new WValueExpression { Value = tuple.Value }
                    });
                }
                return res;
            }
            return null;
        }
    }

    /// <summary>
    /// This is the data structure to maintain the execution order.
    /// Order records all information:
    ///     item1 is an MatchNode or NonFreeTable, called "currentNode", which is going to execute
    ///     item2 is an CompileLink, called "traversalLink", which is an link from previous state to current state.
    ///         It can be a MatchEdge, or a PredicateLink of WEdgeVertexBridgeExpression
    ///     item3 is a list of Tuple<PredicateLink, int>, called "forwardLinks", which contains all predicates and corresponding priorities between currentNode and previous state
    ///     item4 is a list of Tuple<MatchEdge, int>, called "backwardEdges", which contains all edges and corresponding priorities that need to be execute
    ///     item5 is a list of ExecutionOrders, called "localExecutionOrders", which contains all execution orders in this TVFs or Derived tables.
    /// ExistingNodesAndEdges and ExistingPredicateLinks are used to record previous state and avoid redundant work
    /// Cost is to evaluate
    /// </summary>
    internal class ExecutionOrder
    {
        public List<Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>>> Order { get; set; }
        public HashSet<string> ExistingNodesAndEdges { get; set; }
        public HashSet<string> ExistingPredicateLinks { get; set; }
        // public Dictionary<string, MatchEdge> ReadyEdges { get; set; }
        public HashSet<string> ReadyEdges { get; set; }
        public double Cost { get; set; }

        public ExecutionOrder()
        {
            this.Order = new List<Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>>>();
            this.ExistingNodesAndEdges = new HashSet<string>();
            this.ExistingPredicateLinks = new HashSet<string>();
            // this.ReadyEdges = new Dictionary<string, MatchEdge>();
            this.ReadyEdges = new HashSet<string>();
            this.Cost = 0.0;
        }

        /// <summary>
        /// This constructor is used to transfer information to subquery
        /// </summary>
        /// <param name="executionOrder"></param>
        public ExecutionOrder(ExecutionOrder executionOrder)
        {
            this.Order = new List<Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>>>();
            this.ExistingNodesAndEdges = new HashSet<string>(executionOrder.ExistingNodesAndEdges);
            this.ExistingPredicateLinks = new HashSet<string>(executionOrder.ExistingPredicateLinks);
            // It is important that the readyEdges should be empty.
            // If the parentOrder has some readyEdges and this order records, the subquery cannot remove edges from readyEdges
            // because it could see the information from parent's aggregationBlock
            this.ReadyEdges = new HashSet<string>();
            this.Cost = 0.0;
        }

        public ExecutionOrder Duplicate()
        {
            return new ExecutionOrder()
            {
                ExistingNodesAndEdges = new HashSet<string>(this.ExistingNodesAndEdges),
                ExistingPredicateLinks = new HashSet<string>(this.ExistingPredicateLinks),
                ReadyEdges = new HashSet<string>(this.ReadyEdges),
                Order = new List<Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>>>(this.Order),
                Cost = this.Cost
            };
        }

        // TODO: utilize statistic information
        private void UpdateCost()
        {
            Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>> tuple = this.Order.Last();

            double cost = 0;
            cost += tuple.Item1.Position;
            
            // sequential order
            this.Cost = this.Cost * 10 + cost;

            // reverse order
            // this.Cost = this.Cost * 10 + 1 / cost
        }

        public void AddParentLink(ParentLink parentLink)
        {
            if (this.Order.Any())
            {
                Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>> newFirstTuple =
                    new Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>>(
                        this.Order[0].Item1, parentLink, this.Order[0].Item3, this.Order[0].Item4, this.Order[0].Item5);
                this.Order[0] = newFirstTuple;
            }
        }

        /// <summary>
        /// The AggregationTable maybe a side-effect table or other special table, so it need to handle firstly.
        /// </summary>
        /// <param name="aggregationBlock"></param>
        /// <param name="predicateLinksAccessedTableAliases"></param>
        public void AddRootTable(
            AggregationBlock aggregationBlock,
            List<Tuple<PredicateLink, HashSet<string>>> predicateLinksAccessedTableAliases)
        {
            NonFreeTable rootTable = aggregationBlock.NonFreeTables[aggregationBlock.RootTableAlias];

            // Find local execution orders
            List<ExecutionOrder> localExecutionOrders = rootTable.GetLocalExecutionOrder(this).Order.First().Item5;

            // Find connected predicateLinks
            List<Tuple<PredicateLink, int>> forwardLinks;
            List<Tuple<MatchEdge, int>> backwardEdges = new List<Tuple<MatchEdge, int>>();

            // Find predicates that can be evaluated
            forwardLinks = this.FindPredicates(rootTable, null, backwardEdges, predicateLinksAccessedTableAliases);
            // Add a next possible tuple
            this.AddTuple(
                new Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>>(
                    rootTable, null, forwardLinks, backwardEdges, localExecutionOrders));
        }

        /// <summary>
        /// If some node has no dependency, we will take it into consideration
        /// </summary>
        /// <param name="aggregationBlock"></param>
        /// <param name="predicateLinksAccessedTableAliases"></param>
        /// <returns></returns>
        public List<ExecutionOrder> GenerateNextOrders(
            AggregationBlock aggregationBlock, 
            List<Tuple<PredicateLink, HashSet<string>>> predicateLinksAccessedTableAliases)
        {
            // Find all possible next tuples
            List<Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>>> nextTuples =
                new List<Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>>>();
            foreach (KeyValuePair<string, HashSet<string>> pair in aggregationBlock.TableInputDependency)
            {
                if (!this.ExistingNodesAndEdges.Contains(pair.Key) && this.ExistingNodesAndEdges.IsSupersetOf(pair.Value))
                {
                    CompileNode node;
                    aggregationBlock.TryGetNode(pair.Key, out node);
                    nextTuples.AddRange(this.GenerateTuples(predicateLinksAccessedTableAliases, node));
                }
            }

            // Generate all possible next orders
            List<ExecutionOrder> nextOrders = new List<ExecutionOrder>();
            foreach (Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>> tuple in nextTuples)
            {
                ExecutionOrder nextOrder = this.Duplicate();
                nextOrder.AddTuple(tuple);
                nextOrders.Add(nextOrder);
            }
            return nextOrders;
        }

        /// <summary>
        /// Given a node, we need to find it possible traversalLink, forwardLinks and backwardEdges
        /// </summary>
        /// <param name="predicateLinksAccessedTableAliases"></param>
        /// <param name="node"></param>
        /// <returns></returns>
        private List<Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>>> GenerateTuples(
            List<Tuple<PredicateLink, HashSet<string>>> predicateLinksAccessedTableAliases,
            CompileNode node)
        {
            List<Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>>> nextTuples =
                new List<Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>>>();

            // Find local execution orders
            List<ExecutionOrder> localExecutionOrders = node.GetLocalExecutionOrder(this).Order.First().Item5;
            // traversalLinks must be existing edges, ready edges or WEdgeVertexBridgeExpression.
            List<CompileLink> traversalLinks = new List<CompileLink>();
            List<MatchEdge> connectedReadyEdges = new List<MatchEdge>();
            List<MatchEdge> connectedUnreadyEdges = new List<MatchEdge>();

            // The item2 is the corresponding priority.
            // If the priority is 1, 
            //  then the edge should try to retrieve before retrieve a node or execute a TVF, 
            //  and predicates should be evaluated after retrieving the edge
            // If the priority is 2, 
            //  then edges should try to retrieve when retrieving a node (not support in execution part now),
            //  and predicates should be evaluated after retrieving these edges and a node or executing a TVF
            // If the priority is 3, 
            //  then edges should try to retrieve after retrieving a node,
            //  and predicates should be evaluated after retrieving these edges
            List<Tuple<PredicateLink, int>> forwardLinks = new List<Tuple<PredicateLink, int>>();
            List<Tuple<MatchEdge, int>> backwardEdges = new List<Tuple<MatchEdge, int>>();

            // Find forwardLinks, backwardEdges and predicates after adding the alias of node
            if (node is MatchNode)
            {
                MatchNode matchNode = node as MatchNode;

                foreach (MatchEdge edge in matchNode.Neighbors)
                {
                    if (!this.ExistingNodesAndEdges.Contains(edge.LinkAlias))
                    {
                        if (this.ReadyEdges.Contains(edge.LinkAlias))
                        {
                            connectedReadyEdges.Add(edge);
                        }
                        else
                        {
                            connectedUnreadyEdges.Add(edge);
                        }
                    }
                    if (this.ExistingNodesAndEdges.Contains(edge.LinkAlias) || this.ReadyEdges.Contains(edge.LinkAlias))
                    {
                        traversalLinks.Add(edge);
                    }
                }
                foreach (MatchEdge edge in matchNode.ReverseNeighbors)
                {
                    if (!this.ExistingNodesAndEdges.Contains(edge.LinkAlias))
                    {
                        if (this.ReadyEdges.Contains(edge.LinkAlias))
                        {
                            connectedReadyEdges.Add(edge);
                        }
                        else
                        {
                            connectedUnreadyEdges.Add(edge);
                        }
                    }
                    if (this.ExistingNodesAndEdges.Contains(edge.LinkAlias) || this.ReadyEdges.Contains(edge.LinkAlias))
                    {
                        traversalLinks.Add(edge);
                    }
                }
                foreach (MatchEdge edge in matchNode.DanglingEdges)
                {
                    if (!this.ExistingNodesAndEdges.Contains(edge.LinkAlias))
                    {
                        connectedUnreadyEdges.Add(edge);
                    }
                }

                // Add the node's alias temporarily to find WEdgeVertexBridgeExpression
                HashSet<string> temporaryAliases = new HashSet<string>(this.ExistingNodesAndEdges);
                temporaryAliases.Add(matchNode.NodeAlias);
                foreach (Tuple<PredicateLink, HashSet<string>> tuple in predicateLinksAccessedTableAliases)
                {
                    if (!this.ExistingPredicateLinks.Contains(tuple.Item1.LinkAlias) &&
                        temporaryAliases.IsSupersetOf(tuple.Item2) &&
                        tuple.Item1.BooleanExpression is WEdgeVertexBridgeExpression)
                    {
                        traversalLinks.Add(tuple.Item1);
                    }
                }

                // Find all possible sublists of connectedUnreadyEdges
                // Because we may not reterieve all edges at this step
                List<List<MatchEdge>> connectedUnreadyEdgesSublists = GetAllSublists(connectedUnreadyEdges);

                // if traversalLinks is empty, we add a NULL in it.
                if (!traversalLinks.Any())
                {
                    traversalLinks.Add(null);
                }

                // backwardEdges = (connectedReadyEdges) U (a sublist of connectedUnreadyEdges) - (traversalLink)
                // but connnectedREadyEdges' priorities can be 2 or 3, connectedUnreadyEdges must be 2
                foreach (CompileLink traversalLink in traversalLinks)
                {
                    foreach (List<MatchEdge> connectedUnreadyEdgesSublist in connectedUnreadyEdgesSublists)
                    {
                        List<MatchEdge> connectedReadyEdgesSublist = new List<MatchEdge>(connectedReadyEdges);
                        if (traversalLink is MatchEdge)
                        {
                            connectedReadyEdgesSublist.Remove(traversalLink as MatchEdge);
                        }

                        // Find all combinations of connectedReadyEdges
                        // These readyEdges' priorities can be 2 or 3
                        List<List<Tuple<MatchEdge, int>>> connectedReadyEdgesCombinations = GetAllCombinations(connectedReadyEdgesSublist, new List<int>() { 2, 3 });
                        // Find all combinations of connectedUnreadyEdges
                        // All unreadyEdges' priorities must be 2 if retrieving them
                        List<List<Tuple<MatchEdge, int>>> connectedUnreadyEdgesCombinations = GetAllCombinations(connectedUnreadyEdgesSublist, new List<int>() { 2 });

                        // Find all combinations of connectedReadyEdges and connectedUnreadyEdges
                        foreach (List<Tuple<MatchEdge, int>> connectedReadyEdgesCombination in connectedReadyEdgesCombinations)
                        {
                            foreach (List<Tuple<MatchEdge, int>> connectedUnreadyEdgesCombination in connectedUnreadyEdgesCombinations)
                            {
                                backwardEdges = new List<Tuple<MatchEdge, int>>(connectedReadyEdgesCombination);
                                backwardEdges.AddRange(connectedUnreadyEdgesCombination);
                                forwardLinks = this.FindPredicates(matchNode, traversalLink, backwardEdges, predicateLinksAccessedTableAliases);
                                nextTuples.Add(
                                    new Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>,
                                        List<Tuple<MatchEdge, int>>, List<ExecutionOrder>>(matchNode,
                                        traversalLink, forwardLinks, backwardEdges, localExecutionOrders));
                            }
                        }
                    }
                }
            }
            else if (node is NonFreeTable)
            {
                forwardLinks = this.FindPredicates(node, null, backwardEdges, predicateLinksAccessedTableAliases);
                nextTuples.Add(new Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>>(
                    node, null, forwardLinks, backwardEdges, localExecutionOrders));
            }
            else
            {
                throw new QueryCompilationException("Can't find " + node.ToString() + " in AggregationBlock");
            }

            return nextTuples;
        }

        /// <summary>
        /// Given a node / TVF, a traversalLink and backwardEdges, try to find all predicates that can be evaluated
        /// </summary>
        /// <param name="node"></param>
        /// <param name="traversalLink"></param>
        /// <param name="backwardEdges"></param>
        /// <param name="predicateLinksAccessedTableAliases"></param>
        private List<Tuple<PredicateLink, int>> FindPredicates(
            CompileNode node,
            CompileLink traversalLink,
            List<Tuple<MatchEdge, int>> backwardEdges,
            List<Tuple<PredicateLink, HashSet<string>>> predicateLinksAccessedTableAliases)
        {
            // Record temporary aliases and predicates
            HashSet<string> temporaryAliases = new HashSet<string>(this.ExistingNodesAndEdges);
            HashSet<string> temporaryPredicates = new HashSet<string>(this.ExistingPredicateLinks);
            List<Tuple<PredicateLink, int>> forwardLinks = new List<Tuple<PredicateLink, int>>();

            // priority = 1
            // retrieve traversalLink and add predicates
            if (traversalLink != null && !this.ExistingNodesAndEdges.Contains(traversalLink.LinkAlias))
            {
                // Find predicates that can be evaluated after retrieving this traversalLink
                if (traversalLink is MatchEdge)
                {
                    temporaryAliases.Add(traversalLink.LinkAlias);
                }
                else if (traversalLink is PredicateLink)
                {
                    temporaryPredicates.Add(traversalLink.LinkAlias);
                }
                else
                {
                    throw new QueryCompilationException("Cannot support " + traversalLink + " as a traversal link or an edge");
                }

                foreach (Tuple<PredicateLink, HashSet<string>> tuple in predicateLinksAccessedTableAliases)
                {
                    if (!temporaryPredicates.Contains(tuple.Item1.LinkAlias) &&
                        temporaryAliases.IsSupersetOf(tuple.Item2))
                    {
                        temporaryPredicates.Add(tuple.Item1.LinkAlias);
                        forwardLinks.Add(new Tuple<PredicateLink, int>(tuple.Item1, 1));
                    }
                }

                // Make sure the all existing edges and this traversalLink are consistent
                // Because all existing edges are consistent, we just need make one exstring edge and this traversalLink consistent
                MatchNode matchNode = node as MatchNode;
                WColumnReferenceExpression nodeColumnReferenceExprFromTraversalLink = GetNodeColumnReferenceExprFromLink(traversalLink);
                WColumnReferenceExpression nodeColumnReferenceExprFromAnExistingEdge = GetNodeColumnReferenceExprFromAnExistingEdge(matchNode, this.ExistingNodesAndEdges);
                if (nodeColumnReferenceExprFromTraversalLink != null &&
                    nodeColumnReferenceExprFromAnExistingEdge != null)
                {
                    forwardLinks.Add(
                        new Tuple<PredicateLink, int>(
                            new PredicateLink(
                                SqlUtil.GetEqualBooleanComparisonExpr(nodeColumnReferenceExprFromTraversalLink,
                                    nodeColumnReferenceExprFromAnExistingEdge)), 1));
                }
            }

            // priority = 2
            // retrieve node and some edges and add predicates
            temporaryAliases.Add(node.NodeAlias);
            foreach (Tuple<MatchEdge, int> tuple in backwardEdges)
            {
                if (tuple.Item2 == 2)
                {
                    temporaryAliases.Add(tuple.Item1.LinkAlias);
                    MatchNode otherNode = tuple.Item1.SinkNode;
                    WColumnReferenceExpression nodeColumnReferenceExprFromBackwardEdge = null;
                    WColumnReferenceExpression nodeColumnReferenceExprFromAnExistingEdge = GetNodeColumnReferenceExprFromAnExistingEdge(otherNode, this.ExistingNodesAndEdges);
                    if (nodeColumnReferenceExprFromAnExistingEdge != null)
                    {
                        foreach (MatchEdge edge in otherNode.Neighbors)
                        {
                            if (edge.LinkAlias == tuple.Item1.LinkAlias)
                            {
                                nodeColumnReferenceExprFromBackwardEdge = GetNodeColumnReferenceExprFromLink(edge);
                                break;
                            }
                        }
                        if (nodeColumnReferenceExprFromBackwardEdge == null)
                        {
                            foreach (MatchEdge edge in otherNode.ReverseNeighbors)
                            {
                                if (edge.LinkAlias == tuple.Item1.LinkAlias)
                                {
                                    nodeColumnReferenceExprFromBackwardEdge = GetNodeColumnReferenceExprFromLink(edge);
                                    break;
                                }
                            }
                        }
                        if (nodeColumnReferenceExprFromBackwardEdge == null)
                        {
                            foreach (MatchEdge edge in otherNode.DanglingEdges)
                            {
                                if (edge.LinkAlias == tuple.Item1.LinkAlias)
                                {
                                    nodeColumnReferenceExprFromBackwardEdge = GetNodeColumnReferenceExprFromLink(edge);
                                    break;
                                }
                            }
                        }
                    }
                    // Because all existing edges are consistent, we just need make one exstring edge and this edge consistent
                    if (nodeColumnReferenceExprFromBackwardEdge != null &&
                        nodeColumnReferenceExprFromAnExistingEdge != null)
                    {
                        temporaryPredicates.Add(tuple.Item1.LinkAlias);
                        forwardLinks.Add(
                            new Tuple<PredicateLink, int>(
                                new PredicateLink(
                                    SqlUtil.GetEqualBooleanComparisonExpr(nodeColumnReferenceExprFromBackwardEdge,
                                        nodeColumnReferenceExprFromAnExistingEdge)), 2));
                    }
                }
            }

            // Check predicates
            foreach (Tuple<PredicateLink, HashSet<string>> tuple in predicateLinksAccessedTableAliases)
            {
                if (!temporaryPredicates.Contains(tuple.Item1.LinkAlias) &&
                    temporaryAliases.IsSupersetOf(tuple.Item2))
                {
                    temporaryPredicates.Add(tuple.Item1.LinkAlias);
                    forwardLinks.Add(new Tuple<PredicateLink, int>(tuple.Item1, 2));
                }
            }

            // priority = 3
            // retrieve another edges and add predicates
            foreach (Tuple<MatchEdge, int> tuple in backwardEdges)
            {
                if (tuple.Item2 == 3)
                {
                    temporaryAliases.Add(tuple.Item1.LinkAlias);
                    MatchNode otherNode = tuple.Item1.SinkNode;
                    WColumnReferenceExpression nodeColumnReferenceExprFromBackwardEdge = null;
                    WColumnReferenceExpression nodeColumnReferenceExprFromAnExistingEdge = GetNodeColumnReferenceExprFromAnExistingEdge(otherNode, this.ExistingNodesAndEdges);
                    if (nodeColumnReferenceExprFromAnExistingEdge != null)
                    {
                        foreach (MatchEdge edge in otherNode.Neighbors)
                        {
                            if (edge.LinkAlias == tuple.Item1.LinkAlias)
                            {
                                nodeColumnReferenceExprFromBackwardEdge = GetNodeColumnReferenceExprFromLink(edge);
                                break;
                            }
                        }
                        if (nodeColumnReferenceExprFromBackwardEdge == null)
                        {
                            foreach (MatchEdge edge in otherNode.ReverseNeighbors)
                            {
                                if (edge.LinkAlias == tuple.Item1.LinkAlias)
                                {
                                    nodeColumnReferenceExprFromBackwardEdge = GetNodeColumnReferenceExprFromLink(edge);
                                    break;
                                }
                            }
                        }
                        if (nodeColumnReferenceExprFromBackwardEdge == null)
                        {
                            foreach (MatchEdge edge in otherNode.DanglingEdges)
                            {
                                if (edge.LinkAlias == tuple.Item1.LinkAlias)
                                {
                                    nodeColumnReferenceExprFromBackwardEdge = GetNodeColumnReferenceExprFromLink(edge);
                                    break;
                                }
                            }
                        }
                    }
                    // Because all existing edges are consistent, we just need make one exstring edge and this edge consistent
                    if (nodeColumnReferenceExprFromBackwardEdge != null &&
                        nodeColumnReferenceExprFromAnExistingEdge != null)
                    {
                        temporaryPredicates.Add(tuple.Item1.LinkAlias);
                        forwardLinks.Add(
                            new Tuple<PredicateLink, int>(
                                new PredicateLink(
                                    SqlUtil.GetEqualBooleanComparisonExpr(nodeColumnReferenceExprFromBackwardEdge,
                                        nodeColumnReferenceExprFromAnExistingEdge)), 3));
                    }
                }
            }

            // Check predicates
            foreach (Tuple<PredicateLink, HashSet<string>> tuple in predicateLinksAccessedTableAliases)
            {
                if (!temporaryPredicates.Contains(tuple.Item1.LinkAlias) &&
                    temporaryAliases.IsSupersetOf(tuple.Item2))
                {
                    temporaryPredicates.Add(tuple.Item1.LinkAlias);
                    forwardLinks.Add(new Tuple<PredicateLink, int>(tuple.Item1, 3));
                }
            }
            return forwardLinks;
        }

        private static WColumnReferenceExpression GetNodeColumnReferenceExprFromLink(CompileLink link)
        {
            if (link is MatchEdge)
            {
                MatchEdge edge = link as MatchEdge;
                if (edge.EdgeType == WEdgeType.OutEdge)
                {
                    return SqlUtil.GetColumnReferenceExpr(edge.LinkAlias,
                        edge.IsReversed ? GremlinKeyword.EdgeSinkV : GremlinKeyword.EdgeSourceV);
                }
                else if (edge.EdgeType == WEdgeType.InEdge)
                {
                    return SqlUtil.GetColumnReferenceExpr(edge.LinkAlias,
                        edge.IsReversed ? GremlinKeyword.EdgeSourceV : GremlinKeyword.EdgeSinkV);
                }
                else
                {
                    return SqlUtil.GetColumnReferenceExpr(edge.LinkAlias, GremlinKeyword.EdgeOtherV);
                }
            }
            else if (link is PredicateLink)
            {
                PredicateLink predicateLink = link as PredicateLink;
                if (predicateLink.BooleanExpression is WEdgeVertexBridgeExpression)
                {
                    return (predicateLink.BooleanExpression as WEdgeVertexBridgeExpression).FirstExpr as WColumnReferenceExpression;
                }
            }
            throw new QueryCompilationException("Cannot support " + link + " as a traversal link or an edge");
        }

        private static WColumnReferenceExpression GetNodeColumnReferenceExprFromAnExistingEdge(MatchNode node, HashSet<string> existingNodeAndEdges)
        {
            if (node == null)
            {
                return null;
            }
            foreach (MatchEdge edge in node.Neighbors)
            {
                if (existingNodeAndEdges.Contains(edge.LinkAlias))
                {
                    return GetNodeColumnReferenceExprFromLink(edge);
                }
            }
            foreach (MatchEdge edge in node.ReverseNeighbors)
            {
                if (existingNodeAndEdges.Contains(edge.LinkAlias))
                {
                    return GetNodeColumnReferenceExprFromLink(edge);
                }
            }
            foreach (MatchEdge edge in node.DanglingEdges)
            {
                if (existingNodeAndEdges.Contains(edge.LinkAlias))
                {
                    return GetNodeColumnReferenceExprFromLink(edge);
                }
            }
            return null;
        }

        /// <summary>
        /// Find all sublists
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <returns></returns>
        private static List<List<T>> GetAllSublists<T>(List<T> list)
        {
            List<List<T>> all = new List<List<T>>() {new List<T>()};
            CollectAllSublists(all, 0, list);
            return all;
        }

        private static void CollectAllSublists<T>(List<List<T>> all, int index, List<T> list)
        {
            if (index >= list.Count)
            {
                return;
            }
            int count = all.Count;
            for (int index1 = 0; index1 < count; ++index1)
            {
                List<T> newList = new List<T>(all[index1]);
                newList.Add(list[index]);
                all.Add(newList);
            }
            CollectAllSublists(all, index+1, list);
        }

        private static List<List<Tuple<T, int>>> GetAllCombinations<T>(List<T> tupleCollection, List<int> possibleList)
        {
            List<List<Tuple<T, int>>> all = new List<List<Tuple<T, int>>>() { new List<Tuple<T, int>>() };
            CollectAllCombinations(all, 0, tupleCollection, possibleList);
            return all;
        }

        private static void CollectAllCombinations<T>(List<List<Tuple<T, int>>> existingCombinations, int index, List<T> tupleCollection, List<int> possibleList)
        {
            if (index >= tupleCollection.Count)
            {
                return;
            }
            List<List<Tuple<T, int>>> newCombinations = new List<List<Tuple<T, int>>>();
            foreach (List<Tuple<T, int>> existingCombination in existingCombinations)
            {
                foreach (int label in possibleList)
                {
                    List<Tuple<T, int>> newCombination = new List<Tuple<T, int>>(existingCombination);
                    newCombination.Add(new Tuple<T, int>(tupleCollection[index], label));
                    newCombinations.Add(newCombination);
                }
            }
            existingCombinations.Clear();
            existingCombinations.AddRange(newCombinations);
            CollectAllCombinations(existingCombinations, index + 1, tupleCollection, possibleList);
        }

        /// <summary>
        /// Add an tuple to a new order, and update information
        /// </summary>
        /// <param name="tuple"></param>
        public void AddTuple(Tuple<CompileNode, CompileLink, List<Tuple<PredicateLink, int>>, List<Tuple<MatchEdge, int>>, List<ExecutionOrder>> tuple)
        {
            if (tuple.Item2 is PredicateLink)
            {
                this.ExistingPredicateLinks.Add(tuple.Item2.LinkAlias);
            }
            else if (tuple.Item2 is MatchEdge)
            {
                this.ExistingNodesAndEdges.Add(tuple.Item2.LinkAlias);
                this.ReadyEdges.Remove(tuple.Item2.LinkAlias);
            }

            this.ExistingNodesAndEdges.Add(tuple.Item1.NodeAlias);

            // Check ready edges whether to be generated
            foreach (Tuple<PredicateLink, int> predicateLinkTuple in tuple.Item3)
            {
                this.ExistingPredicateLinks.Add(predicateLinkTuple.Item1.LinkAlias);
            }

            if (tuple.Item1 is MatchNode)
            {
                MatchNode matchNode = tuple.Item1 as MatchNode;
                foreach (MatchEdge edge in matchNode.Neighbors)
                {
                    if (this.ExistingNodesAndEdges.Contains(edge.LinkAlias))
                    {
                        continue;
                    }
                    else if (tuple.Item4.Exists(edgeTuple => edgeTuple.Item1.LinkAlias == edge.LinkAlias))
                    {
                        this.ExistingNodesAndEdges.Add(edge.LinkAlias);
                        this.ReadyEdges.Remove(edge.LinkAlias);
                    }
                    else
                    {
                        this.ReadyEdges.Add(edge.LinkAlias);
                    }
                }
                foreach (MatchEdge edge in matchNode.ReverseNeighbors)
                {
                    if (this.ExistingNodesAndEdges.Contains(edge.LinkAlias))
                    {
                        continue;
                    }
                    else if (tuple.Item4.Exists(edgeTuple => edgeTuple.Item1.LinkAlias == edge.LinkAlias))
                    {
                        this.ExistingNodesAndEdges.Add(edge.LinkAlias);
                        this.ReadyEdges.Remove(edge.LinkAlias);
                    }
                    else
                    {
                        this.ReadyEdges.Add(edge.LinkAlias);
                    }
                }
                foreach (MatchEdge edge in matchNode.DanglingEdges)
                {
                    if (this.ExistingNodesAndEdges.Contains(edge.LinkAlias))
                    {
                        continue;
                    }
                    else if (tuple.Item4.Exists(edgeTuple => edgeTuple.Item1.LinkAlias == edge.LinkAlias))
                    {
                        this.ExistingNodesAndEdges.Add(edge.LinkAlias);
                        this.ReadyEdges.Remove(edge.LinkAlias);
                    }
                    else
                    {
                        this.ReadyEdges.Add(edge.LinkAlias);
                    }
                }
            }

            this.Order.Add(tuple);
            this.UpdateCost();
        }
    }

    internal class ExecutionOrderComparer : IComparer<ExecutionOrder>
    {
        public int Compare(ExecutionOrder order1, ExecutionOrder order2)
        {
            return order1.Cost.CompareTo(order2.Cost);
        }
    }
}
