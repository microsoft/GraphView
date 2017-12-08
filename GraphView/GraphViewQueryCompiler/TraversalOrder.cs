using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.GraphViewDBPortal;

namespace GraphView
{
    internal abstract class CompileNode
    {
        public string NodeAlias { get; set; }
        private int hashCode;

        public override int GetHashCode()
        {
            return hashCode == Int32.MaxValue ? hashCode = this.NodeAlias.GetHashCode() : hashCode;
        }

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
    }

    internal abstract class CompileLink
    {
        public string LinkAlias { get; set; }
        private int hashCode;

        public override int GetHashCode()
        {
            return hashCode == Int32.MaxValue ? hashCode = this.LinkAlias.GetHashCode() : hashCode;
        }

        // TODO: get selectivity from database
        public virtual double GetSelectivity()
        {
            return 1.0;
        }

        // TODO: estimate Cost according experience
        public virtual double ComputationalCost()
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
        public override double ComputationalCost()
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

    internal class ExecutionOrder
    {
        public List<Tuple<CompileNode, CompileLink, List<CompileLink>, List<CompileLink>>> Order { get; set; }
        public HashSet<string> ExistingNodesAndEdges { get; set; }
        public HashSet<string> ExistingPredicateLinks { get; set; }
        public double Cost { get; set; }

        private static double alpha = 1.2, beta = 10.0, gama = 1.0;

        public ExecutionOrder(HashSet<string> tableReferences)
        {
            this.Order = new List<Tuple<CompileNode, CompileLink, List<CompileLink>, List<CompileLink>>>();
            this.ExistingNodesAndEdges = new HashSet<string>(tableReferences);
            this.ExistingPredicateLinks = new HashSet<string>();
            this.Cost = 0.0;
        }

        public ExecutionOrder Duplicate()
        {
            return new ExecutionOrder(this.ExistingNodesAndEdges)
            {
                ExistingPredicateLinks = new HashSet<string>(this.ExistingPredicateLinks),
                Order = new List<Tuple<CompileNode, CompileLink, List<CompileLink>, List<CompileLink>>>(this.Order),
                Cost = this.Cost
            };
        }

        // TODO: utilize statistic information
        private void UpdateCost()
        {
            Tuple<CompileNode, CompileLink, List<CompileLink>, List<CompileLink>> tuple = this.Order.Last();
            this.Cost *= alpha;

            this.Cost += tuple.Item1.ComputationalCost() * alpha + tuple.Item1.GetCardinality() * beta;
            this.Cost += tuple.Item2 == null ? 0 : tuple.Item2.ComputationalCost() * alpha + tuple.Item2.GetSelectivity() * gama;
            foreach (CompileLink link in tuple.Item3)
            {
                this.Cost += link.ComputationalCost() * alpha + link.GetSelectivity() * gama;
            }
        }

        public void AddAggregationTable(AggregationBlock aggregationBlock,
            List<Tuple<PredicateLink, HashSet<string>>> predicateLinksAccessedTableAliases)
        {
            NonFreeTable aggregateionTable = aggregationBlock.NonFreeTables[aggregationBlock.AggregationAlias];

            // Find connected predicateLinks
            List<CompileLink> backwardLinks = new List<CompileLink>();
            List<CompileLink> forwardLinks = new List<CompileLink>();

            if (aggregateionTable.NodeAlias == "dummy")
            {
                foreach (Tuple<PredicateLink, HashSet<string>> tuple in predicateLinksAccessedTableAliases)
                {
                    if (!this.ExistingPredicateLinks.Contains(tuple.Item1.LinkAlias))
                    {
                        if (this.ExistingNodesAndEdges.IsSupersetOf(tuple.Item2))
                        {
                            forwardLinks.Add(tuple.Item1);
                        }
                    }
                }
            }
            else
            {
                bool isOnlyTargetTableReferenced;
                AccessedTableColumnVisitor columnVisitor = new AccessedTableColumnVisitor();
                Dictionary<string, HashSet<string>> tableColumnReferences = columnVisitor.Invoke(aggregateionTable.TableReference,
                    aggregationBlock.TableAliases, out isOnlyTargetTableReferenced);

                // Add all aliases temporarily, including the node's alias and those of subqueries, to check predicates
                HashSet<string> temporaryAliases = new HashSet<string>(this.ExistingNodesAndEdges);
                temporaryAliases.Add(aggregateionTable.NodeAlias);
                temporaryAliases.UnionWith(tableColumnReferences.Keys);
                foreach (Tuple<PredicateLink, HashSet<string>> tuple in predicateLinksAccessedTableAliases)
                {
                    if (!this.ExistingPredicateLinks.Contains(tuple.Item1.LinkAlias))
                    {
                        if (this.ExistingNodesAndEdges.IsSupersetOf(tuple.Item2))
                        {
                            backwardLinks.Add(tuple.Item1);
                        }
                        else if (temporaryAliases.IsSupersetOf(tuple.Item2))
                        {
                            forwardLinks.Add(tuple.Item1);
                        }
                    }
                }
            }

            // Add a next possible tuple
            this.AddTuple(new Tuple<CompileNode, CompileLink, List<CompileLink>, List<CompileLink>>(aggregateionTable, null, backwardLinks, forwardLinks));
        }

        public List<ExecutionOrder> GenerateNextOrders(AggregationBlock aggregationBlock, 
            List<Tuple<PredicateLink, HashSet<string>>> predicateLinksAccessedTableAliases)
        {
            List<Tuple<CompileNode, CompileLink, List<CompileLink>, List<CompileLink>>> nextTuples =
                new List<Tuple<CompileNode, CompileLink, List<CompileLink>, List<CompileLink>>>();
            foreach (KeyValuePair<string, HashSet<string>> pair in aggregationBlock.TableInputDependency)
            {
                if (!this.ExistingNodesAndEdges.Contains(pair.Key) && this.ExistingNodesAndEdges.IsSupersetOf(pair.Value))
                {
                    CompileNode node;
                    aggregationBlock.TryGetNode(pair.Key, out node);
                    nextTuples.AddRange(this.GenerateTuples(aggregationBlock, predicateLinksAccessedTableAliases, node));
                }
            }

            List<ExecutionOrder> nextOrders = new List<ExecutionOrder>();
            foreach (Tuple<CompileNode, CompileLink, List<CompileLink>, List<CompileLink>> tuple in nextTuples)
            {
                ExecutionOrder nextOrder = this.Duplicate();
                nextOrder.AddTuple(tuple);
                nextOrders.Add(nextOrder);
            }
            return nextOrders;
        }

        private List<Tuple<CompileNode, CompileLink, List<CompileLink>, List<CompileLink>>> GenerateTuples(
            AggregationBlock aggregationBlock,
            List<Tuple<PredicateLink, HashSet<string>>> predicateLinksAccessedTableAliases,
            CompileNode node)
        {
            List<Tuple<CompileNode, CompileLink, List<CompileLink>, List<CompileLink>>> nextTuples =
                new List<Tuple<CompileNode, CompileLink, List<CompileLink>, List<CompileLink>>>();
            
            if (node is MatchNode)
            {
                MatchNode matchNode = node as MatchNode;
                // Add the alias of node and aliases of edges temporarily to check predicates
                HashSet<string> temporaryAliases = new HashSet<string>(this.ExistingNodesAndEdges);
                temporaryAliases.Add(matchNode.NodeAlias);
                List<CompileLink> backwardLinks = new List<CompileLink>();
                List<CompileLink> forwardLinks = new List<CompileLink>();

                foreach (MatchEdge edge in matchNode.Neighbors)
                {
                    if (!this.ExistingNodesAndEdges.Contains(edge.LinkAlias))
                    {
                        temporaryAliases.Add(edge.LinkAlias);
                        forwardLinks.Add(edge);
                    }
                    else
                    {
                        backwardLinks.Add(edge);
                    }
                }
                foreach (MatchEdge edge in matchNode.ReverseNeighbors)
                {
                    if (!this.ExistingNodesAndEdges.Contains(edge.LinkAlias))
                    {
                        temporaryAliases.Add(edge.LinkAlias);
                        forwardLinks.Add(edge);
                    }
                    else
                    {
                        backwardLinks.Add(edge);
                    }
                }
                foreach (MatchEdge edge in matchNode.DanglingEdges)
                {
                    if (!this.ExistingNodesAndEdges.Contains(edge.LinkAlias))
                    {
                        temporaryAliases.Add(edge.LinkAlias);
                        forwardLinks.Add(edge);
                    }
                    else
                    {
                        backwardLinks.Add(edge);
                    }
                }
                
                // Find connected predicateLinks
                foreach (Tuple<PredicateLink, HashSet<string>> tuple in predicateLinksAccessedTableAliases)
                {
                    if (!this.ExistingPredicateLinks.Contains(tuple.Item1.LinkAlias))
                    {
                        if (temporaryAliases.IsSupersetOf(tuple.Item2))
                        {
                            if (tuple.Item1.BooleanExpression is WEdgeVertexBridgeExpression)
                            {
                                backwardLinks.Add(tuple.Item1);
                            }
                            else
                            {
                                forwardLinks.Add(tuple.Item1);
                            }
                        }
                    }
                }

                // if use other link to get this node
                if (backwardLinks.Any())
                {
                    foreach (CompileLink traversalLink in backwardLinks)
                    {
                        List<CompileLink> tupleBackwardLinks = new List<CompileLink>(backwardLinks);
                        tupleBackwardLinks.Remove(traversalLink);
                        nextTuples.Add(new Tuple<CompileNode, CompileLink, List<CompileLink>, List<CompileLink>>(node,
                            traversalLink, tupleBackwardLinks, forwardLinks));
                    }
                }
                else
                {
                    // if do not use other link to get this node
                    nextTuples.Add(new Tuple<CompileNode, CompileLink, List<CompileLink>, List<CompileLink>>(node, null, backwardLinks, forwardLinks));
                }
            }
            else if (node is NonFreeTable)
            {
                NonFreeTable nonFreeTable = node as NonFreeTable;

                bool isOnlyTargetTableReferenced;
                AccessedTableColumnVisitor columnVisitor = new AccessedTableColumnVisitor();
                Dictionary<string, HashSet<string>> tableColumnReferences = columnVisitor.Invoke(nonFreeTable.TableReference,
                    aggregationBlock.TableAliases, out isOnlyTargetTableReferenced);

                // Add all aliases temporarily, including the node's alias and those of subqueries, to check predicates
                HashSet<string> temporaryAliases = new HashSet<string>(this.ExistingNodesAndEdges);
                temporaryAliases.Add(nonFreeTable.NodeAlias);
                temporaryAliases.UnionWith(tableColumnReferences.Keys);

                // Find connected predicateLinks
                List<CompileLink> backwardLinks = new List<CompileLink>();
                List<CompileLink> forwardLinks = new List<CompileLink>();
                foreach (Tuple<PredicateLink, HashSet<string>> tuple in predicateLinksAccessedTableAliases)
                {
                    if (!this.ExistingPredicateLinks.Contains(tuple.Item1.LinkAlias))
                    {
                        if (temporaryAliases.IsSupersetOf(tuple.Item2))
                        {
                            forwardLinks.Add(tuple.Item1);
                        }
                    }
                }

                // Add a next possible tuple
                nextTuples.Add(new Tuple<CompileNode, CompileLink, List<CompileLink>, List<CompileLink>>(node, null, backwardLinks, forwardLinks));
            }
            else
            {
                throw new QueryCompilationException("Can't find " + node.ToString() + " in AggregationBlock");
            }
            return nextTuples;
        }

        public void AddTuple(Tuple<CompileNode, CompileLink, List<CompileLink>, List<CompileLink>> tuple)
        {
            this.ExistingNodesAndEdges.Add(tuple.Item1.NodeAlias);

            foreach (CompileLink link in tuple.Item4)
            {
                if (link is MatchEdge)
                {
                    this.ExistingNodesAndEdges.Add(link.LinkAlias);
                }
                else if (link is PredicateLink)
                {
                    this.ExistingPredicateLinks.Add(link.LinkAlias);
                }
                else
                {
                    throw new QueryCompilationException(link.LinkAlias + " can not be now supported");
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
