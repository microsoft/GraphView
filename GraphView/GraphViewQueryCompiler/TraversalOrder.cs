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

        // TODO: estimate cost according experience
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

        // TODO: estimate cost according experience
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
            return 1.0 * this.EstimatedRows;
        }
    }

    internal class NonMatchTable : CompileNode
    {
        public WTableReferenceWithAlias TableReference { get; set; }
        public double Cardinality { get; set; }

        public NonMatchTable(WTableReferenceWithAlias tableReference)
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
        }

        // TODO: get selectivity from database and experience
        public override double GetSelectivity()
        {
            return 0.5;
        }
        
        // TODO: get cost from database and experience
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

    internal class TraversalOrder
    {
        public List<Tuple<CompileNode, CompileLink, List<CompileLink>>> Order { get; set; }
        private Dictionary<string, HashSet<string>> tableInputDependency;
        private List<Tuple<WBooleanExpression, HashSet<string>>> predicatesAccessedTableReferences;
        public double cost;

        private static double alpha = 1.0, beta = 1.0, gama = 1.0;

        public TraversalOrder()
        {
            this.Order = new List<Tuple<CompileNode, CompileLink, List<CompileLink>>>();
            this.tableInputDependency = new Dictionary<string, HashSet<string>>();
            this.predicatesAccessedTableReferences = new List<Tuple<WBooleanExpression, HashSet<string>>> ();
            this.cost = 0.0;
        }

        public TraversalOrder(Dictionary<string, HashSet<string>> tableInputDependency,
            List<Tuple<WBooleanExpression, HashSet<string>>> predicatesAccessedTableReferences)
        {
            this.Order = new List<Tuple<CompileNode, CompileLink, List<CompileLink>>>();
            this.tableInputDependency = tableInputDependency;
            this.predicatesAccessedTableReferences = predicatesAccessedTableReferences;
            this.cost = 0.0;
        }

        public TraversalOrder Duplicate()
        {
            return new TraversalOrder()
            {
                Order = this.Order,
                cost = this.cost
            };
        }

        public void Append(TraversalOrder blockOrder)
        {
            foreach (Tuple<CompileNode, CompileLink, List<CompileLink>> tuple in blockOrder.Order)
            {
                this.AddTuple(tuple);
            }
        }

        // TODO: utilize statistic information
        private void UpdateCost()
        {
            this.cost = 0.0;
            foreach (Tuple<CompileNode, CompileLink, List<CompileLink>> tuple in this.Order)
            {
                this.cost += tuple.Item1.ComputationalCost() * alpha + tuple.Item1.GetCardinality() * beta;
                this.cost += tuple.Item2 == null ? 0 : tuple.Item2.ComputationalCost() * alpha + tuple.Item2.GetSelectivity() * gama;
                foreach (CompileLink link in tuple.Item3)
                {
                    this.cost += link.ComputationalCost() * alpha + link.GetSelectivity() * gama;
                }
            }
        }

        public List<TraversalOrder> GenerateNextOrders(AggregationBlock aggregationBlock)
        {
            List<TraversalOrder> nextOrders = new List<TraversalOrder>();

            // TODO: finish this method

            return nextOrders;
        }

        public void AddTuple(Tuple<CompileNode, CompileLink, List<CompileLink>> tuple)
        {
            this.Order.Add(tuple);
            this.UpdateCost();
        }
    }

    internal class TraversalOrderComparer : IComparer<TraversalOrder>
    {
        public int Compare(TraversalOrder order1, TraversalOrder order2)
        {
            return order1.cost.CompareTo(order2.cost);
        }
    }
}
