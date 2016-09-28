using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    partial class WSelectQueryBlock
    {
        internal override GraphViewExecutionOperator Generate(GraphViewConnection pConnection)
        {
            if (WithPathClause != null) WithPathClause.Generate(pConnection);
            // Construct Match graph for later use
            MatchGraph graph = ConstructGraph();
            // Construct a header for the operators.
            List<string> header = ConstructHeader(graph);
            // Attach pre-generated docDB script to the node on Match graph, 
            // and turn predicates that cannot be attached to one node into boolean function.
            List<BooleanFunction> Functions = AttachScriptSegment(graph, header);
            // Construct operators accroding to the match graph, header and boolean function list.
            return ConstructOperator(graph, header, pConnection, Functions);
        }

        private MatchGraph ConstructGraph()
        {
            Dictionary<string, List<string>> EdgeColumnToAliasesDict = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, MatchPath> pathDictionary = new Dictionary<string, MatchPath>(StringComparer.OrdinalIgnoreCase);

            UnionFind UnionFind = new UnionFind();
            Dictionary<string, MatchNode> Nodes = new Dictionary<string, MatchNode>(StringComparer.OrdinalIgnoreCase);
            List<ConnectedComponent> ConnectedSubGraphs = new List<ConnectedComponent>();
            Dictionary<string, ConnectedComponent> SubGrpahMap = new Dictionary<string, ConnectedComponent>(StringComparer.OrdinalIgnoreCase);
            Dictionary<string, string> Parent = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            UnionFind.Parent = Parent;

            // Retrive information from the SelectQueryBlcok
            if (FromClause != null)
            {
                foreach (WTableReferenceWithAlias FromReference in FromClause.TableReferences)
                {
                    Nodes.GetOrCreate(FromReference.Alias.Value);
                    if (!Parent.ContainsKey(FromReference.Alias.Value))
                        Parent[FromReference.Alias.Value] = FromReference.Alias.Value;
                }
            }

            // Consturct nodes and edges of a match graph defined by the SelectQueryBlock
            if (MatchClause != null)
            {
                if (MatchClause.Paths.Count > 0)
                {
                    foreach (var path in MatchClause.Paths)
                    {
                        var index = 0;
                        // Consturct the source node of a path in MatchClause.Paths
                        MatchEdge EdgeToSrcNode = null;
                        for (var count = path.PathEdgeList.Count; index < count; ++index)
                        {
                            var CurrentNodeTableRef = path.PathEdgeList[index].Item1;
                            var CurrentEdgeColumnRef = path.PathEdgeList[index].Item2;
                            var CurrentNodeExposedName = CurrentNodeTableRef.BaseIdentifier.Value;
                            var nextNodeTableRef = index != count - 1
                                ? path.PathEdgeList[index + 1].Item1
                                : path.Tail;
                            var nextNodeExposedName = nextNodeTableRef.BaseIdentifier.Value;
                            var SrcNode = Nodes.GetOrCreate(CurrentNodeExposedName);
                            if (SrcNode.NodeAlias == null)
                            {
                                SrcNode.NodeAlias = CurrentNodeExposedName;
                                SrcNode.Neighbors = new List<MatchEdge>();
                                SrcNode.ReverseNeighbors = new List<MatchEdge>();
                                SrcNode.External = false;
                                SrcNode.Predicates = new List<WBooleanExpression>();
                            }

                            // Consturct the edge of a path in MatchClause.Paths
                            string EdgeAlias = CurrentEdgeColumnRef.Alias;
                            if (EdgeAlias == null)
                            {
                                bool isReversed = path.IsReversed;
                                var CurrentEdgeName = CurrentEdgeColumnRef.MultiPartIdentifier.Identifiers.Last().Value;
                                string originalEdgeName = null;

                                EdgeAlias = string.Format("{0}_{1}_{2}", CurrentNodeExposedName, CurrentEdgeName,
                                    nextNodeExposedName);

                                // when current edge is a reversed edge, the key should still be the original edge name
                                var edgeNameKey = isReversed ? originalEdgeName : CurrentEdgeName;
                                if (EdgeColumnToAliasesDict.ContainsKey(edgeNameKey))
                                {
                                    EdgeColumnToAliasesDict[edgeNameKey].Add(EdgeAlias);
                                }
                                else
                                {
                                    EdgeColumnToAliasesDict.Add(edgeNameKey, new List<string> { EdgeAlias });
                                }
                            }

                            MatchEdge EdgeFromSrcNode;
                            if (CurrentEdgeColumnRef.MinLength == 1 && CurrentEdgeColumnRef.MaxLength == 1)
                            {
                                EdgeFromSrcNode = new MatchEdge
                                {
                                    SourceNode = SrcNode,
                                    EdgeColumn = CurrentEdgeColumnRef,
                                    EdgeAlias = EdgeAlias,
                                    Predicates = new List<WBooleanExpression>(),
                                    BindNodeTableObjName =
                                        new WSchemaObjectName(
                                            ),
                                };
                            }
                            else
                            {
                                MatchPath matchPath = new MatchPath
                                {
                                    SourceNode = SrcNode,
                                    EdgeColumn = CurrentEdgeColumnRef,
                                    EdgeAlias = EdgeAlias,
                                    Predicates = new List<WBooleanExpression>(),
                                    BindNodeTableObjName =
                                        new WSchemaObjectName(
                                            ),
                                    MinLength = CurrentEdgeColumnRef.MinLength,
                                    MaxLength = CurrentEdgeColumnRef.MaxLength,
                                    ReferencePathInfo = false,
                                    AttributeValueDict = CurrentEdgeColumnRef.AttributeValueDict
                                };
                                pathDictionary[EdgeAlias] = matchPath;
                                EdgeFromSrcNode = matchPath;
                            }

                            if (EdgeToSrcNode != null)
                            {
                                EdgeToSrcNode.SinkNode = SrcNode;
                                //Add ReverseEdge
                                MatchEdge reverseEdge;
                                reverseEdge = new MatchEdge
                                {
                                    SourceNode = EdgeToSrcNode.SinkNode,
                                    SinkNode = EdgeToSrcNode.SourceNode,
                                    EdgeColumn = EdgeToSrcNode.EdgeColumn,
                                    EdgeAlias = EdgeToSrcNode.EdgeAlias,
                                    Predicates = EdgeToSrcNode.Predicates,
                                    BindNodeTableObjName =
                                       new WSchemaObjectName(
                                           ),
                                };
                                SrcNode.ReverseNeighbors.Add(reverseEdge);
                            }

                            EdgeToSrcNode = EdgeFromSrcNode;

                            if (!Parent.ContainsKey(CurrentNodeExposedName))
                                Parent[CurrentNodeExposedName] = CurrentNodeExposedName;
                            if (!Parent.ContainsKey(nextNodeExposedName))
                                Parent[nextNodeExposedName] = nextNodeExposedName;

                            UnionFind.Union(CurrentNodeExposedName, nextNodeExposedName);

                            SrcNode.Neighbors.Add(EdgeFromSrcNode);


                        }
                        // Consturct destination node of a path in MatchClause.Paths
                        var tailExposedName = path.Tail.BaseIdentifier.Value;
                        var DestNode = Nodes.GetOrCreate(tailExposedName);
                        if (DestNode.NodeAlias == null)
                        {
                            DestNode.NodeAlias = tailExposedName;
                            DestNode.Neighbors = new List<MatchEdge>();
                            DestNode.ReverseNeighbors = new List<MatchEdge>();
                            DestNode.Predicates = new List<WBooleanExpression>();
                        }
                        if (EdgeToSrcNode != null)
                        {
                            EdgeToSrcNode.SinkNode = DestNode;
                            //Add ReverseEdge
                            MatchEdge reverseEdge;
                            reverseEdge = new MatchEdge
                            {
                                SourceNode = EdgeToSrcNode.SinkNode,
                                SinkNode = EdgeToSrcNode.SourceNode,
                                EdgeColumn = EdgeToSrcNode.EdgeColumn,
                                EdgeAlias = EdgeToSrcNode.EdgeAlias,
                                Predicates = EdgeToSrcNode.Predicates,
                                BindNodeTableObjName =
                                   new WSchemaObjectName(
                                       ),
                            };
                            DestNode.ReverseNeighbors.Add(reverseEdge);
                        }
                    }

                }
            }
            // Use union find algorithmn to define which subgraph does a node belong to and put it into where it belongs to.
            foreach (var node in Nodes)
            {
                string root;

                root = UnionFind.Find(node.Key);  // put them into the same graph

                var patternNode = node.Value;

                if (patternNode.NodeAlias == null)
                {
                    patternNode.NodeAlias = node.Key;
                    patternNode.Neighbors = new List<MatchEdge>();
                    patternNode.ReverseNeighbors = new List<MatchEdge>();
                    patternNode.External = false;
                    patternNode.Predicates = new List<WBooleanExpression>();
                }

                if (!SubGrpahMap.ContainsKey(root))
                {
                    var subGraph = new ConnectedComponent();
                    subGraph.Nodes[node.Key] = node.Value;
                    foreach (var edge in node.Value.Neighbors)
                    {
                        subGraph.Edges[edge.EdgeAlias] = edge;
                    }
                    SubGrpahMap[root] = subGraph;
                    ConnectedSubGraphs.Add(subGraph);
                    subGraph.IsTailNode[node.Value] = false;
                }
                else
                {
                    var subGraph = SubGrpahMap[root];
                    subGraph.Nodes[node.Key] = node.Value;
                    foreach (var edge in node.Value.Neighbors)
                    {
                        subGraph.Edges[edge.EdgeAlias] = edge;
                    }
                    subGraph.IsTailNode[node.Value] = false;
                }
            }

            // Combine all subgraphs into a complete match graph and return it
            MatchGraph Graph = new MatchGraph
            {
                ConnectedSubGraphs = ConnectedSubGraphs,
            };

            return Graph;
        }

        private List<BooleanFunction> AttachScriptSegment(MatchGraph graph, List<string> header)
        {

            // Call attach predicate visitor to attach predicates on nodes.
            AttachWhereClauseVisitor AttachPredicateVistor = new AttachWhereClauseVisitor();
            WSqlTableContext Context = new WSqlTableContext();
            GraphMetaData GraphMeta = new GraphMetaData();
            Dictionary<string, string> ColumnTableMapping = Context.GetColumnToAliasMapping(GraphMeta.ColumnsOfNodeTables);
            AttachPredicateVistor.Invoke(WhereClause, graph, ColumnTableMapping);
            List<BooleanFunction> BooleanList = new List<BooleanFunction>();

            // If some predictaes are failed to be assigned to one node, turn them into boolean functions
            foreach (var predicate in AttachPredicateVistor.FailedToAssign)
            {
                // Analyse what kind of predicates they are, and generate corresponding boolean functions.
                if (predicate is WBooleanComparisonExpression)
                {
                    string FirstExpr = (predicate as WBooleanComparisonExpression).FirstExpr.ToString();
                    string SecondExpr = (predicate as WBooleanComparisonExpression).SecondExpr.ToString();

                    if (header.IndexOf(FirstExpr) == -1) header.Add(FirstExpr);
                    if (header.IndexOf(FirstExpr) == -1) header.Add(FirstExpr);
                    int lhs = header.IndexOf(FirstExpr);
                    int rhs = header.IndexOf(SecondExpr);
                    FieldComparisonFunction NewCBF = null;
                    if ((predicate as WBooleanComparisonExpression).ComparisonType == BooleanComparisonType.Equals)
                        NewCBF = new FieldComparisonFunction(lhs, rhs,
                            ComparisonBooleanFunction.ComparisonType.eq);
                    if ((predicate as WBooleanComparisonExpression).ComparisonType ==
                        BooleanComparisonType.NotEqualToExclamation)
                        NewCBF = new FieldComparisonFunction(lhs, rhs,
                            ComparisonBooleanFunction.ComparisonType.neq);
                    if ((predicate as WBooleanComparisonExpression).ComparisonType ==
    BooleanComparisonType.LessThan)
                        NewCBF = new FieldComparisonFunction(lhs, rhs,
                            ComparisonBooleanFunction.ComparisonType.lt);
                    if ((predicate as WBooleanComparisonExpression).ComparisonType ==
    BooleanComparisonType.GreaterThan)
                        NewCBF = new FieldComparisonFunction(lhs, rhs,
                            ComparisonBooleanFunction.ComparisonType.gt);
                    if ((predicate as WBooleanComparisonExpression).ComparisonType ==
    BooleanComparisonType.GreaterThanOrEqualTo)
                        NewCBF = new FieldComparisonFunction(lhs, rhs,
                            ComparisonBooleanFunction.ComparisonType.gte);
                    if ((predicate as WBooleanComparisonExpression).ComparisonType ==
    BooleanComparisonType.LessThanOrEqualTo)
                        NewCBF = new FieldComparisonFunction(lhs, rhs,
                            ComparisonBooleanFunction.ComparisonType.lte);
                    BooleanList.Add(NewCBF);
                }
            }
            // Calculate how much nodes the whole match graph has.
            int StartOfResult = 0;
            foreach (var subgraph in graph.ConnectedSubGraphs)
                StartOfResult += subgraph.Nodes.Count() * 3;
            foreach (var subgraph in graph.ConnectedSubGraphs)
            {
                // Use Topological Sort to give a sorted node list.
                // Note that if there's a cycle in the match graph, a random node will be chose as the start.
                var SortedNodeList = TopoSorting.TopoSort(subgraph.Nodes);
                // Marking down which node has been processed for later reverse checking.  
                List<string> ProcessedNodeList = new List<string>();
                // Build query segment on both source node and dest node, 
                while (SortedNodeList.Count != 0)
                {
                    MatchNode CurrentProcessingNode = null;
                    var TargetNode = SortedNodeList.Pop();
                    if (!ProcessedNodeList.Contains(TargetNode.Item1.NodeAlias))
                    {
                        CurrentProcessingNode = TargetNode.Item1;
                        BuildQuerySegementOnNode(ProcessedNodeList, CurrentProcessingNode, header, StartOfResult);
                        ProcessedNodeList.Add(CurrentProcessingNode.NodeAlias);
                    }
                    if (TargetNode.Item2 != null)
                    {
                        CurrentProcessingNode = TargetNode.Item2.SinkNode;
                        BuildQuerySegementOnNode(ProcessedNodeList, CurrentProcessingNode, header, StartOfResult);
                        ProcessedNodeList.Add(CurrentProcessingNode.NodeAlias);
                    }
                }
            }
            return BooleanList;
        }
        private List<string> ConstructHeader(MatchGraph graph)
        {
            List<string> header = new List<string>();
            // Construct the first part of the head which is defined as 
            // |Node's Alias|Node's Adjacent list|Node's reverse Adjacent list|Node's Alias|Node's Adjacent list|Node's reverse Adjacent list|...
            // |   "NODE1"  |   "NODE1_ADJ"      |   "NODE1_REVADJ"           |  "NODE2"   |   "NODE2_ADJ"      |   "NODE2_REVADJ"           |...
            foreach (var subgraph in graph.ConnectedSubGraphs)
            {
                foreach (var node in subgraph.Nodes)
                {
                    header.Add(node.Key);
                    header.Add(node.Key + "_ADJ");
                    header.Add(node.Key + "_REVADJ");
                }
            }
            // Construct the second part of the head which is defined as 
            // ...|Select element|Select element|Select element|...
            // ...|  "ELEMENT1"  |  "ELEMENT2"  |  "ELEMENT3"  |...
            foreach (var element in SelectElements)
            {
                if (element is WSelectScalarExpression)
                {
                    if ((element as WSelectScalarExpression).SelectExpr is WValueExpression) continue;
                    var expr = (element as WSelectScalarExpression).SelectExpr as WColumnReferenceExpression;
                    header.Add(expr.MultiPartIdentifier.ToString());
                }
            }
            // Construct a slot for path 
            // ...|   PATH  |...
            // ...|xxx-->yyy|...
            header.Add("PATH");
            return header;
        }
        private GraphViewExecutionOperator ConstructOperator(MatchGraph graph, List<string> header, GraphViewConnection pConnection, List<BooleanFunction> functions)
        {

            List<GraphViewExecutionOperator> ChildrenProcessor = new List<GraphViewExecutionOperator>();
            List<GraphViewExecutionOperator> RootProcessor = new List<GraphViewExecutionOperator>();
            // Init function validality cheking list. 
            // Whenever all the operands of a boolean check function appeared, attach the function to the operator.
            List<int> FunctionVaildalityCheck = new List<int>();
            foreach (var i in functions)
            {
                FunctionVaildalityCheck.Add(0);
            }
            int StartOfResult = 0;
            // Generate operator for each subgraph.
            foreach (var subgraph in graph.ConnectedSubGraphs)
            {
                // Use Topological Sorting to define the order of nodes it will travel.
                var SortedNodes = TopoSorting.TopoSort(subgraph.Nodes);
                StartOfResult += subgraph.Nodes.Count * 3;
                HashSet<MatchNode> ProcessedNode = new HashSet<MatchNode>();
                while (SortedNodes.Count != 0)
                {
                    MatchNode TempNode = null;
                    var CurrentProcessingNode = SortedNodes.Pop();
                    // If a node is a source node and never appeared before, it will be consturcted to a fetchnode operator
                    // Otherwise it will be consturcted to a TraversalOperator.
                    if (!ProcessedNode.Contains(CurrentProcessingNode.Item1))
                    {
                        int node = header.IndexOf(CurrentProcessingNode.Item1.NodeAlias);
                        TempNode = CurrentProcessingNode.Item1;
                        if (ChildrenProcessor.Count == 0)
                            ChildrenProcessor.Add(new FetchNodeOperator(pConnection, CurrentProcessingNode.Item1.AttachedQuerySegment, node, header, StartOfResult, 50));
                        else
                            ChildrenProcessor.Add(new FetchNodeOperator(pConnection, CurrentProcessingNode.Item1.AttachedQuerySegment, node, header, StartOfResult, 50, ChildrenProcessor.Last()));
                        ProcessedNode.Add(CurrentProcessingNode.Item1);
                        if (functions != null && functions.Count != 0)
                            CheckFunctionValidate(ref header, ref functions, ref TempNode, ref FunctionVaildalityCheck, ref ChildrenProcessor);
                    }
                    if (CurrentProcessingNode.Item2 != null)
                    {
                        TempNode = CurrentProcessingNode.Item2.SinkNode;
                        ProcessedNode.Add(TempNode);

                        int src = header.IndexOf(CurrentProcessingNode.Item2.SourceNode.NodeAlias);
                        int dest = header.IndexOf(CurrentProcessingNode.Item2.SinkNode.NodeAlias);

                        List<Tuple<int, string, bool>> ReverseCheckList = new List<Tuple<int, string, bool>>();
                        if (WithPathClause != null)
                        {
                            // if WithPathClause != null, internal operator should be consturcted for the traversal operator that deals with path.
                            Tuple<string, GraphViewExecutionOperator, int> InternalOperator = null;
                            if (
                                (InternalOperator =
                                    WithPathClause.PathOperators.Find(
                                        p => p.Item1 == CurrentProcessingNode.Item2.EdgeAlias)) !=
                                null)
                                ReverseCheckList = ConsturctReverseCheckList(TempNode, ref ProcessedNode, header);
                            ChildrenProcessor.Add(new TraversalOperator(pConnection, ChildrenProcessor.Last(),
                            TempNode.AttachedQuerySegment, src, dest, header, ReverseCheckList, StartOfResult, 50,
                            50, false, InternalOperator.Item2));
                        }
                        else
                        {
                            ReverseCheckList = ConsturctReverseCheckList(TempNode, ref ProcessedNode, header);
                            ChildrenProcessor.Add(new TraversalOperator(pConnection, ChildrenProcessor.Last(),
                                TempNode.AttachedQuerySegment, src, dest, header, ReverseCheckList, StartOfResult, 50,
                                50, CurrentProcessingNode.Item2.IsReversed));
                        }
                        // Check if any boolean function should be attached to this operator.
                        if (functions != null && functions.Count != 0)
                            CheckFunctionValidate(ref header, ref functions, ref TempNode, ref FunctionVaildalityCheck, ref ChildrenProcessor);
                    }

                }
                // The last processor of a sub graph will be added to root processor list for later use.
                RootProcessor.Add(ChildrenProcessor.Last());

                for (int i = 0; i < FunctionVaildalityCheck.Count; i++)
                    if (FunctionVaildalityCheck[i] == 1) FunctionVaildalityCheck[i] = 0;
            }
            GraphViewExecutionOperator root = null;
            if (RootProcessor.Count == 1) root = RootProcessor[0];
            // A cartesian product will be made among all the result from the root processor in order to produce a complete result
            else
            {
                root = new CartesianProductOperator(RootProcessor, header, 100);
                // If some boolean function cannot be attached in any single subgraph, it should either be attached to cartesian product operator.
                // or it cannot be attached anywhere.
                for (int i = 0; i < FunctionVaildalityCheck.Count; i++)
                {
                    if (FunctionVaildalityCheck[i] < 2)
                    {
                        if ((root as CartesianProductOperator).BooleanCheck == null)
                            (root as CartesianProductOperator).BooleanCheck = functions[i];
                        else (root as CartesianProductOperator).BooleanCheck = new BinaryFunction((root as CartesianProductOperator).BooleanCheck,
                                        functions[i], BinaryBooleanFunction.BinaryType.and);
                    }
                }
            }
            if (OrderByClause != null && OrderByClause.OrderByElements != null)
            {
                if (OrderByClause.OrderByElements[0].SortOrder == SortOrder.Ascending)
                    root = new OrderbyOperator(root, OrderByClause.OrderByElements[0].ToString(), root.header, OrderbyOperator.Order.Incr);
                if (OrderByClause.OrderByElements[0].SortOrder == SortOrder.Descending)
                    root = new OrderbyOperator(root, OrderByClause.OrderByElements[0].ToString(), root.header, OrderbyOperator.Order.Decr);
                if (OrderByClause.OrderByElements[0].SortOrder == SortOrder.NotSpecified)
                    root = new OrderbyOperator(root, OrderByClause.OrderByElements[0].ToString(), root.header, OrderbyOperator.Order.NotSpecified);
            }
            List<string> SelectedElement = new List<string>();
            foreach (var x in SelectElements)
            {
                if ((x as WSelectScalarExpression).SelectExpr is WColumnReferenceExpression)
                    SelectedElement.Add(x.ToString());
            }
            if (!OutputPath)
                root = new OutputOperator(root,SelectedElement, root.header);
            else
                root = new OutputOperator(root, true, header);
            return root;
        }

        private void BuildQuerySegementOnNode(List<string> ProcessedNodeList, MatchNode node, List<string> header, int pStartOfResultField)
        {
            // Node predicates will be attached here.
            string FromClauseString = node.NodeAlias;
            string WhereClauseString = "";
            //string AttachedClause = "From " + node.NodeAlias;
            string PredicatesOnReverseEdge = "";
            string PredicatesOnNodes = "";

            foreach (var edge in node.ReverseNeighbors.Concat(node.Neighbors))
            {
                // Join with all the edges it need to use later.
                if (node.ReverseNeighbors.Contains(edge))
                    FromClauseString += " Join " + edge.EdgeAlias + " in " + node.NodeAlias + "._reverse_edge ";
                else
                    FromClauseString += " Join " + edge.EdgeAlias + " in " + node.NodeAlias + "._edge ";
                // Add all the predicates on edges to the where clause.
                if (edge != node.ReverseNeighbors.Concat(node.Neighbors).Last())
                    foreach (var predicate in edge.Predicates)
                    {
                        PredicatesOnReverseEdge += predicate + " AND ";
                    }
                else
                    foreach (var predicate in edge.Predicates)
                    {
                        if (predicate != edge.Predicates.Last())
                            PredicatesOnReverseEdge += predicate + " AND ";
                        else PredicatesOnReverseEdge += predicate;
                    }
            }

            FromClauseString = " FROM " + FromClauseString;

            // Add all the predicates on nodes to the where clause.
            foreach (var predicate in node.Predicates)
            {
                if (predicate != node.Predicates.Last())
                    PredicatesOnNodes += predicate + " AND ";
                else
                    PredicatesOnNodes += predicate;
            }
            if (PredicatesOnNodes != "" || PredicatesOnReverseEdge != "")
            {
                WhereClauseString += " WHERE ";
                if (PredicatesOnNodes != "" && PredicatesOnReverseEdge != "")
                    WhereClauseString += PredicatesOnNodes + " AND " + PredicatesOnReverseEdge;
                else WhereClauseString += PredicatesOnNodes + PredicatesOnReverseEdge;
            }

            // Select elements that related to current node will be attached here.
            List<string> ResultIndexToAppend = new List<string>();
            foreach (string ResultIndex in header.GetRange(pStartOfResultField, header.Count - pStartOfResultField))
            {
                int CutPoint = ResultIndex.Length;
                if (ResultIndex.IndexOf('.') != -1) CutPoint = ResultIndex.IndexOf('.');
                if (ResultIndex.Substring(0, CutPoint) == node.NodeAlias)
                    ResultIndexToAppend.Add(ResultIndex);
                foreach (var edge in node.ReverseNeighbors)
                {
                    if (ResultIndex.Substring(0, CutPoint) == edge.EdgeAlias)
                        ResultIndexToAppend.Add(ResultIndex);
                }
            }

            string ResultIndexString = ",";
            foreach (string ResultIndex in ResultIndexToAppend)
            {
                if (ResultIndex.Length > 3 && ResultIndex.Substring(ResultIndex.Length - 3, 3) == "doc")
                    ResultIndexString += ResultIndex.Substring(0, ResultIndex.Length - 4) + " AS " + ResultIndex.Replace(".", "_") + ",";
                else ResultIndexString += ResultIndex + " AS " + ResultIndex.Replace(".", "_") + ",";
            }
            if (ResultIndexString == ",") ResultIndexString = "";
            ResultIndexString = CutTheTail(ResultIndexString);

            // Reverse checking related script will be attached here.
            string ReverseCheckString = ",";
            foreach (var ReverseEdge in node.ReverseNeighbors.Concat(node.Neighbors))
            {
                //if (ProcessedNodeList.Contains(ReverseEdge.SinkNode.NodeAlias))
                ReverseCheckString += ReverseEdge.EdgeAlias + " AS " + ReverseEdge.EdgeAlias + "_REV,";
            }
            if (ReverseCheckString == ",") ReverseCheckString = "";
            ReverseCheckString = CutTheTail(ReverseCheckString);

            // The DocDb script that related to the giving node will be assembled here.
            string ScriptBase = "SELECT {\"id\":node.id, \"edge\":node._edge, \"reverse\":node._reverse_edge} AS NodeInfo";
            string QuerySegment = QuerySegment = ScriptBase.Replace("node", node.NodeAlias) + ResultIndexString + " " + ReverseCheckString;
            QuerySegment += FromClauseString + WhereClauseString;
            node.AttachedQuerySegment = QuerySegment;
        }

        // Check if any operand of the boolean functions appeared in the operator, increase the corresponding mark if so.
        // Whenever all the operands of a boolean check function appeared, attach the function to the operator.
        private void CheckFunctionValidate(ref List<string> header, ref List<BooleanFunction> functions, ref MatchNode TempNode, ref List<int> FunctionVaildalityCheck, ref List<GraphViewExecutionOperator> ChildrenProcessor)
        {
            for (int i = 0; i < functions.Count; i++)
            {
                if (functions[i] is FieldComparisonFunction)
                {
                    string lhs = header[(functions[i] as FieldComparisonFunction).LhsFieldIndex];
                    string rhs = header[(functions[i] as FieldComparisonFunction).RhsFieldIndex];
                    if (TempNode.AttachedQuerySegment.Contains(lhs))
                        FunctionVaildalityCheck[i]++;
                    if (TempNode.AttachedQuerySegment.Contains(rhs))
                        FunctionVaildalityCheck[i]++;
                    if (FunctionVaildalityCheck[i] == 2)
                    {
                        if (ChildrenProcessor.Last()!= null && ChildrenProcessor.Last() is GraphViewTraversalBaseOperator)
                        {
                            if ((ChildrenProcessor.Last() as GraphViewTraversalBaseOperator).BooleanCheck == null)
                                (ChildrenProcessor.Last() as GraphViewTraversalBaseOperator).BooleanCheck = functions[i];
                            else
                                (ChildrenProcessor.Last() as GraphViewTraversalBaseOperator).BooleanCheck =
                                    new BinaryFunction((ChildrenProcessor.Last() as GraphViewTraversalBaseOperator).BooleanCheck,
                                        functions[i], BinaryBooleanFunction.BinaryType.and);
                        }
                    }
                }
            }
        }

        private List<Tuple<int, string, bool>> ConsturctReverseCheckList(MatchNode TempNode, ref HashSet<MatchNode> ProcessedNode, List<string> header)
        {
            List<Tuple<int, string, bool>> ReverseCheckList = new List<Tuple<int, string, bool>>();
            foreach (var neighbor in TempNode.ReverseNeighbors)
                if (ProcessedNode.Contains(neighbor.SourceNode))
                    ReverseCheckList.Add(new Tuple<int, string, bool>(header.IndexOf(neighbor.SinkNode.NodeAlias),
                        neighbor.EdgeAlias + "_REV", true));
            foreach (var neighbor in TempNode.Neighbors)
                if (ProcessedNode.Contains(neighbor.SinkNode))
                    ReverseCheckList.Add(new Tuple<int, string, bool>(header.IndexOf(neighbor.SinkNode.NodeAlias),
                        neighbor.EdgeAlias + "_REV", false));
            return ReverseCheckList;
        }
        // Cut the last character of a string.
        private string CutTheTail(string InRangeScript)
        {
            if (InRangeScript.Length == 0) return "";
            return InRangeScript.Substring(0, InRangeScript.Length - 1);
        }
        // The implementation of Union find algorithmn.
        private class UnionFind
        {
            public Dictionary<string, string> Parent;

            public string Find(string x)
            {
                string k, j, r;
                r = x;
                while (Parent[r] != r)
                {
                    r = Parent[r];
                }
                k = x;
                while (k != r)
                {
                    j = Parent[k];
                    Parent[k] = r;
                    k = j;
                }
                return r;
            }

            public void Union(string a, string b)
            {
                string aRoot = Find(a);
                string bRoot = Find(b);
                if (aRoot == bRoot)
                    return;
                Parent[aRoot] = bRoot;
            }
        }

        // The implementation of topological sorting using DFS
        // Note that if is there's a cycle, a random node in the cycle will be pick as the start.
        private class TopoSorting
        {
            static internal Stack<Tuple<MatchNode, MatchEdge>> TopoSort(Dictionary<string, MatchNode> graph)
            {
                Dictionary<MatchNode, int> state = new Dictionary<MatchNode, int>();
                Stack<Tuple<MatchNode, MatchEdge>> list = new Stack<Tuple<MatchNode, MatchEdge>>();
                foreach (var node in graph)
                    state.Add(node.Value, 0);
                foreach (var node in graph)
                    if (state[node.Value] == 0)
                        visit(graph, node.Value, list, state, node.Value.NodeAlias, null);
                if (graph.Count == 1) list.Push(new Tuple<MatchNode, MatchEdge>(graph.First().Value, null));
                return list;
            }
            static private void visit(Dictionary<string, MatchNode> graph, MatchNode node, Stack<Tuple<MatchNode, MatchEdge>> list, Dictionary<MatchNode, int> state, string ParentAlias, MatchEdge Edge)
            {
                state[node] = 2;
                foreach (var neighbour in node.Neighbors)
                {
                    if (state[neighbour.SinkNode] == 0)
                        visit(graph, neighbour.SinkNode, list, state, node.NodeAlias, neighbour);
                    if (state[neighbour.SinkNode] == 2)
                        foreach (var neighbour2 in neighbour.SinkNode.ReverseNeighbors)
                        {
                            foreach (var x in neighbour.SinkNode.Neighbors)
                                if (x.SinkNode == node)
                                    list.Push(new Tuple<MatchNode, MatchEdge>(x.SourceNode, x));
                        }

                }
                state[node] = 1;
                foreach (var neighbour in node.ReverseNeighbors)
                {
                    foreach (var x in neighbour.SinkNode.Neighbors)
                        if (x.SinkNode == node)
                            list.Push(new Tuple<MatchNode, MatchEdge>(x.SourceNode, x));
                }
            }
        }
    }

    partial class WWithPathClause
    {
        internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        {
            foreach (var path in Paths)
                PathOperators.Add(new Tuple<string, GraphViewExecutionOperator, int>(path.Item1, path.Item2.Generate(dbConnection), path.Item3));
            if (PathOperators.Count != 0) return PathOperators.First().Item2;
            else return null;
        }
    }

    partial class WChoose
    {
        internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        {
            List<GraphViewExecutionOperator> Source = new List<GraphViewExecutionOperator>();
            foreach (var x in InputExpr)
            {
                Source.Add(x.Generate(dbConnection));
            }
            return new UnionOperator(Source);
        }
    }

    partial class WCoalesce
    {
        internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        {
            List<GraphViewExecutionOperator> Source = new List<GraphViewExecutionOperator>();
            foreach (var x in InputExpr)
            {
                Source.Add(x.Generate(dbConnection));
            }
            var op = new CoalesceOperator(Source, CoalesceNumber);
            return new OutputOperator(op, op.header, null);
        }
    }

    partial class WInsertNodeSpecification
    {
        /// <summary>
        /// Construct a Json's string which contains all the information about the new node.
        /// And then Create a InsertNodeOperator with this string
        /// </summary>
        /// <param name="docDbConnection">The Connection</param>
        /// <returns></returns>
        internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        {
            string Json_str = ConstructNode();

            InsertNodeOperator InsertOp = new InsertNodeOperator(dbConnection, Json_str);

            return InsertOp;
        }
    }

    partial class WInsertEdgeSpecification
    {
        /// <summary>
        /// Construct an edge's string with all informations.
        /// </summary>
        /// <returns></returns>
        public string ConstructEdge()
        {
            var SelectQueryBlock = SelectInsertSource.Select as WSelectQueryBlock;

            string Edge = "{}";
            Edge = GraphViewJsonCommand.insert_property(Edge, "", "_ID").ToString();
            Edge = GraphViewJsonCommand.insert_property(Edge, "", "_reverse_ID").ToString();
            Edge = GraphViewJsonCommand.insert_property(Edge, "", "_sink").ToString();

            var Columns = this.Columns;
            var Values = new List<WValueExpression>();
            var source = "";
            var sink = "";

            foreach (var SelectElement in SelectQueryBlock.SelectElements)
            {
                var SelectScalar = SelectElement as WSelectScalarExpression;
                if (SelectScalar != null)
                {
                    if (SelectScalar.SelectExpr is WValueExpression)
                    {

                        var ValueExpression = SelectScalar.SelectExpr as WValueExpression;
                        Values.Add(ValueExpression);
                    }
                    else if (SelectScalar.SelectExpr is WColumnReferenceExpression)
                    {
                        var ColumnReferenceExpression = SelectScalar.SelectExpr as WColumnReferenceExpression;
                        if (source == "") source = ColumnReferenceExpression.ToString();
                        else
                        {
                            if (sink == "")
                                sink = ColumnReferenceExpression.ToString();
                        }
                    }
                }
            }
            if (Values.Count() != Columns.Count())
                throw new SyntaxErrorException("Columns and Values not match");

            //Add properties to Edge
            for (var index = 0; index < Columns.Count(); index++)
            {
                Edge = GraphViewJsonCommand.insert_property(Edge, Values[index].ToString(),
                        Columns[index].ToString()).ToString();
            }
            return Edge;
        }


        internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        {
            var SelectQueryBlock = SelectInsertSource.Select as WSelectQueryBlock;

            string Edge = ConstructEdge();

            //Add "id" after each identifier
            var iden = new Identifier();
            iden.Value = "id";
            var dic_iden = new Identifier();
            dic_iden.Value = "doc";

            var n1 = SelectQueryBlock.SelectElements[0] as WSelectScalarExpression;
            var identifiers1 = (n1.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers;
            identifiers1.Add(iden);

            var n2 = SelectQueryBlock.SelectElements[1] as WSelectScalarExpression;
            var identifiers2 = (n2.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers;
            identifiers2.Add(iden);

            var n3 = new WSelectScalarExpression(); SelectQueryBlock.SelectElements.Add(n3);
            var n3_SelectExpr = new WColumnReferenceExpression();
            n3.SelectExpr = n3_SelectExpr;
            n3_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n3_SelectExpr.MultiPartIdentifier.Identifiers.Add((n1.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers[0]);
            n3_SelectExpr.MultiPartIdentifier.Identifiers.Add(dic_iden);

            var n4 = new WSelectScalarExpression(); SelectQueryBlock.SelectElements.Add(n4);
            var n4_SelectExpr = new WColumnReferenceExpression();
            n4.SelectExpr = n4_SelectExpr;
            n4_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n4_SelectExpr.MultiPartIdentifier.Identifiers.Add((n2.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers[0]);
            n4_SelectExpr.MultiPartIdentifier.Identifiers.Add(dic_iden);

            GraphViewExecutionOperator input = SelectQueryBlock.Generate(dbConnection);
            if (input == null)
                throw new GraphViewException("The insert source of the INSERT EDGE statement is invalid.");

            InsertEdgeOperator InsertOp = new InsertEdgeOperator(dbConnection, input, Edge, n1.ToString(), n2.ToString());

            return InsertOp;
        }
    }

    partial class WInsertEdgeFromTwoSourceSpecification
    {
        /// <summary>
        /// Construct an edge's string with all informations.
        /// </summary>
        /// <returns></returns>
        public string ConstructEdge()
        {
            var SelectQueryBlock = SrcInsertSource.Select as WSelectQueryBlock;

            string Edge = "{}";
            Edge = GraphViewJsonCommand.insert_property(Edge, "", "_ID").ToString();
            Edge = GraphViewJsonCommand.insert_property(Edge, "", "_reverse_ID").ToString();
            Edge = GraphViewJsonCommand.insert_property(Edge, "", "_sink").ToString();

            var Columns = this.Columns;
            var Values = new List<WValueExpression>();
            var source = "";
            var sink = "";

            foreach (var SelectElement in SelectQueryBlock.SelectElements)
            {
                var SelectScalar = SelectElement as WSelectScalarExpression;
                if (SelectScalar != null)
                {
                    if (SelectScalar.SelectExpr is WValueExpression)
                    {

                        var ValueExpression = SelectScalar.SelectExpr as WValueExpression;
                        Values.Add(ValueExpression);
                    }
                    else if (SelectScalar.SelectExpr is WColumnReferenceExpression)
                    {
                        var ColumnReferenceExpression = SelectScalar.SelectExpr as WColumnReferenceExpression;
                        if (source == "") source = ColumnReferenceExpression.ToString();
                        else
                        {
                            if (sink == "")
                                sink = ColumnReferenceExpression.ToString();
                        }
                    }
                }
            }
            if (Values.Count() != Columns.Count())
                throw new SyntaxErrorException("Columns and Values not match");

            //Add properties to Edge
            for (var index = 0; index < Columns.Count(); index++)
            {
                Edge = GraphViewJsonCommand.insert_property(Edge, Values[index].ToString(),
                        Columns[index].ToString()).ToString();
            }
            return Edge;
        }


        internal override GraphViewExecutionOperator Generate(GraphViewConnection pConnection)
        {
            WSelectQueryBlock SrcSelect;
            WSelectQueryBlock DestSelect;
            if (dir == GraphTraversal.direction.In)
            {
                SrcSelect = DestInsertSource;
                DestSelect = SrcInsertSource.Select as WSelectQueryBlock;
            }
            else
            {
                SrcSelect = SrcInsertSource.Select as WSelectQueryBlock;
                DestSelect = DestInsertSource;
            }

            string Edge = ConstructEdge();

            //Add "id" after each identifier
            var iden = new Identifier();
            iden.Value = "id";
            var dic_iden = new Identifier();
            dic_iden.Value = "doc";

            var n1 = SrcSelect.SelectElements[0] as WSelectScalarExpression;
            var identifiers1 = (n1.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers;
            identifiers1.Add(iden);

            var n2 = DestSelect.SelectElements[0] as WSelectScalarExpression;
            var identifiers2 = (n2.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers;
            identifiers2.Add(iden);

            var n3 = new WSelectScalarExpression(); SrcSelect.SelectElements.Add(n3);
            var n3_SelectExpr = new WColumnReferenceExpression();
            n3.SelectExpr = n3_SelectExpr;
            n3_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n3_SelectExpr.MultiPartIdentifier.Identifiers.Add((n1.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers[0]);
            n3_SelectExpr.MultiPartIdentifier.Identifiers.Add(dic_iden);

            var n4 = new WSelectScalarExpression(); DestSelect.SelectElements.Add(n4);
            var n4_SelectExpr = new WColumnReferenceExpression();
            n4.SelectExpr = n4_SelectExpr;
            n4_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n4_SelectExpr.MultiPartIdentifier.Identifiers.Add((n2.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers[0]);
            n4_SelectExpr.MultiPartIdentifier.Identifiers.Add(dic_iden);

            GraphViewExecutionOperator SrcInput = SrcSelect.Generate(pConnection);
            GraphViewExecutionOperator DestInput = DestSelect.Generate(pConnection);
            if (SrcInput == null || DestInput == null)
                throw new GraphViewException("The insert source of the INSERT EDGE statement is invalid.");

            InsertEdgeFromTwoSourceOperator InsertOp = new InsertEdgeFromTwoSourceOperator(pConnection, SrcInput, DestInput, Edge, n1.ToString(), n2.ToString());

            return InsertOp;
        }
    }

    partial class WDeleteEdgeSpecification
    {
        internal void ChangeSelectQuery()
        {
            var SelectQueryBlock = SelectDeleteExpr as WSelectQueryBlock;
            var edgealias = SelectDeleteExpr.MatchClause.Paths[0].PathEdgeList[0].Item2.Alias;

            #region Add "id" after identifiers
            //Add "id" after identifiers
            var iden = new Identifier();
            iden.Value = "id";

            var n1 = SelectQueryBlock.SelectElements[0] as WSelectScalarExpression;
            var identifiers1 = (n1.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers;
            identifiers1.Add(iden);

            var n2 = SelectQueryBlock.SelectElements[1] as WSelectScalarExpression;
            var identifiers2 = (n2.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers;
            identifiers2.Add(iden);

            #endregion

            #region Add "edge._ID" & "edge._reverse_ID" in Select
            //Add "edge._ID" & "edge._reverse_ID" in Select
            var edge_name = new Identifier();
            var edge_id = new Identifier();
            var edge_reverse_id = new Identifier();
            edge_name.Value = edgealias;
            edge_id.Value = "_ID";
            edge_reverse_id.Value = "_reverse_ID";

            var n3 = new WSelectScalarExpression(); SelectQueryBlock.SelectElements.Add(n3);
            var n3_SelectExpr = new WColumnReferenceExpression();
            n3.SelectExpr = n3_SelectExpr;
            n3_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n3_SelectExpr.MultiPartIdentifier.Identifiers.Add(edge_name);
            n3_SelectExpr.MultiPartIdentifier.Identifiers.Add(edge_id);

            var n4 = new WSelectScalarExpression(); SelectQueryBlock.SelectElements.Add(n4);
            var n4_SelectExpr = new WColumnReferenceExpression();
            n4.SelectExpr = n4_SelectExpr;
            n4_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n4_SelectExpr.MultiPartIdentifier.Identifiers.Add(edge_name);
            n4_SelectExpr.MultiPartIdentifier.Identifiers.Add(edge_reverse_id);
            #endregion

            #region Add ".doc" in Select
            var dic_iden = new Identifier();
            dic_iden.Value = "doc";
            var n5 = new WSelectScalarExpression(); SelectQueryBlock.SelectElements.Add(n5);
            var n5_SelectExpr = new WColumnReferenceExpression();
            n5.SelectExpr = n5_SelectExpr;
            n5_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n5_SelectExpr.MultiPartIdentifier.Identifiers.Add((n1.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers[0]);
            n5_SelectExpr.MultiPartIdentifier.Identifiers.Add(dic_iden);

            var n6 = new WSelectScalarExpression(); SelectQueryBlock.SelectElements.Add(n6);
            var n6_SelectExpr = new WColumnReferenceExpression();
            n6.SelectExpr = n6_SelectExpr;
            n6_SelectExpr.MultiPartIdentifier = new WMultiPartIdentifier();
            n6_SelectExpr.MultiPartIdentifier.Identifiers.Add((n2.SelectExpr as WColumnReferenceExpression).MultiPartIdentifier.Identifiers[0]);
            n6_SelectExpr.MultiPartIdentifier.Identifiers.Add(dic_iden);
            #endregion
        }
        internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        {
            ChangeSelectQuery();

            var SelectQueryBlock = SelectDeleteExpr as WSelectQueryBlock;

            var n1 = SelectQueryBlock.SelectElements[0] as WSelectScalarExpression;

            var n2 = SelectQueryBlock.SelectElements[1] as WSelectScalarExpression;

            var n3 = SelectQueryBlock.SelectElements[2] as WSelectScalarExpression;

            var n4 = SelectQueryBlock.SelectElements[3] as WSelectScalarExpression;

            GraphViewExecutionOperator input = SelectQueryBlock.Generate(dbConnection);
            if (input == null)
            {
                throw new GraphViewException("The delete source of the DELETE EDGE statement is invalid.");
            }
            DeleteEdgeOperator DeleteOp = new DeleteEdgeOperator(dbConnection, input, n1.ToString(), n2.ToString(), n3.ToString(), n4.ToString());

            return DeleteOp;
        }
    }

    partial class WDeleteNodeSpecification
    {
        /// <summary>
        /// Check if there is eligible nodes with edges.
        /// If there is , stop delete nodes.
        /// Else , create a DeleteNodeOperator.
        /// </summary>
        /// <param name="docDbConnection">The Connection</param>
        /// <returns></returns>
        internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        {
            var search = WhereClause.SearchCondition;
            //build up the query
            string Selectstr = "SELECT * " + "FROM N_0 ";
            if (search == null)
            {
                Selectstr += @"WHERE ARRAY_LENGTH(N_0._edge)>0 or ARRAY_LENGTH(N_0._reverse_edge)>0 ";
            }
            else
            {
                Selectstr += @"WHERE " + search.ToString() +
                             @" and (ARRAY_LENGTH(N_0._edge)>0 or ARRAY_LENGTH(N_0._reverse_edge)>0)  ";
            }

            DeleteNodeOperator Deleteop = new DeleteNodeOperator(dbConnection, search, Selectstr);

            return Deleteop;
        }
    }
}

