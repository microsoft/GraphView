using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using GraphView.GraphViewDBPortal;

namespace GraphView
{
    //internal class OperatorChain
    //{
    //    internal GraphViewCommand Command { get; set; }
    //    internal QueryCompilationContext Context { get; set; }
    //    internal AggregationBlock Block { get; set; }
    //    internal List<Tuple<WBooleanExpression, HashSet<string>>> RemainingPredicatesAccessedTableReferences { get; set; }
    //    internal List<GraphViewExecutionOperator> Chain { get; set; }
    //    internal double Cost { get; set; }
    //    internal bool Finished { get; set; }
    //    internal bool IsFirstNodeInTheComponent { get; set; }
    //    internal List<string> OptimalSolution { get; set; }

    //    public OperatorChain(GraphViewCommand command, QueryCompilationContext context, AggregationBlock block,
    //        List<Tuple<WBooleanExpression, HashSet<string>>> predicatesAccessedTableReferences,
    //        List<GraphViewExecutionOperator> chain, List<string> optimalSolution, double cost,
    //        bool isFirstNodeInTheComponent = false)
    //    {
    //        this.Command = command;
    //        this.Context = context;
    //        this.Block = block;
    //        this.RemainingPredicatesAccessedTableReferences = predicatesAccessedTableReferences;
    //        this.Chain = chain;
    //        this.Cost = cost;
    //        this.Finished = false;
    //        this.IsFirstNodeInTheComponent = isFirstNodeInTheComponent;
    //        this.OptimalSolution = optimalSolution;
    //    }

    //    // If the AggregationBlock has one special table, we must generate referring operator first
    //    internal void Initial()
    //    {
    //        if (this.Block.AggregationAlias != "dummy")
    //        {
    //            this.GenerateExecutionOperator(this.Block.TableDict[this.Block.AggregationAlias], true);
    //        }
    //        else
    //        {
    //            if (this.Context.OuterContextOp != null)
    //            {
    //                this.Context.CurrentExecutionOperator = this.Context.OuterContextOp;
    //                CheckRemainingPredicatesAndAppendFilterOp(this.Command, this.Context,
    //                    this.RemainingPredicatesAccessedTableReferences, this.Chain);
    //            }
    //        }

    //        if (this.Context.TableReferences.Any())
    //        {
    //            RemoveSatisfiedIndependency("dummy", this.Block.TableInputDependency);
    //        }
    //        this.OptimalSolution.Clear();
    //    }

    //    internal OperatorChain GenerateSequentialOrder()
    //    {
    //        foreach (string table in this.Block.TableList)
    //        {
    //            if (this.Block.FreeTableList.Contains(table) && table.StartsWith(GremlinKeyword.EdgeTablePrefix))
    //            {
    //                continue;
    //            }
    //            this.GenerateExecutionOperator(this.Block.TableDict[table], true);
    //        }
    //        return this;
    //    }

    //    internal OperatorChain RestoreExecutionOperators(List<string> optimalSolution)
    //    {
    //        foreach (string table in optimalSolution)
    //        {
    //            if (this.Block.FreeTableList.Contains(table) && table.StartsWith(GremlinKeyword.EdgeTablePrefix))
    //            {
    //                continue;
    //            }
    //            this.GenerateExecutionOperator(this.Block.TableDict[table], true);
    //        }
    //        return this;
    //    }

    //    internal List<OperatorChain> GenerateNextStates()
    //    {
    //        List<OperatorChain> nextStates = new List<OperatorChain>();
    //        bool finished = true;

    //        foreach (string table in this.Block.TableList)
    //        {
    //            finished &= this.Context.TableReferences.Contains(table);

    //            if (this.Block.TableInputDependency[table].Count == 0 &&
    //                !this.Context.TableReferences.Contains(table))
    //            {
    //                if (this.Block.FreeTableList.Contains(table) && table.StartsWith(GremlinKeyword.EdgeTablePrefix))
    //                {
    //                    continue;
    //                }
    //                nextStates.Add(this.GenerateExecutionOperator(this.Block.TableDict[table]));
    //            }
    //        }

    //        if (finished && nextStates.Count == 0)
    //        {
    //            this.Finished = true;
    //            return new List<OperatorChain>() { this };
    //        }
    //        else if (!finished && nextStates.Count == 0)
    //        {
    //            return new List<OperatorChain>();
    //        }

    //        return nextStates;
    //    }

    //    // Generate an execution operator
    //    internal OperatorChain GenerateExecutionOperator(WTableReferenceWithAlias tableReference, bool inplace = false)
    //    {
    //        string alias = tableReference.Alias.Value;

    //        GraphViewExecutionOperator op;
    //        QueryCompilationContext context;
    //        Dictionary<string, HashSet<string>> remainingTableInputDependency;
    //        List<Tuple<WBooleanExpression, HashSet<string>>> remainingPredicatesAccessedTableReferences;
    //        List<GraphViewExecutionOperator> chain;
    //        List<string> optimalSolution;

    //        // if inplace is true, we will generate new solution inplace
    //        if (inplace)
    //        {
    //            context = this.Context;
    //            remainingTableInputDependency = this.Block.TableInputDependency;
    //            remainingPredicatesAccessedTableReferences = this.RemainingPredicatesAccessedTableReferences;
    //            chain = this.Chain;
    //            optimalSolution = this.OptimalSolution;
    //        }
    //        else
    //        {
    //            context = this.Context.Duplicate();
    //            remainingTableInputDependency = this.Block.TableInputDependency.Copy();
    //            remainingPredicatesAccessedTableReferences = this.RemainingPredicatesAccessedTableReferences.Copy();
    //            chain = new List<GraphViewExecutionOperator>(this.Chain);
    //            optimalSolution = new List<string>(this.OptimalSolution);
    //        }

    //        // if the table is free variable
    //        if (tableReference is WNamedTableReference)
    //        {
    //            MatchNode currentNode;
    //            this.Block.GraphPattern.TryGetNode(alias, out currentNode);

    //            bool isDummyNode = false;
    //            List<Tuple<string, string>> edgeVertexBridges = FindEdgeVertexBridges(alias, remainingPredicatesAccessedTableReferences);
    //            List<MatchEdge> existingEdges = new List<MatchEdge>(), remainingEdges = new List<MatchEdge>();
    //            MatchEdge pushedToServerEdge = GetPushedToServerEdge(this.Command, remainingEdges);
    //            WSelectQueryBlock.ConstructJsonQueryOnNode(this.Command, currentNode, pushedToServerEdge, this.Command.Connection.RealPartitionKey);

    //            // collect existing edges and remaining edges
    //            foreach (MatchEdge edge in currentNode.Neighbors)
    //            {
    //                if (context.TableReferences.Contains(edge.LinkAlias))
    //                {
    //                    existingEdges.Add(edge);
    //                }
    //                else
    //                {
    //                    remainingEdges.Add(edge);
    //                }
    //            }
    //            foreach (MatchEdge edge in currentNode.ReverseNeighbors)
    //            {
    //                if (context.TableReferences.Contains(edge.LinkAlias))
    //                {
    //                    existingEdges.Add(edge);
    //                }
    //                else
    //                {
    //                    remainingEdges.Add(edge);
    //                }
    //            }
    //            foreach (MatchEdge edge in currentNode.DanglingEdges)
    //            {
    //                if (context.TableReferences.Contains(edge.LinkAlias))
    //                {
    //                    existingEdges.Add(edge);
    //                }
    //                else
    //                {
    //                    if (this.IsFirstNodeInTheComponent && !existingEdges.Any() && !remainingEdges.Any() && edge.EdgeType == WEdgeType.OutEdge &&
    //                        (currentNode.Predicates == null || !currentNode.Predicates.Any()))
    //                    {
    //                        isDummyNode = true;
    //                        WSelectQueryBlock.ConstructJsonQueryOnEdge(this.Command, currentNode, edge);
    //                    }
    //                    remainingEdges.Add(edge);
    //                }
    //            }

    //            // if there are edge-vertex bridge predicates
    //            if (edgeVertexBridges.Any())
    //            {
    //                op = new TraversalOperator(
    //                    context.CurrentExecutionOperator,
    //                    this.Command,
    //                    context.LocateColumnReference(edgeVertexBridges[0].Item1, GremlinKeyword.Star),
    //                    edgeVertexBridges[0].Item2 == GremlinKeyword.EdgeSourceV
    //                        ? TraversalOperator.TraversalTypeEnum.Source
    //                        : TraversalOperator.TraversalTypeEnum.Sink,
    //                    currentNode.AttachedJsonQuery,
    //                    //currentNode.AttachedJsonQueryOfNodesViaExternalAPI, 
    //                    null);
    //                UpdateNodeLayout(currentNode.NodeAlias, currentNode.Properties, context);
    //                chain.Add(op);
    //                context.TableReferences.Add(alias);
    //                optimalSolution.Add(alias);
    //            }
    //            // FetchNode or FetchEdge
    //            else if (this.IsFirstNodeInTheComponent)
    //            {
    //                op = isDummyNode
    //                    ? (GraphViewExecutionOperator)(new FetchEdgeOperator(this.Command,
    //                        currentNode.DanglingEdges[0].AttachedJsonQuery))
    //                    : new FetchNodeOperator(
    //                        this.Command,
    //                        currentNode.AttachedJsonQuery);
    //                //
    //                // The graph contains more than one component
    //                //
    //                if (chain.Any())
    //                {
    //                    chain.Add(new CartesianProductOperator(chain.Last(), op));
    //                }
    //                //
    //                // This WSelectQueryBlock is a sub query
    //                //
    //                else if (context.OuterContextOp != null)
    //                {
    //                    chain.Add(new CartesianProductOperator(context.OuterContextOp, op));
    //                }
    //                else
    //                {
    //                    chain.Add(op);
    //                }

    //                UpdateNodeLayout(currentNode.NodeAlias, currentNode.Properties, context);
    //                context.TableReferences.Add(alias);
    //                optimalSolution.Add(alias);

    //                if (isDummyNode)
    //                {
    //                    MatchEdge danglingEdge = currentNode.DanglingEdges[0];
    //                    string danglingEdgeAlias = danglingEdge.LinkAlias;

    //                    UpdateEdgeLayout(danglingEdgeAlias, danglingEdge.Properties, context);
    //                    context.TableReferences.Add(danglingEdgeAlias);
    //                    optimalSolution.Add(danglingEdgeAlias);
    //                    if (existingEdges.Contains(danglingEdge))
    //                    {
    //                        existingEdges.Add(danglingEdge);
    //                    }

    //                    if (remainingEdges.Contains(danglingEdge))
    //                    {
    //                        remainingEdges.Remove(danglingEdge);
    //                    }
    //                }
    //            }
    //            else
    //            {
    //                // FetchNode
    //                if (existingEdges.Count == 0)
    //                {
    //                    op = new FetchNodeOperator(this.Command,
    //                        currentNode.AttachedJsonQuery);

    //                    //
    //                    // The graph contains more than one component
    //                    //
    //                    if (chain.Any())
    //                    {
    //                        chain.Add(new CartesianProductOperator(chain.Last(), op));
    //                    }
    //                    //
    //                    // This WSelectQueryBlock is a sub query
    //                    //
    //                    else if (context.OuterContextOp != null)
    //                    {
    //                        chain.Add(new CartesianProductOperator(context.OuterContextOp, op));
    //                    }
    //                    else
    //                    {
    //                        chain.Add(op);
    //                    }
    //                }
    //                // TraversalOperator
    //                else if (existingEdges.Count == 1)
    //                {
    //                    op = new TraversalOperator(
    //                        context.CurrentExecutionOperator,
    //                        this.Command,
    //                        context.LocateColumnReference(existingEdges[0].LinkAlias, GremlinKeyword.Star),
    //                        GetTraversalType(existingEdges[0]),
    //                        currentNode.AttachedJsonQuery,
    //                        //currentNode.AttachedJsonQueryOfNodesViaExternalAPI, 
    //                        null);
    //                    chain.Add(op);
    //                }
    //                // Filter and TraversalOperator
    //                else
    //                {
    //                    List<WColumnReferenceExpression> currentNodeList = new List<WColumnReferenceExpression>();
    //                    List<WBooleanExpression> booleanExpressions = new List<WBooleanExpression>();
    //                    foreach (MatchEdge edge in existingEdges)
    //                    {
    //                        if (edge.EdgeType == WEdgeType.OutEdge)
    //                        {
    //                            currentNodeList.Add(SqlUtil.GetColumnReferenceExpr(edge.LinkAlias,
    //                                edge.IsReversed ? GremlinKeyword.EdgeSinkV : GremlinKeyword.EdgeSourceV));
    //                        }
    //                        else if (edge.EdgeType == WEdgeType.InEdge)
    //                        {
    //                            currentNodeList.Add(SqlUtil.GetColumnReferenceExpr(edge.LinkAlias,
    //                                edge.IsReversed ? GremlinKeyword.EdgeSourceV : GremlinKeyword.EdgeSinkV));
    //                        }
    //                        else
    //                        {
    //                            currentNodeList.Add(SqlUtil.GetColumnReferenceExpr(edge.LinkAlias, GremlinKeyword.EdgeOtherV));
    //                        }
    //                    }

    //                    if (currentNodeList.Any())
    //                    {
    //                        for (int index = 1; index < currentNodeList.Count; ++index)
    //                        {
    //                            booleanExpressions.Add(SqlUtil.GetEqualBooleanComparisonExpr(currentNodeList[0], currentNodeList[index]));
    //                        }
    //                        chain.Add(
    //                            new FilterOperator(
    //                                chain.Any()
    //                                    ? chain.Last()
    //                                    : context.OuterContextOp,
    //                                SqlUtil.ConcatBooleanExprWithAnd(booleanExpressions).CompileToFunction(context, this.Command)));
    //                    }

    //                    op = new TraversalOperator(
    //                        chain.Last(),
    //                        this.Command,
    //                        context.LocateColumnReference(existingEdges[0].LinkAlias, GremlinKeyword.Star),
    //                        GetTraversalType(existingEdges[0]),
    //                        currentNode.AttachedJsonQuery,
    //                        //currentNode.AttachedJsonQueryOfNodesViaExternalAPI, 
    //                        null);
    //                    chain.Add(op);
    //                }
    //                context.TableReferences.Add(alias);
    //                optimalSolution.Add(alias);
    //                UpdateNodeLayout(currentNode.NodeAlias, currentNode.Properties, context);
    //            }

    //            context.CurrentExecutionOperator = chain.Last();
    //            CheckRemainingPredicatesAndAppendFilterOp(this.Command, context, remainingPredicatesAccessedTableReferences, chain);
    //            CrossApplyEdges(this.Command, context, chain, remainingEdges, optimalSolution);
    //        }
    //        // if the table is QueryDerivedTable
    //        else if (tableReference is WQueryDerivedTable)
    //        {
    //            op = tableReference.Compile(context, this.Command);
    //            context.TableReferences.Add(alias);
    //            optimalSolution.Add(alias);
    //            chain.Add(op);
    //        }
    //        // if the table is variable table
    //        else if (tableReference is WVariableTableReference)
    //        {
    //            WVariableTableReference variableTable = tableReference as WVariableTableReference;
    //            string tableName = variableTable.Variable.Name;
    //            Tuple<TemporaryTableHeader, GraphViewExecutionOperator> temporaryTableTuple;
    //            if (!this.Context.TemporaryTableCollection.TryGetValue(tableName, out temporaryTableTuple))
    //            {
    //                throw new GraphViewException("Table variable " + tableName + " doesn't exist in the context.");
    //            }

    //            TemporaryTableHeader tableHeader = temporaryTableTuple.Item1;
    //            if (chain.Any())
    //            {
    //                op = new CartesianProductOperator(chain.Last(), temporaryTableTuple.Item2);
    //            }
    //            else
    //            {
    //                op = temporaryTableTuple.Item2;
    //            }
    //            context.TableReferences.Add(alias);
    //            optimalSolution.Add(alias);
    //            chain.Add(op);

    //            // Merge temporary table's header into current context
    //            foreach (var pair in tableHeader.columnSet.OrderBy(e => e.Value.Item1))
    //            {
    //                string columnName = pair.Key;
    //                ColumnGraphType columnGraphType = pair.Value.Item2;

    //                context.AddField(alias, columnName, columnGraphType);
    //            }
    //            context.CurrentExecutionOperator = chain.Last();
    //        }
    //        // if the table is TVF
    //        else if (tableReference is WSchemaObjectFunctionTableReference)
    //        {
    //            WSchemaObjectFunctionTableReference functionTableReference = tableReference as WSchemaObjectFunctionTableReference;
    //            op = functionTableReference.Compile(context, this.Command);
    //            context.TableReferences.Add(alias);
    //            optimalSolution.Add(alias);
    //            chain.Add(op);
    //            context.CurrentExecutionOperator = chain.Last();
    //        }
    //        else
    //        {
    //            throw new NotImplementedException("Not supported type of FROM clause.");
    //        }

    //        RemoveSatisfiedIndependency(context, remainingTableInputDependency);
    //        CheckRemainingPredicatesAndAppendFilterOp(this.Command, context, remainingPredicatesAccessedTableReferences, chain);

    //        if (inplace)
    //        {
    //            this.IsFirstNodeInTheComponent = false;
    //            this.Cost = UpdateCost(this.Cost, chain.Last(), context.TableReferences.Count,
    //                this.Block.TableList.IndexOf(alias));
    //            return this;
    //        }
    //        else
    //        {
    //            return new OperatorChain(this.Command, context, new AggregationBlock()
    //            {
    //                AggregationAlias = this.Block.AggregationAlias,
    //                FreeTableList = this.Block.FreeTableList,
    //                TableList = this.Block.TableList,
    //                TableDict = this.Block.TableDict,
    //                TableInputDependency = remainingTableInputDependency,
    //                GraphPattern = this.Block.GraphPattern
    //            },
    //                remainingPredicatesAccessedTableReferences, chain, optimalSolution,
    //                UpdateCost(this.Cost, chain.Last(), context.TableReferences.Count,
    //                    this.Block.TableList.IndexOf(alias)));
    //        }
    //    }

    //    // TODO: update the naive version to more accurate one
    //    private static double UpdateCost(double preCost, GraphViewExecutionOperator executionOperator, int weight, int indexOfAlias)
    //    {
    //        // reversed order
    //        return weight / (preCost + indexOfAlias + 2);

    //        // sequential order
    //        // return (preCost + 2 + indexOfAlias) * weight;

    //        // random order
    //        // return new Random().NextDouble();
    //    }

    //    private static void UpdateNodeLayout(string nodeAlias, HashSet<string> properties, QueryCompilationContext context)
    //    {
    //        foreach (string propertyName in properties)
    //        {
    //            ColumnGraphType columnGraphType = GraphViewReservedProperties.IsNodeReservedProperty(propertyName)
    //                ? GraphViewReservedProperties.ReservedNodePropertiesColumnGraphTypes[propertyName]
    //                : ColumnGraphType.Value;
    //            context.AddField(nodeAlias, propertyName, columnGraphType);
    //        }
    //    }

    //    private static void UpdateEdgeLayout(string edgeAlias, List<string> properties, QueryCompilationContext context)
    //    {
    //        context.AddField(edgeAlias, GremlinKeyword.EdgeSourceV, ColumnGraphType.EdgeSource);
    //        context.AddField(edgeAlias, GremlinKeyword.EdgeSinkV, ColumnGraphType.EdgeSink);
    //        context.AddField(edgeAlias, GremlinKeyword.EdgeOtherV, ColumnGraphType.Value);
    //        context.AddField(edgeAlias, GremlinKeyword.EdgeID, ColumnGraphType.EdgeId);
    //        context.AddField(edgeAlias, GremlinKeyword.Star, ColumnGraphType.EdgeObject);
    //        for (var i = GraphViewReservedProperties.ReservedEdgeProperties.Count; i < properties.Count; i++)
    //        {
    //            context.AddField(edgeAlias, properties[i], ColumnGraphType.Value);
    //        }
    //    }

    //    private static void CheckRemainingPredicatesAndAppendFilterOp(GraphViewCommand command,
    //        QueryCompilationContext context,
    //        List<Tuple<WBooleanExpression, HashSet<string>>> remainingPredicatesAccessedTableReferences,
    //        List<GraphViewExecutionOperator> chain)
    //    {
    //        List<int> toBeRemovedIndexes = new List<int>();
    //        List<WBooleanExpression> booleanExpressions = new List<WBooleanExpression>();
    //        for (int index = 0; index < remainingPredicatesAccessedTableReferences.Count; ++index)
    //        {
    //            foreach (string alias in context.TableReferences)
    //            {
    //                remainingPredicatesAccessedTableReferences[index].Item2.Remove(alias);
    //                if (remainingPredicatesAccessedTableReferences[index].Item2.Count == 0)
    //                {
    //                    toBeRemovedIndexes.Add(index);
    //                    booleanExpressions.Add(remainingPredicatesAccessedTableReferences[index].Item1);
    //                    break;
    //                }
    //            }
    //        }

    //        if (booleanExpressions.Any())
    //        {
    //            chain.Add(
    //                new FilterInBatchOperator(
    //                    chain.Count != 0
    //                        ? chain.Last()
    //                        : context.OuterContextOp,
    //                    SqlUtil.ConcatBooleanExprWithAnd(booleanExpressions).CompileToBatchFunction(context, command)));
    //            context.CurrentExecutionOperator = chain.Last();
    //        }

    //        for (int index = toBeRemovedIndexes.Count - 1; index >= 0; --index)
    //        {
    //            remainingPredicatesAccessedTableReferences.RemoveAt(toBeRemovedIndexes[index]);
    //        }
    //    }

    //    private static void RemoveSatisfiedIndependency(QueryCompilationContext context, Dictionary<string, HashSet<string>> remainingTableInputDependency)
    //    {
    //        foreach (var tableInputDependency in remainingTableInputDependency)
    //        {
    //            foreach (string alias in context.TableReferences)
    //            {
    //                tableInputDependency.Value.Remove(alias);
    //            }

    //            // some TVFs can not be the first operator
    //            tableInputDependency.Value.Remove("dummy");
    //        }
    //    }

    //    private static void RemoveSatisfiedIndependency(string alias, Dictionary<string, HashSet<string>> remainingTableInputDependency)
    //    {
    //        foreach (var tableInputDependency in remainingTableInputDependency)
    //        {
    //            tableInputDependency.Value.Remove(alias);
    //        }
    //    }

    //    private static TraversalOperator.TraversalTypeEnum GetTraversalType(MatchEdge edge)
    //    {
    //        if (edge.EdgeType == WEdgeType.BothEdge)
    //        {
    //            return TraversalOperator.TraversalTypeEnum.Other;
    //        }

    //        return WSelectQueryBlock.IsTraversalThroughPhysicalReverseEdge(edge)
    //            ? TraversalOperator.TraversalTypeEnum.Sink
    //            : TraversalOperator.TraversalTypeEnum.Source;
    //    }

    //    private static void CrossApplyEdges(GraphViewCommand command, QueryCompilationContext context,
    //        List<GraphViewExecutionOperator> chain, List<MatchEdge> edges, List<string> optimalSolution)
    //    {
    //        foreach (MatchEdge edge in edges)
    //        {
    //            string edgeAlias = edge.LinkAlias;
    //            Tuple<bool, bool> crossApplyTypeTuple = GetAdjDecoderCrossApplyTypeParameter(edge);
    //            QueryCompilationContext localEdgeContext =
    //                GenerateLocalContextForAdjacentListDecoder(edge.LinkAlias, edge.Properties);
    //            WBooleanExpression edgePredicates = edge.RetrievePredicatesExpression();
    //            chain.Add(new AdjacencyListDecoder(
    //                chain.Last(),
    //                context.LocateColumnReference(edge.SourceNode.NodeAlias, GremlinKeyword.Star),
    //                crossApplyTypeTuple.Item1, crossApplyTypeTuple.Item2, edge.EdgeType == WEdgeType.BothEdge || !edge.IsReversed,
    //                edgePredicates != null ? edgePredicates.CompileToFunction(localEdgeContext, command) : null,
    //                edge.Properties, command, context.RawRecordLayout.Count + edge.Properties.Count));
    //            context.CurrentExecutionOperator = chain.Last();
    //            // Update edge's context info
    //            context.TableReferences.Add(edgeAlias);
    //            optimalSolution.Add(edgeAlias);
    //            UpdateEdgeLayout(edgeAlias, edge.Properties, context);
    //        }
    //    }

    //    private static Tuple<bool, bool> GetAdjDecoderCrossApplyTypeParameter(MatchEdge edge)
    //    {
    //        if (edge.EdgeType == WEdgeType.BothEdge)
    //            return new Tuple<bool, bool>(true, true);

    //        if (WSelectQueryBlock.IsTraversalThroughPhysicalReverseEdge(edge))
    //            return new Tuple<bool, bool>(false, true);
    //        else
    //            return new Tuple<bool, bool>(true, false);
    //    }

    //    private static QueryCompilationContext GenerateLocalContextForAdjacentListDecoder(string edgeTableAlias, List<string> projectedFields)
    //    {
    //        var localContext = new QueryCompilationContext();

    //        var localIndex = 0;
    //        foreach (var projectedField in projectedFields)
    //        {
    //            var columnReference = new WColumnReferenceExpression(edgeTableAlias, projectedField);
    //            localContext.RawRecordLayout.Add(columnReference, localIndex++);
    //        }

    //        return localContext;
    //    }

    //    // If there are some predicates, like E_0._sink = N_1.id, then we can use TraversalOperator to get the Node instead of FetchNodeOperator
    //    private static List<Tuple<string, string>> FindEdgeVertexBridges(string alias,
    //        List<Tuple<WBooleanExpression, HashSet<string>>> remainingPredicatesAccessedTableReferences)
    //    {
    //        List<Tuple<string, string>> edgeTuples = new List<Tuple<string, string>>();
    //        List<int> toBeRemovedIndexes = new List<int>();

    //        for (int index = 0; index < remainingPredicatesAccessedTableReferences.Count; ++index)
    //        {
    //            WEdgeVertexBridgeExpression bridgeExpr =
    //                remainingPredicatesAccessedTableReferences[index].Item1 as WEdgeVertexBridgeExpression;
    //            if (bridgeExpr != null && remainingPredicatesAccessedTableReferences[index].Item2.Contains(alias) &&
    //                remainingPredicatesAccessedTableReferences[index].Item2.Count == 1)
    //            {
    //                WColumnReferenceExpression edgeColumnReferenceExpression =
    //                    bridgeExpr.FirstExpr as WColumnReferenceExpression;

    //                if (edgeColumnReferenceExpression == null)
    //                {
    //                    throw new QueryCompilationException(
    //                        "The first expression of WEdgeVertexBridgeExpression must be WColumnReferenceExpression");
    //                }

    //                toBeRemovedIndexes.Add(index);
    //                edgeTuples.Add(new Tuple<string, string>(edgeColumnReferenceExpression.TableReference, edgeColumnReferenceExpression.ColumnName));
    //            }
    //        }

    //        for (int index = toBeRemovedIndexes.Count - 1; index >= 0; --index)
    //        {
    //            remainingPredicatesAccessedTableReferences.RemoveAt(toBeRemovedIndexes[index]);
    //        }

    //        return edgeTuples;
    //    }

    //    private static MatchEdge GetPushedToServerEdge(GraphViewCommand command, List<MatchEdge> remainingEdges)
    //    {
    //        MatchEdge pushedToServerEdge = remainingEdges.Count == 1 && CanBePushedToServer(command, remainingEdges[0])
    //            ? remainingEdges[0]
    //            : null;
    //        return pushedToServerEdge;
    //    }

    //    private static bool CanBePushedToServer(GraphViewCommand command, MatchEdge matchEdge)
    //    {
    //        // For Compatible & Hybrid, we can't push edge predicates to server side
    //        if (command.Connection.GraphType != GraphType.GraphAPIOnly)
    //        {
    //            Debug.Assert(command.Connection.EdgeSpillThreshold == 1);
    //            return false;
    //        }

    //        if (WSelectQueryBlock.IsTraversalThroughPhysicalReverseEdge(matchEdge) && !command.Connection.UseReverseEdges)
    //        {
    //            return false;
    //        }

    //        return matchEdge != null && matchEdge.EdgeType != WEdgeType.BothEdge;
    //    }
    //}

    //internal class OperationChainComparer : IComparer<OperatorChain>
    //{
    //    public int Compare(OperatorChain chain1, OperatorChain chain2)
    //    {
    //        return chain1.Cost.CompareTo(chain2.Cost);
    //    }
    //}

    internal class BlockOptimizer
    {
        internal AggregationBlock Block { get; set; }

        // Upper Bound of the State number
        internal const int MaxStates = 5;

        public BlockOptimizer(AggregationBlock aggregationBlock)
        {
            this.Block = aggregationBlock;
        }

        internal TraversalOrder GenerateOptimalTraversalOrder(
            Dictionary<string, HashSet<string>> tableInputDependency,
            List<Tuple<WBooleanExpression, HashSet<string>>> predicatesAccessedTableReferences)
        {
            TraversalOrder initialTraversalOrder = new TraversalOrder(tableInputDependency, predicatesAccessedTableReferences);
            initialTraversalOrder.AddTuple(new Tuple<CompileNode, CompileLink, List<CompileLink>>(
                    this.Block.NonMatchTables[this.Block.AggregationAlias], null, new List<CompileLink>()));

            // Every time, we will generate multiple next states from queue[index]. If some of them are finished, we put these into queue[1 - index],
            // and we put another into candidateChains. If the size of candidateChains equals or exceeds the upper bound, we will terminate this 
            // algorithm and return the best one in candidateChains.
            int index = 0, numberOfIterations = this.Block.TableList.Count;
            List<List<TraversalOrder>> queue = new List<List<TraversalOrder>>
            {
                new List<TraversalOrder>(),
                new List<TraversalOrder>()

            };
            List<TraversalOrder> candidateOrders = queue[index];

            queue[index].Add(initialTraversalOrder);

            while (numberOfIterations > 0 && candidateOrders.Count < MaxStates)
            {
                foreach (TraversalOrder currentOrder in queue[index])
                {
                    List<TraversalOrder> nextOrders = currentOrder.GenerateNextOrders(this.Block);
                    nextOrders.Sort(new TraversalOrderComparer());
                    if (nextOrders.Count > MaxStates)
                    {
                        queue[1 - index].AddRange(nextOrders.GetRange(0, MaxStates));
                    }
                    else
                    {
                        queue[1 - index].AddRange(nextOrders);
                    }
                }
                queue[index].Clear();
                if (queue[1 - index].Count > MaxStates)
                {
                    queue[1 - index].Sort(new TraversalOrderComparer());
                    queue[1 - index] = queue[1 - index].GetRange(0, MaxStates);
                }
                index = 1 - index;
                candidateOrders = queue[index];
            }
            candidateOrders.Sort(new TraversalOrderComparer());
            return candidateOrders.First();
        }
    }
}
