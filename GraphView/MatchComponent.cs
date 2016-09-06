// GraphView
// 
// Copyright (c) 2015 Microsoft Corporation
// 
// All rights reserved. 
// 
// MIT License
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
// 
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{

    internal class OneHeightTree
    {
        public MatchNode TreeRoot { get; set; }
        public List<MatchEdge> Edges { get; set; }
    }

    /// <summary>
    /// A 1-height tree is a node with one or more outgoing edges. 
    /// A 1-height tree is translated to a table alias, plus one or more Transpose() functions. 
    /// One Transpose() function populates instances of one edge. 
    /// </summary>
    internal class CandidateJoinUnit
    {
        public MatchNode TreeRoot { get; set; }

        // Incoming edges being transposed before Join
        public List<MatchEdge> PreMatIncomingEdges { get; set; }
        // Outgoing edges being transposed before Join
        public List<MatchEdge> PreMatOutgoingEdges { get; set; }
        // Incoming edges being transposed after Join
        public List<MatchEdge> PostMatIncomingEdges { get; set; }
        // Outgoing edges being transposed after Join
        public List<MatchEdge> PostMatOutgoingEdges { get; set; }

        // TreeRoot's outgoing edges that are materialized by Transpose() functions.
        public List<MatchEdge> MaterializedEdges { get; set; }
        // TreeRoot's edges that are not yet materialized. Lazy materialization may be beneficial for performance, 
        // because it reduces the number of intermediate resutls. 
        public List<MatchEdge> UnmaterializedEdges { get; set; }

        // Join Hint
        public JoinHint JoinHint { get; set; } 

        //private double _edgeDegrees = -1;
        //public double EdgeDegrees
        //{
        //    get
        //    {
        //        if (_edgeDegrees < 0)
        //        {
        //            double matEdgeDegrees = MaterializedEdges.Aggregate(1.0, (cur, next) => cur*next.AverageDegree);
        //            double unMatEdgeDegrees = UnmaterializedEdges.Aggregate(1.0, (cur, next) => cur * next.AverageDegree);
        //            _edgeDegrees = unMatEdgeDegrees*matEdgeDegrees;
        //        }
        //        return _edgeDegrees;
        //    }
        //}

        //private double _sqlEstimatedEdgeDegrees = -1;
        //public double SqlEstimatedEdgeDegrees
        //{
        //    get
        //    {
        //        if (_sqlEstimatedEdgeDegrees < 0)
        //        {
        //            _sqlEstimatedEdgeDegrees = Math.Pow(1000, MaterializedEdges.Count + UnmaterializedEdges.Count);
        //        }
        //        return _sqlEstimatedEdgeDegrees;
        //    }
        //}

        //public WTableReference ToTableReference(string nodeAlias, GraphMetaData metaData)
        //{
        //    // Constructs table reference
        //    WTableReference nodeTable = new WNamedTableReference
        //    {
        //        Alias = new Identifier { Value = nodeAlias },
        //        TableObjectName = TreeRoot.NodeTableObjectName
        //    };
        //    nodeTable = MaterializedEdges.Aggregate(nodeTable, (current, edge) => new WUnqualifiedJoin
        //    {
        //        FirstTableRef = current,
        //        SecondTableRef = edge.ToSchemaObjectFunction(nodeAlias, metaData),
        //        UnqualifiedJoinType = UnqualifiedJoinType.CrossApply,
        //    });
        //    return nodeTable;
        //}

        public WTableReference ToTableReference(string nodeAlias, string dumbNode, GraphMetaData metaData)
        {
            // Constructs table reference
            WTableReference nodeTable = new WNamedTableReference
            {
                Alias = new Identifier { Value = nodeAlias },
                TableObjectName = TreeRoot.NodeTableObjectName
            };
            if (PreMatOutgoingEdges != null && PreMatOutgoingEdges.Any())
            {
                nodeTable = PreMatOutgoingEdges.Aggregate(nodeTable, (current, edge) => new WUnqualifiedJoin
                {
                    FirstTableRef = current,
                    SecondTableRef = edge.ToSchemaObjectFunction(nodeAlias, dumbNode, metaData),
                    UnqualifiedJoinType = UnqualifiedJoinType.CrossApply,
                });
            }
            
            return nodeTable;
        }
    }

    internal enum EdgeDir : byte { In, Out };

    internal enum MaterializedOrder { Pre, Post }

    internal enum DumbType { Node, Edge }

    /// <summary>
    /// The Component in the joining process
    /// </summary>
    internal class MatchComponent
    {
        public HashSet<MatchNode> Nodes { get; set; }
        public Dictionary<MatchEdge, bool> EdgeMaterilizedDict { get; set; }  
        // Stores the split count of a materialized node
        public Dictionary<MatchNode, int> MaterializedNodeSplitCount { get; set; }

        public int ActiveNodeCount
        {
            get { return MaterializedNodeSplitCount.Count; }
        }

        // Maps the unmaterialized node to the alias of one of its incoming materialized edges;
        // the join condition between the node and the incoming edge should be added 
        // when the node is materialized.
        public Dictionary<MatchNode, List<MatchEdge>> UnmaterializedNodeMapping { get; set; }

        // A collection of sink nodes and their statistics.
        // A sink node's statistic will be updated as new candidates are added to the component
        // and new edges point to this sink node. 
        public Dictionary<MatchNode, Statistics> SinkNodeStatisticsDict { get; set; }

        // Numbder of edges cross applied after the last JOIN operator
        public int LastJoinPostMatEdgesCount { get; set; }

        // Estimated number of rows of the input going through the last JOIN operator
        public double LastJoinSqlEstCardinality { get; set; }

        // Last join hint
        public JoinHint LastJoinHint { get; set; }

        // Total memory used by the current execution plan
        public double TotalMemory { get; set; }

        // Total memory of this component, if it is to be joined with the next candidate using 
        // the left-deep hash join or the loop join. 
        public double DeltaMemory { get; set; }

        // The last table in the component
        public MatchNode LastTable { get; set; }

        // The alias of the last table in the component
        public string LastTableAlias { get; set; }

        // The alias of the last post materialized edge
        public string LastPostMatEdgeAlias { get; set; }

        // The alias of the rightest probe table in the join
        public string RightestTableAlias { get; set; }

        // The size of the rightest probe table in the join
        public double RightestTableRefSize { get; set; }

        // Estimated number of rows returned by this component
        public double Cardinality { get; set; }

        // Total memory estimated by the SQL engine
        public double SqlEstimatedTotalMemory { get; set; }
        // Total memory estimated by the SQL engine, if this component is to be joined with the
        // next candidate using the left-deep hash join or the loop join.
        public double SqlEstimatedDeltaMemory { get; set; }

        // Number of rows estimated by the SQL engine 
        public double SqlEstimatedSize { get; set; }
        public double Cost { get; set; }
        public WTableReference TableRef { get; set; }
        public WBooleanExpression WhereCondition { get; set; }

        
        public MatchComponent()
        {
            Nodes = new HashSet<MatchNode>();
            EdgeMaterilizedDict = new Dictionary<MatchEdge, bool>();
            MaterializedNodeSplitCount = new Dictionary<MatchNode, int>();
            UnmaterializedNodeMapping = new Dictionary<MatchNode, List<MatchEdge>>();
            SinkNodeStatisticsDict = new Dictionary<MatchNode, Statistics>();
            LastPostMatEdgeAlias = null;
            LastJoinPostMatEdgesCount = 0;
            LastJoinSqlEstCardinality = 1.0;
            LastJoinHint = JoinHint.None;
            Cardinality = 1.0;
            Cost = 0.0;
            TotalMemory = 0.0;
            DeltaMemory = 0.0;
            SqlEstimatedDeltaMemory = 0.0;
            SqlEstimatedTotalMemory = 0.0;
            SqlEstimatedSize = 1.0;
            RightestTableRefSize = 0.0;
            WhereCondition = null;
        }

        public MatchComponent(MatchNode node):this()
        {
            Nodes.Add(node);
            MaterializedNodeSplitCount[node] = 0;
            //SinkNodeStatisticsDict[node] = new Statistics ();
            Cardinality *= node.EstimatedRows;
            SqlEstimatedSize *= node.EstimatedRows;
            TableRef = new WNamedTableReference
            {
                Alias = new Identifier { Value = node.RefAlias},
                TableObjectName = node.NodeTableObjectName
            };
            LastTableAlias = node.RefAlias;
            LastTable = null;

            foreach (var edge in node.Neighbors)
            {
                var edgeList = UnmaterializedNodeMapping.GetOrCreate(edge.SinkNode);
                edgeList.Add(edge);
            }
        }

        /// <summary>
        /// Deep Copy
        /// </summary>
        /// <param name="component"></param>
        public MatchComponent(MatchComponent component)
        {
            Nodes = new HashSet<MatchNode>(component.Nodes);
            EdgeMaterilizedDict = new Dictionary<MatchEdge, bool>(component.EdgeMaterilizedDict);
            MaterializedNodeSplitCount = new Dictionary<MatchNode, int>(component.MaterializedNodeSplitCount);
            UnmaterializedNodeMapping = new Dictionary<MatchNode, List<MatchEdge>>();
            foreach (var nodeMapping in component.UnmaterializedNodeMapping)
            {
                UnmaterializedNodeMapping[nodeMapping.Key] = new List<MatchEdge>(nodeMapping.Value);
            }
            SinkNodeStatisticsDict = new Dictionary<MatchNode, Statistics>(component.SinkNodeStatisticsDict);
            TableRef = component.TableRef;
            LastPostMatEdgeAlias = component.LastPostMatEdgeAlias;
            LastJoinPostMatEdgesCount = component.LastJoinPostMatEdgesCount;
            LastJoinSqlEstCardinality = component.LastJoinSqlEstCardinality;
            LastJoinHint = component.LastJoinHint;
            LastTable = component.LastTable;
            Cardinality = component.Cardinality;
            Cost = component.Cost;
            DeltaMemory = component.DeltaMemory;
            TotalMemory = component.TotalMemory;
            SqlEstimatedDeltaMemory = component.SqlEstimatedDeltaMemory;
            SqlEstimatedTotalMemory = component.SqlEstimatedTotalMemory;
            SqlEstimatedSize = component.SqlEstimatedSize;
            RightestTableAlias = component.RightestTableAlias;
            RightestTableRefSize = component.RightestTableRefSize;
            LastTableAlias = component.LastTableAlias;
            WhereCondition = ObjectExtensions.Copy(component.WhereCondition);
        }

        //public MatchComponent(MatchNode node, List<MatchEdge> populatedEdges,GraphMetaData metaData) : this(node)
        //{
        //    foreach (var edge in populatedEdges)
        //    {
        //        TableRef = SpanTableRef(TableRef, edge, node.RefAlias, metaData);
        //        EdgeMaterilizedDict[edge] = true;
        //        SinkNodeStatisticsDict[edge.SinkNode] = edge.Statistics;
        //        var edgeList = UnmaterializedNodeMapping.GetOrCreate(edge.SinkNode);
        //        edgeList.Add(edge);
        //        if (!Nodes.Contains(edge.SinkNode))
        //            Nodes.Add(edge.SinkNode);
        //        Cardinality *= edge.AverageDegree;
        //        SqlEstimatedSize *= 1000;

        //    }
        //}

        private static WBooleanExpression ConstructDownSizeJoinCondition(string affectedTableAlias)
        {
            // Add disjunctive DownSize predicates into the current join condition
            var downSizeFunctionCall = new WFunctionCall
            {
                CallTarget = new WMultiPartIdentifierCallTarget
                {
                    Identifiers = new WMultiPartIdentifier(new Identifier { Value = "dbo" })
                },
                FunctionName = new Identifier { Value = "DownSizeFunction" },
                Parameters = new List<WScalarExpression>
                    {
                        new WColumnReferenceExpression
                        {
                            MultiPartIdentifier = new WMultiPartIdentifier
                            {
                                Identifiers = new List<Identifier>
                                {
                                    new Identifier {Value = affectedTableAlias},
                                    new Identifier {Value = "LocalNodeid"}
                                }
                            }
                        }
                    }
            };
            return new WBooleanParenthesisExpression
            {
                Expression = new WBooleanBinaryExpression
                {
                    BooleanExpressionType = BooleanBinaryExpressionType.Or,
                    FirstExpr = new WBooleanComparisonExpression
                    {
                        ComparisonType = BooleanComparisonType.Equals,
                        FirstExpr = downSizeFunctionCall,
                        SecondExpr = new WValueExpression("1", false)
                    },
                    SecondExpr = new WBooleanComparisonExpression
                    {
                        ComparisonType = BooleanComparisonType.Equals,
                        FirstExpr = downSizeFunctionCall,
                        SecondExpr = new WValueExpression("2", false)
                    }
                }
            };
        }

        private static WTableReference ConstructUpSizeTableReference(WTableReference tableRef, double upSizeScalar, out double affectedEstimatedSize)
        {
            affectedEstimatedSize = 1.0;
            int pow = (int)(Math.Floor(Math.Log(upSizeScalar, 1000)) + 1);
            int adjustValue = (int)Math.Pow(upSizeScalar, 1.0 / pow);
            while (pow > 0)
            {
                tableRef = new WUnqualifiedJoin
                {
                    FirstTableRef = tableRef,
                    SecondTableRef = new WSchemaObjectFunctionTableReference
                    {
                        SchemaObject = new WSchemaObjectName(
                            new Identifier { Value = "dbo" },
                            new Identifier { Value = "UpSizeFunction" }),
                        Parameters = new List<WScalarExpression>
                            {
                                new WValueExpression {Value = adjustValue.ToString()}
                            },
                        Alias = new Identifier
                        {
                            Value = Path.GetRandomFileName().Replace(".", "").Substring(0, 8),
                        }
                    },
                    UnqualifiedJoinType = UnqualifiedJoinType.CrossApply
                };
                pow--;
                affectedEstimatedSize *= adjustValue;
            }
            return tableRef;
        }

        private static WTableReference ConstructUpSizeTableReference(WTableReference tableRef, double upSizeScalar, string dumb, DumbType dumbType, out double affectedEstimatedSize)
        {
            affectedEstimatedSize = 1.0;
            int pow = (int)(Math.Floor(Math.Log(upSizeScalar, 1000)) + 1);
            int adjustValue = (int)Math.Pow(upSizeScalar, 1.0 / pow);
            while (pow > 0)
            {
                tableRef = new WUnqualifiedJoin
                {
                    FirstTableRef = tableRef,
                    SecondTableRef = new WSchemaObjectFunctionTableReference
                    {
                        SchemaObject = new WSchemaObjectName(
                            new Identifier { Value = "dbo" },
                            new Identifier { Value = "UpSizeFunction2" }),
                        Parameters = new List<WScalarExpression>
                        {
                            new WValueExpression {Value = adjustValue.ToString()},
                            new WColumnReferenceExpression
                            {
                                MultiPartIdentifier =
                                    new WMultiPartIdentifier(
                                        new Identifier {Value = dumb},
                                        new Identifier {Value = dumbType == DumbType.Node ? "GlobalNodeId" : "Sink"})
                            }
                        },
                        Alias = new Identifier
                        {
                            Value = Path.GetRandomFileName().Replace(".", "").Substring(0, 8),
                        }
                    },
                    UnqualifiedJoinType = UnqualifiedJoinType.CrossApply
                };
                pow--;
                affectedEstimatedSize *= adjustValue;
            }
            return tableRef;
        }

        /// <summary>
        /// Calculate the number used for adjusting the SQL Server estimation in the downsize function.
        /// </summary>
        /// <param name="component"></param>
        /// <param name="matEdges"></param>
        /// <param name="joinCondition"></param>
        private static WTableReference AdjustEstimation(MatchComponent component, List<MatchEdge> matEdges, out WBooleanExpression joinCondition, out double affectedSqlEstimatedSize)
        {
            const int sizeFactor = 10;
            int estimateFactor = 0;
            double size = component.Cardinality*
                          matEdges.Select(e => e.AverageDegree).Aggregate(1.0, (cur, next) => cur*next);
            double estimatedSize = component.SqlEstimatedSize * Math.Pow(1000, matEdges.Count);
            double shrinkSize = component.RightestTableRefSize;
            WTableReference tableReference = component.TableRef;
            affectedSqlEstimatedSize = 1.0;
            joinCondition = null;
 
            if (size > sizeFactor*estimatedSize)
            {
                estimateFactor = (int)Math.Ceiling(size / estimatedSize);
            }
            else if (sizeFactor*size < estimatedSize)
            {
                shrinkSize = 1.0/(1 - Math.Pow((1 - 1.0/shrinkSize), 1.5));
                if (estimatedSize < shrinkSize)
                {
                    affectedSqlEstimatedSize /= estimatedSize;
                    estimatedSize = 1;
                }
                else
                {
                    affectedSqlEstimatedSize /= shrinkSize;
                    estimatedSize /= shrinkSize;
                }
                estimateFactor = (int) Math.Ceiling(size/estimatedSize);
                joinCondition = ConstructDownSizeJoinCondition(component.RightestTableAlias);
            }
            if (estimateFactor > 1)
            {
                double affectedUpSize;
                tableReference = ConstructUpSizeTableReference(tableReference, estimateFactor,
                    out affectedUpSize);
                affectedSqlEstimatedSize *= affectedUpSize;
            }
            return tableReference;
        }

        /// <summary>
        /// Calculate the number used for adjusting the SQL Server estimation in the downsize function.
        /// </summary>
        /// <param name="metaData"></param>
        /// <param name="joinCondition"></param>
        /// <param name="candidateJoinUnit"></param>
        /// <param name="nodeAlias"></param>
        /// <param name="affectedSqlEstimatedSize"></param>
        //private static WTableReference AdjustEstimation(CandidateJoinUnit candidateJoinUnit, string nodeAlias, GraphMetaData metaData, out WBooleanExpression joinCondition, out double affectedSqlEstimatedSize)
        //{
        //    const int sizeFactor = 10;
        //    int estimateFactor = 0;
        //    double size = candidateJoinUnit.EdgeDegrees;
        //    double estimatedSize = candidateJoinUnit.SqlEstimatedEdgeDegrees;
        //    double shrinkSize = candidateJoinUnit.TreeRoot.EstimatedRows;
        //    WTableReference tableReference = candidateJoinUnit.ToTableReference(nodeAlias, metaData);
        //    affectedSqlEstimatedSize = 1.0;
        //    joinCondition = null;

        //    if (size > sizeFactor * estimatedSize)
        //    {
        //        estimateFactor = (int)Math.Ceiling(size / estimatedSize);
        //    }
        //    else if (sizeFactor*size < estimatedSize)
        //    {
        //        shrinkSize = 1.0/(1 - Math.Pow((1 - 1.0/shrinkSize), 1.5));
        //        affectedSqlEstimatedSize /= shrinkSize;
        //        estimatedSize /= shrinkSize;
        //        estimateFactor = (int) Math.Ceiling(size/estimatedSize);
        //        joinCondition = ConstructDownSizeJoinCondition(nodeAlias);
        //    }
        //    if (estimateFactor > 1)
        //    {
        //        double affectedUpSize;
        //        tableReference = ConstructUpSizeTableReference(tableReference, estimateFactor,
        //            out affectedUpSize);
        //        affectedSqlEstimatedSize *= affectedUpSize;
        //    }
        //    return tableReference;
        //}

        private static WTableReference AdjustEstimation(CandidateJoinUnit candidateJoinUnit, string nodeAlias, GraphMetaData metaData, out WBooleanExpression joinCondition, out double affectedSqlEstimatedSize)
        {
            const int sizeFactor = 10;
            int estimateFactor = 0;
            List<MatchEdge> matEdges = candidateJoinUnit.PreMatOutgoingEdges;
            double size = matEdges.Select(e => e.AverageDegree).Aggregate(1.0, (cur, next) => cur * next);
            double estimatedSize = Math.Pow(1000, matEdges.Count);
            double shrinkSize = candidateJoinUnit.TreeRoot.EstimatedRows;
            WTableReference tableReference = candidateJoinUnit.ToTableReference(nodeAlias, nodeAlias, metaData);
            affectedSqlEstimatedSize = 1.0;
            joinCondition = null;

            if (size > sizeFactor * estimatedSize)
            {
                estimateFactor = (int)Math.Ceiling(size / estimatedSize);
            }
            else if (sizeFactor * size < estimatedSize)
            {
                shrinkSize = 1.0 / (1 - Math.Pow((1 - 1.0 / shrinkSize), 1.5));
                affectedSqlEstimatedSize /= shrinkSize;
                estimatedSize /= shrinkSize;
                estimateFactor = (int)Math.Ceiling(size / estimatedSize);
                joinCondition = ConstructDownSizeJoinCondition(nodeAlias);
            }
            if (estimateFactor > 1)
            {
                double affectedUpSize;
                tableReference = ConstructUpSizeTableReference(tableReference, estimateFactor,
                    out affectedUpSize);
                affectedSqlEstimatedSize *= affectedUpSize;
            }
            return tableReference;
        }


        /// <summary>
        /// Calculate join costs and update components using optimal join method & order
        /// </summary>
        /// <param name="nodeUnitCandidate"></param>
        /// <param name="joinCondition"></param>
        /// <param name="preJoinSelectivity"></param>
        /// <param name="postJoinSelectivity"></param>
        /// <param name="estimatedSelectivity"></param>
        /// <param name="metaData"></param>
        /// <param name="isExecutable"></param>
        private void ConstructPhysicalJoinAndUpdateCost(
            CandidateJoinUnit nodeUnitCandidate,
            WBooleanExpression joinCondition, 
            double preJoinSelectivity,
            double postJoinSelectivity,
            double estimatedSelectivity,
            GraphMetaData metaData)
        {
            const double scaleFactor = 1.5;
            const int sqlInPreMatEdgeSelectivityThreshold = 5;
            var firstJoin = MaterializedNodeSplitCount.Count == 2;

            var inPreMatEdges = nodeUnitCandidate.PreMatIncomingEdges;
            var inPostMatEdges = nodeUnitCandidate.PostMatIncomingEdges;
            var outPreMatEdges = nodeUnitCandidate.PreMatOutgoingEdges;
            var outPostMatEdges = nodeUnitCandidate.PostMatOutgoingEdges;
            var postMatEdges = inPostMatEdges.Select(e => new Tuple<MatchEdge, EdgeDir>(e, EdgeDir.In))
                               .Union(
                                   outPostMatEdges.Select(e => new Tuple<MatchEdge, EdgeDir>(e, EdgeDir.Out)))
                               .OrderBy(t => t.Item1.AverageDegree)
                               .ToList();

            var compDegrees = inPreMatEdges.Select(e => e.AverageDegree).Aggregate(1.0, (cur, next) => cur*next);
            var nodeDegrees = outPreMatEdges.Select(e => e.AverageDegree).Aggregate(1.0, (cur, next) => cur * next);

            var root = nodeUnitCandidate.TreeRoot;
            var componentSize = Cardinality;

            double sizeFactor = 5; // 1000;
            var loopCost = nodeUnitCandidate.JoinHint == JoinHint.Loop 
                ? componentSize * compDegrees * Math.Log(root.EstimatedRows, 512)
                : double.MaxValue;
            // only calc the size of table used to join
            var matCompSizeWhenJoin = componentSize*compDegrees;
            var matUnitSizeWhenJoin = root.EstimatedRows*nodeDegrees;
            var hashCost = matCompSizeWhenJoin + matUnitSizeWhenJoin;

            double loopJoinOuterThreshold = 1e4;//1e6;
            double maxMemory = 1e8;
            double cost;

            // loop join
            if (nodeUnitCandidate.JoinHint == JoinHint.Loop
                    //inPreMatEdges.Any() && !outPreMatEdges.Any() &&
                    //(
                    //    //componentSize < loopJoinOuterThreshold ||     // the outer table is relatively small
                    //    loopCost < hashCost ||
                    //    (DeltaMemory + matCompSizeWhenJoin > maxMemory && DeltaMemory + matUnitSizeWhenJoin > maxMemory)
                    //    // memory is in pressure
                    //)
                )
            {
                if (firstJoin)
                {
                    RightestTableRefSize = nodeUnitCandidate.TreeRoot.EstimatedRows;
                    RightestTableAlias = root.RefAlias;
                    LastTable = Nodes.First(n => n.NodeAlias != root.NodeAlias);
                    //LastTableAlias = LastTable.RefAlias;
                    LastJoinHint = JoinHint.Loop;
                    LastJoinSqlEstCardinality = LastTable.TableRowCount;
                }

                cost = loopCost;

                var sqlInPreMatEdgesSelectivity = 1.0;
                for (var i = 1; i < inPreMatEdges.Count && i <= sqlInPreMatEdgeSelectivityThreshold; ++i)
                    sqlInPreMatEdgesSelectivity = Math.Sqrt(sqlInPreMatEdgesSelectivity)/10;

                var sqlEstPreJoinEdgeSize = Math.Pow(100, LastJoinPostMatEdgesCount)*Math.Pow(1000, inPreMatEdges.Count)*
                                            sqlInPreMatEdgesSelectivity;
                var sqlEstPreJoinInputSize = LastJoinSqlEstCardinality * (LastJoinHint == JoinHint.Loop 
                                             ? LastTable.EstimatedRows / LastTable.TableRowCount 
                                             : 1.0) *
                                             sqlEstPreJoinEdgeSize;

                var estimateFactor = 0;
                if (matCompSizeWhenJoin >= sqlEstPreJoinInputSize * scaleFactor)
                    estimateFactor = (int) Math.Ceiling(matCompSizeWhenJoin/sqlEstPreJoinInputSize);
                else if (matCompSizeWhenJoin*scaleFactor < sqlEstPreJoinInputSize)
                    estimateFactor = -1;

                var affectedUpsize = 1.0;
                if (estimateFactor >= (int) Math.Ceiling(scaleFactor))
                {
                    if (LastJoinHint == JoinHint.Loop)
                        TableRef = ConstructUpSizeTableReference(TableRef, estimateFactor, LastTableAlias, DumbType.Node, out affectedUpsize);
                    else if (LastJoinPostMatEdgesCount > 0)
                        TableRef = ConstructUpSizeTableReference(TableRef, estimateFactor, LastPostMatEdgeAlias, DumbType.Edge, out affectedUpsize);
                    else
                        TableRef = ConstructUpSizeTableReference(TableRef, estimateFactor, out affectedUpsize);
                }
                else if (estimateFactor == -1 && LastJoinHint == JoinHint.Loop)
                {
                    sqlEstPreJoinInputSize = LastJoinSqlEstCardinality*
                                             Math.Sqrt(LastTable.EstimatedRows/LastTable.TableRowCount)/
                                             LastTable.TableRowCount * 1.5 * sqlEstPreJoinEdgeSize;

                    if (matCompSizeWhenJoin >= sqlEstPreJoinInputSize*scaleFactor)
                        TableRef = ConstructUpSizeTableReference(TableRef, (int) Math.Ceiling(matCompSizeWhenJoin/sqlEstPreJoinInputSize), 
                            LastTableAlias, DumbType.Node, out affectedUpsize);
                }
                
                foreach (var edge in inPreMatEdges)
                    TableRef = SpanTableRef(TableRef, edge, edge.SourceNode.RefAlias, LastTableAlias, metaData);

                WTableReference table = new WQualifiedJoin
                {
                    FirstTableRef = TableRef,
                    SecondTableRef =
                        nodeUnitCandidate.ToTableReference(root.RefAlias, root.RefAlias, metaData),
                    JoinCondition = estimateFactor == -1 
                                    ? WBooleanBinaryExpression.Conjunction(joinCondition, ConstructDownSizeJoinCondition(LastTableAlias)) 
                                    : joinCondition,
                    QualifiedJoinType = QualifiedJoinType.Inner,
                    JoinHint = JoinHint.Loop
                };

                table = postMatEdges.Aggregate(table, (current, next) => new WUnqualifiedJoin
                {
                    FirstTableRef = current,
                    SecondTableRef =
                        next.Item1.ToSchemaObjectFunction(next.Item1.SourceNode.RefAlias,
                                                          next.Item2 == EdgeDir.In ? root.RefAlias : LastTableAlias,
                                                          metaData),
                    UnqualifiedJoinType = UnqualifiedJoinType.CrossApply,
                });

                TableRef = new WParenthesisTableReference
                {
                    Table = table,
                };

                LastTable = root;
                LastTableAlias = root.RefAlias;
                LastJoinHint = JoinHint.Loop;
                LastJoinSqlEstCardinality = sqlEstPreJoinInputSize*affectedUpsize;
                LastJoinPostMatEdgesCount = postMatEdges.Count;
                LastPostMatEdgeAlias = LastJoinPostMatEdgesCount > 0 ? postMatEdges.Last().Item1.EdgeAlias : null;
                SqlEstimatedSize = sqlEstPreJoinInputSize*root.EstimatedRows/root.TableRowCount*
                                   Math.Pow(100, postMatEdges.Count);
                SqlEstimatedSize = SqlEstimatedSize < 1.0 ? 1.0 : SqlEstimatedSize;

                Cardinality = matCompSizeWhenJoin *
                                inPostMatEdges.Select(e => e.AverageDegree).Aggregate(1.0, (cur, next) => cur * next) *
                              matUnitSizeWhenJoin *
                                outPostMatEdges.Select(e => e.AverageDegree).Aggregate(1.0, (cur, next) => cur * next) *
                              preJoinSelectivity / root.TableRowCount * postJoinSelectivity;
            }
            // hash join
            else
            {
                cost = hashCost;
                WBooleanExpression adjustedJoincondition = null;
                WTableReference buildTableReference = nodeUnitCandidate.ToTableReference(root.RefAlias, root.RefAlias, metaData);
                double affectedUpsize = 1.0, sqlEstPreJoinEdgeSize;
                int estimateFactor;
                var sqlEstPreJoinInputSize = root.EstimatedRows;

                if (firstJoin)
                {
                    LastTable = Nodes.First(n => n.NodeAlias != root.NodeAlias);
                    //LastTableAlias = LastTable.RefAlias;
                    LastJoinHint = JoinHint.Loop;
                    LastJoinSqlEstCardinality = LastTable.TableRowCount;
                }

                // Build table adjustment
                if (outPreMatEdges.Any())
                {
                    var sqlOutPreMatEdgesSelectivity = 1.0;
                    foreach (var group in outPreMatEdges.GroupBy(e => e.SinkNode))
                    {
                        var selectivity = 1.0;
                        for (var i = 1; i < group.Count() && i <= sqlInPreMatEdgeSelectivityThreshold; ++i)
                            selectivity = Math.Sqrt(selectivity) / 10;
                        sqlOutPreMatEdgesSelectivity *= selectivity;
                    }
                    
                    sqlEstPreJoinEdgeSize = Math.Pow(1000, outPreMatEdges.Count) * sqlOutPreMatEdgesSelectivity;
                    sqlEstPreJoinInputSize *= sqlEstPreJoinEdgeSize;

                    estimateFactor = 0;
                    if (matUnitSizeWhenJoin >= sqlEstPreJoinInputSize*scaleFactor)
                        estimateFactor = (int) Math.Ceiling(matUnitSizeWhenJoin/sqlEstPreJoinInputSize);
                    else if (matUnitSizeWhenJoin*scaleFactor < sqlEstPreJoinInputSize)
                    {
                        estimateFactor = -1;
                        adjustedJoincondition = ConstructDownSizeJoinCondition(root.RefAlias);
                    }
                        
                    if (estimateFactor >= (int) Math.Ceiling(scaleFactor))
                        buildTableReference = ConstructUpSizeTableReference(buildTableReference, estimateFactor, 
                            root.RefAlias, DumbType.Node, out affectedUpsize);
                    else if (estimateFactor == -1)
                    {
                        sqlEstPreJoinInputSize = Math.Sqrt(root.EstimatedRows/root.TableRowCount)*1.5*sqlEstPreJoinEdgeSize;

                        if (matUnitSizeWhenJoin >= sqlEstPreJoinInputSize*scaleFactor)
                            buildTableReference = ConstructUpSizeTableReference(buildTableReference, 
                                (int)Math.Ceiling(matUnitSizeWhenJoin / sqlEstPreJoinInputSize),
                                root.RefAlias, DumbType.Node, out affectedUpsize);
                    }
                }
                sqlEstPreJoinInputSize *= affectedUpsize;

                // Cardinality update
                Cardinality = matCompSizeWhenJoin *
                                inPostMatEdges.Select(e => e.AverageDegree).Aggregate(1.0, (cur, next) => cur * next) *
                              matUnitSizeWhenJoin *
                                outPostMatEdges.Select(e => e.AverageDegree).Aggregate(1.0, (cur, next) => cur * next) *
                              preJoinSelectivity / root.TableRowCount * postJoinSelectivity;

                // Output adjustment
                var postJoinUpsizeFactor = -1;

                var sqlInPreMatEdgesSelectivity = 1.0;
                for (var i = 1; i < inPreMatEdges.Count; ++i)
                    sqlInPreMatEdgesSelectivity = Math.Sqrt(sqlInPreMatEdgesSelectivity) / 10;

                sqlEstPreJoinEdgeSize = Math.Pow(100, LastJoinPostMatEdgesCount) * Math.Pow(1000, inPreMatEdges.Count) *
                                            sqlInPreMatEdgesSelectivity;
                var probeSqlEstCardinality = LastJoinSqlEstCardinality * (LastJoinHint == JoinHint.Loop
                                             ? LastTable.EstimatedRows / LastTable.TableRowCount
                                             : 1.0)
                                             * sqlEstPreJoinEdgeSize;

                var hashJoinSqlSelectivity = outPreMatEdges.Any()
                    ? Math.Pow(Math.Sqrt(0.001), outPreMatEdges.GroupBy(e => e.SinkNode).Count())
                    : (root.EstimatedRows/root.TableRowCount) / root.EstimatedRows;

                var sqlEstHashCardinality = sqlEstPreJoinInputSize*probeSqlEstCardinality*hashJoinSqlSelectivity*
                                            Math.Pow(100, postMatEdges.Count);

                estimateFactor = 0;
                if (Cardinality >= sqlEstHashCardinality*scaleFactor)
                    estimateFactor = (int) Math.Ceiling(Cardinality/sqlEstHashCardinality);
                else if (Cardinality*scaleFactor < sqlEstHashCardinality)
                {
                    estimateFactor = -1;
                    adjustedJoincondition = WBooleanBinaryExpression.Conjunction(adjustedJoincondition,
                        ConstructDownSizeJoinCondition(LastTableAlias));
                }
                    
                if (estimateFactor >= (int) Math.Ceiling(scaleFactor))
                    postJoinUpsizeFactor = estimateFactor;
                else if (estimateFactor == -1 && LastJoinHint == JoinHint.Loop)
                {
                    probeSqlEstCardinality = LastJoinSqlEstCardinality*
                                             Math.Sqrt(LastTable.EstimatedRows/LastTable.TableRowCount)/
                                             LastTable.TableRowCount * 1.5 * sqlEstPreJoinEdgeSize;
                    sqlEstHashCardinality = sqlEstPreJoinInputSize*probeSqlEstCardinality*hashJoinSqlSelectivity*
                                            Math.Pow(100, postMatEdges.Count);

                    if (Cardinality >= sqlEstHashCardinality*scaleFactor)
                        postJoinUpsizeFactor = (int) Math.Ceiling(Cardinality/sqlEstHashCardinality);
                }
                
                foreach (var edge in inPreMatEdges)
                    TableRef = SpanTableRef(TableRef, edge, edge.SourceNode.RefAlias, LastTableAlias, metaData);

                WTableReference table = new WQualifiedJoin
                {
                    FirstTableRef = buildTableReference,
                    SecondTableRef = TableRef,
                    JoinCondition = WBooleanBinaryExpression.Conjunction(joinCondition, adjustedJoincondition),
                    QualifiedJoinType = QualifiedJoinType.Inner,
                    JoinHint = JoinHint.Hash
                };

                table = postMatEdges.Aggregate(table, (current, next) => new WUnqualifiedJoin
                {
                    FirstTableRef = current,
                    SecondTableRef =
                        next.Item1.ToSchemaObjectFunction(next.Item1.SourceNode.RefAlias,
                                                          next.Item2 == EdgeDir.In ? root.RefAlias : LastTableAlias,
                                                          metaData),
                    UnqualifiedJoinType = UnqualifiedJoinType.CrossApply,
                });

                if (postJoinUpsizeFactor != -1)
                {
                    if (postMatEdges.Any())
                        table = ConstructUpSizeTableReference(table, postJoinUpsizeFactor,
                            postMatEdges.Last().Item1.EdgeAlias, DumbType.Edge, out affectedUpsize);
                    else
                        table = ConstructUpSizeTableReference(table, postJoinUpsizeFactor, out affectedUpsize);
                }

                TableRef = new WParenthesisTableReference
                {
                    Table = table,
                };

                LastJoinHint = JoinHint.Hash;
                LastJoinSqlEstCardinality = sqlEstHashCardinality * affectedUpsize;
                LastJoinPostMatEdgesCount = 0;
                LastPostMatEdgeAlias = null;
                SqlEstimatedSize = LastJoinSqlEstCardinality;
                SqlEstimatedSize = SqlEstimatedSize < 1.0 ? 1.0 : SqlEstimatedSize;
            }

            Cost += cost;

            // Debug
#if DEBUG
            //foreach (var item in MaterializedNodeSplitCount.Where(e => e.Key != node))
            //{
            //    Trace.Write(item.Key.RefAlias + ",");
            //}
            //Trace.Write(root.RefAlias);
            //Trace.Write(" Size:" + Cardinality + " Cost:" + cost);
            //Trace.Write(" Method:" + ((TableRef as WParenthesisTableReference).Table as WQualifiedJoin).JoinHint);
            //Trace.WriteLine(" --> Total Cost:" + Cost);
#endif
        }

        /// <summary>
        /// Span the table given the edge using cross apply
        /// </summary>
        /// <param name="tableRef"></param>
        /// <param name="edge"></param>
        /// <param name="nodeAlias"></param>
        /// <param name="dumbNode"></param>
        /// <returns></returns>
        public WTableReference SpanTableRef(WTableReference tableRef, MatchEdge edge, string nodeAlias, string dumbNode, GraphMetaData metaData)
        {
            tableRef = new WUnqualifiedJoin
            {
                FirstTableRef = tableRef,
                SecondTableRef = edge.ToSchemaObjectFunction(nodeAlias, dumbNode, metaData),
                UnqualifiedJoinType = UnqualifiedJoinType.CrossApply,
            };
            return tableRef;
        }

        public string GetNodeRefName(MatchNode node)
        {
            int count = MaterializedNodeSplitCount[node];
            if (count == 0)
                return node.RefAlias;
            else
                return string.Format("{0}_{1}", node.RefAlias, count);
        }

        private WBooleanExpression ConstructJoinCondition(
            CandidateJoinUnit candidateTree,
            IMatchJoinStatisticsCalculator statisticsCalculator,
            GraphMetaData metaData,
            Dictionary<Tuple<string, bool>, Statistics> srcNodeStatisticsDict,
            out double preJoinSelectivity,
            out double postJoinSelectivity,
            out double sqlEstimatedJoinSelectivity)
        {
            const double sizeThreshold = 1e8;
            const int loopJoinFactorThreshold = 20;

            preJoinSelectivity = 1.0;
            postJoinSelectivity = 1.0;
            sqlEstimatedJoinSelectivity = 1.0;

            var firstJoin = MaterializedNodeSplitCount.Count == 1;
            MatchNode firstNode = null;
            if (firstJoin)
                firstNode = Nodes.First();
            var root = candidateTree.TreeRoot;

            WBooleanExpression joinCondition = null;
            WBooleanExpression whereCondition = null;
            string nodeName = root.RefAlias;

            if (!Nodes.Contains(root))
                Nodes.Add(root);
            MaterializedNodeSplitCount[root] = 0;

            var inEdges =
                candidateTree.PreMatIncomingEdges.Select(
                    e => new Tuple<MaterializedOrder, MatchEdge>(MaterializedOrder.Pre, e))
                    .Union(
                        candidateTree.PostMatIncomingEdges.Select(
                            e => new Tuple<MaterializedOrder, MatchEdge>(MaterializedOrder.Post, e)))
                    .ToList();

            var outEdges =
                candidateTree.PreMatOutgoingEdges.Select(
                    e => new Tuple<MaterializedOrder, MatchEdge>(MaterializedOrder.Pre, e))
                    .Union(
                        candidateTree.PostMatOutgoingEdges.Select(
                            e => new Tuple<MaterializedOrder, MatchEdge>(MaterializedOrder.Post, e)))
                    .ToList();

            var densityList = new List<double>();
            var inPostCount = 0;
            var outPostCount = 0;

            if (inEdges.Any())
            {
                UnmaterializedNodeMapping.Remove(root);
                //joinSelectivity *= 1.0 / root.TableRowCount;

                Statistics statistics = null;
                Statistics srcNodeStat = null;
                foreach (var t in inEdges)
                {
                    var order = t.Item1;
                    var edge = t.Item2;

                    var globalNodeIdRef = new WColumnReferenceExpression
                    {
                        ColumnType = ColumnType.Regular,
                        MultiPartIdentifier = new WMultiPartIdentifier(
                            new Identifier {Value = nodeName},
                            new Identifier {Value = "GlobalNodeId"}
                            )
                    }; 

                    var newCondition = new WBooleanComparisonExpression
                    {
                        FirstExpr = new WColumnReferenceExpression
                        {
                            ColumnType = ColumnType.Regular,
                            MultiPartIdentifier = new WMultiPartIdentifier(
                                new Identifier {Value = edge.EdgeAlias},
                                new Identifier {Value = "Sink"}
                                ),
                        },
                        SecondExpr = order == MaterializedOrder.Post && inPostCount > 0
                            ? new WBinaryExpression
                            {
                                ExpressionType = BinaryExpressionType.Add,
                                FirstExpr = globalNodeIdRef,
                                SecondExpr = new WValueExpression
                                {
                                    SingleQuoted = false,
                                    Value = "0",
                                }
                            }
                            : (WScalarExpression)globalNodeIdRef,
                        ComparisonType = BooleanComparisonType.Equals
                    };
                    EdgeMaterilizedDict[edge] = true;

                    double selectivity;
                    statistics = Statistics.UpdateHistogram(statistics,
                        edge.Statistics, out selectivity);

                    if (order == MaterializedOrder.Pre)
                    {
                        preJoinSelectivity *= selectivity;
                        joinCondition = WBooleanBinaryExpression.Conjunction(joinCondition, newCondition);
                    }
                    else
                    {
                        ++inPostCount;
                        postJoinSelectivity *= selectivity;
                        whereCondition = WBooleanBinaryExpression.Conjunction(whereCondition, newCondition);
                    }

                    if (firstJoin)
                    {
                        double srcNodeSelectivity;
                        srcNodeStat = Statistics.UpdateHistogram(srcNodeStat, 
                            srcNodeStatisticsDict[new Tuple<string, bool>(edge.EdgeAlias, edge.IsReversedEdge)],
                            out srcNodeSelectivity); 
                    }

                    densityList.Add(root.GlobalNodeIdDensity);
                }

                if (firstJoin)
                    SinkNodeStatisticsDict[firstNode] = srcNodeStat;
                SinkNodeStatisticsDict[root] = statistics;
            }

            if (candidateTree.JoinHint == JoinHint.Loop)
            {
                var size = Cardinality*candidateTree.PreMatIncomingEdges.Select(e => e.AverageDegree)
                    .Aggregate(1.0, (cur, next) => cur*next)*preJoinSelectivity;
                if (size >= sizeThreshold && size > root.EstimatedRows * loopJoinFactorThreshold)
                    candidateTree.JoinHint = JoinHint.Hash;
            }

            if (outEdges.Any())
            {
                foreach (var t in outEdges)
                {
                    var order = t.Item1;
                    var edge = t.Item2;
                    var sinkNode = edge.SinkNode;

                    var globalNodeIdRef = new WColumnReferenceExpression
                    {
                        ColumnType = ColumnType.Regular,
                        MultiPartIdentifier = new WMultiPartIdentifier(
                            new Identifier {Value = sinkNode.RefAlias},
                            new Identifier {Value = "GlobalNodeId"}
                            )
                    };

                    var newCondition = new WBooleanComparisonExpression
                    {
                        FirstExpr = new WColumnReferenceExpression
                        {
                            ColumnType = ColumnType.Regular,
                            MultiPartIdentifier = new WMultiPartIdentifier(
                                new Identifier {Value = edge.EdgeAlias},
                                new Identifier {Value = "Sink"}
                                ),
                        },
                        SecondExpr = order == MaterializedOrder.Post && outPostCount > 0
                            ? new WBinaryExpression
                            {
                                ExpressionType = BinaryExpressionType.Add,
                                FirstExpr = globalNodeIdRef,
                                SecondExpr = new WValueExpression
                                {
                                    SingleQuoted = false,
                                    Value = "0",
                                }
                            }
                            : (WScalarExpression)globalNodeIdRef,
                        ComparisonType = BooleanComparisonType.Equals
                    };
                    EdgeMaterilizedDict[edge] = true;

                    Statistics sinkNodeStatistics;
                    if (!SinkNodeStatisticsDict.TryGetValue(sinkNode, out sinkNodeStatistics))
                    {
                        sinkNodeStatistics = null;
                        //joinSelectivity *= 1.0 / sinkNode.TableRowCount;
                    }
                    double selectivity;
                    var statistics = Statistics.UpdateHistogram(sinkNodeStatistics,
                        edge.Statistics, out selectivity);

                    if (order == MaterializedOrder.Pre)
                    {
                        preJoinSelectivity *= selectivity;
                        joinCondition = WBooleanBinaryExpression.Conjunction(joinCondition, newCondition);
                    }
                    else
                    {
                        ++outPostCount;
                        postJoinSelectivity *= selectivity;
                        whereCondition = WBooleanBinaryExpression.Conjunction(whereCondition, newCondition);
                    }

                    SinkNodeStatisticsDict[sinkNode] = statistics;
                    densityList.Add(sinkNode.GlobalNodeIdDensity);
                }
            }

            var unmatEdges = candidateTree.UnmaterializedEdges;
            foreach (var unmatEdge in unmatEdges)
            {
                EdgeMaterilizedDict[unmatEdge] = false;;
                var unmatNodeInEdges = UnmaterializedNodeMapping.GetOrCreate(unmatEdge.SinkNode);
                unmatNodeInEdges.Add(unmatEdge);
            }

            densityList.Sort();
            for (int i = densityList.Count - 1; i >= 0; i--)
            {
                sqlEstimatedJoinSelectivity *= Math.Sqrt(sqlEstimatedJoinSelectivity) * densityList[i];
            }

            WhereCondition = WBooleanBinaryExpression.Conjunction(WhereCondition, whereCondition);

            return joinCondition;
        }


        /// <summary>
        /// Transit from current component to the new component in the next state given the Node Unit
        /// </summary>
        /// <param name="candidateTree"></param>
        /// <param name="statisticsCalculator"></param>
        /// <param name="metaData"></param>
        /// <param name="srcNodeStatisticsDict"></param>
        /// <returns></returns>
        public MatchComponent GetNextState(
            CandidateJoinUnit candidateTree, 
            IMatchJoinStatisticsCalculator statisticsCalculator,
            GraphMetaData metaData,
            Dictionary<Tuple<string, bool>, Statistics> srcNodeStatisticsDict )
        {
            // Deep copy the component
            var newComponent = new MatchComponent(this);

            // Constrcuts join conditions and retrieves join selectivity
            double preJoinSelectivity, postJoinSelectivity, sqlEstimatedJoinSelectivity;

            var joinCondition = newComponent.ConstructJoinCondition(candidateTree, statisticsCalculator, metaData, srcNodeStatisticsDict,
                out preJoinSelectivity, out postJoinSelectivity, out sqlEstimatedJoinSelectivity);

            // Constructs physical join method and join table references
            newComponent.ConstructPhysicalJoinAndUpdateCost(candidateTree, joinCondition,
               preJoinSelectivity, postJoinSelectivity, sqlEstimatedJoinSelectivity, metaData);

            return newComponent;
        }
    }
}


