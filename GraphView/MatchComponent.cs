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
        // TreeRoot's outgoing edges that are materialized by Transpose() functions.
        public List<MatchEdge> MaterializedEdges { get; set; }
        // TreeRoot's edges that are not yet materialized. Lazy materialization may be beneficial for performance, 
        // because it reduces the number of intermediate resutls. 
        public List<MatchEdge> UnmaterializedEdges { get; set; }

        private double _edgeDegrees = -1;
        public double EdgeDegrees
        {
            get
            {
                if (_edgeDegrees < 0)
                {
                    double matEdgeDegrees = MaterializedEdges.Aggregate(1.0, (cur, next) => cur*next.AverageDegree);
                    double unMatEdgeDegrees = UnmaterializedEdges.Aggregate(1.0, (cur, next) => cur * next.AverageDegree);
                    _edgeDegrees = unMatEdgeDegrees*matEdgeDegrees;
                }
                return _edgeDegrees;
            }
        }

        private double _sqlEstimatedEdgeDegrees = -1;
        public double SqlEstimatedEdgeDegrees
        {
            get
            {
                if (_sqlEstimatedEdgeDegrees < 0)
                {
                    _sqlEstimatedEdgeDegrees = Math.Pow(1000, MaterializedEdges.Count + UnmaterializedEdges.Count);
                }
                return _sqlEstimatedEdgeDegrees;
            }
        }

        public WTableReference ToTableReference(string nodeAlias,GraphMetaData metaData)
        {
            // Constructs table reference
            WTableReference nodeTable = new WNamedTableReference
            {
                Alias = new Identifier { Value = nodeAlias },
                TableObjectName = TreeRoot.NodeTableObjectName
            };
            nodeTable = MaterializedEdges.Aggregate(nodeTable, (current, edge) => new WUnqualifiedJoin
            {
                FirstTableRef = current,
                SecondTableRef = edge.ToSchemaObjectFunction(nodeAlias, metaData),
                UnqualifiedJoinType = UnqualifiedJoinType.CrossApply,
            });
            return nodeTable;
        }
    }


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

        // Total memory used by the current execution plan
        public double TotalMemory { get; set; }

        // Total memory of this component, if it is to be joined with the next candidate using 
        // the left-deep hash join or the loop join. 
        public double DeltaMemory { get; set; }

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

        
        public MatchComponent()
        {
            Nodes = new HashSet<MatchNode>();
            EdgeMaterilizedDict = new Dictionary<MatchEdge, bool>();
            MaterializedNodeSplitCount = new Dictionary<MatchNode, int>();
            UnmaterializedNodeMapping = new Dictionary<MatchNode, List<MatchEdge>>();
            SinkNodeStatisticsDict = new Dictionary<MatchNode, Statistics>();
            Cardinality = 1.0;
            Cost = 0.0;
            TotalMemory = 0.0;
            DeltaMemory = 0.0;
            SqlEstimatedDeltaMemory = 0.0;
            SqlEstimatedTotalMemory = 0.0;
            SqlEstimatedSize = 1.0;
            RightestTableRefSize = 0.0;
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
            Cardinality = component.Cardinality;
            Cost = component.Cost;
            DeltaMemory = component.DeltaMemory;
            TotalMemory = component.TotalMemory;
            SqlEstimatedDeltaMemory = component.SqlEstimatedDeltaMemory;
            SqlEstimatedTotalMemory = component.SqlEstimatedTotalMemory;
            SqlEstimatedSize = component.SqlEstimatedSize;
            RightestTableAlias = component.RightestTableAlias;
            RightestTableRefSize = component.RightestTableRefSize;
        }

        public MatchComponent(MatchNode node, List<MatchEdge> populatedEdges,GraphMetaData metaData) : this(node)
        {
            foreach (var edge in populatedEdges)
            {
                TableRef = SpanTableRef(TableRef, edge, node.RefAlias, metaData);
                EdgeMaterilizedDict[edge] = true;
                SinkNodeStatisticsDict[edge.SinkNode] = edge.Statistics;
                var edgeList = UnmaterializedNodeMapping.GetOrCreate(edge.SinkNode);
                edgeList.Add(edge);
                if (!Nodes.Contains(edge.SinkNode))
                    Nodes.Add(edge.SinkNode);
                Cardinality *= edge.AverageDegree;
                SqlEstimatedSize *= 1000;

            }
        }

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

        /// <summary>
        /// Calculate the number used for adjusting the SQL Server estimation in the downsize function.
        /// </summary>
        /// <param name="component"></param>
        /// <param name="joinCondition"></param>
        private static WTableReference AdjustEstimation(MatchComponent component, out WBooleanExpression joinCondition, out double affectedSqlEstimatedSize)
        {
            const int sizeFactor = 10;
            int estimateFactor = 0;
            double size = component.Cardinality;
            double estimatedSize = component.SqlEstimatedSize;
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
        private static WTableReference AdjustEstimation(CandidateJoinUnit candidateJoinUnit, string nodeAlias, GraphMetaData metaData, out WBooleanExpression joinCondition, out double affectedSqlEstimatedSize)
        {
            const int sizeFactor = 10;
            int estimateFactor = 0;
            double size = candidateJoinUnit.EdgeDegrees;
            double estimatedSize = candidateJoinUnit.SqlEstimatedEdgeDegrees;
            double shrinkSize = candidateJoinUnit.TreeRoot.EstimatedRows;
            WTableReference tableReference = candidateJoinUnit.ToTableReference(nodeAlias, metaData);
            affectedSqlEstimatedSize = 1.0;
            joinCondition = null;

            if (size > sizeFactor * estimatedSize)
            {
                estimateFactor = (int)Math.Ceiling(size / estimatedSize);
            }
            else if (sizeFactor*size < estimatedSize)
            {
                shrinkSize = 1.0/(1 - Math.Pow((1 - 1.0/shrinkSize), 1.5));
                affectedSqlEstimatedSize /= shrinkSize;
                estimatedSize /= shrinkSize;
                estimateFactor = (int) Math.Ceiling(size/estimatedSize);
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
        /// <param name="joinSelectivity"></param>
        /// <param name="estimatedSelectivity"></param>
        /// <returns></returns>
        private void ConstructPhysicalJoinAndUpdateCost(
            CandidateJoinUnit nodeUnitCandidate,
            WBooleanExpression joinCondition,
            double joinSelectivity,
            double estimatedSelectivity,
            GraphMetaData metaData)
        {
            var nodeDegrees = nodeUnitCandidate.EdgeDegrees;
            var nodeUnitSize = nodeUnitCandidate.TreeRoot.EstimatedRows * nodeDegrees;
            var estimatedNodeUnitSize = nodeUnitCandidate.TreeRoot.EstimatedRows*
                                        nodeUnitCandidate.SqlEstimatedEdgeDegrees;
            var componentSize = Cardinality;
            var estimatedCompSize = SqlEstimatedSize;

            var node = nodeUnitCandidate.TreeRoot;

            // If the node is already in the component, then only multiply the degree to get the size
            double nodeUnitActualSize;
            double newCompEstSize;
            if (MaterializedNodeSplitCount[node] > 0)
            {
                nodeUnitActualSize = nodeDegrees;
                var cEstEdge = Math.Pow(1000, EdgeMaterilizedDict.Count(e => !e.Value));
                var cSize = SqlEstimatedSize / cEstEdge;
                var nSize = node.EstimatedRows;
                if (nSize > cSize)
                {
                    newCompEstSize = estimatedNodeUnitSize * cEstEdge * estimatedSelectivity;
                }
                else
                {
                    newCompEstSize = SqlEstimatedSize * Math.Pow(1000, nodeUnitCandidate.UnmaterializedEdges.Count) * estimatedSelectivity;
                }
            }
            else
            {
                nodeUnitActualSize = nodeUnitSize;
                newCompEstSize = SqlEstimatedSize * estimatedNodeUnitSize * estimatedSelectivity;
            }
            newCompEstSize = newCompEstSize < 1.0 ? 1.0 : newCompEstSize;

            

            

            bool firstJoin = MaterializedNodeSplitCount.Count == 2 &&
                             MaterializedNodeSplitCount.All(e => e.Value == 0);

            // Update TableRef
            double loopJoinOuterThreshold = 1e4;//1e6;
            double sizeFactor = 5;//1000;
            double maxMemory = 1e8;
            double loopCost = componentSize*Math.Log(nodeUnitCandidate.TreeRoot.EstimatedRows, 512) * 0.20;
            double hashCost = componentSize + nodeUnitSize;
            double cost;

            // Loop Join
            if (
                nodeUnitCandidate.MaterializedEdges.Count == 0 && // the joins are purely leaf to sink join
                (
                    //componentSize < loopJoinOuterThreshold ||     // the outer table is relatively small
                    loopCost < hashCost ||
                    (DeltaMemory + componentSize > maxMemory && DeltaMemory + nodeUnitSize > maxMemory) // memory is in pressure
                ) 
               )
            {
                if (firstJoin)
                {
                    RightestTableRefSize = nodeUnitCandidate.TreeRoot.EstimatedRows;
                    RightestTableAlias = GetNodeRefName(node);
                }
                TotalMemory = DeltaMemory;
                SqlEstimatedTotalMemory = SqlEstimatedDeltaMemory;
                //joinTable.JoinHint = JoinHint.Loop;
                SqlEstimatedSize = estimatedCompSize * estimatedNodeUnitSize /
                                         nodeUnitCandidate.TreeRoot.TableRowCount;

                cost = loopCost; //componentSize*Math.Log(nodeUnitCandidate.TreeRoot.EstimatedRows, 512);
                TableRef = new WParenthesisTableReference
                {
                    Table = new WQualifiedJoin
                    {
                        FirstTableRef = TableRef,
                        SecondTableRef =
                            nodeUnitCandidate.ToTableReference(GetNodeRefName(nodeUnitCandidate.TreeRoot), metaData),
                        JoinCondition = joinCondition,
                        QualifiedJoinType = QualifiedJoinType.Inner,
                        JoinHint = JoinHint.Loop
                    }
                };
            }
            // Hash Join
            else
            {
                cost = hashCost;//componentSize + nodeUnitSize;
                WBooleanExpression adjustedJoincondition;
                double adjustedSqlEstimatedSize;
                WTableReference buildTableReference;
                WTableReference probeTableReference;
                if (firstJoin)
                {
                    var nodeInComp = MaterializedNodeSplitCount.Keys.First(e => e != node);
                    if (nodeUnitSize < componentSize)
                    {
                        buildTableReference = AdjustEstimation(nodeUnitCandidate, GetNodeRefName(node), metaData, out adjustedJoincondition,
                            out adjustedSqlEstimatedSize);
                        probeTableReference = TableRef;
                        TotalMemory = DeltaMemory = nodeUnitSize;
                        SqlEstimatedTotalMemory = SqlEstimatedDeltaMemory = estimatedNodeUnitSize;
                        RightestTableRefSize = nodeInComp.EstimatedRows;
                        RightestTableAlias = GetNodeRefName(nodeInComp);
                        
                    }
                    else
                    {
                        RightestTableRefSize = nodeInComp.EstimatedRows;
                        RightestTableAlias = GetNodeRefName(nodeInComp);
                        buildTableReference = AdjustEstimation(this, out adjustedJoincondition, out adjustedSqlEstimatedSize);
                        probeTableReference =
                            nodeUnitCandidate.ToTableReference(GetNodeRefName(nodeUnitCandidate.TreeRoot), metaData);
                        TotalMemory = DeltaMemory = componentSize;
                        SqlEstimatedTotalMemory = SqlEstimatedDeltaMemory = SqlEstimatedSize;
                        RightestTableRefSize = nodeUnitCandidate.TreeRoot.EstimatedRows;
                        RightestTableAlias = GetNodeRefName(node);
                    }
                }
                // Left Deep
                else if (componentSize*sizeFactor < nodeUnitSize)
                {
                    // Adjust estimation in sql server
                    buildTableReference = AdjustEstimation(this, out adjustedJoincondition, out adjustedSqlEstimatedSize);
                    probeTableReference =
                           nodeUnitCandidate.ToTableReference(GetNodeRefName(nodeUnitCandidate.TreeRoot), metaData);
                    var curDeltaMemory = componentSize;
                    TotalMemory = DeltaMemory + curDeltaMemory;
                    DeltaMemory = curDeltaMemory;
                    var curDeltaEstimateMemory = SqlEstimatedSize;
                    SqlEstimatedTotalMemory = SqlEstimatedDeltaMemory + curDeltaEstimateMemory;
                    SqlEstimatedDeltaMemory = curDeltaEstimateMemory;

                    
                    RightestTableAlias = GetNodeRefName(node);
                    RightestTableRefSize = nodeUnitCandidate.TreeRoot.EstimatedRows;

                }
                // Right Deep
                else
                {
                    buildTableReference = AdjustEstimation(nodeUnitCandidate, GetNodeRefName(node), metaData, out adjustedJoincondition,
                           out adjustedSqlEstimatedSize);
                    probeTableReference = TableRef;

                    TotalMemory += nodeUnitSize;
                    DeltaMemory = TotalMemory;
                    SqlEstimatedTotalMemory += estimatedNodeUnitSize;
                    SqlEstimatedDeltaMemory = SqlEstimatedTotalMemory;
                }
                newCompEstSize *= adjustedSqlEstimatedSize;
                TableRef = new WParenthesisTableReference
                {
                    Table =
                        new WQualifiedJoin
                        {
                            FirstTableRef = buildTableReference,
                            SecondTableRef = probeTableReference,
                            JoinCondition = WBooleanBinaryExpression.Conjunction(joinCondition,adjustedJoincondition),
                            QualifiedJoinType = QualifiedJoinType.Inner,
                            JoinHint = JoinHint.Hash
                        }
                };
                SqlEstimatedSize = newCompEstSize < 1.0 ? 1.0 : newCompEstSize;
            }

            //Update Size
            Cardinality *= nodeUnitActualSize * joinSelectivity;

            // Debug
#if DEBUG
            //foreach (var item in MaterializedNodeSplitCount.Where(e => e.Key != node))
            //{
            //    Trace.Write(item.Key.RefAlias + ",");
            //}
            //Trace.Write(node.RefAlias);
            //Trace.Write(" Size:" + Cardinality + " Cost:" + cost);
            //Trace.Write(" Method:" + ((TableRef as WParenthesisTableReference).Table as WQualifiedJoin).JoinHint);
            //Trace.WriteLine(" --> Total Cost:" + Cost);
#endif


            // Update Cost
            Cost += cost;
            

        }

        /// <summary>
        /// Span the table given the edge using cross apply
        /// </summary>
        /// <param name="tableRef"></param>
        /// <param name="edge"></param>
        /// <param name="nodeAlias"></param>
        /// <returns></returns>
        public WTableReference SpanTableRef(WTableReference tableRef, MatchEdge edge, string nodeAlias, GraphMetaData metaData)
        {
            tableRef = new WUnqualifiedJoin
            {
                FirstTableRef = tableRef,
                SecondTableRef = edge.ToSchemaObjectFunction(nodeAlias, metaData),
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
            out double joinSelectivity, 
            out double sqlEstimatedJoinSelectivity)
        {
            joinSelectivity = 1.0;
            sqlEstimatedJoinSelectivity = 1.0;


            var root = candidateTree.TreeRoot;

            WBooleanExpression joinCondition = null;
            string nodeName = "";


            // Update Nodes
            if (MaterializedNodeSplitCount.ContainsKey(root))
            {
                MaterializedNodeSplitCount[root]++;
                nodeName = GetNodeRefName(root);
                joinCondition = new WBooleanComparisonExpression
                {
                    FirstExpr = new WColumnReferenceExpression
                    {
                        ColumnType = ColumnType.Regular,
                        MultiPartIdentifier = new WMultiPartIdentifier(
                            new Identifier { Value = root.RefAlias },
                            new Identifier { Value = "GlobalNodeId" }
                            ),
                    },
                    SecondExpr = new WColumnReferenceExpression
                    {
                        ColumnType = ColumnType.Regular,
                        MultiPartIdentifier = new WMultiPartIdentifier(
                            new Identifier { Value = nodeName },
                            new Identifier { Value = "GlobalNodeId" }
                            ),
                    },
                    ComparisonType = BooleanComparisonType.Equals
                };
            }
            else
            {
                nodeName = root.RefAlias;
                if (!Nodes.Contains(root))
                    Nodes.Add(root);
                MaterializedNodeSplitCount[root] = 0;
            }

            List<double> densityList = new List<double>();

            List<MatchEdge> inEdges;
            if (UnmaterializedNodeMapping.TryGetValue(root, out inEdges))
            {
                var firstEdge = inEdges.First();
                bool materialized = EdgeMaterilizedDict[firstEdge];
                UnmaterializedNodeMapping.Remove(root);
                joinSelectivity *= 1.0 / root.TableRowCount;

                // Component materialized edge to root
                if (materialized)
                {
                    joinCondition = WBooleanBinaryExpression.Conjunction(joinCondition, new WBooleanComparisonExpression
                    {
                        FirstExpr = new WColumnReferenceExpression
                        {
                            ColumnType = ColumnType.Regular,
                            MultiPartIdentifier = new WMultiPartIdentifier(
                                new Identifier { Value = firstEdge.EdgeAlias },
                                new Identifier { Value = "Sink" }
                                ),
                        },
                        SecondExpr = new WColumnReferenceExpression
                        {
                            ColumnType = ColumnType.Regular,
                            MultiPartIdentifier = new WMultiPartIdentifier(
                                new Identifier { Value = nodeName },
                                new Identifier { Value = "GlobalNodeId" }
                                )
                        },
                        ComparisonType = BooleanComparisonType.Equals
                    });

                    densityList.Add(root.GlobalNodeIdDensity);
                }
                // Component unmaterialized edge to root                
                else
                {
                    Statistics statistics = null;
                    foreach (var edge in inEdges)
                    {
                        // Update component table
                        TableRef = SpanTableRef(TableRef, edge, GetNodeRefName(edge.SourceNode),metaData);

                        EdgeMaterilizedDict[edge] = true;
                        joinCondition = WBooleanBinaryExpression.Conjunction(joinCondition,
                            new WBooleanComparisonExpression
                            {
                                FirstExpr = new WColumnReferenceExpression
                                {
                                    ColumnType = ColumnType.Regular,
                                    MultiPartIdentifier = new WMultiPartIdentifier(
                                        new Identifier { Value = edge.EdgeAlias },
                                        new Identifier { Value = "Sink" }
                                        ),
                                },
                                SecondExpr = new WColumnReferenceExpression
                                {
                                    ColumnType = ColumnType.Regular,
                                    MultiPartIdentifier = new WMultiPartIdentifier(
                                        new Identifier { Value = nodeName },
                                        new Identifier { Value = "GlobalNodeId" }
                                        )
                                },
                                ComparisonType = BooleanComparisonType.Equals
                            });
                        double selectivity;
                        statistics = Statistics.UpdateHistogram(statistics,
                            edge.Statistics, out selectivity);
                        joinSelectivity *= selectivity;
                        densityList.Add(root.GlobalNodeIdDensity);
                    }
                    SinkNodeStatisticsDict[root] = statistics;

                }

            }

            var jointEdges = candidateTree.MaterializedEdges;
            foreach (var jointEdge in jointEdges)
            {


                EdgeMaterilizedDict[jointEdge] = true;
                var sinkNode = jointEdge.SinkNode;
                // Leaf to component materialized node
                if (MaterializedNodeSplitCount.ContainsKey(sinkNode))
                {
                    joinCondition = WBooleanBinaryExpression.Conjunction(joinCondition,
                        new WBooleanComparisonExpression
                        {
                            FirstExpr = new WColumnReferenceExpression
                            {
                                ColumnType = ColumnType.Regular,
                                MultiPartIdentifier = new WMultiPartIdentifier(
                                    new Identifier { Value = jointEdge.EdgeAlias },
                                    new Identifier { Value = "Sink" }
                                    ),
                            },
                            SecondExpr = new WColumnReferenceExpression
                            {
                                ColumnType = ColumnType.Regular,
                                MultiPartIdentifier = new WMultiPartIdentifier(
                                    new Identifier { Value = sinkNode.RefAlias },
                                    new Identifier { Value = "GlobalNodeId" }
                                    )
                            },
                            ComparisonType = BooleanComparisonType.Equals
                        });
                    Statistics sinkNodeStatistics;
                    if (!SinkNodeStatisticsDict.TryGetValue(sinkNode, out sinkNodeStatistics))
                    {
                        sinkNodeStatistics = null;
                        joinSelectivity *= 1.0 / sinkNode.TableRowCount;
                    }
                    double selectivity;
                    var statistics = Statistics.UpdateHistogram(sinkNodeStatistics,
                        jointEdge.Statistics, out selectivity);
                    joinSelectivity *= selectivity;
                    SinkNodeStatisticsDict[sinkNode] = statistics;
                    densityList.Add(sinkNode.GlobalNodeIdDensity);
                }
                // Leaf to component unmaterialized node
                else
                {
                    inEdges = UnmaterializedNodeMapping[sinkNode];
                    var firstEdge = inEdges.First();
                    bool materlizedEdge = EdgeMaterilizedDict[firstEdge];

                    // Leaf to materialized leaf
                    if (materlizedEdge)
                    {
                        joinCondition = WBooleanBinaryExpression.Conjunction(joinCondition,
                            new WBooleanComparisonExpression
                            {
                                FirstExpr = new WColumnReferenceExpression
                                {
                                    ColumnType = ColumnType.Regular,
                                    MultiPartIdentifier = new WMultiPartIdentifier(
                                        new Identifier { Value = jointEdge.EdgeAlias },
                                        new Identifier { Value = "Sink" }
                                        ),
                                },
                                SecondExpr = new WColumnReferenceExpression
                                {
                                    ColumnType = ColumnType.Regular,
                                    MultiPartIdentifier = new WMultiPartIdentifier(
                                        new Identifier { Value = firstEdge.EdgeAlias },
                                        new Identifier { Value = "Sink" }
                                        )
                                },
                                ComparisonType = BooleanComparisonType.Equals
                            });

                        densityList.Add(Statistics.DefaultDensity);
                        double selectivity;
                        var statistics = Statistics.UpdateHistogram(SinkNodeStatisticsDict[sinkNode],
                            jointEdge.Statistics, out selectivity);
                        joinSelectivity *= selectivity;
                        SinkNodeStatisticsDict[sinkNode] = statistics;
                    }
                    // Leaf to unmaterialized leaf
                    else
                    {
                        Statistics compSinkNodeStatistics = null;
                        foreach (var inEdge in inEdges)
                        {
                            TableRef = SpanTableRef(TableRef, inEdge, GetNodeRefName(inEdge.SourceNode),metaData);
                            EdgeMaterilizedDict[inEdge] = true;
                            joinCondition = WBooleanBinaryExpression.Conjunction(joinCondition,
                            new WBooleanComparisonExpression
                            {
                                FirstExpr = new WColumnReferenceExpression
                                {
                                    ColumnType = ColumnType.Regular,
                                    MultiPartIdentifier = new WMultiPartIdentifier(
                                        new Identifier { Value = jointEdge.EdgeAlias },
                                        new Identifier { Value = "Sink" }
                                        ),
                                },
                                SecondExpr = new WColumnReferenceExpression
                                {
                                    ColumnType = ColumnType.Regular,
                                    MultiPartIdentifier = new WMultiPartIdentifier(
                                        new Identifier { Value = inEdge.EdgeAlias },
                                        new Identifier { Value = "Sink" }
                                        )
                                },
                                ComparisonType = BooleanComparisonType.Equals
                            });

                            densityList.Add(Statistics.DefaultDensity);

                            double selectivity;
                            var leafToLeafStatistics = statisticsCalculator.GetLeafToLeafStatistics(jointEdge, inEdge,
                                out selectivity);
                            joinSelectivity *= selectivity;
                            compSinkNodeStatistics =
                                Statistics.UpdateHistogram(compSinkNodeStatistics,
                                    inEdge.Statistics, out selectivity);
                        }
                        SinkNodeStatisticsDict[sinkNode] = compSinkNodeStatistics;
                    }
                }
            }

            var unmatEdges = candidateTree.UnmaterializedEdges;
            foreach (var unmatEdge in unmatEdges)
            {
                EdgeMaterilizedDict[unmatEdge] = false;
                if (!Nodes.Contains(unmatEdge.SinkNode))
                    Nodes.Add(unmatEdge.SinkNode);
                var sinkNodeInEdges = UnmaterializedNodeMapping.GetOrCreate(unmatEdge.SinkNode);
                sinkNodeInEdges.Add(unmatEdge);

            }

            // Calculate Estimated Join Selectivity & Estimated Node Size
            densityList.Sort();
            for (int i = densityList.Count - 1; i >= 0; i--)
            {
                sqlEstimatedJoinSelectivity *= Math.Sqrt(sqlEstimatedJoinSelectivity) * densityList[i];
            }

            return joinCondition;
        }


        /// <summary>
        /// Transit from current component to the new component in the next state given the Node Unit
        /// </summary>
        /// <param name="candidateTree"></param>
        /// <param name="statisticsCalculator"></param>
        /// <returns></returns>
        public MatchComponent GetNextState(
            CandidateJoinUnit candidateTree, 
            IMatchJoinStatisticsCalculator statisticsCalculator,
            GraphMetaData metaData)
        {
            // Deep copy the component
            var newComponent = new MatchComponent(this);

            // Constrcuts join conditions and retrieves join selectivity
            double joinSelectivity;
            double sqlEstimatedJoinSelectivity;
            var joinCondition = newComponent.ConstructJoinCondition(candidateTree, statisticsCalculator,metaData, out joinSelectivity,
                out sqlEstimatedJoinSelectivity);

            // Constructs physical join method and join table references
            newComponent.ConstructPhysicalJoinAndUpdateCost(candidateTree, joinCondition,
               joinSelectivity, sqlEstimatedJoinSelectivity,metaData);

            return newComponent;
        }
    }
}


