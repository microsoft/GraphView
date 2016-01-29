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
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    /// <summary>
    /// A 1-height tree is a node with one or more outgoing edges. 
    /// A 1-height tree is translated to a table alias, plus one or more Transpose() functions. 
    /// One Transpose() function populates instances of one edge. 
    /// </summary>
    internal class OneHeightTree
    {
        public MatchNode TreeRoot { get; set; }
        // TreeRoot's outgoing edges that are materialized by Transpose() functions.
        public List<MatchEdge> MaterializedEdges { get; set; }
        // TreeRoot's edges that are not yet materialized. Lazy materialization may be beneficial for performance, 
        // because it reduces the number of intermediate resutls. 
        public List<MatchEdge> UnmaterializedEdges { get; set; }
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

        public WSqlTableContext Context { get; set; }

        public GraphMetaData MetaData { get; set; }

        
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
            Context = component.Context;
            MetaData = component.MetaData;
        }

        public MatchComponent(MatchNode node, List<MatchEdge> populatedEdges, WSqlTableContext context, GraphMetaData metaData) : this(node)
        {
            Context = context;
            MetaData = metaData;
            foreach (var edge in populatedEdges)
            {
                TableRef = SpanTableRef(TableRef, edge, node.RefAlias);
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

        /// <summary>
        /// Calculate the number used for adjusting the SQL Server estimation in the downsize function.
        /// </summary>
        /// <param name="component"></param>
        /// <param name="tablfRef"></param>
        /// <param name="joinTable"></param>
        /// <param name="size"></param>
        /// <param name="estimatedSize"></param>
        /// <param name="shrinkSize"></param>
        /// <param name="nodeAlias"></param>
        private static void AdjustEstimation(
            MatchComponent component,
            WTableReference tablfRef,
            WQualifiedJoin joinTable,
            double size,
            double estimatedSize,
            double shrinkSize,
            string nodeAlias
            )
        {
            const int sizeFactor = 10;
            int estimateFactor = 0;
            if (size > sizeFactor*estimatedSize)
            {
                estimateFactor = (int)Math.Ceiling(size / estimatedSize);
            }
            else if (sizeFactor*size < estimatedSize)
            {
                shrinkSize = 1.0/(1 - Math.Pow((1 - 1.0/shrinkSize), 1.5));
                if (estimatedSize < shrinkSize)
                {
                    component.SqlEstimatedSize /= estimatedSize;
                    estimatedSize = 1;
                }
                else
                {
                    component.SqlEstimatedSize /= shrinkSize;
                    estimatedSize /= shrinkSize;
                }
                estimateFactor = (int) Math.Ceiling(size/estimatedSize);

                // Add disjunctive DownSize predicates into the current join condition
                var downSizeFunctionCall = new WFunctionCall
                {
                    CallTarget = new WMultiPartIdentifierCallTarget
                    {
                        Identifiers = new WMultiPartIdentifier(new Identifier {Value = "dbo"})
                    },
                    FunctionName = new Identifier {Value = "DownSizeFunction"},
                    Parameters = new List<WScalarExpression>
                    {
                        new WColumnReferenceExpression
                        {
                            MultiPartIdentifier = new WMultiPartIdentifier
                            {
                                Identifiers = new List<Identifier>
                                {
                                    new Identifier {Value = nodeAlias},
                                    new Identifier {Value = "LocalNodeid"}
                                }
                            }
                        }
                    }
                };
                joinTable.JoinCondition = WBooleanBinaryExpression.Conjunction(joinTable.JoinCondition,
                    new WBooleanParenthesisExpression
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
                    });
            }
            if (estimateFactor > 1)
            {
                // Add UpSize table-valued function
                WTableReference crossApplyTable = tablfRef;
                int pow = (int) (Math.Floor(Math.Log(estimateFactor, 1000)) + 1);
                int adjustValue = (int) Math.Pow(estimateFactor, 1.0/pow);
                while (pow > 0)
                {
                    crossApplyTable = new WUnqualifiedJoin
                    {
                        FirstTableRef = crossApplyTable,
                        SecondTableRef = new WSchemaObjectFunctionTableReference
                        {
                            SchemaObject = new WSchemaObjectName(
                                new Identifier {Value = "dbo"},
                                new Identifier {Value = "UpSizeFunction"}),
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
                    component.SqlEstimatedSize *= adjustValue;
                }
                joinTable.FirstTableRef = crossApplyTable;
            }
        }


        /// <summary>
        /// Calculate join costs and update components using optimal join method & order
        /// </summary>
        /// <param name="nodeUnitCandidate"></param>
        /// <param name="component"></param>
        /// <param name="nodeTable"></param>
        /// <param name="componentTable"></param>
        /// <param name="joinCondition"></param>
        /// <param name="nodeDegrees"></param>
        /// <param name="joinSelectivity"></param>
        /// <param name="estimatedNodeUnitSize"></param>
        /// <param name="estimatedSelectivity"></param>
        /// <returns></returns>
        private static WTableReference GetPlanAndUpdateCost(
            OneHeightTree nodeUnitCandidate,
            MatchComponent component,
            WTableReference nodeTable,
            WTableReference componentTable,
            WBooleanExpression joinCondition,
            double nodeDegrees,
            double joinSelectivity,
            double estimatedNodeUnitSize,
            double estimatedSelectivity)
        {
            var nodeUnitSize = nodeUnitCandidate.TreeRoot.EstimatedRows * nodeDegrees;
            var componentSize = component.Cardinality;
            var estimatedCompSize = component.SqlEstimatedSize;
            
            // Sets to leaf deep hash join by default
            WQualifiedJoin joinTable = new WQualifiedJoin
            {
                FirstTableRef = componentTable,
                SecondTableRef = nodeTable,
                JoinCondition = joinCondition,
                QualifiedJoinType = QualifiedJoinType.Inner,
                JoinHint = JoinHint.Hash
            };

            var node = nodeUnitCandidate.TreeRoot;

            // If the node is already in the component, then only multiply the degree to get the size
            double nodeUnitActualSize;
            double newCompEstSize;
            if (component.MaterializedNodeSplitCount[node] > 0)
            {
                nodeUnitActualSize = nodeDegrees;
                var cEstEdge = Math.Pow(1000, component.EdgeMaterilizedDict.Count(e => !e.Value));
                var cSize = component.SqlEstimatedSize / cEstEdge;
                var nSize = node.EstimatedRows;
                if (nSize > cSize)
                {
                    newCompEstSize = estimatedNodeUnitSize * cEstEdge * estimatedSelectivity;
                }
                else
                {
                    newCompEstSize = component.SqlEstimatedSize * Math.Pow(1000, nodeUnitCandidate.UnmaterializedEdges.Count) * estimatedSelectivity;
                }
            }
            else
            {
                nodeUnitActualSize = nodeUnitSize;
                newCompEstSize = component.SqlEstimatedSize * estimatedNodeUnitSize * estimatedSelectivity;
            }
            component.SqlEstimatedSize = newCompEstSize < 1.0 ? 1.0 : newCompEstSize;
           

            //Update Size
            component.Cardinality *= nodeUnitActualSize * joinSelectivity;

            

            bool firstJoin = component.MaterializedNodeSplitCount.Count == 2 &&
                             component.MaterializedNodeSplitCount.All(e => e.Value == 0);

            // Update TableRef
            double loopJoinOuterThreshold = 1e4;//1e6;
            double sizeFactor = 5;//1000;
            double maxMemory = 1e8;
            double loopCost = componentSize*Math.Log(nodeUnitCandidate.TreeRoot.EstimatedRows, 512);
            double hashCost = componentSize + nodeUnitSize;
            double cost;

            // Loop Join
            if (
                nodeUnitCandidate.MaterializedEdges.Count == 0 && // the joins are purely leaf to sink join
                (
                    //componentSize < loopJoinOuterThreshold ||     // the outer table is relatively small
                    loopCost < hashCost ||
                    (component.DeltaMemory + componentSize > maxMemory && component.DeltaMemory + nodeUnitSize > maxMemory) // memory is in pressure
                ) 
               )
            {
                if (firstJoin)
                {
                    component.RightestTableRefSize = nodeUnitCandidate.TreeRoot.EstimatedRows;
                    component.RightestTableAlias = component.GetNodeRefName(node);
                }
                component.TotalMemory = component.DeltaMemory;
                component.SqlEstimatedTotalMemory = component.SqlEstimatedDeltaMemory;
                joinTable.JoinHint = JoinHint.Loop;
                component.SqlEstimatedSize = estimatedCompSize * estimatedNodeUnitSize /
                                         nodeUnitCandidate.TreeRoot.TableRowCount;

                cost = loopCost; //componentSize*Math.Log(nodeUnitCandidate.TreeRoot.EstimatedRows, 512);
            }
            // Hash Join
            else
            {
                cost = hashCost;//componentSize + nodeUnitSize;
                if (firstJoin)
                {
                    var nodeInComp = component.MaterializedNodeSplitCount.Keys.First(e => e != node);
                    if (nodeUnitSize < componentSize)
                    {
                        joinTable.FirstTableRef = nodeTable;
                        joinTable.SecondTableRef = componentTable;
                        component.TotalMemory = component.DeltaMemory = nodeUnitSize;
                        component.SqlEstimatedTotalMemory = component.SqlEstimatedDeltaMemory = estimatedNodeUnitSize;
                        component.RightestTableRefSize = nodeInComp.EstimatedRows;
                        component.RightestTableAlias = component.GetNodeRefName(nodeInComp);
                        AdjustEstimation(component, nodeTable, joinTable, nodeUnitSize, estimatedNodeUnitSize,
                            nodeUnitCandidate.TreeRoot.EstimatedRows, component.GetNodeRefName(node));
                    }
                    else
                    {
                        component.TotalMemory = component.DeltaMemory = componentSize;
                        component.SqlEstimatedTotalMemory = component.SqlEstimatedDeltaMemory = component.SqlEstimatedSize;
                        component.RightestTableRefSize = nodeUnitCandidate.TreeRoot.EstimatedRows;
                        component.RightestTableAlias = component.GetNodeRefName(node);
                        AdjustEstimation(component, componentTable, joinTable, componentSize, estimatedCompSize,
                            nodeInComp.EstimatedRows, component.GetNodeRefName(nodeInComp));
                    }
                }
                // Left Deep
                else if (componentSize*sizeFactor < nodeUnitSize)
                {
                    var curDeltaMemory = componentSize;
                    component.TotalMemory = component.DeltaMemory + curDeltaMemory;
                    component.DeltaMemory = curDeltaMemory;
                    var curDeltaEstimateMemory = component.SqlEstimatedSize;
                    component.SqlEstimatedTotalMemory = component.SqlEstimatedDeltaMemory + curDeltaEstimateMemory;
                    component.SqlEstimatedDeltaMemory = curDeltaEstimateMemory;

                    // Adjust estimation in sql server
                    AdjustEstimation(component, componentTable, joinTable, componentSize, estimatedCompSize,
                        component.RightestTableRefSize, component.RightestTableAlias);
                    component.RightestTableAlias = component.GetNodeRefName(node);
                    component.RightestTableRefSize = nodeUnitCandidate.TreeRoot.EstimatedRows;

                }
                // Right Deep
                else
                {
                    joinTable.FirstTableRef = nodeTable;
                    joinTable.SecondTableRef = componentTable;

                    AdjustEstimation(component, nodeTable, joinTable, nodeUnitSize, estimatedNodeUnitSize,
                        node.EstimatedRows, component.GetNodeRefName(node));

                    component.TotalMemory += nodeUnitSize;
                    component.DeltaMemory = component.TotalMemory;
                    component.SqlEstimatedTotalMemory += estimatedNodeUnitSize;
                    component.SqlEstimatedDeltaMemory = component.SqlEstimatedTotalMemory;
                }
            }



            // Debug
#if DEBUG
            //foreach (var item in component.MaterializedNodeSplitCount.Where(e => e.Key != node))
            //{
            //    Trace.Write(item.Key.RefAlias + ",");
            //}
            //Trace.Write(node.RefAlias);
            //Trace.Write(" Size:" + component.Cardinality + " Cost:" + cost);
            //Trace.Write(" Method:" + joinTable.JoinHint);
            //Trace.WriteLine(" --> Total Cost:" + component.Cost);
#endif


            // Update Cost
            component.Cost += cost;

            return new WParenthesisTableReference
            {
                Table = joinTable
            };
        }

        /// <summary>
        /// Span the table given the edge using cross apply
        /// </summary>
        /// <param name="tableRef"></param>
        /// <param name="edge"></param>
        /// <param name="nodeAlias"></param>
        /// <returns></returns>
        public WTableReference SpanTableRef(WTableReference tableRef, MatchEdge edge, string nodeAlias)
        {
            tableRef = new WUnqualifiedJoin
            {
                FirstTableRef = tableRef,
                SecondTableRef = edge.ToSchemaObjectFunction(nodeAlias,MetaData),
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


        /// <summary>
        /// Transit from current component to the new component in the next state given the Node Unit
        /// </summary>
        /// <param name="candidateTree"></param>
        /// <param name="statisticsCalculator"></param>
        /// <returns></returns>
        public MatchComponent GetNextState(
            OneHeightTree candidateTree, 
            IMatchJoinStatisticsCalculator statisticsCalculator)
        {
            var newComponent = new MatchComponent(this);
            var root = candidateTree.TreeRoot;

            WBooleanExpression joinCondition = null;
            string nodeName = "";


            // Update Nodes
            if (newComponent.MaterializedNodeSplitCount.ContainsKey(root))
            {
                newComponent.MaterializedNodeSplitCount[root]++;
                nodeName = newComponent.GetNodeRefName(root);
                joinCondition = new WBooleanComparisonExpression
                {
                    FirstExpr = new WColumnReferenceExpression
                    {
                        ColumnType = ColumnType.Regular,
                        MultiPartIdentifier = new WMultiPartIdentifier(
                            new Identifier {Value = root.RefAlias},
                            new Identifier {Value = "GlobalNodeId"}
                            ),
                    },
                    SecondExpr = new WColumnReferenceExpression
                    {
                        ColumnType = ColumnType.Regular,
                        MultiPartIdentifier = new WMultiPartIdentifier(
                            new Identifier {Value = nodeName},
                            new Identifier {Value = "GlobalNodeId"}
                            ),
                    },
                    ComparisonType = BooleanComparisonType.Equals
                };
            }
            else
            {
                nodeName = root.RefAlias;
                if (!Nodes.Contains(root))
                    newComponent.Nodes.Add(root);
                newComponent.MaterializedNodeSplitCount[root] = 0;
            }

            // Constructs table reference
            WTableReference nodeTable = new WNamedTableReference
            {
                Alias = new Identifier { Value = nodeName },
                TableObjectName = root.NodeTableObjectName
            };
            WTableReference compTable = newComponent.TableRef;

            // Updates join conditions
            double joinSelectivity = 1.0;
            double degrees = 1.0;
            List<double> densityList = new List<double>();

            List<MatchEdge> inEdges;
            if (newComponent.UnmaterializedNodeMapping.TryGetValue(root, out inEdges))
            {
                var firstEdge = inEdges.First();
                bool materialized = newComponent.EdgeMaterilizedDict[firstEdge];
                newComponent.UnmaterializedNodeMapping.Remove(root);
                joinSelectivity *= 1.0/root.TableRowCount;

                // Component materialized edge to root
                if (materialized)
                {
                    joinCondition = WBooleanBinaryExpression.Conjunction(joinCondition, new WBooleanComparisonExpression
                    {
                        FirstExpr = new WColumnReferenceExpression
                        {
                            ColumnType = ColumnType.Regular,
                            MultiPartIdentifier = new WMultiPartIdentifier(
                                new Identifier {Value = firstEdge.EdgeAlias},
                                new Identifier {Value = "Sink"}
                                ),
                        },
                        SecondExpr = new WColumnReferenceExpression
                        {
                            ColumnType = ColumnType.Regular,
                            MultiPartIdentifier = new WMultiPartIdentifier(
                                new Identifier {Value = nodeName},
                                new Identifier {Value = "GlobalNodeId"}
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
                        compTable = SpanTableRef(compTable, edge, newComponent.GetNodeRefName(edge.SourceNode));

                        newComponent.EdgeMaterilizedDict[edge] = true;
                        joinCondition = WBooleanBinaryExpression.Conjunction(joinCondition,
                            new WBooleanComparisonExpression
                            {
                                FirstExpr = new WColumnReferenceExpression
                                {
                                    ColumnType = ColumnType.Regular,
                                    MultiPartIdentifier = new WMultiPartIdentifier(
                                        new Identifier {Value = edge.EdgeAlias},
                                        new Identifier {Value = "Sink"}
                                        ),
                                },
                                SecondExpr = new WColumnReferenceExpression
                                {
                                    ColumnType = ColumnType.Regular,
                                    MultiPartIdentifier = new WMultiPartIdentifier(
                                        new Identifier {Value = nodeName},
                                        new Identifier {Value = "GlobalNodeId"}
                                        )
                                },
                                ComparisonType = BooleanComparisonType.Equals
                            });
                        double selectivity;
                        statistics = Statistics.UpdateHistogram(statistics,
                            edge.Statistics,out selectivity);
                        joinSelectivity *= selectivity;
                        densityList.Add(root.GlobalNodeIdDensity);
                    }
                    newComponent.SinkNodeStatisticsDict[root] = statistics;

                }

            }

            var jointEdges = candidateTree.MaterializedEdges;
            foreach (var jointEdge in jointEdges)
            {
                // Update node table
                nodeTable = SpanTableRef(nodeTable, jointEdge, nodeName);
                degrees *= jointEdge.AverageDegree;

                newComponent.EdgeMaterilizedDict[jointEdge] = true;
                var sinkNode = jointEdge.SinkNode;
                // Leaf to component materialized node
                if (newComponent.MaterializedNodeSplitCount.ContainsKey(sinkNode))
                {
                    joinCondition = WBooleanBinaryExpression.Conjunction(joinCondition,
                        new WBooleanComparisonExpression
                        {
                            FirstExpr = new WColumnReferenceExpression
                            {
                                ColumnType = ColumnType.Regular,
                                MultiPartIdentifier = new WMultiPartIdentifier(
                                    new Identifier {Value = jointEdge.EdgeAlias},
                                    new Identifier {Value = "Sink"}
                                    ),
                            },
                            SecondExpr = new WColumnReferenceExpression
                            {
                                ColumnType = ColumnType.Regular,
                                MultiPartIdentifier = new WMultiPartIdentifier(
                                    new Identifier {Value = sinkNode.RefAlias},
                                    new Identifier {Value = "GlobalNodeId"}
                                    )
                            },
                            ComparisonType = BooleanComparisonType.Equals
                        });
                    Statistics sinkNodeStatistics;
                    if (!newComponent.SinkNodeStatisticsDict.TryGetValue(sinkNode, out sinkNodeStatistics))
                    {
                        sinkNodeStatistics = null;
                        joinSelectivity *= 1.0/sinkNode.TableRowCount;
                    }
                    double selectivity;
                    var statistics = Statistics.UpdateHistogram(sinkNodeStatistics,
                        jointEdge.Statistics, out selectivity);
                    joinSelectivity *= selectivity;
                    newComponent.SinkNodeStatisticsDict[sinkNode] = statistics;
                    densityList.Add(sinkNode.GlobalNodeIdDensity);
                }
                // Leaf to component unmaterialized node
                else
                {
                    inEdges = newComponent.UnmaterializedNodeMapping[sinkNode];
                    var firstEdge = inEdges.First();
                    bool materlizedEdge = newComponent.EdgeMaterilizedDict[firstEdge];
                   
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
                                        new Identifier {Value = jointEdge.EdgeAlias},
                                        new Identifier {Value = "Sink"}
                                        ),
                                },
                                SecondExpr = new WColumnReferenceExpression
                                {
                                    ColumnType = ColumnType.Regular,
                                    MultiPartIdentifier = new WMultiPartIdentifier(
                                        new Identifier { Value = firstEdge.EdgeAlias},
                                        new Identifier {Value = "Sink"}
                                        )
                                },
                                ComparisonType = BooleanComparisonType.Equals
                            });

                        densityList.Add(Statistics.DefaultDensity);
                        double selectivity;
                        var statistics = Statistics.UpdateHistogram(newComponent.SinkNodeStatisticsDict[sinkNode],
                            jointEdge.Statistics,out selectivity);
                        joinSelectivity *= selectivity;
                        newComponent.SinkNodeStatisticsDict[sinkNode] = statistics;
                    }
                    // Leaf to unmaterialized leaf
                    else
                    {
                        Statistics compSinkNodeStatistics = null;
                        foreach (var inEdge in inEdges)
                        {
                            compTable = SpanTableRef(compTable, inEdge, newComponent.GetNodeRefName(inEdge.SourceNode));
                            newComponent.EdgeMaterilizedDict[inEdge] = true;
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
                                    inEdge.Statistics,out selectivity);
                        }
                        newComponent.SinkNodeStatisticsDict[sinkNode] = compSinkNodeStatistics;
                    }
                }
            }

            var unmatEdges = candidateTree.UnmaterializedEdges;
            foreach (var unmatEdge in unmatEdges)
            {
                newComponent.EdgeMaterilizedDict[unmatEdge] = false;
                if (!Nodes.Contains(unmatEdge.SinkNode))
                    newComponent.Nodes.Add(unmatEdge.SinkNode);
                var sinkNodeInEdges = newComponent.UnmaterializedNodeMapping.GetOrCreate(unmatEdge.SinkNode);
                sinkNodeInEdges.Add(unmatEdge);
                degrees *= unmatEdge.AverageDegree;

            }

            // Calculate Estimated Join Selectivity & Estimated Node Size
            double estimatedSelectity = 1.0;
            densityList.Sort();
            for (int i = densityList.Count-1; i >=0; i--)
            {
                estimatedSelectity *= Math.Sqrt(estimatedSelectity)*densityList[i];
            }

            var estimatedNodeUnitSize = root.EstimatedRows*
                                        Math.Pow(1000, candidateTree.MaterializedEdges.Count + candidateTree.UnmaterializedEdges.Count);


            // Update Table Reference
            newComponent.TableRef = GetPlanAndUpdateCost(candidateTree, newComponent, nodeTable, compTable, joinCondition,
                degrees, joinSelectivity, estimatedNodeUnitSize, estimatedSelectity);

            return newComponent;
        }
    }
}


