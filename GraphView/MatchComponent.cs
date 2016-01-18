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
        public List<MatchNode> Nodes { get; set; }
        public Dictionary<MatchEdge, bool> EdgeMaterilizedDict { get; set; }  
        //public List<MatchEdge> MaterializaedEdges { get; set; }
        //public List<MatchEdge> UnmaterializedEdges { get; set; }

        // Stores the split count of a materialized node
        public Dictionary<MatchNode, int> MaterializedNodeSplitCount { get; set; }

        // Maps the unmaterialized node to the alias of one of its incoming materialized edges;
        // the join condition between the node and the incoming edge should be added 
        // when the node is materialized.
        //public Dictionary<MatchNode, string> UnmaterializedNodeMapping { get; set; }
        public Dictionary<MatchNode, List<MatchEdge>> UnmaterializedNodeMapping { get; set; }

        // Store the statistics of the nodes in the component for further joins
        public Dictionary<MatchNode, ColumnStatistics> StatisticsDict { get; set; }

        // Used memory of the component in total
        public double TotalMemory { get; set; }

        // Latest used memory of the component. Only DeltaMemory will survive if the previous memory is released.
        public double DeltaMemory { get; set; }

        // Record the parent of the rightest table in the join to do the adjustment. Tuple(Table Reference, Table Alias)
        public Tuple<WQualifiedJoin, String> FatherOfRightestTableRef { get; set; }

        // Record the size of the rightest table in the join
        public double RightestTableRefSize { get; set; }

        // The list of the tables which additional down size predicates should be applied. List of Tuple(Table Reference, Table Alias)
        public List<Tuple<WQualifiedJoin, String>> FatherListofDownSizeTable { get; set; }

        public double Size { get; set; }
        public double EstimateTotalMemory { get; set; }
        public double EstimateDeltaMemory { get; set; }
        public double EstimateSize { get; set; }
       
        public double Cost { get; set; }
        public WTableReference TableRef { get; set; }

        public WSqlTableContext Context { get; set; }

        public GraphMetaData MetaData { get; set; }

        
        public MatchComponent()
        {
            Nodes = new List<MatchNode>();
            EdgeMaterilizedDict = new Dictionary<MatchEdge, bool>();
            MaterializedNodeSplitCount = new Dictionary<MatchNode, int>();
            UnmaterializedNodeMapping = new Dictionary<MatchNode, List<MatchEdge>>();
            StatisticsDict = new Dictionary<MatchNode, ColumnStatistics>();
            Size = 1.0;
            Cost = 0.0;
            TotalMemory = 0.0;
            DeltaMemory = 0.0;
            EstimateDeltaMemory = 0.0;
            EstimateTotalMemory = 0.0;
            EstimateSize = 1.0;
            RightestTableRefSize = 0.0;
            FatherListofDownSizeTable = new List<Tuple<WQualifiedJoin, String>>();
        }

        public MatchComponent(MatchNode node):this()
        {
            Nodes.Add(node);
            MaterializedNodeSplitCount[node] = 0;
            StatisticsDict[node] = new ColumnStatistics{Selectivity = 1.0/node.TableRowCount};

            Size *= node.EstimatedRows;
            EstimateSize *= node.EstimatedRows;
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
            Nodes = new List<MatchNode>(component.Nodes);
            EdgeMaterilizedDict = new Dictionary<MatchEdge, bool>(component.EdgeMaterilizedDict);
            MaterializedNodeSplitCount = new Dictionary<MatchNode, int>(component.MaterializedNodeSplitCount);
            UnmaterializedNodeMapping = new Dictionary<MatchNode, List<MatchEdge>>();
            foreach (var nodeMapping in component.UnmaterializedNodeMapping)
            {
                UnmaterializedNodeMapping[nodeMapping.Key] = new List<MatchEdge>(nodeMapping.Value);
            }
            StatisticsDict = new Dictionary<MatchNode, ColumnStatistics>(component.StatisticsDict);
            TableRef = component.TableRef;
            Size = component.Size;
            Cost = component.Cost;
            DeltaMemory = component.DeltaMemory;
            TotalMemory = component.TotalMemory;
            EstimateDeltaMemory = component.EstimateDeltaMemory;
            EstimateTotalMemory = component.EstimateTotalMemory;
            EstimateSize = component.EstimateSize;
            FatherOfRightestTableRef = component.FatherOfRightestTableRef;
            RightestTableRefSize = component.RightestTableRefSize;
            FatherListofDownSizeTable = new List<Tuple<WQualifiedJoin, String>>(component.FatherListofDownSizeTable);
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
                StatisticsDict[edge.SinkNode] = Context.GetEdgeStatistics(edge);
                var edgeList = UnmaterializedNodeMapping.GetOrCreate(edge.SinkNode);
                edgeList.Add(edge);
                Nodes.Add(edge.SinkNode);
                Size *= edge.AverageDegree;
                EstimateSize *= 1000;

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
        /// <param name="joinTableTuple"></param>
        private static void AdjustEstimation(
            MatchComponent component,
            WTableReference tablfRef,
            WQualifiedJoin joinTable,
            double size,
            double estimatedSize,
            double shrinkSize,
            Tuple<WQualifiedJoin,String> joinTableTuple)
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
                    component.EstimateSize /= estimatedSize;
                    estimatedSize = 1;
                }
                else
                {
                    component.EstimateSize /= shrinkSize;
                    estimatedSize /= shrinkSize;
                }
                component.FatherListofDownSizeTable.Add(joinTableTuple);
                estimateFactor = (int) Math.Ceiling(size/estimatedSize);
            }
            if (estimateFactor > 1)
            {
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
                    component.EstimateSize *= adjustValue;
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
            var componentSize = component.Size;
            var estimatedCompSize = component.EstimateSize;
            //var cost = nodeUnitSize + componentSize;
            
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
                var cSize = component.EstimateSize / cEstEdge;
                var nSize = node.EstimatedRows;
                if (nSize > cSize)
                {
                    newCompEstSize = estimatedNodeUnitSize * cEstEdge * estimatedSelectivity;
                }
                else
                {
                    newCompEstSize = component.EstimateSize * Math.Pow(1000, nodeUnitCandidate.UnmaterializedEdges.Count) * estimatedSelectivity;
                }
            }
            else
            {
                nodeUnitActualSize = nodeUnitSize;
                newCompEstSize = component.EstimateSize * estimatedNodeUnitSize * estimatedSelectivity;
            }
            component.EstimateSize = newCompEstSize < 1.0 ? 1.0 : newCompEstSize;
           

            //Update Size
            component.Size *= nodeUnitActualSize * joinSelectivity;

            

            bool firstJoin = component.MaterializedNodeSplitCount.Count == 2 &&
                             component.MaterializedNodeSplitCount.All(e => e.Value == 0);

            // Update TableRef
            double loopJoinOuterThreshold = 1e4;//1e6;
            double sizeFactor = 5;//1000;
            double maxMemory = 1e8;
            double cost;

            // Loop Join
            if (
                nodeUnitCandidate.MaterializedEdges.Count == 0 && // the joins are purely leaf to sink join
                (
                    componentSize < loopJoinOuterThreshold ||     // the outer table is relatively small
                    (component.DeltaMemory + componentSize > maxMemory && component.DeltaMemory + nodeUnitSize > maxMemory) // memory is in pressure
                ) 
               )
            {
                if (firstJoin)
                {
                    component.RightestTableRefSize = nodeUnitCandidate.TreeRoot.EstimatedRows;
                    component.FatherOfRightestTableRef = new Tuple<WQualifiedJoin, String>(joinTable, component.GetNodeRefName(node));
                }
                component.TotalMemory = component.DeltaMemory;
                component.EstimateTotalMemory = component.EstimateDeltaMemory;
                joinTable.JoinHint = JoinHint.Loop;
                component.EstimateSize = estimatedCompSize * estimatedNodeUnitSize /
                                         nodeUnitCandidate.TreeRoot.TableRowCount;
                
                //cost = componentSize*Math.Log10(nodeUnitSize);
                cost = componentSize*Math.Log(nodeUnitCandidate.TreeRoot.EstimatedRows, 512);
                //cost = 2*componentSize;
                //cost = componentSize + nodeUnitSize;
            }
            // Hash Join
            else
            {
                cost = componentSize + nodeUnitSize;
                if (firstJoin)
                {
                    var nodeInComp = component.MaterializedNodeSplitCount.Keys.First(e => e != node);
                    if (nodeUnitSize < componentSize)
                    {
                        joinTable.FirstTableRef = nodeTable;
                        joinTable.SecondTableRef = componentTable;
                        component.TotalMemory = component.DeltaMemory = nodeUnitSize;
                        component.EstimateTotalMemory = component.EstimateDeltaMemory = estimatedNodeUnitSize;
                        component.RightestTableRefSize = nodeInComp.EstimatedRows;
                        component.FatherOfRightestTableRef = new Tuple<WQualifiedJoin, String>(joinTable,
                            component.GetNodeRefName(nodeInComp));
                        AdjustEstimation(component, nodeTable, joinTable, nodeUnitSize, estimatedNodeUnitSize,
                            nodeUnitCandidate.TreeRoot.EstimatedRows, new Tuple<WQualifiedJoin, string>(joinTable, component.GetNodeRefName(node)));
                    }
                    else
                    {
                        component.TotalMemory = component.DeltaMemory = componentSize;
                        component.EstimateTotalMemory = component.EstimateDeltaMemory = component.EstimateSize;
                        component.RightestTableRefSize = nodeUnitCandidate.TreeRoot.EstimatedRows;
                        component.FatherOfRightestTableRef = new Tuple<WQualifiedJoin, String>(joinTable, component.GetNodeRefName(node));
                        AdjustEstimation(component, componentTable, joinTable, componentSize, estimatedCompSize,
                            nodeInComp.EstimatedRows, new Tuple<WQualifiedJoin, string>(joinTable, component.GetNodeRefName(nodeInComp)));
                    }
                }
                // Left Deep
                else if (componentSize*sizeFactor < nodeUnitSize)
                {
                    var curDeltaMemory = componentSize;
                    component.TotalMemory = component.DeltaMemory + curDeltaMemory;
                    component.DeltaMemory = curDeltaMemory;
                    var curDeltaEstimateMemory = component.EstimateSize;
                    component.EstimateTotalMemory = component.EstimateDeltaMemory + curDeltaEstimateMemory;
                    component.EstimateDeltaMemory = curDeltaEstimateMemory;

                    // Adjust estimation in sql server
                    AdjustEstimation(component, componentTable, joinTable, componentSize, estimatedCompSize,
                        component.RightestTableRefSize, component.FatherOfRightestTableRef);
                    component.FatherOfRightestTableRef = new Tuple<WQualifiedJoin, string>(joinTable,
                        component.GetNodeRefName(node));
                    component.RightestTableRefSize = nodeUnitCandidate.TreeRoot.EstimatedRows;

                }
                // Right Deep
                else
                {
                    joinTable.FirstTableRef = nodeTable;
                    joinTable.SecondTableRef = componentTable;

                    AdjustEstimation(component, nodeTable, joinTable, nodeUnitSize, estimatedNodeUnitSize,
                        node.EstimatedRows, new Tuple<WQualifiedJoin, string>(joinTable, component.GetNodeRefName(node)));

                    component.TotalMemory += nodeUnitSize;
                    component.DeltaMemory = component.TotalMemory;
                    component.EstimateTotalMemory += estimatedNodeUnitSize;
                    component.EstimateDeltaMemory = component.EstimateTotalMemory;
                }
            }



            // Debug
#if DEBUG
            //foreach (var item in component.MaterializedNodeSplitCount.Where(e => e.Key != node))
            //{
            //    Trace.Write(item.Key.RefAlias + ",");
            //}
            //Trace.Write(node.RefAlias);
            //Trace.Write(" Size:" + component.Size + " Cost:" +
            //                (componentSize + nodeUnitSize));
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
        /// Generate the Table-Valued Function by the edge given the node alias
        /// </summary>
        /// <param name="edge"></param>
        /// <param name="nodeAlias"></param>
        /// <returns></returns>
        private WTableReference EdgeToTableReference(MatchEdge edge, string nodeAlias)
        {
            var edgeIdentifiers = edge.EdgeColumn.MultiPartIdentifier.Identifiers;
            var edgeColIdentifier = edgeIdentifiers.Last();
            Identifier srcNodeIdentifier = new Identifier{Value = nodeAlias};
            
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            // The source is a regular node
            if (edge.SourceNode.IncludedNodeNames == null)
            {
                // The edge is a regular edge
                if (edge.IncludedEdgeNames == null)
                {
                    parameters.Add(new WColumnReferenceExpression
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier(srcNodeIdentifier, edgeColIdentifier)
                    });
                    parameters.Add(new WColumnReferenceExpression
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier(srcNodeIdentifier,
                                new Identifier {Value = edgeColIdentifier.Value + "DeleteCol"})
                    });
                }
                // The edge is a edge view
                else
                {
                    foreach (var column in edge.IncludedEdgeNames)
                    {
                        Identifier includedEdgeColumnIdentifier = new Identifier{Value = column.Item2};
                        parameters.Add(new WColumnReferenceExpression
                        {
                            MultiPartIdentifier =
                                new WMultiPartIdentifier(srcNodeIdentifier, includedEdgeColumnIdentifier)
                        });
                        parameters.Add(new WColumnReferenceExpression
                        {
                            MultiPartIdentifier =
                                new WMultiPartIdentifier(srcNodeIdentifier,
                                    new Identifier { Value = includedEdgeColumnIdentifier.Value + "DeleteCol" })
                        });
                    }
                }
            }
            // The source is a node view
            else
            {
                // The edge is a regular edge
                if (edge.IncludedEdgeNames == null)
                {
                    string srcTableName = edge.BindNodeTableObjName.BaseIdentifier.Value;
                    Identifier nodeViewEdgeColIdentifier = new Identifier
                    {
                        Value = srcTableName + "_" + edgeColIdentifier.Value
                    };
                    parameters.Add(new WColumnReferenceExpression
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier(srcNodeIdentifier, nodeViewEdgeColIdentifier)
                    });
                    parameters.Add(new WColumnReferenceExpression
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier(srcNodeIdentifier,
                                new Identifier { Value = nodeViewEdgeColIdentifier.Value + "DeleteCol" })
                    });
                    
                }
                // The edge is a edge view
                else
                {
                    foreach (var column in edge.IncludedEdgeNames)
                    {
                        if (edge.SourceNode.IncludedNodeNames.Contains(column.Item1))
                        {
                            Identifier includedEdgeColumnIdentifier = new Identifier
                            {
                                Value = column.Item1 + "_" + column.Item2
                            };
                            parameters.Add(new WColumnReferenceExpression
                            {
                                MultiPartIdentifier =
                                    new WMultiPartIdentifier(srcNodeIdentifier, includedEdgeColumnIdentifier)
                            });
                            parameters.Add(new WColumnReferenceExpression
                            {
                                MultiPartIdentifier =
                                    new WMultiPartIdentifier(srcNodeIdentifier,
                                        new Identifier {Value = includedEdgeColumnIdentifier.Value + "DeleteCol"})
                            });
                        }
                        else
                        {
                            parameters.Add(new WValueExpression{Value = "null"});
                            parameters.Add(new WValueExpression {Value = "null"});
                        }
                    }
                }
            }

            string decoderFunctionName;
            if (edge.IsPath)
            {
                decoderFunctionName = edge.BindNodeTableObjName.SchemaIdentifier.Value + '_' +
                                      edge.BindNodeTableObjName.BaseIdentifier.Value + '_' +
                                      edgeColIdentifier.Value + '_' +
                                      "bfs";
                parameters.Insert(0,new WValueExpression { Value = edge.MaxLength.ToString() });
                parameters.Insert(0,new WValueExpression { Value = edge.MinLength.ToString() });
                parameters.Insert(0,
                    new WColumnReferenceExpression
                    {
                        MultiPartIdentifier =
                            new WMultiPartIdentifier(new[] {srcNodeIdentifier, new Identifier {Value = "GlobalNodeId"},})
                    });
                
                var attributes =
                        MetaData.ColumnsOfNodeTables[WNamedTableReference.SchemaNameToTuple(edge.BindNodeTableObjName)][
                            edgeColIdentifier.Value.ToLower()].EdgeInfo.ColumnAttributes;
                if (edge.AttributeValueDict == null)
                {
                    WValueExpression nullExpression = new WValueExpression {Value = "null"};
                    for (int i = 0; i < attributes.Count; i++)
                        parameters.Add(nullExpression);
                }
                else
                {
                    foreach (var attribute in attributes)
                    {
                        string value;
                        var valueExpression = new WValueExpression
                        {
                            Value = edge.AttributeValueDict.TryGetValue(attribute, out value) ? value : "null"
                        };

                        parameters.Add(valueExpression);
                    }
                }
            }
            else
            {
                decoderFunctionName = edge.BindNodeTableObjName.SchemaIdentifier.Value + '_' +
                                      edge.BindNodeTableObjName.BaseIdentifier.Value + '_' +
                                      edgeColIdentifier.Value + '_' +
                                      "Decoder";
            }
            var decoderFunction = new Identifier {Value = decoderFunctionName};
            var tableRef = new WSchemaObjectFunctionTableReference
            {
                SchemaObject = new WSchemaObjectName(
                    new Identifier { Value = "dbo" },
                    decoderFunction),
                Parameters = parameters,
                Alias = new Identifier
                {
                    Value = edge.EdgeAlias,
                }
            };
            return tableRef;
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
                SecondTableRef = EdgeToTableReference(edge, nodeAlias),
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
        /// <param name="graphMetaData"></param>
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
                newComponent.Nodes.Add(root);
                newComponent.MaterializedNodeSplitCount[root] = 0;
                newComponent.StatisticsDict[root] = new ColumnStatistics {Selectivity = 1.0/root.TableRowCount};

            }

            // Constructs table reference
            WTableReference nodeTable = new WNamedTableReference
            {
                Alias = new Identifier { Value = nodeName },
                TableObjectName = root.NodeTableObjectName
            };
            WTableReference compTable = newComponent.TableRef;

            // Updates join conditions
            double selectivity = 1.0;
            double degrees = 1.0;
            var DensityCount = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            List<MatchEdge> inEdges;
            if (newComponent.UnmaterializedNodeMapping.TryGetValue(root, out inEdges))
            {
                var firstEdge = inEdges.First();
                bool materialized = newComponent.EdgeMaterilizedDict[firstEdge];
                newComponent.UnmaterializedNodeMapping.Remove(root);
                selectivity *= 1.0/root.TableRowCount;

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

                    //var statistics = ColumnStatistics.UpdateHistogram(newComponent.StatisticsDict[root],
                    //    new ColumnStatistics {Selectivity = 1.0/root.TableRowCount});
                    //selectivity *= statistics.Selectivity;
                    //newComponent.StatisticsDict[root] = statistics;

                    if (DensityCount.ContainsKey(root.NodeTableObjectName.ToString()))
                        DensityCount[root.NodeTableObjectName.ToString()]++;
                    else
                        DensityCount[root.NodeTableObjectName.ToString()] = 1;
                }
                // Component unmaterialized edge to root                
                else
                {
                    ColumnStatistics statistics = null;
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
                        statistics = ColumnStatistics.UpdateHistogram(statistics,
                            newComponent.Context.GetEdgeStatistics(edge));
                        selectivity *= statistics.Selectivity;

                        
                    }
                    newComponent.StatisticsDict[root] = statistics;

                    if (DensityCount.ContainsKey(root.NodeTableObjectName.ToString()))
                        DensityCount[root.NodeTableObjectName.ToString()]+=inEdges.Count;
                    else
                        DensityCount[root.NodeTableObjectName.ToString()] = inEdges.Count;
                }
            }

            var jointEdges = candidateTree.MaterializedEdges;
            int sinkToSinkCount = 0;
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
                    var statistics = ColumnStatistics.UpdateHistogram(newComponent.StatisticsDict[sinkNode],
                        newComponent.Context.GetEdgeStatistics(jointEdge));
                    selectivity *= statistics.Selectivity;
                    newComponent.StatisticsDict[sinkNode] = statistics;

                    if (DensityCount.ContainsKey(sinkNode.NodeTableObjectName.ToString()))
                        DensityCount[sinkNode.NodeTableObjectName.ToString()]++;
                    else
                        DensityCount[sinkNode.NodeTableObjectName.ToString()] = 1;
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

                        sinkToSinkCount++;
                        var statistics = ColumnStatistics.UpdateHistogram(newComponent.StatisticsDict[sinkNode],
                            newComponent.Context.GetEdgeStatistics(jointEdge));
                        selectivity *= statistics.Selectivity;
                        newComponent.StatisticsDict[sinkNode] = statistics;
                    }
                    // Leaf to unmaterialized leaf
                    else
                    {
                        ColumnStatistics compSinkNodeStatistics = null;
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

                            sinkToSinkCount++;
                            var leafToLeafStatistics = statisticsCalculator.GetLeafToLeafStatistics(jointEdge, inEdge);
                            selectivity *= leafToLeafStatistics.Selectivity;
                            compSinkNodeStatistics =
                                ColumnStatistics.UpdateHistogram(compSinkNodeStatistics,
                                    newComponent.Context.GetEdgeStatistics(inEdge));
                        }
                        newComponent.StatisticsDict[sinkNode] = compSinkNodeStatistics;
                    }
                }
            }

            var unmatEdges = candidateTree.UnmaterializedEdges;
            foreach (var unmatEdge in unmatEdges)
            {
                newComponent.EdgeMaterilizedDict[unmatEdge] = false;
                newComponent.Nodes.Add(unmatEdge.SinkNode);
                var sinkNodeInEdges = newComponent.UnmaterializedNodeMapping.GetOrCreate(unmatEdge.SinkNode);
                sinkNodeInEdges.Add(unmatEdge);
                degrees *= unmatEdge.AverageDegree;

            }

            // Calculate Estimated Join Selectivity & Estimated Node Size
            var densityDict = MetaData.TableIdDensity;
            double estimatedSelectity = 1.0;
            int count = 0;
            bool sinkJoin = false;
            foreach (var item in densityDict.Where(e => DensityCount.ContainsKey(e.Key)))
            {
                var density = item.Value;
                var curJoinCount = DensityCount[item.Key];
                var curJoinSelectitivy = Math.Pow(density, 2 - Math.Pow(2, 1 - curJoinCount));
                if (!sinkJoin && ColumnStatistics.DefaultDensity < density)
                {
                    var curSinkJoinSelectivity = Math.Pow(ColumnStatistics.DefaultDensity,
                        2 - Math.Pow(2, 1 - sinkToSinkCount));
                    estimatedSelectity *= Math.Pow(curSinkJoinSelectivity, Math.Pow(2, -count));
                    count += sinkToSinkCount;
                    sinkJoin = true;
                }
                estimatedSelectity *= Math.Pow(curJoinSelectitivy, Math.Pow(2, -count));
                count += curJoinCount;
            }

            var estimatedNodeUnitSize = root.EstimatedRows*
                                        Math.Pow(1000, candidateTree.MaterializedEdges.Count + candidateTree.UnmaterializedEdges.Count);


            // Update Table Reference
            newComponent.TableRef = GetPlanAndUpdateCost(candidateTree, newComponent, nodeTable, compTable, joinCondition,
                degrees, selectivity, estimatedNodeUnitSize, estimatedSelectity);

            return newComponent;
        }
    }
}


