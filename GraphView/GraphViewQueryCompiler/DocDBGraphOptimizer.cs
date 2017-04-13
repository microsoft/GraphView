using System;
using System.Collections.Generic;
using System.Linq;

namespace GraphView
{
    public class DocDbGraphOptimizer : GraphOptimizer
    {
        public DocDbGraphOptimizer(MatchGraph graph) : base(graph)
        { }

        public override List<Tuple<MatchNode, MatchEdge, List<MatchEdge>, List<MatchEdge>, List<MatchEdge>>> GetOptimizedTraversalOrder(ConnectedComponent subGraph)
        {
            if (subGraph.Nodes.Count == 1)
                return
                    this.GenerateTraversalOrderFromTraversalChain(
                        new List<Tuple<MatchNode, MatchEdge, MatchNode, List<MatchEdge>, List<MatchEdge>>>
                        {
                            new Tuple<MatchNode, MatchEdge, MatchNode, List<MatchEdge>, List<MatchEdge>>(
                                subGraph.Nodes.First().Value, null, null, null, null)
                        });

            // If it exists, pick a node without incoming edges as the start point
            List<MatchComponent> componentStates = subGraph.Nodes.Where(node => node.Value.ReverseNeighbors.Count == 0).
                                    Select(node => new MatchComponent(node.Value)).Take(1).ToList();
            // Otherwise, pick a node randomly as the start point
            if (!componentStates.Any())
                componentStates.Add(new MatchComponent(subGraph.Nodes.First().Value));

            // DP
            while (componentStates.Any())
            {
                List<MatchComponent> nextCompnentStates = new List<MatchComponent>();

                // Iterate on current components
                foreach (MatchComponent curComponent in componentStates)
                {
                    OneHeightTree nodeUnits = this.GetNodeUnits(subGraph, curComponent);
                    if (nodeUnits == null
                        && curComponent.ActiveNodeCount == subGraph.ActiveNodeCount
                        && curComponent.EdgeMaterilizedDict.Count(e => e.Value == true) == subGraph.Edges.Count(e => e.Value.IsDanglingEdge == false))
                    {
                        return this.GenerateTraversalOrderFromTraversalChain(curComponent.TraversalChain);
                    }

                    CandidateJoinUnit candidateUnit = this.GetCandidateUnits2(nodeUnits, curComponent);
                    // Add it to the current component to generate next states
                    MatchComponent newComponent = this.GetNextState(curComponent, candidateUnit);

                    if (nextCompnentStates.Count >= MaxStates)
                        throw new GraphViewException("This graph pattern is not supported yet.");

                    nextCompnentStates.Add(newComponent);
                }
                componentStates = nextCompnentStates;
            }

            return null;
        }

        /// <summary>
        /// Get a full one height tree with joint edges and unmaterlized edges.
        /// </summary>
        /// <param name="graph"></param>
        /// <param name="component"></param>
        /// <returns></returns>
        private OneHeightTree GetNodeUnits(ConnectedComponent graph, MatchComponent component)
        {
            foreach (MatchNode node in graph.Nodes.Values.Where(n => !component.Nodes.ContainsKey(n.NodeAlias)))
            {
                List<MatchEdge> remainingEdges = node.Neighbors.Where(e => !component.EdgeMaterilizedDict.ContainsKey(e)).ToList();
                if (component.UnmaterializedNodeMapping.ContainsKey(node) ||
                    remainingEdges.Any(e => component.Nodes.ContainsKey(e.SinkNode.NodeAlias)))
                {
                    return new OneHeightTree
                    {
                        TreeRoot = node,
                        Edges = remainingEdges,
                    };
                }
            }

            return null;
        }

        private CandidateJoinUnit GetCandidateUnits2(OneHeightTree tree, MatchComponent component)
        {
            Dictionary<string, MatchEdge> revEdgeDict = this.Graph.ReversedEdgeDict;
            MatchNode root = tree.TreeRoot;

            List<MatchEdge> inEdges;
            component.UnmaterializedNodeMapping.TryGetValue(root, out inEdges);
            List<MatchEdge> outEdges = new List<MatchEdge>();
            List<MatchEdge> unpopEdges = new List<MatchEdge>();
            foreach (MatchEdge edge in tree.Edges) {
                if (component.Nodes.ContainsKey(edge.SinkNode.NodeAlias))
                    outEdges.Add(edge);
                else
                    unpopEdges.Add(edge);
            }

            Dictionary<string, Tuple<MatchEdge, EdgeDir>> rawEdges = new Dictionary<string, Tuple<MatchEdge, EdgeDir>>();
            Dictionary<string, MatchEdge> extInEdges = new Dictionary<string, MatchEdge>();
            if (inEdges != null)
            {
                rawEdges = inEdges.ToDictionary(edge => edge.EdgeAlias,
                    edge => new Tuple<MatchEdge, EdgeDir>(edge, EdgeDir.In));
                extInEdges = inEdges.ToDictionary(edge => edge.EdgeAlias);
            }
            foreach (MatchEdge edge in outEdges)
            {
                string key = edge.EdgeAlias;
                rawEdges.Add(key, new Tuple<MatchEdge, EdgeDir>(edge, EdgeDir.Out));
                extInEdges.Add(key, revEdgeDict[key]);
            }

            if (extInEdges.Any())
            {
                KeyValuePair<string, MatchEdge> firstEdge = extInEdges.FirstOrDefault(e => e.Value.IsReversed == false);
                if (firstEdge.Value == null) firstEdge = extInEdges.First();
                Dictionary<string, MatchEdge> preMatInEdges = new Dictionary<string, MatchEdge>
                {
                    {firstEdge.Key, firstEdge.Value}
                };

                List<Tuple<MatchEdge, EdgeDir>> postMatEdges = rawEdges.Where(entry => !preMatInEdges.ContainsKey(entry.Key))
                                    .Select(entry => entry.Value).ToList();

                List<MatchEdge> postMatIncomingEdges =
                    postMatEdges.Where(entry => entry.Item2 == EdgeDir.In).Select(entry => entry.Item1).ToList();
                List<MatchEdge> postMatOutgoingEdges =
                    postMatEdges.Where(entry => entry.Item2 == EdgeDir.Out).Select(entry => entry.Item1).ToList();

                return new CandidateJoinUnit
                {
                    TreeRoot = root,
                    PreMatIncomingEdges = preMatInEdges.Select(entry => entry.Value).ToList(),
                    PreMatOutgoingEdges = new List<MatchEdge>(),
                    PostMatIncomingEdges = postMatIncomingEdges,
                    PostMatOutgoingEdges = postMatOutgoingEdges,
                    UnmaterializedEdges = unpopEdges,
                };
            }
            else
                throw new GraphViewException("This graph pattern is not yet supported.");
        }

        internal MatchComponent GetNextState(MatchComponent curComponent, CandidateJoinUnit candidateTree)
        {
            // Deep copy the component
            MatchComponent newComponent = new MatchComponent(curComponent);

            // Update component
            this.UpdateComponent(newComponent, candidateTree);

            // Construct traversal chain and Update join cost
            this.ConstructTraversalChainAndUpdateCost(newComponent, candidateTree);

            return newComponent;
        }

        private List<string> PopulateAdjacencyListProperties(MatchEdge edge)
        {
            if (edge.EdgeType == WEdgeType.BothEdge)
                return new List<string> { GremlinKeyword.EdgeAdj, GremlinKeyword.ReverseEdgeAdj };
            //
            // IsTraversalThroughPhysicalReverseEdge
            //
            if ((edge.EdgeType == WEdgeType.OutEdge && edge.IsReversed)
                || edge.EdgeType == WEdgeType.InEdge && !edge.IsReversed)
                return new List<string> { GremlinKeyword.ReverseEdgeAdj };
            else
                return new List<string> { GremlinKeyword.EdgeAdj };
        }

        private void UpdateComponent(MatchComponent curComponent, CandidateJoinUnit candidateTree)
        {
            Dictionary<string, MatchNode> nodes = curComponent.Nodes;
            Dictionary<MatchEdge, bool> edgeMaterializedDict = curComponent.EdgeMaterilizedDict;
            Dictionary<MatchNode, List<MatchEdge>> unmaterializedNodeMapping = curComponent.UnmaterializedNodeMapping;
            MatchNode root = candidateTree.TreeRoot;

            if (!nodes.ContainsKey(root.NodeAlias))
                nodes.Add(root.NodeAlias, new MatchNode(root));
            curComponent.MaterializedNodeSplitCount[root] = 0;

            List<Tuple<MaterializedOrder, MatchEdge>> inEdges =
                candidateTree.PreMatIncomingEdges.Select(
                    e => new Tuple<MaterializedOrder, MatchEdge>(MaterializedOrder.Pre, e))
                    .Union(
                        candidateTree.PostMatIncomingEdges.Select(
                            e => new Tuple<MaterializedOrder, MatchEdge>(MaterializedOrder.Post, e)))
                    .ToList();

            List<Tuple<MaterializedOrder, MatchEdge>> outEdges =
                candidateTree.PreMatOutgoingEdges.Select(
                    e => new Tuple<MaterializedOrder, MatchEdge>(MaterializedOrder.Pre, e))
                    .Union(
                        candidateTree.PostMatOutgoingEdges.Select(
                            e => new Tuple<MaterializedOrder, MatchEdge>(MaterializedOrder.Post, e)))
                    .ToList();

            if (inEdges.Any())
            {
                unmaterializedNodeMapping.Remove(root);

                foreach (Tuple<MaterializedOrder, MatchEdge> t in inEdges)
                {
                    MaterializedOrder order = t.Item1;
                    MatchEdge edge = t.Item2;

                    edgeMaterializedDict[edge] = true;
                    List<string> adjListProperties = this.PopulateAdjacencyListProperties(edge);
                    MatchNode node = curComponent.Nodes[edge.SourceNode.NodeAlias];
                    foreach (string adjListProperty in adjListProperties) {
                        node.Properties.Add(adjListProperty);
                    }
                }
            }

            if (outEdges.Any())
            {
                foreach (Tuple<MaterializedOrder, MatchEdge> t in outEdges)
                {
                    MaterializedOrder order = t.Item1;
                    MatchEdge edge = t.Item2;

                    edgeMaterializedDict[edge] = true;
                    List<string> adjListProperties = this.PopulateAdjacencyListProperties(edge);
                    MatchNode node = curComponent.Nodes[edge.SourceNode.NodeAlias];
                    foreach (string adjListProperty in adjListProperties) {
                        node.Properties.Add(adjListProperty);
                    }
                }
            }

            List<MatchEdge> unmatEdges = candidateTree.UnmaterializedEdges;
            foreach (MatchEdge unmatEdge in unmatEdges)
            {
                edgeMaterializedDict[unmatEdge] = false; ;
                List<MatchEdge> unmatNodeInEdges = unmaterializedNodeMapping.GetOrCreate(unmatEdge.SinkNode);
                unmatNodeInEdges.Add(unmatEdge);
            }
        }

        private void ConstructTraversalChainAndUpdateCost(MatchComponent curComponent, CandidateJoinUnit nodeUnitCandidate)
        {
            List<MatchEdge> inPreMatEdges = nodeUnitCandidate.PreMatIncomingEdges;
            List<MatchEdge> inPostMatEdges = nodeUnitCandidate.PostMatIncomingEdges;
            List<MatchEdge> outPostMatEdges = nodeUnitCandidate.PostMatOutgoingEdges;

            //
            // Item1: sourceNode
            // Item2: traversalEdge
            // Item3: sinkNode
            // Item4: backwardingEdges
            // Item5: forwardingEdges
            //
            curComponent.TraversalChain.Add(
                new Tuple<MatchNode, MatchEdge, MatchNode, List<MatchEdge>, List<MatchEdge>>(
                    curComponent.Nodes[inPreMatEdges[0].SourceNode.NodeAlias],
                    inPreMatEdges[0],
                    curComponent.Nodes[inPreMatEdges[0].SinkNode.NodeAlias], 
                    outPostMatEdges, 
                    inPostMatEdges));
        }
    }
}
