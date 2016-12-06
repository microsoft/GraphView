using System;
using System.Collections.Generic;
using System.Linq;

namespace GraphView
{
    public class DocDbGraphOptimizer : GraphOptimizer
    {
        public DocDbGraphOptimizer(MatchGraph graph) : base(graph)
        { }

        public override List<Tuple<MatchNode, MatchEdge>> GetOptimizedTraversalOrder(ConnectedComponent subGraph, out Dictionary<string, List<MatchEdge>> nodeToMaterializedEdgesDict)
        {
            nodeToMaterializedEdgesDict = null;
            if (subGraph.Nodes.Count == 1)
                return
                    new List<Tuple<MatchNode, MatchEdge>>
                    {new Tuple<MatchNode, MatchEdge>(subGraph.Nodes.First().Value, null),};

            // If it exists, pick a node without incoming edges as the start point
            var componentStates = subGraph.Nodes.Where(node => node.Value.ReverseNeighbors.Count == 0).
                                    Select(node => new MatchComponent(node.Value)).Take(1).ToList();
            // Otherwise, pick a node randomly as the start point
            if (!componentStates.Any())
                componentStates.Add(new MatchComponent(subGraph.Nodes.First().Value));

            // DP
            while (componentStates.Any())
            {
                var nextCompnentStates = new List<MatchComponent>();

                // Iterate on current components
                foreach (var curComponent in componentStates)
                {
                    var nodeUnits = GetNodeUnits(subGraph, curComponent);
                    if (nodeUnits == null
                        && curComponent.ActiveNodeCount == subGraph.ActiveNodeCount
                        && curComponent.EdgeMaterilizedDict.Count(e => e.Value == true) == subGraph.EdgeCount)
                    {
                        curComponent.TraversalChain.Reverse();
                        nodeToMaterializedEdgesDict = curComponent.NodeToMaterializedEdgesDict;
                        return curComponent.TraversalChain;
                    }

                    var candidateUnit = GetCandidateUnits(nodeUnits, curComponent);
                    // Add it to the current component to generate next states
                    var newComponent = GetNextState(curComponent, candidateUnit);

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
            var useOriginalEdge = new List<OneHeightTree>();
            var useRevEdge = new List<OneHeightTree>();
            foreach (var node in graph.Nodes.Values.Where(n => !component.Nodes.Contains(n)))
            {
                var remainingEdges = node.Neighbors.Where(e => !component.EdgeMaterilizedDict.ContainsKey(e)).ToList();
                if (component.UnmaterializedNodeMapping.ContainsKey(node))
                {
                    useOriginalEdge.Add(new OneHeightTree
                    {
                        TreeRoot = node,
                        Edges = remainingEdges,
                    });
                    break;
                }
                if (remainingEdges.Any(e => component.Nodes.Contains(e.SinkNode)))
                {
                    useRevEdge.Add(new OneHeightTree
                    {
                        TreeRoot = node,
                        Edges = remainingEdges
                    });
                }
            }

            if (useOriginalEdge.Any()) return useOriginalEdge[0];
            if (useRevEdge.Any()) return useRevEdge[0];
            return null;
        }

        private CandidateJoinUnit GetCandidateUnits(OneHeightTree tree, MatchComponent component)
        {
            var nodeMatEdgesDict = component.NodeToMaterializedEdgesDict;
            var revEdgeDict = Graph.ReversedEdgeDict;
            var root = tree.TreeRoot;
            nodeMatEdgesDict[root.NodeAlias] = new List<MatchEdge>();

            List<MatchEdge> inEdges;
            component.UnmaterializedNodeMapping.TryGetValue(root, out inEdges);
            var outEdges = new List<MatchEdge>();
            var unpopEdges = new List<MatchEdge>();
            foreach (var edge in tree.Edges)
            {
                if (component.Nodes.Contains(edge.SinkNode))
                    outEdges.Add(edge);
                else
                    unpopEdges.Add(edge);
            }

            var rawEdges = new Dictionary<string, Tuple<MatchEdge, EdgeDir>>();
            var extInEdges = new Dictionary<string, MatchEdge>();
            if (inEdges != null)
            {
                rawEdges = inEdges.ToDictionary(edge => edge.EdgeAlias,
                    edge => new Tuple<MatchEdge, EdgeDir>(edge, EdgeDir.In));
                extInEdges = inEdges.ToDictionary(edge => edge.EdgeAlias);
            }
            foreach (var edge in outEdges)
            {
                var key = edge.EdgeAlias;
                rawEdges.Add(key, new Tuple<MatchEdge, EdgeDir>(edge, EdgeDir.Out));
                extInEdges.Add(key, revEdgeDict[key]);
            }

            if (extInEdges.Any())
            {
                var firstEdge = extInEdges.FirstOrDefault(e => e.Value.IsReversed == false);
                if (firstEdge.Value == null) firstEdge = extInEdges.First();
                var preMatInEdges = new Dictionary<string, MatchEdge>
                {
                    {firstEdge.Key, firstEdge.Value}
                };

                var postMatEdges = rawEdges.Where(entry => !preMatInEdges.ContainsKey(entry.Key))
                                    .Select(entry => entry.Value).ToList();
                var postMatIncomingEdges = postMatEdges.Where(entry => entry.Item2 == EdgeDir.In)
                                            .Select(entry => entry.Item1).ToList();
                var postMatOutgoingEdges = postMatEdges.Where(entry => entry.Item2 == EdgeDir.Out)
                                            .Select(entry => entry.Item1).ToList();

                nodeMatEdgesDict[firstEdge.Value.SourceNode.NodeAlias].Add(firstEdge.Value);
                foreach (var edge in postMatEdges.Select(t => t.Item1))
                {
                    nodeMatEdgesDict[edge.SourceNode.NodeAlias].Add(edge);
                }

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

        /// <summary>
        /// Transit from current component to the new component in the next state given the candidate Unit
        /// </summary>
        /// <param name="curComponent"></param>
        /// <param name="candidateTree"></param>
        /// <returns></returns>
        internal MatchComponent GetNextState(MatchComponent curComponent, CandidateJoinUnit candidateTree)
        {
            // Deep copy the component
            var newComponent = new MatchComponent(curComponent);

            // Update component
            UpdateComponent(newComponent, candidateTree);

            // Construct traversal chain and Update join cost
            ConstructTraversalChainAndUpdateCost(newComponent, candidateTree);

            return newComponent;
        }

        private void UpdateComponent(MatchComponent curComponent, CandidateJoinUnit candidateTree)
        {
            var nodes = curComponent.Nodes;
            var edgeMaterializedDict = curComponent.EdgeMaterilizedDict;
            var unmaterializedNodeMapping = curComponent.UnmaterializedNodeMapping;
            var root = candidateTree.TreeRoot;

            if (!nodes.Contains(root))
                nodes.Add(root);
            curComponent.MaterializedNodeSplitCount[root] = 0;

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

            if (inEdges.Any())
            {
                unmaterializedNodeMapping.Remove(root);

                foreach (var t in inEdges)
                {
                    var order = t.Item1;
                    var edge = t.Item2;

                    edgeMaterializedDict[edge] = true;
                }
            }

            if (outEdges.Any())
            {
                foreach (var t in outEdges)
                {
                    var order = t.Item1;
                    var edge = t.Item2;

                    edgeMaterializedDict[edge] = true;
                }
            }

            var unmatEdges = candidateTree.UnmaterializedEdges;
            foreach (var unmatEdge in unmatEdges)
            {
                edgeMaterializedDict[unmatEdge] = false; ;
                var unmatNodeInEdges = unmaterializedNodeMapping.GetOrCreate(unmatEdge.SinkNode);
                unmatNodeInEdges.Add(unmatEdge);
            }
        }

        private void ConstructTraversalChainAndUpdateCost(MatchComponent curComponent, CandidateJoinUnit nodeUnitCandidate)
        {
            var inPreMatEdges = nodeUnitCandidate.PreMatIncomingEdges;

            curComponent.TraversalChain.Add(new Tuple<MatchNode, MatchEdge>(inPreMatEdges[0].SourceNode, inPreMatEdges[0]));
        }
    }
}
