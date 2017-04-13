using System;
using System.Collections.Generic;
using System.Linq;

namespace GraphView
{
    public abstract class GraphOptimizer
    {
        protected MatchGraph Graph;

        // Upper Bound of the State number
        protected const int MaxStates = 1000;

        protected GraphOptimizer(MatchGraph graph)
        {
            this.Graph = graph;
        }

        public abstract List<Tuple<MatchNode, MatchEdge, List<MatchEdge>, List<MatchEdge>, List<MatchEdge>>> GetOptimizedTraversalOrder(ConnectedComponent subGraph);

        /// <summary>
        /// Item1: current node. A query will be sent to the server to fetch this node if this is the first time it appears in the whole list.
        /// Item2: the traversalEdge whose sink is current node.
        /// Item3: traversalEdges whose source is currentNode. 
        ///        This list will either contain 0 or 1 traversal edge in the current version, and it will be pushed to server if possible.
        /// Item4: backwardMatchingEdges.
        /// Item5: forwardMatchingEdges.
        /// </summary>
        /// <param name="traversalChain"></param>
        /// <returns></returns>
        protected List<Tuple<MatchNode, MatchEdge, List<MatchEdge>, List<MatchEdge>, List<MatchEdge>>> GenerateTraversalOrderFromTraversalChain
            (List<Tuple<MatchNode, MatchEdge, MatchNode, List<MatchEdge>, List<MatchEdge>>> traversalChain)
        {
            Dictionary<string, int> nodeFetchingOrderDict = new Dictionary<string, int>();
            List<Tuple<MatchNode, MatchEdge, List<MatchEdge>, List<MatchEdge>, List<MatchEdge>>> optimizedTraversalOrder
                = new List<Tuple<MatchNode, MatchEdge, List<MatchEdge>, List<MatchEdge>, List<MatchEdge>>>();

            foreach (Tuple<MatchNode, MatchEdge, MatchNode, List<MatchEdge>, List<MatchEdge>> tuple in traversalChain)
            {
                MatchNode srcNode = tuple.Item1;
                MatchEdge traversalEdge = tuple.Item2;

                int nodeFetchingOrder;
                if (nodeFetchingOrderDict.TryGetValue(srcNode.NodeAlias, out nodeFetchingOrder))
                {
                    List<MatchEdge> traversalEdges = optimizedTraversalOrder[nodeFetchingOrder].Item3;
                    if (traversalEdges.Count == 0) {
                        traversalEdges.Add(traversalEdge);
                    }
                    else
                    {
                        optimizedTraversalOrder.Add(
                            new Tuple<MatchNode, MatchEdge, List<MatchEdge>, List<MatchEdge>, List<MatchEdge>>(
                                srcNode,
                                null,
                                new List<MatchEdge> { traversalEdge },
                                new List<MatchEdge>(), new List<MatchEdge>()));
                    }
                }
                else
                {
                    nodeFetchingOrderDict.Add(srcNode.NodeAlias, optimizedTraversalOrder.Count);
                    optimizedTraversalOrder.Add(
                        new Tuple<MatchNode, MatchEdge, List<MatchEdge>, List<MatchEdge>, List<MatchEdge>>(
                            srcNode,
                            null,
                            traversalEdge != null ? new List<MatchEdge> { traversalEdge } : new List<MatchEdge>(),
                            new List<MatchEdge>(), new List<MatchEdge>()));
                }

                if (traversalEdge != null)
                {
                    MatchNode sinkNode = tuple.Item3;
                    List<MatchEdge> backwardEdges = tuple.Item4;
                    List<MatchEdge> forwardEdges = tuple.Item5;

                    nodeFetchingOrderDict.Add(sinkNode.NodeAlias, optimizedTraversalOrder.Count);
                    optimizedTraversalOrder.Add(
                        new Tuple<MatchNode, MatchEdge, List<MatchEdge>, List<MatchEdge>, List<MatchEdge>>(
                            sinkNode,
                            traversalEdge,
                            new List<MatchEdge>(),
                            backwardEdges, forwardEdges));
                }
            }

            return optimizedTraversalOrder;
        }
    }
}
