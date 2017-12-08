using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using GraphView.GraphViewDBPortal;

namespace GraphView
{
    internal class ExecutionOrderOptimizer
    {
        private List<AggregationBlock> blocks;
        private List<Tuple<PredicateLink, HashSet<string>>> predicateLinksAccessedTableAliases;

        // Upper Bound of the State number
        internal const int MaxStates = 5;

        public ExecutionOrderOptimizer(List<AggregationBlock> aggregationBlocks, 
            List<Tuple<WBooleanExpression, HashSet<string>>> predicatesAccessedTableAliases)
        {
            this.blocks = aggregationBlocks;
            this.predicateLinksAccessedTableAliases = new List<Tuple<PredicateLink, HashSet<string>>>();
            foreach (Tuple<WBooleanExpression, HashSet<string>> tuple in predicatesAccessedTableAliases)
            {
                predicateLinksAccessedTableAliases.Add(new Tuple<PredicateLink, HashSet<string>>(new PredicateLink(tuple.Item1), tuple.Item2));
            }
        }

        internal ExecutionOrder GenerateOptimalExecutionOrder(HashSet<string> tableReferences)
        {
            // Every time, we will generate multiple next states from queue[index]. If some of them are finished, we put these into queue[1 - index],
            // and we put another into candidateChains. If the size of candidateChains equals or exceeds the upper bound, we will terminate this 
            // algorithm and return the best one in candidateChains.
            int queueIndex = 0, blockIndex = 0;
            int blocksCount = this.blocks.Count;
            List<List<ExecutionOrder>> queue = new List<List<ExecutionOrder>>
            {
                new List<ExecutionOrder>(),
                new List<ExecutionOrder>()

            };

            queue[queueIndex].Add(new ExecutionOrder(tableReferences));

            while (blockIndex < blocksCount)
            {
                // Firstly, add aggregationTable
                foreach (ExecutionOrder currentOrder in queue[queueIndex])
                {
                    currentOrder.AddAggregationTable(this.blocks[blockIndex],
                        this.predicateLinksAccessedTableAliases);
                }

                int numberOfIterations = this.blocks[blockIndex].TableAliases.Count - 1;

                while (numberOfIterations-- > 0)
                {
                    foreach (ExecutionOrder currentOrder in queue[queueIndex])
                    {
                        List<ExecutionOrder> nextOrders = currentOrder.GenerateNextOrders(this.blocks[blockIndex],
                            this.predicateLinksAccessedTableAliases);
                        if (nextOrders.Count > MaxStates)
                        {
                            nextOrders.Sort(new ExecutionOrderComparer());
                            queue[1 - queueIndex].AddRange(nextOrders.GetRange(0, MaxStates));
                        }
                        else
                        {
                            queue[1 - queueIndex].AddRange(nextOrders);
                        }
                    }
                    queue[queueIndex].Clear();
                    if (queue[1 - queueIndex].Count > MaxStates)
                    {
                        queue[1 - queueIndex].Sort(new ExecutionOrderComparer());
                        queue[1 - queueIndex] = queue[1 - queueIndex].GetRange(0, MaxStates);
                    }
                    queueIndex = 1 - queueIndex;
                }

                ++blockIndex;
            }

            queue[queueIndex].Sort(new ExecutionOrderComparer());
            return queue[queueIndex].First();
        }
    }
}
