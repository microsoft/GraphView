using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Windows.Forms;
using GraphView.GraphViewDBPortal;

namespace GraphView
{
    /// <summary>
    /// This is the optimizer to find the best execution order.
    /// We use beam search algorithm. 
    /// Beam search is an optimization of best-first search that reduces its memory requirements. 
    /// In beam search, only a predetermined number of best partial solutions are kept as candidates.
    /// </summary>
    internal class ExecutionOrderOptimizer
    {
        private List<AggregationBlock> blocks;
        private List<Tuple<PredicateLink, HashSet<string>>> predicateLinksAccessedTableAliases;

        // Upper Bound of the number of orders
        internal const int MaxNumberOfOrders = 100;

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

        /// <summary>
        /// Every time, we will generate multiple next orders from queue[index]. If some of them are finished, we put these into queue[1 - index].
        /// If the size of candidate orders equals or exceeds the upper bound, we will leave a predetermined number of best partial solutions.
        /// </summary>
        /// <param name="tableReferences"></param>
        /// <returns></returns>
        internal ExecutionOrder GenerateOptimalExecutionOrder(ExecutionOrder parentExecutionOrder)
        {
            // Two queues, queue[index] keeps forthcoming orders and queue[1 - index] keeps results from queue[index]
            int queueIndex = 0, blockIndex = 0;
            int blocksCount = this.blocks.Count;
            List<List<ExecutionOrder>> queue = new List<List<ExecutionOrder>>
            {
                new List<ExecutionOrder>(),
                new List<ExecutionOrder>()

            };

            queue[queueIndex].Add(new ExecutionOrder(parentExecutionOrder));

            while (blockIndex < blocksCount)
            {
                // Firstly, we need to add the root table
                foreach (ExecutionOrder currentOrder in queue[queueIndex])
                {
                    currentOrder.AddRootTable(this.blocks[blockIndex], this.predicateLinksAccessedTableAliases);
                }

                int numberOfIterations = this.blocks[blockIndex].TableInputDependency.Count - 1;

                while (numberOfIterations-- > 0)
                {
                    foreach (ExecutionOrder currentOrder in queue[queueIndex])
                    {
                        List<ExecutionOrder> nextOrders = currentOrder.GenerateNextOrders(this.blocks[blockIndex],
                            this.predicateLinksAccessedTableAliases);
                        if (nextOrders.Count > MaxNumberOfOrders)
                        {
                            nextOrders.Sort(new ExecutionOrderComparer());
                            queue[1 - queueIndex].AddRange(nextOrders.GetRange(0, MaxNumberOfOrders));
                        }
                        else
                        {
                            queue[1 - queueIndex].AddRange(nextOrders);
                        }
                    }
                    queue[queueIndex].Clear();
                    if (queue[1 - queueIndex].Count > MaxNumberOfOrders)
                    {
                        queue[1 - queueIndex].Sort(new ExecutionOrderComparer());
                        queue[1 - queueIndex] = queue[1 - queueIndex].GetRange(0, MaxNumberOfOrders);
                    }
                    queueIndex = 1 - queueIndex;
                }

                // Finally, check whether there is any ready edge. If there is, we discard this order
                for (int index = queue[queueIndex].Count - 1; index >= 0; --index)
                {
                    if (queue[queueIndex][index].ReadyEdges.Any())
                    {
                        queue[queueIndex].RemoveAt(index);
                    }
                }
                ++blockIndex;
            }
            
            queue[queueIndex].Sort(new ExecutionOrderComparer());
            return queue[queueIndex].First();
        }
    }
}
