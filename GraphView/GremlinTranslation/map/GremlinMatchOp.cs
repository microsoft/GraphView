using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal class GremlinMatchOp : GremlinTranslationOperator
    {
        public List<GraphTraversal2> MatchTraversals { get; set; }
        public GraphTraversal2 JoinedMatchTraversal { get; set; }
        public List<Tuple<string, string>> StartAndEndLabelsPairList;
        public HashSet<string> Labels;

        public GremlinMatchOp(params GraphTraversal2[] matchTraversals)
        {
            this.MatchTraversals = new List<GraphTraversal2>();
            this.StartAndEndLabelsPairList = new List<Tuple<string, string>>();
            this.Labels = new HashSet<string>();
            foreach (var traversal in matchTraversals)
            {
                this.configureStartAndEndOperators(traversal);
                this.MatchTraversals.Add(traversal);
            }

            this.sortMatchTraversals();

            this.JoinedMatchTraversal = joinMatchTraversals(this.MatchTraversals);

            // match(...) which include 'a', 'b', 'c' means it will select('a', 'b', 'c') and generate a map
            // this.JoinedMatchTraversal.AddGremlinOperator(new GremlinSelectOp(GremlinKeyword.Pop.All, this.Labels.ToArray()));
            this.JoinedMatchTraversal.Select(this.Labels.ToArray());
        }

        internal void configureStartAndEndOperators(GraphTraversal2 traversal)
        {
            string startLabel = null, endLabel = null;
            
            GremlinParentContextOp startOperator = traversal.GetFirstOp() as GremlinParentContextOp;
            if (startOperator != null)
            {
                traversal.RemoveGremlinOperator(0);
            }

            GremlinAsOp asOperator = traversal.GetFirstOp() as GremlinAsOp;

            if (asOperator == null)
            {
                throw new QueryCompilationException("NOW, The first and second operator of each match()-traversal should be '__()' and 'As()'.");
            }

            List<string> startLabels = asOperator.Labels;

            if (startLabels.Count != 1)
            {
                throw new QueryCompilationException("All match()-traversals must have a single start label.");
            }

            startLabel = startLabels[0];

            traversal.ReplaceGremlinOperator(0, new GremlinMatchStartOp(startLabel));

            // ---

            GremlinAsOp endOperator = traversal.GetLastOp() as GremlinAsOp;
            if (endOperator != null)
            {
                // as('a')...as('b'): both the start and end of the traversal have a declared variable.
                List<string> endLabels = endOperator.Labels;
                if (endLabels.Count <= 1)
                {
                    traversal.PopGremlinOperator();
                    if (endLabels.Count == 1)
                    {
                        endLabel = endLabels[0];
                    }
                    traversal.AddGremlinOperator(new GremlinMatchEndOp(endLabel)); // if no label, add new GremlinMatchEndOp(null)
                }
                else
                {
                    throw new QueryCompilationException("The end operator of a match()-traversal can have at most one label.");
                }
            }
            else
            {
                traversal.AddGremlinOperator(new GremlinMatchEndOp(null));
            }

            traversal.LastGremlinTranslationOp = traversal.GetLastOp();

            this.StartAndEndLabelsPairList.Add(new Tuple<string, string>(startLabel, endLabel));

            if (startLabel != null)
            {
                this.Labels.Add(startLabel);
            }

            if (endLabel != null)
            {
                this.Labels.Add(endLabel);
            }
        }

        internal static GraphTraversal2 joinMatchTraversals(List<GraphTraversal2> matchTraversals)
        {
            GraphTraversal2 traversal = GraphTraversal2.__();
            foreach (GraphTraversal2 matchTraversal in matchTraversals)
            {
                List<GremlinTranslationOperator> opList = matchTraversal.GetGremlinTranslationOpList();
                if ((opList.First() as GremlinParentContextOp) != null)
                {
                    opList.RemoveAt(0);
                }
                foreach (GremlinTranslationOperator op in opList)
                {
                    traversal.AddGremlinOperator(op);
                }
            }
            return traversal;
        }

        // similar topological sorting, MatchTraversals and StartAndEndLabelsPairList will be sorted synchronously
        internal void sortMatchTraversals()
        {
            Dictionary<string, List<string>> edges = new Dictionary<string, List<string>>();
            // find all the valid edges which have a source vertex and sink vertex
            foreach (Tuple<string, string> pair in this.StartAndEndLabelsPairList)
            {
                if (pair.Item1 != null && pair.Item2 != null)
                {
                    if (edges.ContainsKey(pair.Item1))
                    {
                        edges[pair.Item1].Add(pair.Item2);
                    }
                    else
                    {
                        edges.Add(pair.Item1, new List<string> { pair.Item2 });
                    }
                }
            }

            // We have a graph consisting of many edges storing in 'edges'.
            // Each time, We will cut the graph to a subgraph and generate a list which is composed of the vertexs in the cut part meanwhile.
            // And then put this list at the front of the final sorted list which will include all vertexs(labels) finally.
            // We will not stop until the graph has no vertex.

            HashSet<string> vertexs = this.Labels.Copy();

            List<string> totalSortedList = new List<string>();

            while (vertexs.Count > 0)
            {
                List<string> longestPartialSortedList = new List<string>();
                foreach (string vertex in vertexs)
                {
                    List<string> partialSortedList = breadthFirstSearch(edges, vertex);
                    if (partialSortedList.Count > longestPartialSortedList.Count)
                    {
                        longestPartialSortedList = partialSortedList;
                    }
                }
                // cut it off from the graph
                foreach (string vertex in longestPartialSortedList)
                {
                    vertexs.Remove(vertex);
                    foreach (KeyValuePair<string, List<string>> pair in edges)
                    {
                        pair.Value.Remove(vertex);
                    }
                    edges.Remove(vertex);
                }
                totalSortedList.InsertRange(0, longestPartialSortedList);
            }

            Debug.Assert(this.MatchTraversals.Count == this.StartAndEndLabelsPairList.Count);

            List<GraphTraversal2> sortedMatchTraversals = new List<GraphTraversal2>();
            List<Tuple<string, string>> sortedStartAndEndLabelsPairList = new List<Tuple<string, string>>();

            foreach (string label in totalSortedList)
            {
                for (int i = 0; i < this.MatchTraversals.Count; i++)
                {
                    string startLabel = this.StartAndEndLabelsPairList[i].Item1; ;
                    if (startLabel == label)
                    {
                        sortedMatchTraversals.Add(this.MatchTraversals[i]);
                        sortedStartAndEndLabelsPairList.Add(this.StartAndEndLabelsPairList[i]);
                    }
                }
            }

            // could be remove when stable
            Debug.Assert(this.MatchTraversals.Count == sortedMatchTraversals.Count);
            Debug.Assert(this.StartAndEndLabelsPairList.Count == sortedStartAndEndLabelsPairList.Count);
            Debug.Assert(sortedMatchTraversals.TrueForAll(t => this.MatchTraversals.Contains(t)));
            Debug.Assert(this.MatchTraversals.TrueForAll(t => sortedMatchTraversals.Contains(t)));
            Debug.Assert(sortedStartAndEndLabelsPairList.TrueForAll(t => this.StartAndEndLabelsPairList.Contains(t)));
            Debug.Assert(this.StartAndEndLabelsPairList.TrueForAll(t => sortedStartAndEndLabelsPairList.Contains(t)));

            this.MatchTraversals = sortedMatchTraversals;
            this.StartAndEndLabelsPairList = sortedStartAndEndLabelsPairList;
        }

        internal List<string> breadthFirstSearch(Dictionary<string, List<string>> edges, string start)
        {
            List<string> record = new List<string>();

            Queue<string> queue = new Queue<string>();
            queue.Enqueue(start);
            while (queue.Count > 0)
            {
                string current = queue.Dequeue();
                record.Add(current);
                
                if (!edges.ContainsKey(current))
                {
                    continue; // no next possible vertex
                }
                
                foreach (string nextVertex in edges[current])
                {
                    if (!record.Contains(nextVertex))
                    {
                        queue.Enqueue(nextVertex);
                    }
                }
            }

            return record;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = this.GetInputContext();

            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            this.JoinedMatchTraversal.GetFirstOp().InheritedVariableFromParent(inputContext);
            GremlinToSqlContext matchContext = this.JoinedMatchTraversal.GetLastOp().GetContext();

            inputContext.PivotVariable.Match(inputContext, matchContext);

            return inputContext;
        }
    }

    internal class GremlinMatchStartOp : GremlinTranslationOperator
    {
        public string SelectKey { get; set; }

        public GremlinMatchStartOp(string selectKey)
        {
            this.SelectKey = selectKey;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = this.GetInputContext();

            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            inputContext.PivotVariable.MatchStart(inputContext, this.SelectKey);
            return inputContext;
        }
    }

    internal class GremlinMatchEndOp : GremlinTranslationOperator
    {
        public string MatchKey { get; set; }

        public GremlinMatchEndOp(string matchKey)
        {
            this.MatchKey = matchKey;
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = this.GetInputContext();

            if (inputContext.PivotVariable == null)
            {
                throw new QueryCompilationException("The PivotVariable can't be null.");
            }

            inputContext.PivotVariable.MatchEnd(inputContext, this.MatchKey);
            return inputContext;
        }
    }
}
