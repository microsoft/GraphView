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
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    // TODO: do some pruning, deal with unpopulated & populated leaf to leaf
    /// <summary>
    /// 
    /// </summary>
    internal interface IMatchJoinPruning
    {
        IEnumerable<CandidateJoinUnit> GetCandidateUnits(IEnumerable<OneHeightTree> trees,
            MatchComponent component, Dictionary<string, MatchEdge> revEdgeDict);
    }

    internal class PruneJointEdge : IMatchJoinPruning
    {
        private enum EdgeDir : byte { In, Out };
        public IEnumerable<CandidateJoinUnit> GetCandidateUnits(IEnumerable<OneHeightTree> trees, 
            MatchComponent component, Dictionary<string, MatchEdge> revEdgeDict)
        {
            foreach (var tree in trees)
            {
                //bool singleNode = treeTuple.Item2;
                var root = tree.TreeRoot;

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
                    if (edge.HasReversedEdge)
                        extInEdges.Add(key, revEdgeDict[key]);
                }

                var extOutEdges = outEdges.ToDictionary(edge => edge.EdgeAlias);
                if (inEdges != null)
                {
                    foreach (var key in inEdges.Where(edge => edge.HasReversedEdge).Select(edge => edge.EdgeAlias))
                    {
                        extOutEdges.Add(key, revEdgeDict[key]);
                    }
                }

                var sortedExtInEdges = (from entry in extInEdges
                    orderby entry.Value.AverageDegree ascending
                    select entry).ToList();
                var sortedExtOutEdges = (from entry in extOutEdges
                    orderby entry.Value.AverageDegree ascending
                    select entry).ToList();

                // plan type 1: A => B
                if (sortedExtInEdges.Any())
                {
                    var preMatInEdges = new Dictionary<string, MatchEdge>
                    {
                        {sortedExtInEdges[0].Key, sortedExtInEdges[0].Value}
                    };
                    for (var i = 1; i < sortedExtInEdges.Count; ++i)
                    {
                        if (sortedExtInEdges[i].Value.AverageDegree < 1)
                        {
                            preMatInEdges.Add(sortedExtInEdges[i].Key, sortedExtInEdges[i].Value);
                        }
                    }
                    var postMatEdges =
                        rawEdges.Where(entry => !preMatInEdges.ContainsKey(entry.Key))
                            .Select(entry => entry.Value).ToList();

                    yield return new CandidateJoinUnit
                    {
                        TreeRoot = root,
                        PreMatIncomingEdges = preMatInEdges.Select(entry => entry.Value).ToList(),
                        PreMatOutgoingEdges = new List<MatchEdge>(),
                        PostMatIncomingEdges = postMatEdges.Where(entry => entry.Item2 == EdgeDir.In)
                                                   .Select(entry => entry.Item1)
                                                   .OrderBy(edge => edge.AverageDegree).ToList(),
                        PostMatOutgoingEdges = postMatEdges.Where(entry => entry.Item2 == EdgeDir.Out)
                                                   .Select(entry => entry.Item1)
                                                   .OrderBy(edge => edge.AverageDegree).ToList(),
                        UnmaterializedEdges = unpopEdges,
                    };
                }

                // plan type 2: A <= B
                if (sortedExtOutEdges.Any())
                {
                    var preMatOutEdges = new Dictionary<string, MatchEdge>
                    {
                        {sortedExtOutEdges[0].Key, sortedExtOutEdges[0].Value}
                    };
                    for (var i = 1; i < sortedExtOutEdges.Count; ++i)
                    {
                        if (sortedExtOutEdges[i].Value.AverageDegree < 1)
                        {
                            preMatOutEdges.Add(sortedExtOutEdges[i].Key, sortedExtOutEdges[i].Value);
                        }
                    }
                    var postMatEdges =
                        rawEdges.Where(entry => !preMatOutEdges.ContainsKey(entry.Key))
                            .Select(entry => entry.Value).ToList();

                    yield return new CandidateJoinUnit
                    {
                        TreeRoot = root,
                        PreMatIncomingEdges = new List<MatchEdge>(),
                        PreMatOutgoingEdges = preMatOutEdges.Select(entry => entry.Value).ToList(),
                        PostMatIncomingEdges = postMatEdges.Where(entry => entry.Item2 == EdgeDir.In)
                                                   .Select(entry => entry.Item1)
                                                   .OrderBy(edge => edge.AverageDegree).ToList(),
                        PostMatOutgoingEdges = postMatEdges.Where(entry => entry.Item2 == EdgeDir.Out)
                                                   .Select(entry => entry.Item1)
                                                   .OrderBy(edge => edge.AverageDegree).ToList(),
                        UnmaterializedEdges = unpopEdges,
                    };
                }

                // plan type 3: A <=> B
                var hasRevEdgeCount = rawEdges.Count(x => x.Value.Item1.HasReversedEdge == true);
                if (hasRevEdgeCount > 1 
                    || (hasRevEdgeCount == 1 && rawEdges.Count > 1) 
                    || (hasRevEdgeCount == 0 && sortedExtInEdges.Count > 0 && sortedExtOutEdges.Count > 0))
                {
                    var firstMatInEdges1 = new Tuple<string, MatchEdge>(sortedExtInEdges[0].Key,
                        sortedExtInEdges[0].Value);
                    Tuple<string, MatchEdge> firstMatOutEdges1 = null;
                    foreach (var entry in sortedExtOutEdges)
                    {
                        if (!firstMatInEdges1.Item1.Equals(entry.Key))
                        {
                            firstMatOutEdges1 = new Tuple<string, MatchEdge>(entry.Key, entry.Value);
                            break;
                        }
                    }
                    var cost1 = firstMatOutEdges1 != null
                        ? component.Cardinality*firstMatInEdges1.Item2.AverageDegree +
                          root.EstimatedRows*firstMatOutEdges1.Item2.AverageDegree
                        : double.MaxValue;

                    var firstMatOutEdges2 = new Tuple<string, MatchEdge>(sortedExtOutEdges[0].Key,
                        sortedExtOutEdges[0].Value);
                    Tuple<string, MatchEdge> firstMatInEdges2 = null;
                    foreach (var entry in sortedExtInEdges)
                    {
                        if (!firstMatOutEdges2.Item1.Equals(entry.Key))
                        {
                            firstMatInEdges2 = new Tuple<string, MatchEdge>(entry.Key, entry.Value);
                            break;
                        }
                    }
                    var cost2 = firstMatInEdges2 != null
                        ? component.Cardinality*firstMatInEdges2.Item2.AverageDegree +
                          root.EstimatedRows*firstMatOutEdges2.Item2.AverageDegree
                        : double.MaxValue;

                    var preMatInEdges = new Dictionary<string, MatchEdge>();
                    var preMatOutEdges = new Dictionary<string, MatchEdge>();
                    if (cost1 < cost2 || firstMatInEdges2 == null)
                    {
                        preMatInEdges.Add(sortedExtInEdges[0].Key, sortedExtInEdges[0].Value);
                        preMatOutEdges.Add(firstMatOutEdges1.Item1, firstMatOutEdges1.Item2);
                    }
                    else    
                    {
                        preMatOutEdges.Add(sortedExtOutEdges[0].Key, sortedExtOutEdges[0].Value);
                        preMatInEdges.Add(firstMatInEdges2.Item1, firstMatInEdges2.Item2);
                    }

                    var remainingInEdges =
                        rawEdges.Where(
                            entry =>
                                entry.Value.Item2 == EdgeDir.In && !preMatInEdges.ContainsKey(entry.Key) &&
                                !preMatOutEdges.ContainsKey(entry.Key))
                            .Select(entry => entry.Value.Item1)
                            .ToList();
                    var remainingOutEdges = 
                        rawEdges.Where(
                            entry =>
                                entry.Value.Item2 == EdgeDir.Out && !preMatInEdges.ContainsKey(entry.Key) &&
                                !preMatOutEdges.ContainsKey(entry.Key))
                            .Select(entry => entry.Value.Item1)
                            .ToList();
                    var preMatIncomingEdges = preMatInEdges.Select(entry => entry.Value).ToList();
                    preMatIncomingEdges.AddRange(
                        remainingInEdges.Where(edge => edge.AverageDegree < 1).ToList());
                    var preMatOutgoingEdges = preMatOutEdges.Select(entry => entry.Value).ToList();
                    preMatOutgoingEdges.AddRange(
                        remainingOutEdges.Where(edge => edge.AverageDegree < 1).ToList());

                    yield return new CandidateJoinUnit
                    {
                        TreeRoot = root,
                        PreMatIncomingEdges = preMatIncomingEdges,
                        PreMatOutgoingEdges = preMatOutgoingEdges,
                        PostMatIncomingEdges = remainingInEdges.Except(preMatIncomingEdges)
                                                   .OrderBy(edge => edge.AverageDegree).ToList(),
                        PostMatOutgoingEdges = remainingOutEdges.Except(preMatOutgoingEdges)
                                                   .OrderBy(edge => edge.AverageDegree).ToList(),
                        UnmaterializedEdges = unpopEdges,
                    };
                }
            }
        }
    }

    internal class MatchEdgeTupleEqualityComparer : IEqualityComparer<Tuple<string, string>>
    {
        public bool Equals(Tuple<string, string> x, Tuple<string, string> y)
        {
            if ((x.Item1 == y.Item1 && x.Item2 == y.Item2) || (x.Item1 == y.Item2 && x.Item2 == y.Item1))
                return true;
            return false;
        }

        public int GetHashCode(Tuple<string, string> obj)
        {
            String str = obj.Item2 + obj.Item1;
            if (String.CompareOrdinal(obj.Item1, obj.Item2) > 0)
                str = obj.Item1 + obj.Item2;
            return str.GetHashCode();
        }
    }

    internal interface IMatchJoinStatisticsCalculator
    {
        Statistics GetLeafToLeafStatistics(MatchEdge nodeEdge, MatchEdge componentEdge,out double selectivity);
    }

    internal class HistogramCalculator : IMatchJoinStatisticsCalculator
    {
        private readonly Dictionary<Tuple<string, string>, Tuple<Statistics,double>> _leafToLeafStatistics;


        public HistogramCalculator()
        {
            _leafToLeafStatistics =
                new Dictionary<Tuple<string, string>, Tuple<Statistics, double>>(new MatchEdgeTupleEqualityComparer());

        }
        public Statistics GetLeafToLeafStatistics(MatchEdge nodeEdge, MatchEdge componentEdge, out double selectivity)
        {
            var edgeTuple = new Tuple<string, string>(nodeEdge.EdgeAlias, componentEdge.EdgeAlias);
            Tuple<Statistics, double> edgeStatisticsTuple;
            if (_leafToLeafStatistics.TryGetValue(edgeTuple, out edgeStatisticsTuple))
            {
                selectivity = edgeStatisticsTuple.Item2;
                return edgeStatisticsTuple.Item1;
            }

            var mergedStatistics = Statistics.UpdateHistogram(nodeEdge.Statistics, componentEdge.Statistics,
                out selectivity);
            _leafToLeafStatistics[edgeTuple] = new Tuple<Statistics, double>(mergedStatistics, selectivity);
            return mergedStatistics;
        }

    }
}
