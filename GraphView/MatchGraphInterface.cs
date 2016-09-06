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
using Microsoft.SqlServer.TransactSql.ScriptDom;

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

                // plan type 1: A => B, Loop Join, might use Hash if |A|/|B| is too large
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
                    for (var i = 0; i < postMatEdges.Count; ++i)
                    {
                        var t = postMatEdges[i];
                        var edge = t.Item1;
                        var dir = t.Item2;
                        if (!edge.HasReversedEdge) continue;

                        var revEdge = revEdgeDict[edge.EdgeAlias];
                        if (revEdge.AverageDegree < edge.AverageDegree)
                            postMatEdges[i] = new Tuple<MatchEdge, EdgeDir>(revEdge,
                                dir == EdgeDir.In ? EdgeDir.Out : EdgeDir.In);
                    }

                    yield return new CandidateJoinUnit
                    {
                        TreeRoot = root,
                        PreMatIncomingEdges = preMatInEdges.Select(entry => entry.Value)
                                                  .OrderBy(edge => edge.AverageDegree).ToList(),
                        PreMatOutgoingEdges = new List<MatchEdge>(),
                        PostMatIncomingEdges = postMatEdges.Where(entry => entry.Item2 == EdgeDir.In)
                                                   .Select(entry => entry.Item1).ToList(),
                        PostMatOutgoingEdges = postMatEdges.Where(entry => entry.Item2 == EdgeDir.Out)
                                                   .Select(entry => entry.Item1).ToList(),
                        UnmaterializedEdges = unpopEdges,
                        JoinHint = JoinHint.Loop,
                    };
                }
                // plan type 2: A <= B, Hash join
                else if (sortedExtOutEdges.Any())
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
                    for (var i = 0; i < postMatEdges.Count; ++i)
                    {
                        var t = postMatEdges[i];
                        var edge = t.Item1;
                        var dir = t.Item2;
                        if (!edge.HasReversedEdge) continue;

                        var revEdge = revEdgeDict[edge.EdgeAlias];
                        if (revEdge.AverageDegree < edge.AverageDegree)
                            postMatEdges[i] = new Tuple<MatchEdge, EdgeDir>(revEdge,
                                dir == EdgeDir.In ? EdgeDir.Out : EdgeDir.In);
                    }

                    yield return new CandidateJoinUnit
                    {
                        TreeRoot = root,
                        PreMatIncomingEdges = new List<MatchEdge>(),
                        PreMatOutgoingEdges = preMatOutEdges.Select(entry => entry.Value).ToList(),
                        PostMatIncomingEdges = postMatEdges.Where(entry => entry.Item2 == EdgeDir.In)
                                                   .Select(entry => entry.Item1).ToList(),
                        PostMatOutgoingEdges = postMatEdges.Where(entry => entry.Item2 == EdgeDir.Out)
                                                   .Select(entry => entry.Item1).ToList(),
                        UnmaterializedEdges = unpopEdges,
                        JoinHint = JoinHint.Hash,
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
