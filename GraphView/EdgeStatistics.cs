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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class EdgeStatistics
    {
        public const double DefaultDensity = 0.0316228;

        public EdgeStatistics()
        {
            Histogram = new Dictionary<long, Tuple<double, bool>>();
            Selectivity = 1.0;
        }

        /// <summary>
        /// GlobalNodeid -> (Count, IsPopular)
        /// </summary>
        public Dictionary<long, Tuple<double, bool>> Histogram;
        public Double RowCount;
        public Double Density;
        public Double Selectivity;
        public long MaxValue;

        /// <summary>
        /// Merger Two Histograms
        /// </summary>
        /// <param name="curStatistics"></param>
        /// <param name="newStatistics"></param>
        /// <param name="currentJoin"></param>
        /// <returns></returns>
        internal static EdgeStatistics UpdateHistogram(EdgeStatistics curStatistics, EdgeStatistics newStatistics)
        {
            if (curStatistics == null)
                return newStatistics;
            else if (newStatistics == null)
                return curStatistics;
            var resHistogram = new Dictionary<long, Tuple<double, bool>>();
            var curHistogram = curStatistics.Histogram;
            var newHistogram = newStatistics.Histogram;
            if (!curHistogram.Any())
            {
                return new EdgeStatistics
                {
                    Density = newStatistics.Density,
                    Histogram = newHistogram,
                    Selectivity = curStatistics.Selectivity * newStatistics.Selectivity,
                };
            }
            if (!newHistogram.Any())
            {
                return new EdgeStatistics
                {
                    Density = curStatistics.Density,
                    Histogram = curHistogram,
                    Selectivity = curStatistics.Selectivity * newStatistics.Selectivity,
                };
            }
            var curNotPopularCount = 0.0;
            var newNotPopularCount = 0.0;
            var curDefaultRow = curStatistics.Density * curStatistics.RowCount;
            var newDefaultRow = newStatistics.Density * newStatistics.RowCount;
            IEnumerator<KeyValuePair<long, Tuple<double, bool>>> newEntry = null;
            bool fisrstMatch = false;
            bool newHistogramEnd = false;
            double resRowCount = 0.0;
            List<long> notPopularValues = new List<long>();

            foreach (var entry in curHistogram)
            {
                if (!fisrstMatch)
                {
                    if (newHistogram.ContainsKey(entry.Key))
                    {
                        fisrstMatch = true;
                        var entry1 = entry;
                        newEntry = newHistogram.SkipWhile(e => e.Key != entry1.Key).GetEnumerator();
                        newEntry.MoveNext();
                        if (!entry.Value.Item2)
                            curNotPopularCount -= entry.Value.Item1;
                        if (!newEntry.Current.Value.Item2)
                            newNotPopularCount -= newEntry.Current.Value.Item1;
                    }
                }
                if (fisrstMatch)
                {
                    if (newHistogramEnd || entry.Key < newEntry.Current.Key)
                    {
                        var curTuple = entry.Value;
                        if (curTuple.Item2 == true)
                        {
                            var tmpCount = curTuple.Item1 * newDefaultRow;
                            resRowCount += tmpCount;
                            resHistogram.Add(entry.Key, new Tuple<double, bool>(tmpCount, true));
                        }
                        else
                        {
                            notPopularValues.Add(entry.Key);
                            curNotPopularCount += curTuple.Item1;
                            resHistogram.Add(entry.Key, null);
                        }
                    }
                    else if (entry.Key > newEntry.Current.Key)
                    {
                        while (entry.Key > newEntry.Current.Key)
                        {
                            var newTuple = newEntry.Current.Value;
                            if (newTuple.Item2 == true)
                            {
                                var tmpCount = newTuple.Item1 * curDefaultRow;
                                resRowCount += tmpCount;
                                resHistogram.Add(newEntry.Current.Key, new Tuple<double, bool>(tmpCount, true));
                            }
                            else
                            {
                                notPopularValues.Add(newEntry.Current.Key);
                                newNotPopularCount += newTuple.Item1;
                                resHistogram.Add(newEntry.Current.Key, null);
                            }
                            if (!newEntry.MoveNext())
                            {
                                newHistogramEnd = true;
                                break;
                            }
                        }
                        if (newHistogramEnd)
                        {
                            break;
                        }
                    }
                    else
                    {
                        var curTuple = entry.Value;
                        var newTuple = newEntry.Current.Value;
                        if (curTuple.Item2 == false && newTuple.Item2 == false)
                        {
                            notPopularValues.Add(entry.Key);
                            curNotPopularCount += curTuple.Item1;
                            newNotPopularCount += newTuple.Item1;
                            resHistogram.Add(entry.Key, null);
                        }
                        else
                        {
                            var count1 = curTuple.Item2 ? curTuple.Item1 : curDefaultRow;
                            var count2 = newTuple.Item2 ? newTuple.Item1 : newDefaultRow;
                            var tmpCount = count1 * count2;
                            resRowCount += tmpCount;
                            resHistogram.Add(entry.Key, new Tuple<double, bool>(tmpCount, true));
                        }
                        if (!newEntry.MoveNext())
                        {
                            newHistogramEnd = true;
                        }
                    }


                }
            }
            double density = -1;
            if (notPopularValues.Any())
            {
                var resDefaultRow = curNotPopularCount * newNotPopularCount *
                                    Math.Min(curDefaultRow / curNotPopularCount, newDefaultRow / newNotPopularCount);
                resRowCount += resDefaultRow;
                resDefaultRow = resDefaultRow / notPopularValues.Count;
                density = resDefaultRow / resRowCount;
                foreach (var value in notPopularValues)
                {
                    resHistogram[value] = new Tuple<double, bool>(resDefaultRow, false);
                }
            }

            return new EdgeStatistics
            {
                Histogram = resHistogram,
                Density = density < 0 ? Math.Max(curStatistics.Density, newStatistics.Density) : density,
                MaxValue = Math.Max(curStatistics.MaxValue, newStatistics.MaxValue),
                RowCount = resRowCount,
                Selectivity = resRowCount / (curStatistics.RowCount * newStatistics.RowCount),
            };
        }
    }
}
