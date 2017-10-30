using System;
using System.Collections.Generic;

namespace GraphView
{
    internal static class SamplingAlgorithm
    {

        public static void WeightedReservoirSample<T>(List<T> sampleSource, List<double> weight, int amountToSample,
            Random random, out List<T> sampleResult)
        {
            sampleResult = new List<T>();
            if (amountToSample <= 0)
            {
                return;
            }
            if (sampleSource.Count <= amountToSample)
            {
                sampleResult = sampleSource;
                return;
            }

            // modify weight
            for (int i = 0; i < weight.Count; ++i)
            {
                weight[i] = Math.Abs(weight[i]);
            }

            double WSum = 0;

            for (int i = 0; i < amountToSample; i++)
            {
                sampleResult.Add(sampleSource[i]);
                WSum += weight[i] / amountToSample;
            }

            if (WSum < 1e-9)
            {
                return;
            }

            for (int i = amountToSample; i < sampleSource.Count; i++)
            {
                WSum += weight[i] / amountToSample;
                double probability = weight[i] / WSum;
                if (random.NextDouble() <= probability)
                {
                    sampleResult[random.Next(0, amountToSample)] = sampleSource[i];
                }
            }
        }

        public static void ReservoirSample<T>(List<T> sampleSource, int amountToSample,
            Random random, out List<T> sampleResult)
        {
            sampleResult = new List<T>();
            if (amountToSample <= 0)
            {
                return;
            }
            if (sampleSource.Count <= amountToSample)
            {
                sampleResult = sampleSource;
                return;
            }

            for (int i = 0; i < amountToSample; i++)
            {
                sampleResult.Add(sampleSource[i]);
            }

            int threshold = amountToSample * 4;
            int sourceIndex = amountToSample;
            while (sourceIndex < sampleSource.Count && sourceIndex < threshold)
            {
                int resIndex = random.Next(0, sourceIndex);
                if (resIndex < amountToSample)
                {
                    sampleResult[resIndex] = sampleSource[sourceIndex];
                }
                sourceIndex++;
            }

            while (sourceIndex < sampleSource.Count)
            {
                double probability = (double)amountToSample / sourceIndex;
                // need a random in (0, 1]
                // NextDouble() -> [0, 1)
                // The actual upper bound of the random number returned by NextDouble() is 0.99999999999999978.
                double rand = random.NextDouble() + 2.2e-16;
                int gapSize = (int)Math.Floor(Math.Log(rand) / Math.Log(1 - probability));
                sourceIndex = checked(sourceIndex + gapSize);
                if (sourceIndex < sampleSource.Count)
                {
                    int resIndex = random.Next(0, amountToSample);
                    sampleResult[resIndex] = sampleSource[sourceIndex];
                    sourceIndex++;
                }
            }
        }
    }
}
