using GraphView;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Metrics
{
    public static class Metrics
    {
        // ===================================================================================
        // First part: Exact algorithms
        // Try to speed up them!

        // triangle counting
        public static long TriangleCounting(GraphViewCommand g)
        {
            long counter = 0;

            foreach (var v in g.g().V().Values("id"))
            {
                int deg = int.Parse(g.g().V().HasId(v).Both().Count().Next()[0]);
                var neighbors = g.g().V().HasId(v).Both().Where(
                    GraphTraversal2.__().Both().Count().Is(Predicate.gt(deg))
                ).Values("id").Next();

                neighbors.AddRange(g.g().V().HasId(v).Both().Where(
                    GraphTraversal2.__().Both().Count().Is(Predicate.eq(deg))
                ).Values("id").Next().Where(new Func<string, bool>(u => String.Compare(v, u) < 0)));

                Debug.Print("handling {0}, with {1} neighbors", v, neighbors.Count);

                for (int i = 0; i < neighbors.Count(); i++)
                    for (int j = 0; j < i; j++)
                        counter += g.g().V().HasId(neighbors[i]).Both().HasId(neighbors[j]).Next().Count;
            }

            return counter;
        }
        

        // clustering coefficient
        public static double GlobalClusteringCoefficient(GraphViewCommand g)
        {
            long tri = TriangleCounting(g);
            long deg = 0;
            foreach (var v in g.g().V().Values("id"))
            {
                long d = g.g().V().HasId(v).Both().Next().Count();
                deg += d * (d - 1);
            }
            return 6.0 * tri / deg;
        }
        public static double LocalClusteringCoefficient(GraphViewCommand g)
        {
            throw new NotImplementedException();
        }

        // ===================================================================================
        // For one vertex:
        public static long TriangleCountingForOneVertex(GraphViewCommand g, string vertedId)
        {
            long counter = 0;

            int deg = int.Parse(g.g().V().HasId(vertedId).Both().Count().Next()[0]);
            var neighbors = g.g().V().HasId(vertedId).Both().Values("id").Next();

            for (int i = 0; i < neighbors.Count(); i++)
                for (int j = 0; j < i; j++)
                    counter += g.g().V().HasId(neighbors[i]).Both().HasId(neighbors[j]).Next().Count;

            return counter;
        }

        public static double ClusteringCoefficientForOneVertex(GraphViewCommand g, string vertedId)
        {
            throw new NotImplementedException();
        }

        // ===================================================================================
        // Second part: Approximation algorithms
        // Approximation algorithm may need additional parameters, e.g., p, \epsilon, etc.
        // You can add them on your own. All of them must be user-friendly.

        // For one vertex:

        public static double ApproxTriangleCountingForOneVertex(GraphViewCommand g, string vertedId)
        {
            throw new NotImplementedException();
        }
        public static double ApproxClusteringCoefficientForOneVertex(GraphViewCommand g, string vertedId)
        {
            throw new NotImplementedException();
        }

        // ===================================================================================
        // For all vertices: (by sampling)

        public static double ApproxTriangleCountingBySamplingA(GraphViewCommand g)
        {
            throw new NotImplementedException();
        }
        public static double ApproxTriangleCountingBySamplingB(GraphViewCommand g)
        {
            throw new NotImplementedException();
        }
        public static double ApproxTriangleCountingBySamplingC(GraphViewCommand g)
        {
            throw new NotImplementedException();
        }
        public static double ApproxTriangleCountingBySamplingD(GraphViewCommand g)
        {
            throw new NotImplementedException();
        }

        public static double ApproxGlobalClusteringCoefficientBySamplingA(GraphViewCommand g)
        {
            throw new NotImplementedException();
        }
        public static double ApproxGlobalClusteringCoefficientBySamplingB(GraphViewCommand g)
        {
            throw new NotImplementedException();
        }
        public static double ApproxGlobalClusteringCoefficientBySamplingC(GraphViewCommand g)
        {
            throw new NotImplementedException();
        }
        public static double ApproxGlobalClusteringCoefficientBySamplingD(GraphViewCommand g)
        {
            throw new NotImplementedException();
        }

        public static double ApproxLocalClusteringCoefficientBySamplingA(GraphViewCommand g)
        {
            throw new NotImplementedException();
        }
        public static double ApproxLocalClusteringCoefficientBySamplingB(GraphViewCommand g)
        {
            throw new NotImplementedException();
        }
        public static double ApproxLocalClusteringCoefficientBySamplingC(GraphViewCommand g)
        {
            throw new NotImplementedException();
        }
        public static double ApproxLocalClusteringCoefficientBySamplingD(GraphViewCommand g)
        {
            throw new NotImplementedException();
        }

        // ===================================================================================
        // For all vertices: (by random walking)

        public static double ApproxTriangleCountingByRandomWalking(GraphViewCommand g)
        {
            throw new NotImplementedException();
        }
        public static double ApproxGlobalClusteringCoefficientByRandomWalking(GraphViewCommand g)
        {
            throw new NotImplementedException();
        }
        public static double ApproxLocalClusteringCoefficientByRandomWalking(GraphViewCommand g)
        {
            throw new NotImplementedException();
        }
    }
}
