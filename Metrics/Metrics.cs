using GraphView;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using Newtonsoft.Json;
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
		public static int RandToInt(double t)
		{
			Random rnd = new Random();
			int f = (int)Math.Floor(t);
			return f + (rnd.NextDouble() < t - f ? 1 : 0);
		}
		public static double ApproxTriangleCountingBySamplingD(GraphViewCommand g)
		{
			int nv = g.g().V().Next().Count;
			int ne = g.g().E().Next().Count;

			Random rnd = new Random();
			int sq = (int)Math.Sqrt(nv);
			int eTry = (int)1e6;

			long Usize = 0;
			int n_big = 0;

			for (int v = 0; v < nv; v++)
			{
				List<string> adj = g.g().V().HasId(v).Both().Values("id").Next();

				int d = adj.Count;
				if (d <= sq)
				{
					long d2 = (long)d * (d - 1) / 2;
					Usize += d2;
				}
				else
				{
					Usize += ne;
					n_big++;
				}
			}

			int n_blist = 0;
			string[] blist = new string[n_big];

			int hit = 0;
			int shoot = 0;

			foreach (var v in g.g().V())
			{
				List<string> adj = g.g().V().HasId(v).Both().Values("id").Next();
				int d = adj.Count;
				if (d <= sq)
				{
					if (d >= 2)
					{
						int tn = RandToInt((double)d * (d - 1) / 2 / Usize * eTry);

						for (int i = 0; i < tn; i++)
						{
							int ui, vi;
							do
							{
								ui = rnd.Next() % d;
								vi = rnd.Next() % d;
							} while (ui >= vi);

							shoot++;
							string uu = adj[ui];
							string vv = adj[vi];
							hit += g.g().V().HasId(uu).Both().HasId(vv).Next().Count;
						}
					}
				}
				else
				{
					blist[n_blist++] = v;
				}
			}

			double p_big = (double)n_big * ne / Usize;

			g.OutputFormat = OutputFormat.GraphSON;
			foreach (var stre in g.g().E())
			{
				var e = JsonConvert.DeserializeObject<dynamic>(stre);
				string su = (string)e["inV"], sv = (string)e["outV"];
				int tn = RandToInt(p_big / n_big * eTry);

				for (int i = 0; i < tn; ++i)
				{
					string v = blist[rnd.Next() % n_blist];

					shoot++;
					if (g.g().V().HasId(su).Both().HasId(v).Next().Count == 1
						&& g.g().V().HasId(sv).Both().HasId(v).Next().Count == 1)
						hit++;
				}
			}

			return ((double)hit / shoot) * Usize;
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
