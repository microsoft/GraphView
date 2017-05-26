using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Metrics
{
    class LocalGraph
    {
        public int NV, NE;
        public List<string> Label = new List<string>();
        public Dictionary<string, int> LabelMap = new Dictionary<string, int>();
        public Dictionary<int, Dictionary<int, double>> Adj = new Dictionary<int, Dictionary<int, double>>();

        public int AddV(string label)
        {
            int v = Label.Count;
            NV++;
            Label.Add(label);
            LabelMap[label] = v;
            Adj[v] = new Dictionary<int, double>();
            return v;
        }
        public void AddE(string vlabel, string ulabel, double w = 1.0)
        {
            int v = GetV(vlabel), u = GetV(ulabel);
            Adj[v][u] = w;
            Adj[u][v] = w;
            NE++;
        }

        public int GetV(string label)
        {
            if (!LabelMap.ContainsKey(label))
                return AddV(label);
            else
                return LabelMap[label];
        }

        public IEnumerable<Edge> E()
        {
            for (int v = 0; v < NV; v++)
                foreach (var e in Adj[v])
                    if (v < e.Key)
                        yield return new Edge(v, e.Key, e.Value);
        }
        public IEnumerable<Edge> BothE()
        {
            for (int v = 0; v < NV; v++)
                foreach (var e in Adj[v])
                    yield return new Edge(v, e.Key, e.Value);
        }

        public long CountTriangles()
        {
            long counter = 0;
            for (int v = 0; v < NV; v++)
            {
                List<int> g = new List<int>();
                foreach (int u in Adj[v].Keys)
                    if (Adj[u].Count > Adj[v].Count || (Adj[u].Count == Adj[v].Count && u > v))
                        g.Add(u);

                for (int i = 0; i < g.Count; i++)
                    for (int j = 0; j < i; j++)
                        counter += Adj[g[i]].ContainsKey(g[j]) ? 1 : 0;
            }
            return counter;
        }
    }
}
