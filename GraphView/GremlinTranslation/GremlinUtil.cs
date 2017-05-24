using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinUtil
    {
        protected static int _vertexCount = 0;
        protected static int _edgeCount = 0;
        protected static int _tableCount = 0;

        internal static void CheckIsValueOrPredicate(params object[] valueOrPredicates)
        {
            foreach (var valueOrPredicate in valueOrPredicates)
            {
                if (!(valueOrPredicate is string
                     || IsNumber(valueOrPredicate)
                     || valueOrPredicate is bool
                     || valueOrPredicate is Predicate))
                                    throw new ArgumentException();
            }
        }

        internal static string GenerateTableAlias(GremlinVariableType variableType)
        {
            switch (variableType)
            {
                case GremlinVariableType.Vertex:
                    return "N_" + _vertexCount++;
                case GremlinVariableType.Edge:
                    return "E_" + _edgeCount++;
            }
            return "R_" + _tableCount++;
        }

        internal static GremlinVariableType GetContextListType(List<GremlinToSqlContext> contextList)
        {
            if (contextList.Count == 0) return GremlinVariableType.Table;
            if (contextList.Count == 1) return contextList.First().PivotVariable.GetVariableType();
            bool isSameType = true;
            for (var i = 1; i < contextList.Count; i++)
            {
                isSameType = contextList[i - 1].PivotVariable.GetVariableType() ==
                              contextList[i].PivotVariable.GetVariableType();
                if (isSameType == false) return GremlinVariableType.Table;
            }
            return contextList.First().PivotVariable.GetVariableType();
        }

        internal static bool IsVertexProperty(string property)
        {
            if (property == GremlinKeyword.EdgeAdj
                || property == GremlinKeyword.ReverseEdgeAdj)
            {
                return true;
            }
            return false;
        }

        internal static bool IsEdgeProperty(string property)
        {
            if (property == GremlinKeyword.EdgeSourceV
                || property == GremlinKeyword.EdgeSinkV
                || property == GremlinKeyword.EdgeOtherV
            )
            {
                return true;
            }
            return false;
        }

        public static bool IsNumber(object value)
        {
            return value is sbyte
                    || value is byte
                    || value is short
                    || value is ushort
                    || value is int
                    || value is uint
                    || value is long
                    || value is ulong
                    || value is float
                    || value is double
                    || value is decimal;
        }

        public static bool IsList(object o)
        {
            if (o == null) return false;
            return o is IList &&
                   o.GetType().IsGenericType &&
                   o.GetType().GetGenericTypeDefinition().IsAssignableFrom(typeof(List<>));
        }

        public static bool IsArray(object o)
        {
            if (o == null) return false;
            return o.GetType().IsArray;
        }
    }

    public class GremlinProperty
    {
        public GremlinProperty(GremlinKeyword.PropertyCardinality cardinality,
            string key, object value, Dictionary<string, object> metaProperties)
        {
            Cardinality = cardinality;
            Key = key;
            Value = value;
            MetaProperties = metaProperties ?? new Dictionary<string, object>();
        }

        public GremlinKeyword.PropertyCardinality Cardinality { get; set; }
        public string Key { get; set; }
        public object Value { get; set; }
        public Dictionary<string, object> MetaProperties { get; set; }

        public WPropertyExpression ToPropertyExpr()
        {
            Dictionary<WValueExpression, WValueExpression> metaPropertiesExpr = new Dictionary<WValueExpression, WValueExpression>();
            foreach (var property in MetaProperties)
            {
                metaPropertiesExpr[SqlUtil.GetValueExpr(property.Key)] = SqlUtil.GetValueExpr(property.Value);
            }
            return new WPropertyExpression()
            {
                Cardinality = Cardinality,
                Key = SqlUtil.GetValueExpr(Key),
                Value = SqlUtil.GetValueExpr(Value),
                MetaProperties = metaPropertiesExpr
            };
        }
    }

    internal class GremlinMatchPath
    {
        public GremlinFreeVertexVariable SourceVariable { get; set; }
        public GremlinFreeEdgeVariable EdgeVariable { get; set; }
        public GremlinFreeVertexVariable SinkVariable { get; set; }

        public GremlinMatchPath(GremlinFreeVertexVariable sourceVariable, GremlinFreeEdgeVariable edgeVariable, GremlinFreeVertexVariable sinkVariable)
        {
            SourceVariable = sourceVariable;
            EdgeVariable = edgeVariable;
            SinkVariable = sinkVariable;
        }
    }

    internal class TraversalRing
    {
        public List<GraphTraversal2> Traversals { get; set; }
        public int CurrentTravsersal { get; set; }

        public TraversalRing(List<GraphTraversal2> traversals)
        {
            Traversals = new List<GraphTraversal2>(traversals);
            CurrentTravsersal = -1;
        }

        public GraphTraversal2 Next()
        {
            if (Traversals.Count == 0)
            {
                return null;
            }
            else
            {
                this.CurrentTravsersal = (this.CurrentTravsersal + 1) % this.Traversals.Count;
                return this.Traversals[this.CurrentTravsersal];
            }
        }
    }

    public class IncrOrder : IComparer
    {
        public int Compare(object x, object y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (x is string)
                return ((IComparable)x).CompareTo((IComparable)y);
            else
                return ((IComparable)Convert.ToDouble(x)).CompareTo(Convert.ToDouble(y));
        }
    }

    public class DecrOrder : IComparer
    {
        public int Compare(object x, object y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (x is string)
                return ((IComparable)y).CompareTo((IComparable)x);
            else
                return ((IComparable)Convert.ToDouble(y)).CompareTo(Convert.ToDouble(x));
        }
    }

    public class ShuffleOrder : IComparer
    {
        private Random random = new Random();
        public int Compare(object x, object y)
        {
            return random.NextDouble() > 0.5 ? 1 : -1;
        }
    }
}
