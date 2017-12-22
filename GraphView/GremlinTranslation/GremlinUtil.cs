using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
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

        public static void ClearCounters()
        {
            _vertexCount = 0;
            _edgeCount = 0;
            _tableCount = 0;
        }

        internal static GremlinVariableType GetContextListType(List<GremlinToSqlContext> contextList)
        {
            if (contextList.Count == 0)
            {
                return GremlinVariableType.NULL;
            }

            HashSet<GremlinVariableType> variableTypes = new HashSet<GremlinVariableType>();
            foreach (var context in contextList)
            {
                variableTypes.Add(context.PivotVariable.GetVariableType());
            }
            variableTypes.Remove(GremlinVariableType.NULL);

            GremlinVariableType variableType;
            if (variableTypes.Count == 0)
            {
                variableType = GremlinVariableType.NULL;
            }
            else if (variableTypes.Count == 1)
            {
                variableType = variableTypes.First();
            }
            else
            {
                variableTypes.Remove(GremlinVariableType.Vertex);
                variableTypes.Remove(GremlinVariableType.Edge);
                variableTypes.Remove(GremlinVariableType.VertexAndEdge);

                if (variableTypes.Count == 0)
                {
                    variableType = GremlinVariableType.VertexAndEdge;
                }
                else
                {
                    variableType = GremlinVariableType.Mixed;
                }
            }
           

            return variableType;
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
        public GremlinKeyword.PropertyCardinality Cardinality { get; set; }
        public string Key { get; set; }
        public object Value { get; set; }
        public Dictionary<string, object> MetaProperties { get; set; }

        public GremlinProperty(GremlinKeyword.PropertyCardinality cardinality,
            string key, object value, Dictionary<string, object> metaProperties)
        {
            Cardinality = cardinality;
            Key = key;
            Value = value;
            MetaProperties = metaProperties ?? new Dictionary<string, object>();
        }

        internal WScalarExpression ReplaceContextToScalerExpr(object value)
        {
            if (value is GremlinToSqlContext)
            {
                GremlinToSqlContext valueContext = value as GremlinToSqlContext;
                return SqlUtil.GetScalarSubquery(valueContext.ToSelectQueryBlock());
            }
            return SqlUtil.GetValueExpr(value);
        }

        public WPropertyExpression ToPropertyExpr()
        {
            WScalarExpression valueExpr = ReplaceContextToScalerExpr(this.Value);
            Dictionary<WValueExpression, WScalarExpression> metaPropertiesExpr = new Dictionary<WValueExpression, WScalarExpression>();
            foreach (string metaKey in MetaProperties.Keys)
            {
                metaPropertiesExpr[SqlUtil.GetValueExpr(metaKey)] = ReplaceContextToScalerExpr(MetaProperties[metaKey]);
            }

            return new WPropertyExpression()
            {
                Cardinality = Cardinality,
                Key = SqlUtil.GetValueExpr(Key),
                Value = valueExpr,
                MetaProperties = metaPropertiesExpr
            };
        }
    }

    internal class GremlinMatchPath
    {
        public GremlinFreeVertexVariable SourceVariable { get; set; }
        public GremlinFreeEdgeVariable EdgeVariable { get; set; }
        public GremlinFreeVertexVariable SinkVariable { get; set; }
        public bool IsReversed { get; set; }

        public GremlinMatchPath(GremlinFreeVertexVariable sourceVariable, GremlinFreeEdgeVariable edgeVariable, GremlinFreeVertexVariable sinkVariable, bool isReversed)
        {
            SourceVariable = sourceVariable;
            EdgeVariable = edgeVariable;
            SinkVariable = sinkVariable;
            IsReversed = isReversed;
        }
    }

    internal class TraversalRing
    {
        public List<GraphTraversal> Traversals { get; set; }
        public int CurrentTravsersal { get; set; }

        public TraversalRing(List<GraphTraversal> traversals)
        {
            Traversals = new List<GraphTraversal>(traversals);
            CurrentTravsersal = -1;
        }

        public GraphTraversal Next()
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

    [Serializable]
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

    [Serializable]
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

    [Serializable]
    public class ShuffleOrder : IComparer
    {
        private Random random = new Random();
        public int Compare(object x, object y)
        {
            return random.NextDouble() > 0.5 ? 1 : -1;
        }

        [OnDeserialized]
        private void Reconstruct(StreamingContext context)
        {
            this.random = new Random();
        }

    }
}
