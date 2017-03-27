using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    public class GremlinKeyword
    {
        public const bool OLD = true;

        public const string DefaultId = "id";
        public const string Label = "label";
        public const string SinkLabel = "_sinkLabel";
        public const string EdgeID = "id";
        public const string EdgeReverseID = "_reverse_ID";
        public const string EdgeSourceV = "_source";
        public const string EdgeSinkV = "_sink";
        public const string EdgeOtherV = "_other";
        //public const string EdgeOffset = "_offset";
        public const string NodeID = "id";
        public const string EdgeAdj = "_edge";
        public const string ReverseEdgeAdj = "_reverse_edge";
        public const string Path = "_path";
        public const string Star = "*";
        public const string Compose1TableDefaultName = "C";
        public const string RepeatInitalTableName = "R";
        public const string RepeatColumnPrefix = "key_";
        public static string TableDefaultColumnName = "value$" + Guid.NewGuid().ToString().Substring(0, 8);

        public static class func
        {
            public const string Coalesce = "Coalesce";
            public const string Constant = "Constant";
            public const string OutE = "OutE";
            public const string InE = "InE";
            public const string BothE = "BothE";
            public const string FlatMap = "FlatMap";
            public const string Key = "Key";
            public const string Local = "Local";
            public const string OutV = "OutV";
            public const string InV = "InV";
            public const string BothV = "BothV";
            public const string EtoV = "EtoV";
            public const string V = "V";
            public const string Optional = "Optional";
            public const string Properties = "Properties";
            public const string AllProperties = "AllProperties";
            public const string VertexProperties = "Properties";
            public const string Repeat = "Repeat";
            public const string Value = "Value";
            public const string Values = "Values";
            public const string AllValues = "AllValues";
            public const string Unfold = "Unfold";
            public const string Union = "Union";
            public const string Project = "Project";
            public const string AddV = "AddV";
            public const string AddE = "AddE";
            public const string SideEffect = "SideEffect";
            public const string DedupGlobal = "DedupGlobal";
            public const string DedupLocal = "DedupLocal";
            public const string Fold = "fold";
            public const string Count = "count";
            public const string Drop = "Drop";
            public const string UpdateProperties = "UpdateProperties";
            public const string Path = "Path";
            public const string Inject = "Inject";
            public const string Tree = "Tree";
            public const string OtherV = "OtherV";
            public const string Expand = "Expand";
            public const string Map = "Map";
            public const string Compose1 = "Compose1";
            public const string Compose2 = "Compose2";
            public const string Group = "Group";
            public const string Cap = "Cap";
            public const string Store = "Store";
            public const string Aggregate = "Aggregate";
            public const string Coin = "Coin";
            public const string CountLocal = "CountLocal";
            public const string RangeLocal = "RangeLocal";
            public const string MinLocal = "MinLocal";
            public const string MaxLocal = "MaxLocal";
            public const string MeanLocal = "MeanLocal";
            public const string Min = "Min";
            public const string Max = "Max";
            public const string Mean = "Mean";
            public const string Sum = "Sum";
            public const string SumLocal = "SumLocal";
            public const string OrderGlobal = "OrderGlobal";
            public const string OrderLocal = "OrderLocal";
            public const string Path2 = "Path2";
            public const string Range = "Range";
            public const string Decompose1 = "Decompose1";
            public const string SimplePath = "SimplePath";
            public const string CyclicPath = "CyclicPath";
            public const string ValueMap = "ValueMap";
            public const string PropertyMap = "PropertyMap";
            public const string SampleGlobal = "SampleGlobal";
            public const string SampleLocal = "SampleLocal";
            public const string Barrier = "Barrier";
            public const string Choose = "Choose";
            public const string ChooseWithOptions = "ChooseWithOptions";
            public const string Select = "Select";
            public const string SelectOne = "SelectOne";
            public const string SelectColumn = "SelectColumn";
            public const string GraphViewId = "Id";
            public const string GraphViewLabel = "Label";
        }

        public enum Pop
        {
            All,
            First,
            Last
        }

        public enum Column
        {
            //The Values and Keys enums are from Column which is used to select "columns" from a Map, Map.Entry, or Path. 
            Keys,
            Values
        }

        public enum Scope
        {
            Local,
            Global
        }


        public enum Order
        {
            Shuffle,
            Decr,
            Incr
        }

        public enum PropertyCardinality
        {
            Single,   // Set
            List     // Append
            //set
        }

        public enum Pick
        {
            Any,
            None
        }

        public static readonly Dictionary<string, string> GremlinStepToGraphTraversalDict = new Dictionary
            <string, string>()
            {
                {"as", "As"},
                {"addV", "AddV"},
                {"addE", "AddE"},
                {"aggregate", "Aggregate"},
                {"and", "And"},
                {"barrier", "Barrier"},
                {"both", "Both"},
                {"bothE", "BothE"},
                {"bothV", "BothV"},
                {"by", "By"},
                {"cap", "Cap"},
                {"count", "Count"},
                {"choose", "Choose"},
                {"constant", "Constant"},
                {"coalesce", "Coalesce"},
                {"cyclicPath", "CyclicPath"},
                {"coin", "Coin"},
                {"drop", "Drop"},
                {"dedup", "Dedup"},
                {"emit", "Emit"},
                {"fold", "Fold"},
                {"from", "From"},
                {"flatMap", "FlatMap"},
                {"group", "Group"},
                {"groupCount", "GroupCount"},
                {"has", "Has"},
                {"hasId", "HasId"},
                {"hasLabel", "HasLabel"},
                {"hasKey", "HasKey"},
                {"hasValue", "HasValue"},
                {"is", "Is"},
                {"inject", "Inject"},
                {"id", "Id"},
                {"identity", "Identity"},
                {"inV", "InV"},
                {"in", "In"},
                {"inE", "InE"},
                {"key", "Key"},
                {"label", "Label"},
                {"local", "Local"},
                {"limit", "Limit"},
                {"map", "Map"},
                {"match", "Match"},
                {"max", "Max"},
                {"mean", "Mean"},
                {"min", "Min"},
                {"not", "Not"},
                {"option", "Option"},
                {"optional", "Optional"},
                {"or", "Or" },
                {"order", "Order"},
                {"otherV", "OtherV"},
                {"out", "Out"},
                {"outE", "OutE"},
                {"outV", "OutV"},
                {"path", "Path"},
                {"property", "Property"},
                {"properties", "Properties"},
                {"project", "Project"},
                {"propertyMap", "PropertyMap"},
                {"range", "Range"},
                {"repeat", "Repeat"},
                {"sample", "Sample"},
                {"sideEffect", "SideEffect"},
                {"select", "Select"},
                {"simplePath", "SimplePath"},
                {"store", "Store"},
                {"sum", "Sum"},
                {"until", "Until"},
                {"to", "To"},
                {"tree", "Tree"},
                {"tail", "Tail"},
                {"timeLimit", "TimeLimit"},
                {"times", "Times"},
                {"unfold", "Unfold"},
                {"union", "Union"},
                {"values", "Values"},
                {"V", "V"},
                {"where", "Where"},
                {"value", "Value"},
                {"valueMap", "ValueMap"},
                {"next", "Next"},
                {"toList", "ToList"}
            };

        public static readonly Dictionary<string, string> GremlinMainStepToGraphTraversalDict = new Dictionary
            <string, string>()
            {
                {"__.", "GraphTraversal2.__()."},
                {"g.", "graph.g()."}

            };

        public static readonly Dictionary<string, string> GremlinPredicateToGraphTraversalDict = new Dictionary
            <string, string>()
            {
                {"eq", "Predicate.eq"},
                {"neq", "Predicate.neq"},
                {"lt", "Predicate.lt"},
                {"lte", "Predicate.lte"},
                {"gt", "Predicate.gt"},
                {"gte", "Predicate.gte"},
                {"inside", "Predicate.inside"},
                {"outside", "Predicate.outside"},
                {"between", "Predicate.between"},
                {"within", "Predicate.within"},
                {"without", "Predicate.without"}
            };

        public static readonly Dictionary<string, string> GremlinKeywordToGraphTraversalDict = new Dictionary<string, string>()
        {
            {"last", "GremlinKeyword.Pop.Last"},
            {"first", "GremlinKeyword.Pop.First"},

            {"decr", "GremlinKeyword.Order.Decr"},
            {"incr", "GremlinKeyword.Order.Incr"},
            {"shuffle", "GremlinKeyword.Order.Shuffle"}
        };
    }
}
