using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    public class GremlinKeyword
    {
        public const string Label = "label";
        public const string EdgeID = "_ID";
        public const string NodeID = "id";

        public static class func
        {
            public const string Coalesce = "Coalesce";
            public const string Constant = "Constant";
            public const string OutE = "OutE";
            public const string BothE = "BothE";
            public const string FlatMap = "FlatMap";
            public const string Key = "Key";
            public const string Local = "Local";
            public const string OutV = "OutV";
            public const string BothV = "BothV";
            public const string Optional = "Optional";
            public const string Properties = "Properties";
            public const string Repeat = "Repeat";
            public const string Value = "Value";
            public const string Values = "Values";
            public const string Unfold = "Unfold";
            public const string Union = "Union";
            public const string Project = "Project";
            public const string AddV = "AddV";
            public const string AddE = "AddE";
        }
        public enum Pop
        {
            Default,
            first,
            last
        }

        public enum Column
        {
            //The values and keys enums are from Column which is used to select "columns" from a Map, Map.Entry, or Path. 
            keys,
            values
        }

        public enum Scope
        {
            local,
            global
        }
        public enum Order
        {
            Shuffle,
            Desr,
            Incr
        }
    }
}
