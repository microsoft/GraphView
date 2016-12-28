using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps
{
    public class GremlinKeyword
    {
        public enum Pop
        {
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
