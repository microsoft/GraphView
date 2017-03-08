using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView {
    internal class GremlinSelectedVariable: GremlinVariable
    {
        public bool IsFromSelect { get; set; }
        public GremlinKeyword.Pop Pop { get; set; }
        public string SelectKey { get; set; }
        public GremlinVariable RealVariable { get; set; }

        //internal override WEdgeType GetEdgeType()
        //{
        //    return RealVariable.GetEdgeType();
        //}

        internal override string GetProjectKey()
        {
            return RealVariable.GetProjectKey();
        }

        internal override GremlinVariableType GetVariableType()
        {
            return RealVariable.GetVariableType();
        }

        internal override GremlinVariableType GetUnfoldVariableType()
        {
            return RealVariable.GetUnfoldVariableType();
        }
    }
}
