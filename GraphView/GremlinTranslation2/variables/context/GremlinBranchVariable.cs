using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinBranchVariable: GremlinVariable
    {
        public List<List<GremlinVariable>> BrachVariableList { get; set; }

        public GremlinBranchVariable()
        {
            BrachVariableList = new List<List<GremlinVariable>>();
        }

        internal override GremlinVariableType GetVariableType()
        {
            foreach (var branchVariable in BrachVariableList)
            {
                if (branchVariable.Count() > 1) return GremlinVariableType.Table;
            }
            
            List<GremlinVariable> checkList = new List<GremlinVariable>();
            foreach (var branchVariable in BrachVariableList)
            {
                checkList.Add(branchVariable.First());
            }
            if (GremlinUtil.IsTheSameType(checkList))
            {
                return checkList.First().GetVariableType();
            }
            else
            {
                return GremlinVariableType.Table;
            }
        }
    }
}
