using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinUtil
    {
        internal static bool IsTheSameOutputType(List<GremlinToSqlContext> contextList)
        {
            if (contextList.Count <= 1) return true;
            bool isSameType = true;
            for (var i = 1; i < contextList.Count; i++)
            {
                isSameType = contextList[i - 1].PivotVariable.GetVariableType() ==
                             contextList[i].PivotVariable.GetVariableType();
                if (isSameType == false) return false;
            }
            return isSameType;
        }

        internal static bool IsTheSameType(List<GremlinVariable> variableList)
        {
            if (variableList.Count <= 1) return true;
            bool isSameType = true;
            for (var i = 1; i < variableList.Count; i++)
            {
                isSameType = variableList[i - 1].GetVariableType() ==
                             variableList[i].GetVariableType();
                if (isSameType == false) return false;
            }
            return isSameType;
        }
    }
}
