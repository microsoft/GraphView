using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinBranchVariable : GremlinVariable
    {
        public List<List<GremlinVariable>> BrachVariableList { get; set; }
        public GremlinVariable ParentVariable { get; set; }
        public string Label { get; set; }

        public GremlinBranchVariable(string label,
                                    GremlinVariable parentVariable,
                                    List<List<GremlinVariable>> brachVariableList)
        {
            Label = label;
            BrachVariableList = brachVariableList;
            ParentVariable = parentVariable;
        }

        internal override GremlinVariableType GetVariableType()
        {
            List<GremlinVariable> checkList = new List<GremlinVariable>();
            foreach (var branchVariable in BrachVariableList)
            {
                checkList.Add(branchVariable.First());
            }
            if (GremlinUtil.IsTheSameType(checkList))
            {
                return BrachVariableList.First().First().GetVariableType();
            }
            return GremlinVariableType.Table;;
        }

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);

            foreach (var variableList in BrachVariableList)
            {
                foreach (var variable in variableList)
                {
                    variable.BottomUpPopulate(ParentVariable, property, Label + "_" + property);
                }
            }
        }

        internal override GremlinVariableProperty GetVariableProperty(string property)
        {
            Populate(property);
            return new GremlinVariableProperty(ParentVariable, property);
        }

        internal override void BottomUpPopulate(GremlinVariable terminateVariable, string property, string columnName)
        {
            foreach (var variableList in BrachVariableList)
            {
                foreach (var variable in variableList)
                {
                    variable.BottomUpPopulate(terminateVariable, property, columnName);
                }
            }
        }
    }
}
