using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinBranchVariable : GremlinVariable
    {
        protected static int _count = 0;

        internal virtual string GenerateTableAlias()
        {
            return "B_" + _count++;
        }

        public List<List<GremlinVariable>> BrachVariableList { get; set; }
        public GremlinVariable ParentVariable { get; set; }
        public string Label { get; set; }

        public GremlinBranchVariable(string label, GremlinVariable parentVariable)
        {
            Label = label;
            BrachVariableList = new List<List<GremlinVariable>>();
            ParentVariable = parentVariable;
            VariableName = GenerateTableAlias();
        }

        internal override GremlinVariableType GetVariableType()
        {
            foreach (var branchVariable in BrachVariableList)
            {
                if (branchVariable.Count() > 1) return GremlinVariableType.Table;
            }

            if (checkIsTheSameType())
            {
                return BrachVariableList.First().First().GetVariableType();
            }
            else
            {
                return GremlinVariableType.Table;
            }
        }

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            foreach (var branchVariable in BrachVariableList)
            {
                if (branchVariable.Count() > 1)
                {
                    throw new NotImplementedException();
                    //return new GremlinVariableProperty(ParentVariable, Label);
                }
            }
            if (checkIsTheSameType())
            {
                switch (BrachVariableList.First().First().GetVariableType())
                {
                    case GremlinVariableType.Table:
                        throw new NotImplementedException();
                        //return new GremlinVariableProperty(ParentVariable, GremlinKeyword.TableValue);
                    case GremlinVariableType.Edge:
                        return ParentVariable.GetVariableProperty(GremlinKeyword.EdgeID);
                    case GremlinVariableType.Scalar:
                        return ParentVariable.GetVariableProperty(GremlinKeyword.ScalarValue);
                    case GremlinVariableType.Vertex:
                        return ParentVariable.GetVariableProperty(GremlinKeyword.NodeID);
                }
            }
            throw new NotImplementedException();
            //return new GremlinVariableProperty(ParentVariable, GremlinKeyword.TableValue);
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            foreach (var branchVariable in BrachVariableList)
            {
                if (branchVariable.Count() > 1)
                {
                    throw new NotImplementedException();
                    return new GremlinVariableProperty(ParentVariable, Label);
                }
            }
            if (checkIsTheSameType())
            {
                switch (BrachVariableList.First().First().GetVariableType())
                {
                    case GremlinVariableType.Table:
                        throw new NotImplementedException();
                        //return new GremlinVariableProperty(ParentVariable, GremlinKeyword.TableValue);
                    case GremlinVariableType.Edge:
                        return ParentVariable.GetVariableProperty(GremlinKeyword.Star);
                    case GremlinVariableType.Scalar:
                        return ParentVariable.GetVariableProperty(GremlinKeyword.ScalarValue);
                    case GremlinVariableType.Vertex:
                        return ParentVariable.GetVariableProperty(GremlinKeyword.Star);
                }
            }
            throw new NotImplementedException();
        }

        private bool checkIsTheSameType()
        {
            List<GremlinVariable> checkList = new List<GremlinVariable>();
            foreach (var branchVariable in BrachVariableList)
            {
                checkList.Add(branchVariable.First());
            }
            return GremlinUtil.IsTheSameType(checkList);
        }

        internal override string BottomUpPopulate(string property, GremlinVariable terminateVariable, string alias,
            string columnName = null)
        {
            foreach (var variableList in BrachVariableList)
            {
                foreach (var variable in variableList)
                {
                    variable.BottomUpPopulate(property, terminateVariable, alias, columnName);
                }
            }
            return alias + "_" + property;
        }
    }
}
