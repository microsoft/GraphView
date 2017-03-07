using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinBranchVariable : GremlinVariable
    {
        public static GremlinBranchVariable Create(string label,
                                                    GremlinVariable parentVariable,
                                                    List<List<GremlinVariable>> brachVariableList)
        {
            //TODO: refactor
            if (CheckIsTheSameType(brachVariableList))
            {
                switch (brachVariableList.First().First().GetVariableType())
                {
                    case GremlinVariableType.Vertex:
                        return new GremlinBranchVertexVariable(label, parentVariable, brachVariableList);
                    case GremlinVariableType.Edge:
                        return new GremlinBranchEdgeVariable(label, parentVariable, brachVariableList);
                    case GremlinVariableType.Scalar:
                        return new GremlinBranchScalarVariable(label, parentVariable, brachVariableList);
                    case GremlinVariableType.Property:
                        return new GremlinBranchPropertyVariable(label, parentVariable, brachVariableList);
                }
            }
            return new GremlinBranchVariable(label, parentVariable, brachVariableList);
        }

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
            return GremlinVariableType.Table;
        }

        internal override string GetProjectKey()
        {
            Populate(GremlinKeyword.TableDefaultColumnName);
            return GremlinKeyword.TableDefaultColumnName;
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

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            string primaryKey;
            switch (GetVariableType())
            {
                case GremlinVariableType.Edge:
                    primaryKey = GremlinKeyword.EdgeID;
                    break;
                case GremlinVariableType.Vertex:
                    primaryKey = GremlinKeyword.NodeID;
                    break;
                default:
                    primaryKey = GremlinKeyword.TableDefaultColumnName;
                    break;
            }
            return GetVariableProperty(primaryKey);
            //return GetVariableProperty(GetPrimaryKey());
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            return GetVariableProperty(GetProjectKey());
        }

        public static bool CheckIsTheSameType(List<List<GremlinVariable>> brachVariableList)
        {
            List<GremlinVariable> checkList = new List<GremlinVariable>();
            foreach (var branchVariable in brachVariableList)
            {
                checkList.Add(branchVariable.First());
            }
            return GremlinUtil.IsTheSameType(checkList);
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

    internal class GremlinBranchVertexVariable : GremlinBranchVariable
    {
        public GremlinBranchVertexVariable(string label, GremlinVariable parentVariable, List<List<GremlinVariable>> brachVariableList)
            :base (label, parentVariable, brachVariableList)
        {
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Vertex;
        }

        //internal override string GetPrimaryKey()
        //{
        //    Populate(GremlinKeyword.ScalarValue);
        //    return GremlinKeyword.ScalarValue;
        //}

        internal override string GetProjectKey()
        {
            Populate(GremlinKeyword.Star);
            return GremlinKeyword.Star;
        }
    }

    internal class GremlinBranchEdgeVariable : GremlinBranchVariable
    {
        public GremlinBranchEdgeVariable(string label, GremlinVariable parentVariable, List<List<GremlinVariable>> brachVariableList)
            : base(label, parentVariable, brachVariableList)
        {
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }

        //internal override string GetPrimaryKey()
        //{
        //    Populate(GremlinKeyword.EdgeID);
        //    return GremlinKeyword.EdgeID;
        //}

        internal override string GetProjectKey()
        {
            Populate(GremlinKeyword.Star);
            return GremlinKeyword.Star;
        }
    }

    internal class GremlinBranchScalarVariable : GremlinBranchVariable
    {
        public GremlinBranchScalarVariable(string label, GremlinVariable parentVariable, List<List<GremlinVariable>> brachVariableList)
            : base(label, parentVariable, brachVariableList)
        {
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Scalar;
        }

        //internal override string GetPrimaryKey()
        //{
        //    Populate(GremlinKeyword.ScalarValue);
        //    return GremlinKeyword.ScalarValue;
        //}

        //internal override string GetProjectKey()
        //{
        //    Populate(GremlinKeyword.ScalarValue);
        //    return GremlinKeyword.ScalarValue;
        //}
    }

    internal class GremlinBranchPropertyVariable : GremlinBranchVariable
    {
        public GremlinBranchPropertyVariable(string label, GremlinVariable parentVariable, List<List<GremlinVariable>> brachVariableList)
            : base(label, parentVariable, brachVariableList)
        {
        }

        internal override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Property;
        }

        //internal override string GetPrimaryKey()
        //{
        //    Populate(GremlinKeyword.PropertyValue);
        //    return GremlinKeyword.PropertyValue;
        //}

        //internal override string GetProjectKey()
        //{
        //    Populate(GremlinKeyword.PropertyValue);
        //    return GremlinKeyword.PropertyValue;
        //}
    }
}
