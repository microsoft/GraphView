using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinToSqlContext2
    {
        public GremlinVariable2 PivotVariable { get; set; }
        public Dictionary<string, GremlinVariable2> TaggedVariables;
        public List<GremlinScalarVariable> ProjectedVariables;

        public List<GremlinVariable2> VariableList { get; private set; }

        public GremlinGroupVariable GroupVariable { get; set; }

        public GremlinToSqlContext2()
        {
            TaggedVariables = new Dictionary<string, GremlinVariable2>();
            ProjectedVariables = new List<GremlinScalarVariable>();
            VariableList = new List<GremlinVariable2>();
        }

        internal void As(string asName)
        {
            TaggedVariables[asName] = PivotVariable;
        }

        internal void By(GremlinToSqlContext2 byContext)
        {
            // To consider: By steps may be applied to variables in order
            // PivotVariable.By(this, byContext);
            if (VariableList.Count > 0)
            {
                VariableList[VariableList.Count - 1].By(this, byContext);
            }
        }

        internal void By(string byName)
        {
            // To consider: BY steps may be applied to variables in order
            // PivotVariable.By(this, byName);
            if (VariableList.Count > 0)
            {
                VariableList[VariableList.Count - 1].By(this, byName);
            }
        }

        internal void Cap(params string[] keys)
        {
            ProjectedVariables.Clear();

            foreach (string key in keys)
            {
                if (!TaggedVariables.ContainsKey(key))
                {
                    throw new QueryCompilationException(string.Format("The specified tag \"{0}\" cannot be found.", key));
                }
            }
        }

        internal void Coalesce(
            GremlinToSqlContext2 traversal1,
            GremlinToSqlContext2 traversal2)
        {
            GremlinVariableType type1 = traversal1.ProjectedVariables.Count == 1 ?
                traversal1.ProjectedVariables[0].GetVariableType() : GremlinVariableType.Table;
            GremlinVariableType type2 = traversal2.ProjectedVariables.Count == 1 ?
                traversal2.ProjectedVariables[1].GetVariableType() : GremlinVariableType.Table;

            if (type1 == type2)
            {
                switch (type1)
                {
                    case GremlinVariableType.Vertex:
                        GremlinCoalesceVertexVariable vertexVariable = new GremlinCoalesceVertexVariable(traversal1, traversal2);
                        VariableList.Add(vertexVariable);
                        PivotVariable = vertexVariable;
                        break;
                    case GremlinVariableType.Edge:
                        GremlinCoalesceEdgeVariable edgeVariable = new GremlinCoalesceEdgeVariable(traversal1, traversal2);
                        VariableList.Add(edgeVariable);
                        PivotVariable = edgeVariable;
                        break;
                    case GremlinVariableType.Table:
                        GremlinCoalesceTableVariable tabledValue = new GremlinCoalesceTableVariable(traversal1, traversal2);
                        VariableList.Add(tabledValue);
                        PivotVariable = tabledValue;
                        break;
                    case GremlinVariableType.Scalar:
                        PivotVariable = new GremlinCoalesceValueVariable(traversal1, traversal2);
                        break;
                    default:
                        break;
                }
            }
            else
            {
                GremlinCoalesceTableVariable tabledValue = new GremlinCoalesceTableVariable(traversal1, traversal2);
                VariableList.Add(tabledValue);
                PivotVariable = tabledValue;
            }
        }

        internal void Populate(string propertyName)
        {
            // For a query with a GROUP BY clause, the ouptut format is determined
            // by the aggregation functions following GROUP BY and cannot be changed.
            if (GroupVariable != null)
            {
                return;
            }

            PivotVariable.Populate(propertyName);
            ProjectedVariables.Add(new GremlinVariableProperty(PivotVariable, propertyName));
        }

        internal void Group()
        {
            GroupVariable = new GremlinGroupVariable();
            VariableList.Add(GroupVariable);
        }

        internal void Both()
        {

        }
    }

    
}
