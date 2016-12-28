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
        public List<GremlinVariable2> VariableList { get; private set; }


        public List<ISqlTable> TableReferences { get; private set; }
        public List<ISqlScalar> ProjectedVariables;
        public GremlinGroupVariable GroupVariable { get; set; }

        public GremlinToSqlContext2()
        {
            TaggedVariables = new Dictionary<string, GremlinVariable2>();
            ProjectedVariables = new List<ISqlScalar>();
            VariableList = new List<GremlinVariable2>();
        }

        public GremlinToSqlContext2 Duplicate()
        {
            return new GremlinToSqlContext2()
            {
                VariableList = new List<GremlinVariable2>(this.VariableList),
                TaggedVariables = new Dictionary<string, GremlinVariable2>(this.TaggedVariables),
                PivotVariable = this.PivotVariable,
                TableReferences = new List<ISqlTable>(this.TableReferences),
                ProjectedVariables = new List<ISqlScalar>(ProjectedVariables),
                GroupVariable = GroupVariable   // more properties need to be added when GremlinToSqlContext2 is changed.
            };
        }

        public void Reset()
        {
            PivotVariable = null;
            TaggedVariables.Clear();
            VariableList.Clear();

            TableReferences.Clear();
            ProjectedVariables.Clear();
            GroupVariable = null;

            // More resetting goes here when more properties are added to GremlinToSqlContext2
        }

        internal void Coalesce(
            GremlinToSqlContext2 traversal1,
            GremlinToSqlContext2 traversal2)
        {
            
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
        }
    }

    
}
