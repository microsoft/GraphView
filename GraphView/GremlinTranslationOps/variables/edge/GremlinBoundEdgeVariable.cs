using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinBoundEdgeVariable : GremlinEdgeVariable2
    {
        private GremlinVariableProperty adjacencyList;
        // A list of edge properties to project for this edge table
        private List<string> projectedProperties;

        public GremlinBoundEdgeVariable(GremlinVariableProperty adjacencyList)
        {
            VariableName = GenerateTableAlias();
            this.adjacencyList = adjacencyList;
            projectedProperties = new List<string>();
        }

        public override GremlinVariableType GetVariableType()
        {
            return GremlinVariableType.Edge;
        }

        // To confirm: what is the default projection of edges in Gremlin
        internal override GremlinScalarVariable DefaultProjection()
        {
            return base.DefaultProjection();
        }

        internal override void Populate(string name)
        {
            if (projectedProperties.Contains(name))
            {
                projectedProperties.Add(name);
            }
        }

        internal override void OutV(GremlinToSqlContext2 currentContext)
        {
            // A naive implementation would be: add a new bound/free vertex to the variable list. 
            // A better implementation should reason the status of the edge variable and only reset
            // the pivot variable if possible, thereby avoiding adding a new vertex variable  
            // and reducing one join. 
        }
    }
}
