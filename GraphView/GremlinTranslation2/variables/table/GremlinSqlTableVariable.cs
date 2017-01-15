using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;

namespace GraphView { 
    internal abstract class GremlinSqlTableVariable: ISqlTable
    {
        internal virtual void Populate(string property)
        {
            throw new NotImplementedException();
        }

        internal virtual void PopulateGremlinPath()
        {
            throw new NotImplementedException();    
        }

        internal virtual List<GremlinVariable> PopulateAllTaggedVariable(string label)
        {
            throw new NotImplementedException();
        }

        //internal virtual GremlinVariable PopulateFirstTaggedVariable(string label)
        //{
        //    throw new NotImplementedException();
        //}

        //internal virtual GremlinVariable PopulateLastTaggedVariable(string label)
        //{
        //    throw new NotImplementedException();
        //}

        public virtual WTableReference ToTableReference(List<string> projectProperties, string tableName, GremlinVariable gremlinVariable)
        {
            throw new NotImplementedException();
        }

        internal virtual bool ContainsLabel(string label)
        {
            throw new NotImplementedException();
        }
    }
}
