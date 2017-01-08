using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;

namespace GraphView { 
    internal class GremlinSqlTableVariable: ISqlTable
    {
        internal virtual void Populate(string property)
        {
            
        }

        public virtual WTableReference ToTableReference(List<string> projectProperties, string tableName)
        {
            throw new NotImplementedException();
        }
    }
}
