using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.RecordRuntime
{
    internal class IndexValue
    {
        private List<object> keys;

        public List<object> Keys
        {
            get
            {
                return keys;
            }
            set
            {
                this.keys = value;
            }
        }
    }

    public class IndexSpecification
    {
        // A list of properties to be indexed    
        IList<string> properties;
    }

}
