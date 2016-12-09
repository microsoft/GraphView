using GraphView.GremlinTranslationOps.map;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps
{
    internal interface IGremlinByModulating
    {
        void ModulateBy();
        void ModulateBy(GraphTraversal2 paramOp);
        void ModulateBy(string key);
        void ModulateBy(Order order);

    }
}
