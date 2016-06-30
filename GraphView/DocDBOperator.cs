using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal interface DocDBOperator
    {
        void Open();
        void Close();
        object Next();
    }

    internal abstract class DocDBOperatorProcessor : DocDBOperator
    {
        public bool statue;
        public void Open()
        {
            statue = true;
        }
        public void Close()
        {
            statue = false;
        }
        public abstract object Next();
    }
}
