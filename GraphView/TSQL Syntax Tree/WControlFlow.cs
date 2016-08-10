using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.TSQL_Syntax_Tree
{
    internal class WChoose : WSqlStatement
    {
        internal List<WSelectQueryBlock> InputExpr { get; set; }

        public override string ToString()
        {
            List<string> ChooseString = new List<string>();
            foreach (var x in InputExpr)
                ChooseString.Add(x.ToString());
            return string.Join("", ChooseString);
        }

        public override GraphViewOperator Generate(GraphViewConnection dbConnection)
        {
            List<GraphViewOperator> Source = new List<GraphViewOperator>();
            foreach (var x in InputExpr)
            {
                Source.Add(x.Generate(dbConnection));
            }
            return new UnionOperator(dbConnection,Source,Source[0].header);
        }
    }

    internal class WCoalesce : WSqlStatement
    {
        internal List<WSelectQueryBlock> InputExpr { get; set; }
        internal int CoalesceNumber { get; set; }
        public override string ToString()
        {
            List<string> ChooseString = new List<string>();
            foreach (var x in InputExpr)
                ChooseString.Add(x.ToString());
            return string.Join("", ChooseString);
        }

        public override GraphViewOperator Generate(GraphViewConnection dbConnection)
        {
            List<GraphViewOperator> Source = new List<GraphViewOperator>();
            foreach (var x in InputExpr)
            {
                Source.Add(x.Generate(dbConnection));
            }
            return new CoalesceOperator(dbConnection, Source, CoalesceNumber, Source[0].header);
        }
    }
}
