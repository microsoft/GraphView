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

        internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        {
            List<GraphViewExecutionOperator> Source = new List<GraphViewExecutionOperator>();
            foreach (var x in InputExpr)
            {
                Source.Add(x.Generate(dbConnection));
            }
            return new UnionOperator(dbConnection,Source);
        }
    }

    internal class WCoalesce : WSqlStatement
    {
        internal List<WSqlFragment> InputExpr { get; set; }
        internal int CoalesceNumber { get; set; }
        public override string ToString()
        {
            List<string> ChooseString = new List<string>();
            foreach (var x in InputExpr)
                ChooseString.Add(x.ToString());
            return string.Join("", ChooseString);
        }

        internal override GraphViewExecutionOperator Generate(GraphViewConnection dbConnection)
        {
            List<GraphViewExecutionOperator> Source = new List<GraphViewExecutionOperator>();
            foreach (var x in InputExpr)
            {
                Source.Add(x.Generate(dbConnection));
            }
            var op = new CoalesceOperator(dbConnection, Source, CoalesceNumber);
            return new OutputOperator(op,dbConnection,op.header,null);
        }
    }
}
