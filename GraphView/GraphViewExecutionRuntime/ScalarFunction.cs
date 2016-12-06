using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    /// <summary>
    /// A scalar function takes input as a raw record and outputs a scalar value.
    /// </summary>
    internal abstract class ScalarFunction
    {
        public abstract string Evaluate(RawRecord record);
    }

    internal class ScalarSubqueryFunction : ScalarFunction
    {
        // When a subquery is compiled, the tuple from the outer context
        // is injected into the subquery through a constant-source scan, 
        // which is in a Cartesian product with the operators compiled from the query. 
        private GraphViewExecutionOperator subqueryOp;
        private ConstantSourceOperator constantSourceOp;

        public ScalarSubqueryFunction(GraphViewExecutionOperator subqueryOp, ConstantSourceOperator constantSourceOp)
        {
            this.subqueryOp = subqueryOp;
            this.constantSourceOp = constantSourceOp;
        }

        public override string Evaluate(RawRecord record)
        {
            constantSourceOp.ConstantSource = record;
            subqueryOp.Open();
            RawRecord firstResult = subqueryOp.Next();
            subqueryOp.Close();

            return firstResult == null ? null : firstResult.RetriveData(0);
        }
    }
}
