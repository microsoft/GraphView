using System;
using System.Collections.Generic;

namespace GraphView
{
    internal abstract class BooleanFunction
    {
        internal List<string> header { get; set; }
        internal abstract bool Evaluate(RawRecord r);
    }

    internal abstract class ComparisonBooleanFunction : BooleanFunction
    {
        internal enum ComparisonType
        {
            neq,
            eq,
            lt,
            gt,
            gte,
            lte
        }
    }

    internal class FieldComparisonFunction : ComparisonBooleanFunction
    {
        //internal int LhsFieldIndex;
        //internal int RhsFieldIndex;
        internal string LhsFieldName;
        internal string RhsFieldName;
        internal ComparisonType type;

        //internal FieldComparisonFunction(int lhs, int rhs, ComparisonType pType)
        //{
        //    LhsFieldIndex = lhs;
        //    RhsFieldIndex = rhs;
        //    type = pType;
        //}

        internal FieldComparisonFunction(string lhs, string rhs, ComparisonType pType)
        {
            LhsFieldName = lhs;
            RhsFieldName = rhs;
            type = pType;
        }
        internal override bool Evaluate(RawRecord r)
        {
            var lhsIndex = header.IndexOf(LhsFieldName);
            var rhsIndex = header.IndexOf(RhsFieldName);
            switch (type)
            {
                case ComparisonType.eq:
                    return r.RetriveData(lhsIndex) == r.RetriveData(rhsIndex);
                case ComparisonType.neq:
                    return r.RetriveData(lhsIndex) != r.RetriveData(rhsIndex);
                case ComparisonType.lt:
                    return double.Parse(r.RetriveData(lhsIndex)) < double.Parse(r.RetriveData(rhsIndex));
                case ComparisonType.gt:
                    return double.Parse(r.RetriveData(lhsIndex)) > double.Parse(r.RetriveData(rhsIndex));
                case ComparisonType.gte:
                    return double.Parse(r.RetriveData(lhsIndex)) >= double.Parse(r.RetriveData(rhsIndex));
                case ComparisonType.lte:
                    return double.Parse(r.RetriveData(lhsIndex)) <= double.Parse(r.RetriveData(rhsIndex));
                default:
                    return false;
            }
            
        }
    }
    internal abstract class BinaryBooleanFunction : BooleanFunction
    {
        internal enum BinaryType
        {
            or,
            and,
        }
    }
    internal class BinaryFunction : BinaryBooleanFunction
    {
        private BinaryType type;
        private BooleanFunction lhs;
        private BooleanFunction rhs;
        internal BinaryFunction(BooleanFunction plhs, BooleanFunction prhs, BinaryType ptype)
        {
            lhs = plhs;
            rhs = prhs;
            type = ptype;
        }
        internal override bool Evaluate(RawRecord r)
        {
            if (type == BinaryType.and) return lhs.Evaluate(r) && rhs.Evaluate(r);
            if (type == BinaryType.or) return lhs.Evaluate(r) || rhs.Evaluate(r);
            return false;
        }
    }

    internal class ExistsFunction : BooleanFunction
    {
        // When a subquery is compiled, the tuple from the outer context
        // is injected into the subquery through a constant-source scan, 
        // which is in a Cartesian product with the operators compiled from the query. 
        private GraphViewExecutionOperator subqueryOp;
        private ConstantSourceOperator constantSourceOp;

        public ExistsFunction(GraphViewExecutionOperator subqueryOp, ConstantSourceOperator constantSourceOp)
        {
            this.subqueryOp = subqueryOp;
            this.constantSourceOp = constantSourceOp;
        }

        internal override bool Evaluate(RawRecord r)
        {
            constantSourceOp.ConstantSource = r;
            subqueryOp.Open();
            RawRecord firstResult = subqueryOp.Next();
            subqueryOp.Close();

            return firstResult != null;
        }
    }
}
