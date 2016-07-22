using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    abstract internal class BooleanFunction
    {
        internal abstract bool eval(Record r);
    }

    abstract internal class ComparisonBooleanFunction : BooleanFunction
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
        internal int LhsFieldIndex;
        internal int RhsFieldIndex;
        internal ComparisonType type;
        internal FieldComparisonFunction(int lhs, int rhs, ComparisonType pType)
        {
            LhsFieldIndex = lhs;
            RhsFieldIndex = rhs;
            type = pType;
        }
        override internal bool eval(Record r)
        {
            switch (type)
            {
                case ComparisonType.eq:
                    return r.RetriveData(LhsFieldIndex) == r.RetriveData(RhsFieldIndex);
                case ComparisonType.neq:
                    return r.RetriveData(LhsFieldIndex) != r.RetriveData(RhsFieldIndex);
                case ComparisonType.lt:
                    return double.Parse(r.RetriveData(LhsFieldIndex)) < double.Parse(r.RetriveData(RhsFieldIndex));
                case ComparisonType.gt:
                    return double.Parse(r.RetriveData(LhsFieldIndex)) > double.Parse(r.RetriveData(RhsFieldIndex));
                case ComparisonType.gte:
                    return double.Parse(r.RetriveData(LhsFieldIndex)) >= double.Parse(r.RetriveData(RhsFieldIndex));
                case ComparisonType.lte:
                    return double.Parse(r.RetriveData(LhsFieldIndex)) <= double.Parse(r.RetriveData(RhsFieldIndex));
                default:
                    return false;
            }
            
        }
    }
    abstract internal class BinaryBooleanFunction : BooleanFunction
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
        override internal bool eval(Record r)
        {
            if (type == BinaryType.and) return lhs.eval(r) && rhs.eval(r);
            if (type == BinaryType.or) return lhs.eval(r) || rhs.eval(r);
            return false;
        }
    }
}
