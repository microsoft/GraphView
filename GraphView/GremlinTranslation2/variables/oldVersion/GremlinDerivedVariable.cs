using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinDerivedVariable : GremlinVariable
    {
        private static long _count = 0;
        private static Dictionary<DerivedType, long> _typeToCount;

        public WSqlStatement Statement { get; set; }
        public DerivedType Type { get; set; }
        public WVariableReference Variable { get; set; }

        public GremlinDerivedVariable() { }

        public GremlinDerivedVariable(WSqlStatement statement, string derivedType = null)
        {
            VariableName = "D" + derivedType + "_" + getCount(GetDerivedType(derivedType));
            _count += 1;
            Statement = statement;
            Type = GetDerivedType(derivedType);

            Variable = GremlinUtil.GetVariableReference(VariableName);
        }

        public DerivedType GetDerivedType(string derivedType)
        {
            if (derivedType == "union") return DerivedType.UNION;
            if (derivedType == "fold") return DerivedType.FOLD;
            if (derivedType == "inject") return DerivedType.INJECT;
            return DerivedType.DEFAULT;
        }

        private long getCount(DerivedType type)
        {
            if (_typeToCount == null)
            {
                _typeToCount = new Dictionary<DerivedType, long>();
                _typeToCount[DerivedType.UNION] = 0;
                _typeToCount[DerivedType.FOLD] = 0;
                _typeToCount[DerivedType.INJECT] = 0;
                _typeToCount[DerivedType.DEFAULT] = 0;
            }
            return _typeToCount[type]++;
        }

        public enum DerivedType
        {
            UNION,
            FOLD,
            INJECT,
            DEFAULT
        }

    }
}
