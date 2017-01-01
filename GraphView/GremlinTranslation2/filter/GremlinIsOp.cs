using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinIsOp: GremlinTranslationOperator
    {
        public object Value { get; set; }
        public Predicate Predicate { get; set; }
        public IsType Type { get; set; }

        public GremlinIsOp(object value)
        {
            Value = value;
            Type = IsType.IsValue;
        }

        public GremlinIsOp(Predicate predicate)
        {
            Predicate = predicate;
            Type = IsType.IsPredicate;
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            throw new NotImplementedException();
            return inputContext;
        }

        public enum IsType
        {
            IsValue,
            IsPredicate
        }
    }
}
