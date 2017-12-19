using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinInjectVariable : GremlinTableVariable
    {
        public object Injection { get; set; }
        public GremlinVariable InputVariable { get; set; }

        public GremlinInjectVariable(GremlinVariable inputVariable, object injection) : base(
            inputVariable?.GetVariableType() == GremlinVariableType.Scalar ? GremlinVariableType.Scalar : GremlinVariableType.Mixed)
        {
            this.InputVariable = inputVariable;
            this.Injection = injection;
        }

        internal override bool Populate(string property, string label = null)
        {
            return false;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            if (this.InputVariable == null)
            {
                //g.Inject()
                parameters.Add(SqlUtil.GetValueExpr(null));
            }
            else
            {
                parameters.Add(this.InputVariable.DefaultProjection().ToScalarExpression());
            }

            bool isList = false;
            if (GremlinUtil.IsList(this.Injection) || GremlinUtil.IsArray(this.Injection))
            {
                isList = true;  //1 It's a list
                foreach (var value in (IEnumerable)this.Injection)
                {
                    parameters.Add(SqlUtil.GetValueExpr(value));
                }
            }
            else if (GremlinUtil.IsNumber(this.Injection) || this.Injection is string || this.Injection is bool)
            {
                parameters.Add(SqlUtil.GetValueExpr(this.Injection));
            }
            else
            {
                throw new ArgumentException();
            }

            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Inject, parameters, GetVariableName());
            ((WInjectTableReference)tableRef).IsList = isList;
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }
}
