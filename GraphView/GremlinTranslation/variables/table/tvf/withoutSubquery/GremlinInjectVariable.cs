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

        public GremlinInjectVariable(GremlinVariable inputVariable, object injection): base(GremlinVariableType.Table)
        {
            InputVariable =inputVariable ;
            Injection = injection;
            ProjectedProperties.Add(GremlinKeyword.TableDefaultColumnName);
        }

        internal override void Populate(string property)
        {
            return;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();

            if (InputVariable == null)
            {
                //g.Inject()
                parameters.Add(SqlUtil.GetValueExpr(null));
            }
            else
            {
                parameters.Add(InputVariable.DefaultProjection().ToScalarExpression());
            }

            bool isList = false;
            if (GremlinUtil.IsList(Injection) || GremlinUtil.IsArray(Injection))
            {
                isList = true;  //1 It's a list
                foreach (var value in (IEnumerable)Injection)
                {
                    parameters.Add(SqlUtil.GetValueExpr(value));
                }
            }
            else if (GremlinUtil.IsNumber(Injection) || Injection is string || Injection is bool)
            {
                parameters.Add(SqlUtil.GetValueExpr(Injection));
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
