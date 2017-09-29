using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinSelectColumnVariable : GremlinTableVariable
    {
        public GremlinContextVariable InputVariable { get; set; }
        public GremlinKeyword.Column Column { get; set; }

        public GremlinSelectColumnVariable(GremlinVariable inputVariable, GremlinKeyword.Column column) : base(GremlinVariableType.Table)
        {
            this.InputVariable = new GremlinContextVariable(inputVariable);
            this.Column = column;
        }

        internal override bool Populate(string property, string label = null)
        {
            if (base.Populate(property, label))
            {
                return this.InputVariable.Populate(property, null);
            }
            else if (this.InputVariable.Populate(property, label))
            {
                return base.Populate(property, null);
            }
            else
            {
                return false;
            }
        }

        internal override List<GremlinVariable> FetchAllVars()
        {
            List<GremlinVariable> variableList = new List<GremlinVariable>() { this };
            variableList.AddRange(this.InputVariable.FetchAllVars());
            return variableList;
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            parameters.Add(this.InputVariable.DefaultProjection().ToScalarExpression());
            parameters.Add(SqlUtil.GetValueExpr(this.Column == GremlinKeyword.Column.Keys ? "Keys" : "Values"));
            parameters.Add(SqlUtil.GetValueExpr(this.DefaultProperty()));
            parameters.AddRange(this.ProjectedProperties.Select(SqlUtil.GetValueExpr));
            var tableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.SelectColumn, parameters, GetVariableName());
            return SqlUtil.GetCrossApplyTableReference(tableRef);
        }
    }

    internal class GremlinOrderLocalInitVariable : GremlinVariable
    {
        public GremlinOrderLocalInitVariable()
        {
            this.VariableName = GremlinKeyword.Compose1TableDefaultName;
        }

        internal override GremlinVariableProperty DefaultProjection()
        {
            return new GremlinVariableProperty(this, GremlinKeyword.TableDefaultColumnName);
        }
    }
}
