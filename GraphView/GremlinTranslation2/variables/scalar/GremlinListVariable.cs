using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinListVariable: GremlinSelectedVariable
    {
        public List<GremlinVariable> GremlinVariableList;

        public GremlinListVariable(List<GremlinVariable> gremlinVariableList)
        {
            GremlinVariableList = new List<GremlinVariable>(gremlinVariableList);
        }

        internal override GremlinVariableProperty DefaultVariableProperty()
        {
            return new GremlinVariableProperty(null, GremlinKeyword.ScalarValue);
        }

        internal override void Populate(string property)
        {
            foreach (var variable in GremlinVariableList)
            {
                variable.Populate(property);
            }
            base.Populate(property);
        }

        internal WScalarExpression ToScalarExpression()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            foreach (var variable in GremlinVariableList)
            {
                if (variable is GremlinGhostVariable && (variable as GremlinGhostVariable).RealVariable is GremlinRepeatSelectedVariable)
                {
                    parameters.Add((variable as GremlinGhostVariable).RealVariable.DefaultVariableProperty().ToScalarExpression());
                }
                else
                {
                    List<WScalarExpression> compose1Parameters = new List<WScalarExpression>();
                    foreach (var property in ProjectedProperties)
                    {
                        compose1Parameters.Add(variable.GetVariableProperty(property).ToScalarExpression());
                    }
                    WFunctionCall compose1 = SqlUtil.GetFunctionCall(GremlinKeyword.func.Compose1, compose1Parameters);
                    parameters.Add(compose1);
                }
            }
            return SqlUtil.GetFunctionCall(GremlinKeyword.func.Compose2, parameters);
        }

        internal override GremlinVariableType GetVariableType()
        {
            if (GremlinUtil.IsTheSameType(GremlinVariableList)) return GremlinVariableList.First().GetVariableType();
            return GremlinVariableType.Table;
        }

        internal override void Unfold(GremlinToSqlContext currentContext)
        {
            GremlinTableVariable newVariable = GremlinUnfoldVariable.Create(this);
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }
    }
}
