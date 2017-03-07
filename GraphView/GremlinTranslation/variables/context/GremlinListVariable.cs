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

        //internal override GremlinVariableProperty DefaultVariableProperty()
        //{
        //    return new GremlinVariableProperty(null, GremlinKeyword.ScalarValue);
        //}

        internal override void Populate(string property)
        {
            if (ProjectedProperties.Contains(property)) return;
            base.Populate(property);

            foreach (var variable in GremlinVariableList)
            {
                variable.Populate(property);
            }
        }

        internal WScalarExpression ToScalarExpression()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            foreach (var variable in GremlinVariableList)
            {
                if (variable is GremlinGhostVariable && (variable as GremlinGhostVariable).RealVariable is GremlinRepeatSelectedVariable)
                {
                    parameters.Add(variable.DefaultVariableProperty().ToScalarExpression());
                }
                else
                {
                    parameters.Add(variable.ToCompose1());
                }
            }
            return SqlUtil.GetFunctionCall(GremlinKeyword.func.Compose2, parameters);
        }

        internal override GremlinVariableType GetVariableType()
        {
            if (GremlinUtil.IsTheSameType(GremlinVariableList)) return GremlinVariableList.First().GetVariableType();
            return GremlinVariableType.Table;
        }

        internal override GremlinVariableType GetUnfoldVariableType()
        {
            return GetVariableType();
        }
    }
}
