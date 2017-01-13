using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinPropertiesVariable: GremlinScalarTableVariable
    {
        public List<string> PropertyKeys { get; set; }
        public GremlinTableVariable ProjectVariable { get; set; }

        public GremlinPropertiesVariable(GremlinTableVariable projectVariable, List<string> propertyKeys)
        {
            ProjectVariable = projectVariable;
            PropertyKeys = new List<string>(propertyKeys);
        }

        public override WTableReference ToTableReference()
        {
            List<WScalarExpression> parameters = new List<WScalarExpression>();
            foreach (var property in PropertyKeys)
            {
                parameters.Add(SqlUtil.GetColumnReferenceExpr(ProjectVariable.VariableName, property));
            }
            var secondTableRef = SqlUtil.GetFunctionTableReference(GremlinKeyword.func.Properties, parameters, this, VariableName);
            return SqlUtil.GetCrossApplyTableReference(null, secondTableRef);
        }

        internal override void Key(GremlinToSqlContext currentContext)
        {
            GremlinKeyVariable newVariable = new GremlinKeyVariable(new GremlinVariableProperty(this, GremlinKeyword.TableValue));
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal override void Value(GremlinToSqlContext currentContext)
        {
            GremlinValueVariable newVariable = new GremlinValueVariable(new GremlinVariableProperty(this, GremlinKeyword.TableValue));
            currentContext.VariableList.Add(newVariable);
            currentContext.TableReferences.Add(newVariable);
            currentContext.SetPivotVariable(newVariable);
        }

        internal override void Drop(GremlinToSqlContext currentContext)
        {
            Dictionary<string, object> properties = new Dictionary<string, object>();
            foreach (var propertyKey in PropertyKeys)
            {
                properties[propertyKey] = null;
            }
            if (ProjectVariable is GremlinVertexTableVariable)
            {
                UpdateVariable = new GremlinUpdateNodePropertiesVariable(ProjectVariable.DefaultProjection(), properties);
                currentContext.VariableList.Add(UpdateVariable);
                currentContext.TableReferences.Add(UpdateVariable);
            }
            else if (ProjectVariable is GremlinEdgeTableVariable)
            {
                GremlinVariableProperty nodeProperty = currentContext.GetSourceVariableProperty(ProjectVariable);
                GremlinVariableProperty edgeProperty = currentContext.GetEdgeVariableProperty(ProjectVariable);
                UpdateVariable = new GremlinUpdateEdgePropertiesVariable(nodeProperty, edgeProperty, properties);
                currentContext.VariableList.Add(UpdateVariable);
                currentContext.TableReferences.Add(UpdateVariable);
            }
            else
            {
                throw new QueryCompilationException();
            }
        }

        internal override void HasKey(GremlinToSqlContext currentContext, List<string> values)
        {
            throw new NotImplementedException();
            //foreach (var value in values)
            //{
            //    Has(currentContext, "_value", value);
            //}
        }

        internal override void HasValue(GremlinToSqlContext currentContext, List<object> values)
        {
            throw new NotImplementedException();
            //foreach (var value in values)
            //{
            //    Has(currentContext, "_value", value);
            //}
        }
    }
}
