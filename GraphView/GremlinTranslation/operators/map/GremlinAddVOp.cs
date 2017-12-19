using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal class GremlinAddVOp: GremlinTranslationOperator
    {
        public string VertexLabel { get; set; }
        public HashSet<GremlinPropertyOp> PropertyOps { get; set; }

        public GremlinAddVOp()
        {
            PropertyOps = new HashSet<GremlinPropertyOp>();
            VertexLabel = "vertex";
        }

        public GremlinAddVOp(params object[] propertyKeyValues)
        {
            PropertyOps = new HashSet<GremlinPropertyOp>();

            if (propertyKeyValues.Length % 2 != 0)
            {
                throw new Exception("The parameter of property should be even");
            }

            for (var i = 0; i < propertyKeyValues.Length; i += 2)
            {
                PropertyOps.Add(new GremlinPropertyOp(GremlinKeyword.PropertyCardinality.List, 
                                                                propertyKeyValues[i] as string,
                                                                propertyKeyValues[i+1], null));
            }
            VertexLabel = "vertex";
        }

        public GremlinAddVOp(string vertexLabel)
        {
            VertexLabel = vertexLabel;
            PropertyOps = new HashSet<GremlinPropertyOp>();
        }

        internal override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            HashSet<GremlinProperty> properties = new HashSet<GremlinProperty>();
            foreach (GremlinPropertyOp propertyOp in PropertyOps)
            {
                GremlinProperty property = propertyOp.ToGremlinProperty(inputContext);
                property.Cardinality = GremlinKeyword.PropertyCardinality.List;
                properties.Add(property);
            }

            if (inputContext.PivotVariable == null)
            {
                GremlinAddVVariable newVariable = new GremlinAddVVariable(null, VertexLabel, properties, true);
                inputContext.VariableList.Add(newVariable);
                inputContext.TableReferencesInFromClause.Add(newVariable);
                inputContext.SetPivotVariable(newVariable);
            }
            else
            {
                inputContext.PivotVariable.AddV(inputContext, VertexLabel, properties);
            }

            return inputContext;
        }
    }
}
