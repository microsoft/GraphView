using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView.GremlinTranslationOps.sideEffect
{
    internal class GremlinPropertyOp: GremlinTranslationOperator
    {
        public Dictionary<string, object> Properties;

        public GremlinPropertyOp(params object[] properties)
        {
            if (properties.Length % 2 != 0) throw new Exception("The parameter of property should be even");
            if (properties.Length < 2) throw new Exception("The number of parameter of property should be larger than 2");
            Properties = new Dictionary<string, object>();
            for (int i = 0; i < properties.Length; i += 2)
            {
                Properties[properties[i] as string] = properties[i + 1];
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            if (inputContext.CurrVariable is GremlinAddEVariable)
            {
                foreach (var property in Properties)
                {
                    if (!(inputContext.CurrVariable as GremlinAddEVariable).Properties.ContainsKey(property.Key))
                    {
                        (inputContext.CurrVariable as GremlinAddEVariable).Properties[property.Key] = new List<object>();
                    }
                    (inputContext.CurrVariable as GremlinAddEVariable).Properties[property.Key] = property.Value;
                }
            }
            else if (inputContext.CurrVariable is GremlinAddVVariable)
            {
                foreach (var property in Properties)
                {
                    if (!(inputContext.CurrVariable as GremlinAddVVariable).Properties.ContainsKey(property.Key))
                    {
                        (inputContext.CurrVariable as GremlinAddVVariable).Properties[property.Key] = new List<object>();
                    }
                    (inputContext.CurrVariable as GremlinAddVVariable).Properties[property.Key] = property.Value;
                }
            }
            else
            {
                string tableName;
                if (inputContext.CurrVariable is GremlinEdgeVariable)
                {
                    tableName = "Edge";
                }
                else
                {
                    tableName = "Node";
                }
                //Add or update properties
                WNamedTableReference target = GremlinUtil.GetNamedTableReference(tableName);

                List<WSetClause> setClause = new List<WSetClause>();
                foreach (var property in Properties)
                {
                    WAssignmentSetClause assignmentSetClause = new WAssignmentSetClause()
                    {
                        Column = GremlinUtil.GetColumnReferenceExpression(property.Key),
                        NewValue =  GremlinUtil.GetValueExpression(property.Value)
                    };
                    setClause.Add(assignmentSetClause);
                }
                WUpdateSpecification updateSpec = new WUpdateSpecification()
                {
                    FromClause = inputContext.GetFromClause(),
                    WhereClause =  inputContext.GetWhereClause(),
                    Target = target,
                    SetClauses = setClause
                };

                inputContext.Statements.Add(updateSpec);

            }
            return inputContext;
        }
    }
}
