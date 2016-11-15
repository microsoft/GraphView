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

        public GremlinPropertyOp(params string[] properties)
        {
            if (properties.Length % 2 != 0) throw new Exception("The parameter of property should be even");
            if (properties.Length < 2) throw new Exception("The number of parameter of property should be larger than 2");
            Properties = new Dictionary<string, object>();
            for (int i = 0; i < properties.Length; i += 2)
            {
                Properties[properties[i]] = properties[i + 1];
            }
        }

        public override GremlinToSqlContext GetContext()
        {
            GremlinToSqlContext inputContext = GetInputContext();

            if (inputContext.CurrVariable is GremlinAddEVariable)
            {
                (inputContext.CurrVariable as GremlinAddEVariable).Properties = Properties.Copy();
            }
            else if (inputContext.CurrVariable is GremlinAddVVariable)
            {
                (inputContext.CurrVariable as GremlinAddVVariable).Properties = Properties.Copy();
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

                //TODO
                // execute the update sql

            }
            return inputContext;
        }
    }
}
