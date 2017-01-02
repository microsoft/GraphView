using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphView
{
    internal abstract class GremlinTableVariable : GremlinVariable2, ISqlTable
    {
        protected static int _count = 0;

        internal virtual string GenerateTableAlias()
        {
            return "R_" + _count++;
        }

        public virtual WTableReference ToTableReference()
        {
            throw new NotImplementedException();
        }

        //Dictionary<string, bool> = (_edge, variable+"_"+"_edge")
        //for example: N_0._edge as N_0__edge
        protected Dictionary<string, bool> projectedProperties;

        internal override void Populate(string property, bool isAlias = false)
        {
            if (projectedProperties == null)
            {
                //projectedProperties = new List<string>();
                projectedProperties = new Dictionary<string, bool>();
            }
            projectedProperties[property] = isAlias;
            if (!UsedProperties.Contains(property))
            {
                UsedProperties.Add(property);
            }
        }

        public virtual List<WSelectElement> ToSelectElementList()
        {
            if (projectedProperties == null) return null;
            List<WSelectElement> selectElementList = new List<WSelectElement>();
            foreach (var propertyItem in projectedProperties)
            {
                if (propertyItem.Value)
                {
                    selectElementList.Add(new WSelectScalarExpression()
                    {
                        ColumnName = VariableName + "_" + propertyItem.Key,
                        SelectExpr = GremlinUtil.GetColumnReferenceExpression(VariableName, propertyItem.Key)
                    });
                }
                else
                {
                    selectElementList.Add(new WSelectScalarExpression()
                    {
                        SelectExpr = GremlinUtil.GetColumnReferenceExpression(VariableName, propertyItem.Key)
                    });
                }
                
            }
            return selectElementList;
        }
    }
}
