using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.TSQL_Syntax_Tree;

namespace GraphView
{
    internal class TableClassifyVisitor : WSqlFragmentVisitor
    {
        private List<WNamedTableReference> vertexTableList;
        private List<WTableReferenceWithAlias> nonVertexTableList;
        
        public void Invoke(
            WFromClause fromClause, 
            List<WNamedTableReference> vertexTableList, 
            List<WTableReferenceWithAlias> nonVertexTableList)
        {
            this.vertexTableList = vertexTableList;
            this.nonVertexTableList = nonVertexTableList;

            foreach (WTableReference tabRef in fromClause.TableReferences)
            {
                tabRef.Accept(this);
            }
        }

        public override void Visit(WNamedTableReference node)
        {
            vertexTableList.Add(node);
        }

        public override void Visit(WQueryDerivedTable node)
        {
            nonVertexTableList.Add(node);
        }

        public override void Visit(WSchemaObjectFunctionTableReference node)
        {
            nonVertexTableList.Add(node);
        }

        public override void Visit(WVariableTableReference node)
        {
            nonVertexTableList.Add(node);
        }
    }
}
