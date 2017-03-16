using System;
using System.Collections;
using System.Collections.Generic;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    internal abstract class GremlinTranslationOperator
    {
        public GremlinTranslationOperator InputOperator { get; set; }

        internal virtual GremlinToSqlContext GetContext()
        {
            return null;
        }

        internal virtual WSqlScript ToSqlScript() {
            return GetContext().ToSqlScript();
        }

        internal virtual void InheritedVariableFromParent(GremlinToSqlContext parentContext)
        {
            if (this is GremlinParentContextOp)
            {
                GremlinParentContextOp rootAsContextOp = this as GremlinParentContextOp;
                rootAsContextOp.InheritedPivotVariable = parentContext.PivotVariable;
                //rootAsContextOp.InheritedPathList = new List<GremlinMatchPath>(parentContext.PathList);
                rootAsContextOp.ParentContext = parentContext;
            }
        }

        internal virtual void InheritedContextFromParent(GremlinToSqlContext parentContext)
        {
            if (this is GremlinParentContextOp)
            {
                GremlinParentContextOp rootAsContextOp = this as GremlinParentContextOp;
                rootAsContextOp.InheritedContext = parentContext.Duplicate();
            }
        }

        internal GremlinToSqlContext GetInputContext()
        {
            return InputOperator != null ? InputOperator.GetContext() : new GremlinToSqlContext();
        }

        public virtual void ModulateBy(GraphTraversal2 traversal)
        {
            throw new NotImplementedException();
        }

        public virtual void ModulateBy(GraphTraversal2 traversal, IComparer order)
        {
            throw new NotImplementedException();
        }

        public virtual void ModulateBy(GremlinKeyword.Order order)
        {
            ModulateBy(GraphTraversal2.__(), order);
        }

        public virtual void ModulateBy(IComparer comparer)
        {
            ModulateBy(GraphTraversal2.__(), comparer);
        }

        public virtual void ModulateBy(GraphTraversal2 traversal, GremlinKeyword.Order order)
        {
            switch (order)
            {
                case GremlinKeyword.Order.Incr:
                    ModulateBy(traversal, new IncrOrder());
                    break;
                case GremlinKeyword.Order.Decr:
                    ModulateBy(traversal, new DecrOrder());
                    break;
                case GremlinKeyword.Order.Shuffle:
                    ModulateBy(traversal, new ShuffleOrder());
                    break;
            }
        }

        public virtual void ModulateBy()
        {
            ModulateBy(GraphTraversal2.__());
        }

        public virtual void ModulateBy(string key)
        {
            ModulateBy(GraphTraversal2.__().Values(key));
        }

        public virtual void ModulateBy(string key, GremlinKeyword.Order order)
        {
            ModulateBy(GraphTraversal2.__().Values(key), order);
        }

        public virtual void ModulateBy(string key, IComparer comparer)
        {
            ModulateBy(GraphTraversal2.__().Values(key), comparer);
        }

        public virtual void ModulateBy(GremlinKeyword.Column column)
        {
            ModulateBy(GraphTraversal2.__().Select(column));
        }

        public virtual void ModulateBy(GremlinKeyword.Column column, GremlinKeyword.Order order)
        {
            ModulateBy(GraphTraversal2.__().Select(column), order);
        }

        public virtual void ModulateBy(GremlinKeyword.Column column, IComparer comparer)
        {
            ModulateBy(GraphTraversal2.__().Select(column), comparer);
        }
    }
    
    internal class GremlinParentContextOp : GremlinTranslationOperator
    {
        public GremlinVariable InheritedPivotVariable { get; set; }
        public GremlinToSqlContext InheritedContext { get; set; }
        //public List<GremlinMatchPath> InheritedPathList { get; set; }
        public GremlinToSqlContext ParentContext { get; set; }

        internal override GremlinToSqlContext GetContext()
        {
            if (InheritedContext != null) return InheritedContext;
            GremlinToSqlContext newContext = new GremlinToSqlContext();
            //newContext.PathList = InheritedPathList;
            newContext.ParentContext = ParentContext;
            if (InheritedPivotVariable != null)
            {
                GremlinContextVariable newVariable = new GremlinContextVariable(InheritedPivotVariable);
                newVariable.HomeContext = newContext;
                newContext.VariableList.Add(newVariable);
                newContext.PivotVariable = newVariable;
            } 
            return newContext;
        }
    }
}
