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
            throw new NotImplementedException();
        }

        internal virtual WSqlScript ToSqlScript() {
            return GetContext().ToSqlScript();
        }

        internal virtual void InheritedVariableFromParent(GremlinToSqlContext parentContext)
        {
            GremlinParentContextOp rootAsContextOp = this as GremlinParentContextOp;
            if (rootAsContextOp != null)
            {
                rootAsContextOp.InheritedPivotVariable = parentContext.PivotVariable;
                rootAsContextOp.ParentContext = parentContext;
            }
        }

        internal virtual void InheritedContextFromParent(GremlinToSqlContext parentContext)
        {
            GremlinParentContextOp rootAsContextOp = this as GremlinParentContextOp;
            if (rootAsContextOp != null)
            {
                rootAsContextOp.InheritedContext = parentContext.Duplicate();
            }
        }

        internal GremlinToSqlContext GetInputContext()
        {
            return InputOperator != null ? InputOperator.GetContext() : new GremlinToSqlContext();
        }

        public virtual void ModulateBy(GraphTraversal traversal)
        {
            throw new NotImplementedException();
        }

        public virtual void ModulateBy(GraphTraversal traversal, IComparer order)
        {
            throw new NotImplementedException();
        }

        public virtual void ModulateBy(GremlinKeyword.Order order)
        {
            ModulateBy(GraphTraversal.__(), order);
        }

        public virtual void ModulateBy(IComparer comparer)
        {
            ModulateBy(GraphTraversal.__(), comparer);
        }

        public virtual void ModulateBy(GraphTraversal traversal, GremlinKeyword.Order order)
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
            ModulateBy(GraphTraversal.__());
        }

        public virtual void ModulateBy(string key)
        {
            ModulateBy(GraphTraversal.__().Values(key));
        }

        public virtual void ModulateBy(string key, GremlinKeyword.Order order)
        {
            ModulateBy(GraphTraversal.__().Values(key), order);
        }

        public virtual void ModulateBy(string key, IComparer comparer)
        {
            ModulateBy(GraphTraversal.__().Values(key), comparer);
        }

        public virtual void ModulateBy(GremlinKeyword.Column column)
        {
            ModulateBy(GraphTraversal.__().Select(column));
        }

        public virtual void ModulateBy(GremlinKeyword.Column column, GremlinKeyword.Order order)
        {
            ModulateBy(GraphTraversal.__().Select(column), order);
        }

        public virtual void ModulateBy(GremlinKeyword.Column column, IComparer comparer)
        {
            ModulateBy(GraphTraversal.__().Select(column), comparer);
        }

        public virtual void ModulateBy(GremlinKeyword.T token)
        {
            switch (token)
            {
                case GremlinKeyword.T.Id:
                    ModulateBy(GraphTraversal.__().Id());
                    break;
                case GremlinKeyword.T.Label:
                    ModulateBy(GraphTraversal.__().Label());
                    break;
                case GremlinKeyword.T.Key:
                    ModulateBy(GraphTraversal.__().Key());
                    break;
                case GremlinKeyword.T.Value:
                    ModulateBy(GraphTraversal.__().Value());
                    break;
                default:
                    throw new TranslationException("Unknow GremlinKeyword.T");
            }
        }
    }
    
    internal class GremlinParentContextOp : GremlinTranslationOperator
    {
        public GremlinVariable InheritedPivotVariable { get; set; }
        public GremlinToSqlContext InheritedContext { get; set; }
        public GremlinToSqlContext ParentContext { get; set; }

        internal override GremlinToSqlContext GetContext()
        {
            if (InheritedContext != null) return InheritedContext;
            GremlinToSqlContext newContext = new GremlinToSqlContext {ParentContext = ParentContext};
            if (InheritedPivotVariable != null)
            {
                GremlinContextVariable newVariable = new GremlinContextVariable(InheritedPivotVariable);
                newContext.VariableList.Add(newVariable);
                newContext.SetPivotVariable(newVariable);
            } 
            return newContext;
        }
    }

    // The "__" in Repeat(__.Out())
    internal class GremlinRepeatParentContextOp : GremlinParentContextOp
    {
        public GremlinRepeatParentContextOp(GremlinParentContextOp pt)
        {
            this.InheritedPivotVariable = pt.InheritedPivotVariable;
            this.InheritedContext = pt.InheritedContext;
            this.ParentContext = pt.ParentContext;
        }

        internal override GremlinToSqlContext GetContext()
        {
            if (InheritedContext != null) return InheritedContext;
            GremlinToSqlContext newContext = new GremlinToSqlContext { ParentContext = ParentContext };
            if (InheritedPivotVariable != null)
            {
                GremlinRepeatContextVariable newVariable = new GremlinRepeatContextVariable(InheritedPivotVariable);
                newContext.VariableList.Add(newVariable);
                newContext.SetPivotVariable(newVariable);
            }
            return newContext;
        }
    }

    // The "__" in Until(__.Out()), almost same as the one in Repeat
    internal class GremlinUntilParentContextOp : GremlinParentContextOp
    {
        public GremlinUntilParentContextOp(GremlinParentContextOp pt)
        {
            this.InheritedPivotVariable = pt.InheritedPivotVariable;
            this.InheritedContext = pt.InheritedContext;
            this.ParentContext = pt.ParentContext;
        }

        internal override GremlinToSqlContext GetContext()
        {
            if (InheritedContext != null) return InheritedContext;
            GremlinToSqlContext newContext = new GremlinToSqlContext { ParentContext = ParentContext };
            if (InheritedPivotVariable != null)
            {
                GremlinUntilContextVariable newVariable = new GremlinUntilContextVariable(InheritedPivotVariable);
                newContext.VariableList.Add(newVariable);
                newContext.SetPivotVariable(newVariable);
            }
            return newContext;
        }
    }

    // The "__" in Emit(__.Out()), almost same as the one in Repeat
    internal class GremlinEmitParentContextOp : GremlinParentContextOp
    {
        public GremlinEmitParentContextOp(GremlinParentContextOp pt)
        {
            this.InheritedPivotVariable = pt.InheritedPivotVariable;
            this.InheritedContext = pt.InheritedContext;
            this.ParentContext = pt.ParentContext;
        }

        internal override GremlinToSqlContext GetContext()
        {
            if (InheritedContext != null) return InheritedContext;
            GremlinToSqlContext newContext = new GremlinToSqlContext { ParentContext = ParentContext };
            if (InheritedPivotVariable != null)
            {
                GremlinEmitContextVariable newVariable = new GremlinEmitContextVariable(InheritedPivotVariable);
                newContext.VariableList.Add(newVariable);
                newContext.SetPivotVariable(newVariable);
            }
            return newContext;
        }
    }
}
