using System;
using System.CodeDom.Compiler;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.TSQL_Syntax_Tree;

namespace GraphView
{
    class GremlinPipeline : IEnumerable
    {
        public class GremlinPipelineIterator :IEnumerator
        {
            private GraphViewOperator CurrentOperator;
            internal GremlinPipelineIterator(GraphViewOperator pCurrentOperator)
            {
                CurrentOperator = pCurrentOperator;
            }
            private Func<GraphViewGremlinSematicAnalyser.Context> Modifier;
            public bool MoveNext()
            {
                if (CurrentOperator == null) Reset();

                if (CurrentOperator.Status())
                {
                    Current = CurrentOperator.Next();
                    return true;
                }
                else return false;
            }

            public void Reset()
            {
               
            }

            public object Current { get; set; }
        }

        internal GraphViewGremlinSematicAnalyser.Context pContext;

        internal GraphViewOperator CurrentOperator;
        internal GremlinPipelineIterator it;
        internal GraphViewConnection connection;
        internal string AppendExecutableString;
        internal string SubExecutableString;

        internal static GremlinPipeline held;
        public IEnumerator GetEnumerator()
        {
            if (it == null)
            {
                if (CurrentOperator == null)
                {
                    WSqlStatement SqlTree;
                    GraphViewGremlinSematicAnalyser inst = new GraphViewGremlinSematicAnalyser();
                    inst.SematicContext = pContext;
                    if (inst.SematicContext.BranchContexts != null && inst.SematicContext.BranchContexts.Count != 0 && inst.SematicContext.BranchNote != null)
                    {
                        var choose = new WChoose() {InputExpr = new List<WSelectQueryBlock>()};
                        foreach (var x in inst.SematicContext.BranchContexts)
                        {
                            var branch = inst.Transform(x);
                            choose.InputExpr.Add(branch as WSelectQueryBlock);
                        }
                        SqlTree = choose;
                    }
                    else if (inst.SematicContext.BranchContexts != null && inst.SematicContext.BranchContexts.Count != 0 && inst.SematicContext.BranchNote == null)
                    {
                        var choose = new WCoalesce()
                        {
                            InputExpr = new List<WSelectQueryBlock>(),
                            CoalesceNumber = inst.SematicContext.NodeCount
                        };
                        foreach (var x in inst.SematicContext.BranchContexts)
                        {
                            var branch = inst.Transform(x);
                            choose.InputExpr.Add(branch as WSelectQueryBlock);
                        }
                        SqlTree = choose;
                    }
                    else
                    {
                        inst.Transform(inst.SematicContext);
                        SqlTree = inst.SqlTree;
                    }
                    CurrentOperator = SqlTree.Generate(connection);
                }
                it = new GremlinPipelineIterator(CurrentOperator);
            }
            return it;
        }
       public GremlinPipeline(GraphViewGremlinSematicAnalyser.Context Context)
        {
            CurrentOperator = null;
            pContext = Context;
           AppendExecutableString = "";
        }

        public GremlinPipeline()
        {
            CurrentOperator = null;
            pContext = new GraphViewGremlinSematicAnalyser.Context();
            AppendExecutableString = "";
        }

        public GremlinPipeline(GraphViewGremlinSematicAnalyser.Context Context, string pAES)
        {
            pContext = Context;
            CurrentOperator = null;
            AppendExecutableString = pAES;
        }

        public GremlinPipeline(GraphViewGremlinSematicAnalyser.Context Context, string pAES, string pSES)
        {
            pContext = Context;
            CurrentOperator = null;
            AppendExecutableString = pAES;
            SubExecutableString = pSES;
        }

        internal void AddNewAlias(string alias, ref GraphViewGremlinSematicAnalyser.Context context, string predicates = "")
        {
            context.InternalAliasList.Add(alias);
            context.AliasPredicates.Add(new List<string>());
            if (alias[0] == 'N') context.NodeCount++;
            else context.EdgeCount++;
            if (predicates != "")
                context.AliasPredicates.Last().Add(predicates);
        }

        internal void ChangePrimaryAlias(string alias, ref GraphViewGremlinSematicAnalyser.Context context)
        {
            context.PrimaryInternalAlias.Clear();
            context.PrimaryInternalAlias.Add(alias);
        }

        private int index;
        private string SrcNode;
        private string DestNode;
        private string Edge;
        private string Parameter;
        private List<string> StatueKeeper = new List<string>();
        private List<string> NewPrimaryInternalAlias = new List<string>();

        public static Tuple<int, GraphViewGremlinParser.Keywords> lt(int i)
        {
            return new Tuple<int, GraphViewGremlinParser.Keywords>(i, GraphViewGremlinParser.Keywords.lt);
        }
        public static Tuple<int, GraphViewGremlinParser.Keywords> gt(int i)
        {
            return new Tuple<int, GraphViewGremlinParser.Keywords>(i, GraphViewGremlinParser.Keywords.gt);

        }
        public static Tuple<int, GraphViewGremlinParser.Keywords> eq(int i)
        {
            return new Tuple<int, GraphViewGremlinParser.Keywords>(i, GraphViewGremlinParser.Keywords.eq);

        }
        public static Tuple<int, GraphViewGremlinParser.Keywords> lte(int i)
        {
            return new Tuple<int, GraphViewGremlinParser.Keywords>(i, GraphViewGremlinParser.Keywords.lte);

        }

        public static Tuple<int, GraphViewGremlinParser.Keywords> gte(int i)
        {
            return new Tuple<int, GraphViewGremlinParser.Keywords>(i, GraphViewGremlinParser.Keywords.gte);

        }

        public static Tuple<int, GraphViewGremlinParser.Keywords> neq(int i)
        {
            return new Tuple<int, GraphViewGremlinParser.Keywords>(i, GraphViewGremlinParser.Keywords.neq);

        }
        public static Tuple<string, GraphViewGremlinParser.Keywords> lt(string i)
        {
            return new Tuple<string, GraphViewGremlinParser.Keywords>(i, GraphViewGremlinParser.Keywords.lt);
        }
        public static Tuple<string, GraphViewGremlinParser.Keywords> gt(string i)
        {
            return new Tuple<string, GraphViewGremlinParser.Keywords>(i, GraphViewGremlinParser.Keywords.gt);

        }
        public static Tuple<string, GraphViewGremlinParser.Keywords> eq(string i)
        {
            return new Tuple<string, GraphViewGremlinParser.Keywords>(i, GraphViewGremlinParser.Keywords.eq);

        }
        public static Tuple<string, GraphViewGremlinParser.Keywords> lte(string i)
        {
            return new Tuple<string, GraphViewGremlinParser.Keywords>(i, GraphViewGremlinParser.Keywords.lte);

        }

        public static Tuple<string, GraphViewGremlinParser.Keywords> gte(string i)
        {
            return new Tuple<string, GraphViewGremlinParser.Keywords>(i, GraphViewGremlinParser.Keywords.gte);

        }

        public static Tuple<string, GraphViewGremlinParser.Keywords> neq(string i)
        {
            return new Tuple<string, GraphViewGremlinParser.Keywords>(i, GraphViewGremlinParser.Keywords.neq);

        }

        public static GremlinPipeline _underscore()
        {
            GremlinPipeline HeldPipe = new GremlinPipeline(held.pContext);
            HeldPipe.pContext.HoldMark = false;
            return HeldPipe;
        }
        public GremlinPipeline V()
        {
            SrcNode = "N_" + pContext.NodeCount.ToString();
            AddNewAlias(SrcNode, ref pContext);
            ChangePrimaryAlias(SrcNode, ref pContext);
            if (pContext.HoldMark == true) held = this;

            AppendExecutableString += "V().";

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext),AppendExecutableString);
        }

        public GremlinPipeline E()
        {
            SrcNode = "N_" + pContext.NodeCount.ToString();
            AddNewAlias(SrcNode, ref pContext);
            DestNode = "N_" + pContext.NodeCount.ToString();
            AddNewAlias(DestNode, ref pContext);
            Edge = "E_" + pContext.EdgeCount.ToString();
            AddNewAlias(Edge, ref pContext);
            ChangePrimaryAlias(Edge, ref pContext);
            pContext.Paths.Add((new Tuple<string, string, string>(SrcNode, Edge, DestNode)));
            if (pContext.HoldMark == true) held = this;

            AppendExecutableString += "E().";

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext), AppendExecutableString);

        }

        public GremlinPipeline next()
        {
            for (int i = 0; i < pContext.PrimaryInternalAlias.Count; i++)
                pContext.PrimaryInternalAlias[i] += ".id";
            if (pContext.HoldMark == true) held = this;

            AppendExecutableString += "next.";

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext), AppendExecutableString);

        }

        public GremlinPipeline has(string name, string value)
        {
            foreach (var alias in pContext.PrimaryInternalAlias)
            {
                pContext.AliasPredicates[index].Add(alias + "." + name + " = " +
                                                    value);
            }
            if (pContext.HoldMark == true) held = this;

            AppendExecutableString += "has(\'" + name + "\', " + "\'" + value + "\').";
            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext), AppendExecutableString);

        }

        public GremlinPipeline has(string name, Tuple<int, GraphViewGremlinParser.Keywords> ComparisonFunc)
        {
            Tuple<int, GraphViewGremlinParser.Keywords> des = ComparisonFunc;
            AppendExecutableString += "has(\'" + name + "\', ";
            foreach (var alias in pContext.PrimaryInternalAlias)
            {
                switch (des.Item2)
                {
                    case GraphViewGremlinParser.Keywords.lt:
                        AppendExecutableString += "lt("+des.Item1+ ")";
                        pContext.AliasPredicates[index].Add(alias + "." + name + " < " + des.Item1.ToString());
                        break;
                    case GraphViewGremlinParser.Keywords.gt:
                        AppendExecutableString += "gt(" + des.Item1 + ")";
                        pContext.AliasPredicates[index].Add(alias + "." + name + " > " + des.Item1.ToString());
                        break;
                    case GraphViewGremlinParser.Keywords.eq:
                        AppendExecutableString += "eq(" + des.Item1 + ")";
                        pContext.AliasPredicates[index].Add(alias + "." + name + " = " + des.Item1.ToString());
                        break;
                    case GraphViewGremlinParser.Keywords.lte:
                        AppendExecutableString += "lte(" + des.Item1 + ")";
                        pContext.AliasPredicates[index].Add(alias + "." + name + " [ " + des.Item1.ToString());
                        break;
                    case GraphViewGremlinParser.Keywords.gte:
                        AppendExecutableString += "gte(" + des.Item1 + ")";
                        pContext.AliasPredicates[index].Add(alias + "." + name + " ] " + des.Item1.ToString());
                        break;
                    case GraphViewGremlinParser.Keywords.neq:
                        AppendExecutableString += "neq(" + des.Item1 + ")";
                        pContext.AliasPredicates[index].Add(alias + "." + name + " ! " + des.Item1.ToString());
                        break;
                }
            }
            AppendExecutableString += ").";
            if (pContext.HoldMark == true) held = this;

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext), AppendExecutableString);

        }

        public GremlinPipeline Out(params string[] Parameters)
        {
            if (Parameters == null)
            {
                Edge = "E_" + pContext.EdgeCount.ToString();
                AddNewAlias(Edge, ref pContext);
                index = pContext.InternalAliasList.Count;
                foreach (var alias in pContext.PrimaryInternalAlias)
                {
                    DestNode = "N_" + pContext.NodeCount.ToString();
                    AddNewAlias(DestNode, ref pContext);
                    SrcNode = alias;
                    pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                        DestNode));
                    NewPrimaryInternalAlias.Add(DestNode);
                }
                AppendExecutableString += "out().";
            }
            else
            {
                foreach (var para in Parameters)
                {
                    Edge = "E_" + pContext.EdgeCount.ToString();
                    AddNewAlias(Edge, ref pContext);
                    index = pContext.InternalAliasList.Count;
                    pContext.AliasPredicates[index - 1].Add(Edge + ".type" + " = " +
                                                            para);
                    foreach (var alias in pContext.PrimaryInternalAlias)
                    {
                        DestNode = "N_" + pContext.NodeCount.ToString();
                        AddNewAlias(DestNode, ref pContext);
                        SrcNode = alias;
                        pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                            DestNode));
                        NewPrimaryInternalAlias.Add(DestNode);
                    }
                }
                AppendExecutableString += "out(\'"+string.Join("\', \'",Parameters)+"\')";
            }
            pContext.PrimaryInternalAlias.Clear();
            foreach (var a in NewPrimaryInternalAlias)
            {
                pContext.PrimaryInternalAlias.Add(a);
            }
            if (pContext.HoldMark == true) held = this;

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext), AppendExecutableString);

        }

        public GremlinPipeline In(params string[] Parameters)
        {
            if (Parameters == null)
            {
                Edge = "E_" + pContext.EdgeCount.ToString();
                AddNewAlias(Edge, ref pContext);
                index = pContext.InternalAliasList.Count;
                foreach (var alias in pContext.PrimaryInternalAlias)
                {
                    SrcNode = "N_" + pContext.NodeCount.ToString();
                    AddNewAlias(SrcNode, ref pContext);
                    DestNode = alias;
                    pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                        DestNode));
                    NewPrimaryInternalAlias.Add(SrcNode);
                }
                AppendExecutableString += "in().";
            }
            else
            {
                foreach (var para in Parameters)
                {
                    Edge = "E_" + pContext.EdgeCount.ToString();
                    AddNewAlias(Edge, ref pContext);
                    index = pContext.InternalAliasList.Count;
                    pContext.AliasPredicates[index - 1].Add(Edge + ".type" + " = " +
                                                            para);
                    foreach (var alias in pContext.PrimaryInternalAlias)
                    {
                        SrcNode = "N_" + pContext.NodeCount.ToString();
                        AddNewAlias(SrcNode, ref pContext);
                        DestNode = alias;
                        pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                            DestNode));
                        NewPrimaryInternalAlias.Add(SrcNode);
                    }
                }
                AppendExecutableString += "out(\'" + string.Join("\', \'", Parameters) + "\').";
            }
            pContext.PrimaryInternalAlias.Clear();
            foreach (var a in NewPrimaryInternalAlias)
            {
                pContext.PrimaryInternalAlias.Add(a);
            }
            if (pContext.HoldMark == true) held = this;

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext), AppendExecutableString);

        }

        public GremlinPipeline outE(params string[] Parameters)
        {
            SrcNode = pContext.PrimaryInternalAlias[0];
            DestNode = "N_" + pContext.NodeCount.ToString();
            pContext.InternalAliasList.Add(DestNode);
            pContext.AliasPredicates.Add(new List<string>());
            pContext.NodeCount++;
            Edge = "E_" + pContext.EdgeCount.ToString();
            pContext.InternalAliasList.Add(Edge);
            if (Parameters != null)
            {
                Parameter = Parameters[0];
                pContext.AliasPredicates.Add(new List<string>());
                pContext.AliasPredicates.Last().Add(Edge + ".type = " + Parameter);
                AppendExecutableString += "outE(\'" + Parameter + "\').";
            }
            else
            {
                AppendExecutableString += "outE().";
                pContext.AliasPredicates.Add(new List<string>());
            }
            pContext.EdgeCount++;
            pContext.PrimaryInternalAlias.Clear();
            pContext.PrimaryInternalAlias.Add(Edge);
            foreach (var alias in pContext.PrimaryInternalAlias)
            {
                Edge = alias;
                pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                     DestNode));
            }
            if (pContext.HoldMark == true) held = this;

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext), AppendExecutableString);

        }

        public GremlinPipeline inE(params string[] Parameters)
        {
            DestNode = pContext.PrimaryInternalAlias[0];
            SrcNode = "N_" + pContext.NodeCount.ToString();
            AddNewAlias(SrcNode, ref pContext);
            Edge = "E_" + pContext.EdgeCount.ToString();
            pContext.InternalAliasList.Add(Edge);
            if (Parameters != null)
            {
                Parameter = Parameters[0];
                pContext.AliasPredicates.Add(new List<string>());
                pContext.AliasPredicates.Last().Add(Edge + ".type = " + Parameter);
                AppendExecutableString += "inE(\'" + Parameter + "\').";
            }
            else
            {
                AppendExecutableString += "inE().";
                pContext.AliasPredicates.Add(new List<string>());
            }
            pContext.EdgeCount++;
            ChangePrimaryAlias(Edge, ref pContext);
            foreach (var alias in pContext.PrimaryInternalAlias)
            {
                Edge = alias;
                pContext.Paths.Add(new Tuple<string, string, string>(SrcNode, Edge,
                     DestNode));
            }
            if (pContext.HoldMark == true) held = this;

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext), AppendExecutableString);

        }

        public GremlinPipeline inV()
        {
            Edge = pContext.PrimaryInternalAlias[0];
            var ExistInPath = pContext.Paths.Find(p => p.Item2 == Edge);
            ChangePrimaryAlias(ExistInPath.Item3, ref pContext);
            if (pContext.HoldMark == true) held = this;

            AppendExecutableString += "inV().";

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext), AppendExecutableString);

        }

        public GremlinPipeline outV()
        {
            Edge = pContext.PrimaryInternalAlias[0];
            var ExistOutPath = pContext.Paths.Find(p => p.Item2 == Edge);
            ChangePrimaryAlias(ExistOutPath.Item1, ref pContext);
            if (pContext.HoldMark == true) held = this;

            AppendExecutableString += "outE().";

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext), AppendExecutableString);

        }
        public GremlinPipeline As(string alias)
        {
            pContext.ExplictAliasToInternalAlias.Add(alias, pContext.PrimaryInternalAlias[0]);
            if (pContext.HoldMark == true) held = this;

            AppendExecutableString += "as(\'"+alias+"\').";

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext), AppendExecutableString);

        }

        public GremlinPipeline select(params string[] Parameters)
        {
            pContext.PrimaryInternalAlias.Clear();
            if (Parameters == null)
            {
                AppendExecutableString += "select().";
                foreach (var a in pContext.ExplictAliasToInternalAlias)
                    pContext.PrimaryInternalAlias.Add(a.Value);
            }
            else
            {
                foreach (var a in Parameters)
                    pContext.PrimaryInternalAlias.Add(pContext.ExplictAliasToInternalAlias[a]);
                AppendExecutableString += "select(\'"+ string.Join("\',\'",Parameters)+"\').";
            }
            if (pContext.HoldMark == true) held = this;
            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext), AppendExecutableString);

        }

        public GremlinPipeline addV(params string[] Parameters)
        {
            foreach (var a in Parameters.ToList().FindAll(p => Parameters.ToList().IndexOf(p) % 2 == 0))
            {
                pContext.Properties.Add(a, Parameters[Parameters.ToList().IndexOf(a) + 1]);
            }
            pContext.AddVMark = true;
            if (pContext.HoldMark == true) held = this;

            AppendExecutableString += "addV(\'" + string.Join("\',\'", Parameters) + "\').";

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext), AppendExecutableString);

        }

        public GremlinPipeline addOutE(params string[] Parameters)
        {
            pContext.AddEMark = true;
            SrcNode = Parameters.ToList().First();
            if (!pContext.ExplictAliasToInternalAlias.ContainsKey(SrcNode))
            {
                SrcNode = pContext.PrimaryInternalAlias[0];
                DestNode = pContext.ExplictAliasToInternalAlias[Parameters[1]];
                pContext.Properties.Add("type", Parameters[0]);
                for (int i = 2; i < Parameters.Length; i += 2)
                    pContext.Properties.Add(Parameters[i],
                        Parameters[i + 1]);
            }
            else
            {
                SrcNode = pContext.ExplictAliasToInternalAlias[Parameters[0]];
                DestNode = pContext.ExplictAliasToInternalAlias[Parameters[2]];
                pContext.Properties.Add("type", Parameters[1]);
                for (int i = 3; i < Parameters.Length; i += 2)
                    pContext.Properties.Add(Parameters[i],
                        Parameters[i + 1]);
            }
            pContext.PrimaryInternalAlias.Clear();
            pContext.PrimaryInternalAlias.Add(SrcNode);
            pContext.PrimaryInternalAlias.Add(DestNode);
            if (pContext.HoldMark == true) held = this;

            AppendExecutableString += "addOutE(\'" + string.Join("\',\'", Parameters) + "\').";

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext), AppendExecutableString);

        }

        public GremlinPipeline addInE(params string[] Parameters)
        {
            pContext.AddEMark = true;
            SrcNode = Parameters.ToList().First();
            if (!pContext.ExplictAliasToInternalAlias.ContainsKey(SrcNode))
            {
                DestNode = pContext.PrimaryInternalAlias[0];
                SrcNode = Parameters[1];
                pContext.Properties.Add("type", Parameters[0]);
                for (int i = 2; i < Parameters.Length; i += 2)
                    pContext.Properties.Add(Parameters[i],
                        Parameters[i + 1]);
            }
            else
            {
                SrcNode = pContext.ExplictAliasToInternalAlias[Parameters[0]];
                DestNode = pContext.ExplictAliasToInternalAlias[Parameters[2]];
                pContext.Properties.Add("type", Parameters[1]);
                for (int i = 3; i < Parameters.Length; i += 2)
                    pContext.Properties.Add(Parameters[i],
                        Parameters[i + 1]);
            }
            pContext.PrimaryInternalAlias.Clear();
            pContext.PrimaryInternalAlias.Add(SrcNode);
            pContext.PrimaryInternalAlias.Add(DestNode);
            if (pContext.HoldMark == true) held = this;

            AppendExecutableString += "addInE(\'" + string.Join("\',\'", Parameters) + "\').";

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext), AppendExecutableString);

        }

        public GremlinPipeline values(string name)
        {
            string ValuePara = name;
            for (int i = 0; i < pContext.PrimaryInternalAlias.Count; i++)
                pContext.PrimaryInternalAlias[i] += "." + ValuePara;
            if (pContext.HoldMark == true) held = this;

            AppendExecutableString += "values(\'" + name + "\').";

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext), AppendExecutableString);

        }

        public GremlinPipeline where(Tuple<string, GraphViewGremlinParser.Keywords> ComparisonFunc)
        {
            foreach (var alias in pContext.PrimaryInternalAlias)
            {
                index =
                    pContext.InternalAliasList.IndexOf(alias.IndexOf('.') == -1
                        ? alias
                        : alias.Substring(0, alias.IndexOf('.')));
                string QuotedString = ComparisonFunc.Item1;
                string Comp1 = alias;
                string Comp2 = "";
                if (Comp1.IndexOf('.') == -1)
                    Comp1 += ".id";
                if (Comp2.IndexOf('.') == -1)
                    Comp2 = pContext.ExplictAliasToInternalAlias[
                        ComparisonFunc.Item1];
                else
                    Comp2 = pContext.ExplictAliasToInternalAlias[ComparisonFunc.Item1] + ".id";
                if (ComparisonFunc.Item2 ==GraphViewGremlinParser.Keywords.eq)
                    pContext.AliasPredicates[index].Add(Comp1 + " = " + Comp2);
                if (ComparisonFunc.Item2 == GraphViewGremlinParser.Keywords.neq)
                    pContext.AliasPredicates[index].Add(Comp1 + " ! " + Comp2);
            }
            if (pContext.HoldMark == true) held = this;

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext));
        }

        public GremlinPipeline match(params Func<GremlinPipeline, GremlinPipeline>[] funcs)
        {
            StatueKeeper.Clear();
            foreach (var x in pContext.PrimaryInternalAlias)
            {
                StatueKeeper.Add(x);
            }
            foreach (var func in funcs)
            {
                GremlinPipeline NewPipe = func.Invoke(this);
                pContext = NewPipe.pContext;
                pContext.PrimaryInternalAlias.Clear();
                foreach (var x in StatueKeeper)
                {
                    pContext.PrimaryInternalAlias.Add(x);
                }
            }
            if (pContext.HoldMark == true) held = this;

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext));
        }

        public GremlinPipeline aggregate(string name)
        {
            pContext.ExplictAliasToInternalAlias.Add(name, pContext.PrimaryInternalAlias[0]);
            if (pContext.HoldMark == true) held = this;

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext));
        }

        public GremlinPipeline and(params GremlinPipeline[] pipes)
        {
            foreach (var pipe in pipes)
            {
                GremlinPipeline NewPipe = pipe;
                pContext = NewPipe.pContext;
                held = NewPipe;
            }
            if (pContext.HoldMark == true) held = this;

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext));
        }

        public GremlinPipeline or(params GremlinPipeline[] pipes)
        {
            StatueKeeper.Clear();
            foreach (var x in pContext.PrimaryInternalAlias)
            {
                StatueKeeper.Add(x);
            }
            NewPrimaryInternalAlias.Clear();
            foreach (var pipe in pipes)
            {
                GremlinPipeline NewPipe = pipe;
                pContext = NewPipe.pContext;
                foreach (var x in pContext.PrimaryInternalAlias)
                {
                    NewPrimaryInternalAlias.Add(x);
                }
                pContext.PrimaryInternalAlias.Clear();
                foreach (var x in StatueKeeper)
                {
                    pContext.PrimaryInternalAlias.Add(x);
                }
            }
            foreach (var x in NewPrimaryInternalAlias)
            {
                pContext.PrimaryInternalAlias.Add(x);
            }
            if (pContext.HoldMark == true) held = this;

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext));
        }

        public GremlinPipeline drop()
        {
            pContext.RemoveMark = true;
            if (pContext.HoldMark == true) held = this;

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext));
        }

        public GremlinPipeline Is(Tuple<string, GraphViewGremlinParser.Keywords> ComparisonFunc)
        {
            foreach (var alias in pContext.PrimaryInternalAlias)
            {
                index =
                    pContext.InternalAliasList.IndexOf(alias.IndexOf('.') == -1
                        ? alias
                        : alias.Substring(0, alias.IndexOf('.')));
                string QuotedString = ComparisonFunc.Item1;
                string Comp1 = alias;
                string Comp2 = "";
                if (Comp1.IndexOf('.') == -1)
                    Comp1 += ".id";
                if (Comp2.IndexOf('.') == -1)
                    Comp2 = pContext.ExplictAliasToInternalAlias[
                        ComparisonFunc.Item1];
                else
                    Comp2 = pContext.ExplictAliasToInternalAlias[ComparisonFunc.Item1] + ".id";
                if (ComparisonFunc.Item2 == GraphViewGremlinParser.Keywords.eq)
                    pContext.AliasPredicates[index].Add(Comp1 + " = " + Comp2);
                if (ComparisonFunc.Item2 == GraphViewGremlinParser.Keywords.neq)
                    pContext.AliasPredicates[index].Add(Comp1 + " ! " + Comp2);
            }
            if (pContext.HoldMark == true) held = this;

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext));
        }

        public GremlinPipeline Limit(int i)
        {
            pContext.limit = i;
            if (pContext.HoldMark == true) held = this;

            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext));
        }

        public GremlinPipeline repeat(GremlinPipeline pipe)
        {
            SubExecutableString = pipe.AppendExecutableString;
            return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext),AppendExecutableString,SubExecutableString);
        }

        public GremlinPipeline times(int i)
        {
            if (SubExecutableString != null)
            {
                AppendExecutableString += "repeat(" + SubExecutableString.Substring(0, SubExecutableString.Length - 1) +
                                          ").times(" + i + ").";
                return new GremlinPipeline(new GraphViewGremlinSematicAnalyser.Context(pContext), AppendExecutableString);
            }
            else return null;
        }
    }

}
