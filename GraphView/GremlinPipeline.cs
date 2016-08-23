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
    class GremlinPipeline : IEnumerable<Record>
    {
        public class GremlinPipelineIterator :IEnumerator<Record>
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

            object IEnumerator.Current { get; }
            public Record Current { get; set; }

            public void Dispose()
            {
                
            }
        }

        internal GraphViewOperator CurrentOperator;
        internal GremlinPipelineIterator it;
        internal GraphViewConnection connection;
        internal List<int> TokenIndex;
        internal string AppendExecutableString;
        internal bool HoldMark;

        internal static GremlinPipeline held;

        public List<Record> ToList()
        {
            List<Record> RecordList = new List<Record>(); 
            foreach (var x in this)
                RecordList.Add(x);
            return RecordList;
        }
        public IEnumerator<Record> GetEnumerator()
        {
            if (it == null)
            {
                if (CurrentOperator == null)
                {
                    GraphViewGremlinParser parser = new GraphViewGremlinParser();
                    CurrentOperator = parser.Parse(CutTail(AppendExecutableString)).Generate(connection);
                }
                it = new GremlinPipelineIterator(CurrentOperator);
            }
            return it;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return null;
        }
       public GremlinPipeline(GraphViewGremlinSematicAnalyser.Context Context)
        {
            CurrentOperator = null;
           AppendExecutableString = "";
           HoldMark = true;
            TokenIndex = new List<int>();
        }

        public GremlinPipeline()
        {
            CurrentOperator = null;
            AppendExecutableString = "";
            HoldMark = true;
            TokenIndex = new List<int>();
        }

        public GremlinPipeline(string pAES)
        {
            CurrentOperator = null;
            AppendExecutableString = pAES;
            HoldMark = true;
            TokenIndex = new List<int>();
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
            GremlinPipeline HeldPipe = new GremlinPipeline();
            HeldPipe.HoldMark = false;
            HeldPipe.AppendExecutableString += "__.";
            return HeldPipe;
        }
        public GremlinPipeline V()
        {
            if (HoldMark == true) held = this;

            AppendExecutableString += "V().";
            return new GremlinPipeline(AppendExecutableString);
        }

        public GremlinPipeline E()
        {
            if (HoldMark == true) held = this;

            AppendExecutableString += "E().";

            return new GremlinPipeline(AppendExecutableString);

        }

        public GremlinPipeline next()
        {
            if (HoldMark == true) held = this;

            AppendExecutableString += "next().";

            return new GremlinPipeline(AppendExecutableString);

        }

        public GremlinPipeline has(string name, string value)
        {
            if (HoldMark == true) held = this;

            AppendExecutableString += "has(\'" + name + "\', " + "\'" + value + "\').";
            return new GremlinPipeline(AppendExecutableString);

        }

        public GremlinPipeline has(string name, Tuple<int, GraphViewGremlinParser.Keywords> ComparisonFunc)
        {
            Tuple<int, GraphViewGremlinParser.Keywords> des = ComparisonFunc;
            AppendExecutableString += "has(\'" + name + "\', ";
                switch (des.Item2)
                {
                    case GraphViewGremlinParser.Keywords.lt:
                        AppendExecutableString += "lt("+des.Item1+ ")";
                        break;
                    case GraphViewGremlinParser.Keywords.gt:
                        AppendExecutableString += "gt(" + des.Item1 + ")";
                        break;
                    case GraphViewGremlinParser.Keywords.eq:
                        AppendExecutableString += "eq(" + des.Item1 + ")";
                        break;
                    case GraphViewGremlinParser.Keywords.lte:
                        AppendExecutableString += "lte(" + des.Item1 + ")";
                        break;
                    case GraphViewGremlinParser.Keywords.gte:
                        AppendExecutableString += "gte(" + des.Item1 + ")";
                        break;
                    case GraphViewGremlinParser.Keywords.neq:
                        AppendExecutableString += "neq(" + des.Item1 + ")";
                        break;
                }
            AppendExecutableString += ").";
            if (HoldMark == true) held = this;

            return new GremlinPipeline(AppendExecutableString);

        }

        public GremlinPipeline Out(params string[] Parameters)
        {
            if (Parameters == null)
                AppendExecutableString += "out().";
            else
                AppendExecutableString += "out(\'"+string.Join("\', \'",Parameters)+"\').";
            if (HoldMark == true) held = this;

            return new GremlinPipeline(AppendExecutableString);

        }

        public GremlinPipeline In(params string[] Parameters)
        {
            if (Parameters == null)
                AppendExecutableString += "in().";
            else
                AppendExecutableString += "in(\'" + string.Join("\', \'", Parameters) + "\').";
            if (HoldMark == true) held = this;

            return new GremlinPipeline(AppendExecutableString);

        }

        public GremlinPipeline outE(params string[] Parameters)
        {
            if (Parameters != null)
            {
                List<string> StringList = new List<string>();
                AppendExecutableString += "outE(";
                foreach (var x in Parameters)
AppendExecutableString += "\'" + x + "\'";
                AppendExecutableString += ").";
            }
            else
                AppendExecutableString += "outE().";
            if (HoldMark == true) held = this;

            return new GremlinPipeline(AppendExecutableString);

        }

        public GremlinPipeline inE(params string[] Parameters)
        {
            if (Parameters != null)
            {
                List<string> StringList = new List<string>();
                AppendExecutableString += "inE(";
                foreach (var x in Parameters)
                    AppendExecutableString += "\'" + x + "\'";
                AppendExecutableString += ").";
            }
            else
                AppendExecutableString += "inE().";
            if (HoldMark == true) held = this;

            return new GremlinPipeline(AppendExecutableString);
        }

        public GremlinPipeline inV()
        {
            if (HoldMark == true) held = this;

            AppendExecutableString += "inV().";
            return new GremlinPipeline(AppendExecutableString);

        }

        public GremlinPipeline outV()
        {
            if (HoldMark == true) held = this;

            AppendExecutableString += "outE().";

            return new GremlinPipeline(AppendExecutableString);

        }
        public GremlinPipeline As(string alias)
        {
            if (HoldMark == true) held = this;

            AppendExecutableString += "as(\'"+alias+"\').";

            return new GremlinPipeline(AppendExecutableString);

        }

        public GremlinPipeline select(params string[] Parameters)
        {
            if (Parameters == null)
                AppendExecutableString += "select().";
            else
                AppendExecutableString += "select(\'"+ string.Join("\',\'",Parameters)+"\').";
            if (HoldMark == true) held = this;
            return new GremlinPipeline(AppendExecutableString);

        }

        public GremlinPipeline addV(params string[] Parameters)
        {
            if (HoldMark == true) held = this;

            AppendExecutableString += "addV(\'" + string.Join("\',\'", Parameters) + "\').";

            return new GremlinPipeline(AppendExecutableString);

        }

        public GremlinPipeline addOutE(params string[] Parameters)
        {

            AppendExecutableString += "addOutE(\'" + string.Join("\',\'", Parameters) + "\').";
            return new GremlinPipeline(AppendExecutableString);

        }

        public GremlinPipeline addInE(params string[] Parameters)
        {

            if (HoldMark == true) held = this;

            AppendExecutableString += "addInE(\'" + string.Join("\',\'", Parameters) + "\').";

            return new GremlinPipeline(AppendExecutableString);

        }

        public GremlinPipeline values(string name)
        {
            if (HoldMark == true) held = this;

            AppendExecutableString += "values(\'" + name + "\').";

            return new GremlinPipeline(AppendExecutableString);

        }

        public GremlinPipeline where(Tuple<string, GraphViewGremlinParser.Keywords> ComparisonFunc)
        {

            if (HoldMark == true) held = this;

            if (ComparisonFunc.Item2 == GraphViewGremlinParser.Keywords.eq)
                AppendExecutableString += "where(eq(\'" + ComparisonFunc.Item1 + "\')).";
            if (ComparisonFunc.Item2 == GraphViewGremlinParser.Keywords.neq)
                AppendExecutableString += "where(neq(\'" + ComparisonFunc.Item1 + "\')).";

            return new GremlinPipeline(AppendExecutableString);
        }

        public GremlinPipeline match(params GremlinPipeline[] pipes)
        {
            List<string> StringList = new List<string>();
            foreach (var x in pipes) StringList.Add(x.AppendExecutableString);
            AppendExecutableString += "match(\'" + String.Join(",", StringList) + ").";
            return new GremlinPipeline(AppendExecutableString);
        }

        public GremlinPipeline aggregate(string name)
        {
            if (HoldMark == true) held = this;

            return new GremlinPipeline(AppendExecutableString);
        }

        public GremlinPipeline and(params GremlinPipeline[] pipes)
        {

            List<string> PipeString = new List<string>();
            foreach(var x in pipes) PipeString.Add(x.AppendExecutableString);
            AppendExecutableString += "and(" + String.Join(",", PipeString)+").";
            return new GremlinPipeline(AppendExecutableString);
        }

        public GremlinPipeline or(params GremlinPipeline[] pipes)
        {
            List<string> PipeString = new List<string>();
            foreach (var x in pipes) PipeString.Add(x.AppendExecutableString);
            AppendExecutableString += "or(" + String.Join(",", PipeString) + ").";
            return new GremlinPipeline(AppendExecutableString);
        }

        public GremlinPipeline drop()
        {
            if (HoldMark == true) held = this;

            AppendExecutableString += "drop().";
            return new GremlinPipeline(AppendExecutableString);
        }

        public GremlinPipeline Is(Tuple<string, GraphViewGremlinParser.Keywords> ComparisonFunc)
        {
            if (HoldMark == true) held = this;

            if (ComparisonFunc.Item2 == GraphViewGremlinParser.Keywords.eq)
                AppendExecutableString += "is(eq(\'" + ComparisonFunc.Item1 + "\')).";
            if (ComparisonFunc.Item2 == GraphViewGremlinParser.Keywords.neq)
                AppendExecutableString += "is(neq(\'" + ComparisonFunc.Item1 + "\')).";

            return new GremlinPipeline(AppendExecutableString);
        }

        public GremlinPipeline Limit(int i)
        {
            if (HoldMark == true) held = this;

            return new GremlinPipeline(AppendExecutableString);
        }

        public GremlinPipeline repeat(GremlinPipeline pipe)
        {
            AppendExecutableString += "repeat(" + CutTail(pipe.AppendExecutableString)+").";
            return new GremlinPipeline(AppendExecutableString);
        }

        public GremlinPipeline times(int i)
        {
                AppendExecutableString += "times(" + i + ").";
            return new GremlinPipeline(AppendExecutableString);
        }

        public GremlinPipeline choose(GremlinPipeline pipe)
        {
            AppendExecutableString += "choose(" + CutTail(pipe.AppendExecutableString) + ").";
            return new GremlinPipeline(AppendExecutableString);
        }

        public GremlinPipeline option(string name, GremlinPipeline pipe)
        {
            AppendExecutableString += "option(\'"+name+"\'" + CutTail(pipe.AppendExecutableString)+").";
            return new GremlinPipeline(AppendExecutableString);
        }

        public GremlinPipeline coalesce(params GremlinPipeline[] pipes)
        {
            List<string> StringList = new List<string>();
            foreach(var x in pipes) StringList.Add(CutTail(x.AppendExecutableString));
            AppendExecutableString += "coalesce(" + String.Join(",", StringList)+").";
            return new GremlinPipeline(AppendExecutableString);
        }

        internal string CutTail(string some)
        {
            if (some.Length < 1) return null;
            return some.Substring(0, some.Length - 1);
        }
    }

}
