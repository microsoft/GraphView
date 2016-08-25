using System;
using System.CodeDom.Compiler;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView.TSQL_Syntax_Tree;
using Microsoft.SqlServer.TransactSql.ScriptDom;

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
                elements = new List<int>();
            }
            private Func<GraphViewGremlinSematicAnalyser.Context> Modifier;
            internal Record CurrentRecord;
            internal List<int> elements;
            public bool MoveNext()
            {
                if (CurrentOperator == null) Reset();

                if (CurrentOperator.Status())
                {
                    CurrentRecord = CurrentOperator.Next();
                    return true;
                }
                else return false;
            }

            public void Reset()
            {
            }

            object IEnumerator.Current
            {
                get
                {
                    Record res = new Record();
                    res.field = new List<string>();
                    foreach (var x in elements)
                    {
                        res.field.Add(CurrentRecord.RetriveData(x));
                    }
                    return res;
                }
            }

            public Record Current
            {
                get
                {
                    Record res = new Record();
                    res.field = new List<string>();
                    foreach (var x in elements)
                    {
                     res.field.Add(CurrentRecord.RetriveData(x));   
                    }
                    return res;
                }
            }

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
        internal List<string> elements;

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
                    it = new GremlinPipelineIterator(CurrentOperator);
                    foreach (var x in parser.elements) it.elements.Add(CurrentOperator.header.IndexOf(x));
                }
            }
            return it;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public GremlinPipeline(GremlinPipeline rhs)
        {
            CurrentOperator = rhs.CurrentOperator;
            AppendExecutableString = rhs.AppendExecutableString;
            HoldMark = rhs.HoldMark;
            TokenIndex = rhs.TokenIndex;
            connection = rhs.connection;
        }

        public GremlinPipeline(ref GraphViewConnection pConnection)
        {
            CurrentOperator = null;
            AppendExecutableString = "";
            HoldMark = true;
            TokenIndex = new List<int>();
            connection = pConnection;
        }

        public GremlinPipeline(GremlinPipeline rhs, string NewAES)
        {
            CurrentOperator = rhs.CurrentOperator;
            AppendExecutableString = NewAES;
            HoldMark = rhs.HoldMark;
            TokenIndex = rhs.TokenIndex;
            connection = rhs.connection;
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

        public static Tuple<string[], GraphViewGremlinParser.Keywords> within(params string[] i)
        {
            return new Tuple<string[], GraphViewGremlinParser.Keywords>(i, GraphViewGremlinParser.Keywords.within);
        }

        public static Tuple<string[], GraphViewGremlinParser.Keywords> without(params string[] i)
        {
            return new Tuple<string[], GraphViewGremlinParser.Keywords>(i, GraphViewGremlinParser.Keywords.without);
        }

        public static string incr()
        {
            return "incr";
        }

        public static string decr()
        {
            return "decr";
        }

        public static GremlinPipeline _underscore()
        {
            GraphViewConnection NullConnection = new GraphViewConnection();
            GremlinPipeline HeldPipe = new GremlinPipeline(ref NullConnection);
            HeldPipe.HoldMark = false;
            HeldPipe.AppendExecutableString += "__.";
            return HeldPipe;
        }
        public GremlinPipeline V()
        {
            if (HoldMark == true) held = this;

            return new GremlinPipeline(this, AppendExecutableString + "V().");
        }

        public GremlinPipeline E()
        {
            if (HoldMark == true) held = this;

            return new GremlinPipeline(this, AppendExecutableString + "E().");

        }

        public GremlinPipeline next()
        {
            if (HoldMark == true) held = this;

            return new GremlinPipeline(this, AppendExecutableString + "next().");

        }

        public GremlinPipeline has(string name, string value)
        {
            if (HoldMark == true) held = this;

            return new GremlinPipeline(this, AppendExecutableString + "has(\'" + name + "\', " + "\'" + value + "\').");

        }

        public GremlinPipeline has(string name, Tuple<int, GraphViewGremlinParser.Keywords> ComparisonFunc)
        {
            Tuple<int, GraphViewGremlinParser.Keywords> des = ComparisonFunc;
            string AES = AppendExecutableString;
            AES += "has(\'" + name + "\', ";
                switch (des.Item2)
                {
                    case GraphViewGremlinParser.Keywords.lt:
                        AES += "lt("+des.Item1+ ")";
                        break;
                    case GraphViewGremlinParser.Keywords.gt:
                        AES += "gt(" + des.Item1 + ")";
                        break;
                    case GraphViewGremlinParser.Keywords.eq:
                        AES += "eq(" + des.Item1 + ")";
                        break;
                    case GraphViewGremlinParser.Keywords.lte:
                        AES += "lte(" + des.Item1 + ")";
                        break;
                    case GraphViewGremlinParser.Keywords.gte:
                        AES += "gte(" + des.Item1 + ")";
                        break;
                    case GraphViewGremlinParser.Keywords.neq:
                        AES += "neq(" + des.Item1 + ")";
                        break;
                  }
            AES += ").";
            if (HoldMark == true) held = this;

            return new GremlinPipeline(this, AES);
        }
        public GremlinPipeline has(string name, Tuple<string[], GraphViewGremlinParser.Keywords> ComparisonFunc)
        {
            Tuple<string[], GraphViewGremlinParser.Keywords> des = ComparisonFunc;
            string AES = AppendExecutableString;
            AES += "has(\'" + name + "\', ";
            switch (des.Item2)
            {
                case GraphViewGremlinParser.Keywords.within:
                    AES += "within(\'" + String.Join("\',\'", des.Item1) + "\')";
                    break;
                case GraphViewGremlinParser.Keywords.without:
                    AES += "without(\'" + String.Join("\',\'", des.Item1) + "\')";
                    break;
            }
            AES += ").";
            if (HoldMark == true) held = this;

            return new GremlinPipeline(this, AES);
        }

        public GremlinPipeline Out(params string[] Parameters)
        {
            string AES = AppendExecutableString;
            if (Parameters == null)
                AES += "out().";
            else
                AES += "out(\'"+string.Join("\', \'",Parameters)+"\').";
            if (HoldMark == true) held = this;

            return new GremlinPipeline(this, AES);

        }

        public GremlinPipeline In(params string[] Parameters)
        {
            string AES = AppendExecutableString;
            if (Parameters == null)
                AES += "in().";
            else
                AES += "in(\'" + string.Join("\', \'", Parameters) + "\').";
            if (HoldMark == true) held = this;

            return new GremlinPipeline(this, AES);

        }

        public GremlinPipeline outE(params string[] Parameters)
        {
            string AES = AppendExecutableString;
            if (Parameters != null)
            {
                List<string> StringList = new List<string>();
                AES += "outE(";
                foreach (var x in Parameters)
AES += "\'" + x + "\'";
                AES += ").";
            }
            else
                AES += "outE().";
            if (HoldMark == true) held = this;

            return new GremlinPipeline(this);

        }

        public GremlinPipeline inE(params string[] Parameters)
        {
            string AES = AppendExecutableString;
            if (Parameters != null)
            {
                List<string> StringList = new List<string>();
                AES += "inE(";
                foreach (var x in Parameters)
                    AES += "\'" + x + "\'";
                AES += ").";
            }
            else
                AES += "inE().";
            if (HoldMark == true) held = this;

            return new GremlinPipeline(this);
        }

        public GremlinPipeline inV()
        {
            if (HoldMark == true) held = this;

            return new GremlinPipeline(this, AppendExecutableString + "inV().");

        }

        public GremlinPipeline outV()
        {
            if (HoldMark == true) held = this;

            return new GremlinPipeline(this, AppendExecutableString + "outE().");

        }
        public GremlinPipeline As(string alias)
        {
            if (HoldMark == true) held = this;

            return new GremlinPipeline(this, AppendExecutableString + "as(\'" + alias + "\').");

        }

        public GremlinPipeline select(params string[] Parameters)
        {
            string AES = AppendExecutableString;
            if (Parameters == null)
                AES += "select().";
            else
                AES += "select(\'"+ string.Join("\',\'",Parameters)+"\').";
            if (HoldMark == true) held = this;
            return new GremlinPipeline(this,AES);

        }

        public GremlinPipeline addV(params string[] Parameters)
        {

            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            parser.Parse(CutTail(AppendExecutableString + "addV(\'" + string.Join("\',\'", Parameters) + "\').")).Generate(connection).Next();

            if (HoldMark == true) held = this;

            return new GremlinPipeline(this);

        }

        public GremlinPipeline addOutE(params string[] Parameters)
        {
            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            parser.Parse(CutTail(AppendExecutableString + "addOutE(\'" + string.Join("\',\'", Parameters) + "\').")).Generate(connection).Next();

            if (HoldMark == true) held = this;


            return new GremlinPipeline(this);

        }

        public GremlinPipeline addInE(params string[] Parameters)
        {


            GraphViewGremlinParser parser = new GraphViewGremlinParser();
            parser.Parse(CutTail(AppendExecutableString + "addInE(\'" + string.Join("\',\'", Parameters) + "\').")).Generate(connection).Next();

            if (HoldMark == true) held = this;

            return new GremlinPipeline(this);

        }

        public GremlinPipeline values(string name)
        {
            if (HoldMark == true) held = this;

            return new GremlinPipeline(this, AppendExecutableString + "values(\'" + name + "\').");

        }

        public GremlinPipeline where(Tuple<string, GraphViewGremlinParser.Keywords> ComparisonFunc)
        {

            string AES = AppendExecutableString;
            if (HoldMark == true) held = this;

            if (ComparisonFunc.Item2 == GraphViewGremlinParser.Keywords.eq)
                AES += "where(eq(\'" + ComparisonFunc.Item1 + "\')).";
            if (ComparisonFunc.Item2 == GraphViewGremlinParser.Keywords.neq)
                AES += "where(neq(\'" + ComparisonFunc.Item1 + "\')).";

            return new GremlinPipeline(this,AES);
        }

        public GremlinPipeline match(params GremlinPipeline[] pipes)
        {
            string AES = AppendExecutableString;
            List<string> StringList = new List<string>();
            foreach (var x in pipes) StringList.Add(x.AppendExecutableString);
            AES += "match(\'" + String.Join(",", StringList) + ").";
            return new GremlinPipeline(this,AES);
        }

        public GremlinPipeline aggregate(string name)
        {
            if (HoldMark == true) held = this;

            return new GremlinPipeline(this);
        }

        public GremlinPipeline and(params GremlinPipeline[] pipes)
        {

            List<string> PipeString = new List<string>();
            foreach(var x in pipes) PipeString.Add(x.AppendExecutableString);
            return new GremlinPipeline(this, AppendExecutableString + "and(" + String.Join(",", PipeString) + ").");
        }

        public GremlinPipeline or(params GremlinPipeline[] pipes)
        {
            List<string> PipeString = new List<string>();
            foreach (var x in pipes) PipeString.Add(x.AppendExecutableString);
            return new GremlinPipeline(this, AppendExecutableString + "or(" + String.Join(",", PipeString) + ").");
        }

        public GremlinPipeline drop()
        {
            if (HoldMark == true) held = this;

            return new GremlinPipeline(this, AppendExecutableString + "drop().");
        }

        public GremlinPipeline Is(Tuple<string, GraphViewGremlinParser.Keywords> ComparisonFunc)
        {
            string AES = AppendExecutableString;
            if (HoldMark == true) held = this;

            if (ComparisonFunc.Item2 == GraphViewGremlinParser.Keywords.eq)
                AES += "is(eq(\'" + ComparisonFunc.Item1 + "\')).";
            if (ComparisonFunc.Item2 == GraphViewGremlinParser.Keywords.neq)
                AES += "is(neq(\'" + ComparisonFunc.Item1 + "\')).";

            return new GremlinPipeline(this);
        }

        public GremlinPipeline Limit(int i)
        {
            if (HoldMark == true) held = this;

            return new GremlinPipeline(this);
        }

        public GremlinPipeline repeat(GremlinPipeline pipe)
        {

            return new GremlinPipeline(this, AppendExecutableString + "repeat(" + CutTail(pipe.AppendExecutableString) + ").");
        }

        public GremlinPipeline times(int i)
        {
            return new GremlinPipeline(this, AppendExecutableString + "times(" + i + ").");
        }

        public GremlinPipeline choose(GremlinPipeline pipe)
        {
            return new GremlinPipeline(this, AppendExecutableString + "choose(" + CutTail(pipe.AppendExecutableString) + ").");
        }

        public GremlinPipeline option(string name, GremlinPipeline pipe)
        {
            return new GremlinPipeline(this, AppendExecutableString + "option(\'" + name + "\'" + CutTail(pipe.AppendExecutableString) + ").");
        }

        public GremlinPipeline coalesce(params GremlinPipeline[] pipes)
        {
            List<string> StringList = new List<string>();
            foreach(var x in pipes) StringList.Add(CutTail(x.AppendExecutableString));
            return new GremlinPipeline(this, AppendExecutableString + "coalesce(" + String.Join(",", StringList) + ").");
        }

        public GremlinPipeline order()
        {
            return new GremlinPipeline(this, AppendExecutableString + "order().");
        }

        public GremlinPipeline by(string bywhat,string order ="")
        {
            if (order == "" && bywhat =="incr")
                return new GremlinPipeline(this, AppendExecutableString + "by(incr).");
            if (order == "" && bywhat == "decr")
                return new GremlinPipeline(this, AppendExecutableString + "by(decr).");
            return new GremlinPipeline(this, AppendExecutableString + "by(\'" + bywhat + "\', " + order + ").");
        }

        public GremlinPipeline max()
        {
            return new GremlinPipeline(this, AppendExecutableString + "max().");
        }
        public GremlinPipeline count()
        {
            return new GremlinPipeline(this, AppendExecutableString + "count().");
        }
        public GremlinPipeline min()
        {
            return new GremlinPipeline(this, AppendExecutableString + "min().");
        }
        public GremlinPipeline mean()
        {
            return new GremlinPipeline(this, AppendExecutableString + "mean().");
        }
        internal string CutTail(string some)
        {
            if (some.Length < 1) return null;
            return some.Substring(0, some.Length - 1);
        }
    }

}
