using System;
using System.CodeDom.Compiler;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Text;
using System.Threading.Tasks;
using GraphView.TSQL_Syntax_Tree;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace GraphView
{
    public class GraphTraversal : IEnumerable<Record>
    {
        public enum direction
        {
            In,
            Out,
            Undefine
        }

        public class GraphTraversalIterator :IEnumerator<Record>
        {
            private GraphViewOperator CurrentOperator;
            internal GraphTraversalIterator(GraphViewOperator pCurrentOperator)
            {
                CurrentOperator = pCurrentOperator;
                elements = new List<string>();
            }
            internal GraphTraversalIterator(GraphViewOperator pCurrentOperator, List<string> pElements )
            {
                CurrentOperator = pCurrentOperator;
                elements = pElements;
            }
            private Func<GraphViewGremlinSematicAnalyser.Context> Modifier;
            internal Record CurrentRecord;
            internal List<string> elements;
            public bool MoveNext()
            {
                if (CurrentOperator == null) Reset();

                if (CurrentOperator.Status())
                {
                    RawRecord Temp = CurrentOperator.Next();
                    CurrentRecord = new Record(Temp, elements);
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
                    return CurrentRecord;
                }
            }

            public Record Current
            {
                get
                {
                    return CurrentRecord;
                }
            }

            public void Dispose()
            {
                
            }
        }

        internal GraphViewOperator CurrentOperator;
        internal GraphTraversalIterator it;
        internal GraphViewConnection connection;
        internal List<int> TokenIndex;
        internal string AppendExecutableString;
        internal string AddEdgeOtherSource;
        internal string RepeatSubstring;
        internal bool HoldMark;
        internal List<string> elements;
        internal direction dir;
        internal List<string> UnionString;
        internal bool LazyMark;

        internal static GraphTraversal held;

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
                    if (UnionString != null)
                    {
                        List<GraphViewOperator> OpList = new List<GraphViewOperator>();
                        foreach (var x in UnionString)
                        {
                            GraphViewGremlinParser Parser = new GraphViewGremlinParser();
                            OpList.Add(Parser.Parse(CutTail(RepeatSubstring)).Generate(connection));
                            it = new GraphTraversalIterator(new UnionOperator(connection, OpList));
                            return it;
                        }
                    }
                    if (AddEdgeOtherSource != null)
                    {
                        GraphViewGremlinParser ExtendParser1 = new GraphViewGremlinParser();
                        ExtendParser1.Parse(CutTail(AppendExecutableString));
                        GraphViewGremlinParser ExtendParser2 = new GraphViewGremlinParser();
                        ExtendParser2.Parse(AddEdgeOtherSource);
                        var X = new WInsertEdgeFromTwoSourceSpecification(ExtendParser1.SqlTree, ExtendParser2.SqlTree, dir);
                        it = new GraphTraversalIterator(X.Generate(connection));
                        return it;
                    }
                    if (RepeatSubstring != null)
                    {
                        GraphViewGremlinParser ExtendParser1 = new GraphViewGremlinParser();
                        ExtendParser1.Parse(CutTail(RepeatSubstring));
                        var X = new WWithPathClause(new Tuple<string, WSelectQueryBlock, int>("P_0",ExtendParser1.SqlTree as WSelectQueryBlock, -1));
                        GraphViewGremlinParser ExtendParser2 = new GraphViewGremlinParser();
                        ExtendParser2.Parse(CutTail(AppendExecutableString));
                        var Y = ExtendParser2.SqlTree as WSelectQueryBlock;
                        Y.WithPathClause = X;
                        CurrentOperator = Y.Generate(connection);
                        it = new GraphTraversalIterator(CurrentOperator);
                        elements = new List<string>();
                        foreach (var x in (CurrentOperator as OutputOperator).SelectedElement)
                        {
                            if (ExtendParser2.AliasBinding.ContainsValue(x))
                                elements.Add(ExtendParser2.AliasBinding.FirstOrDefault(p => p.Value == x).Key);
                            else elements.Add(x);
                        }
                        it = new GraphTraversalIterator(CurrentOperator, elements);
                        return it;
                    }
                    GraphViewGremlinParser parser = new GraphViewGremlinParser();
                    CurrentOperator = parser.Parse(CutTail(AppendExecutableString)).Generate(connection);
                    elements = new List<string>();
                    if (CurrentOperator is OutputOperator)
                    {
                        foreach (var x in (CurrentOperator as OutputOperator).SelectedElement)
                        {
                            if (parser.AliasBinding.ContainsValue(x))
                                elements.Add(parser.AliasBinding.FirstOrDefault(p => p.Value == x).Key);
                            else elements.Add(x);
                        }
                    }
                    it = new GraphTraversalIterator(CurrentOperator,elements);
                }
            }
            return it;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public GraphTraversal(GraphTraversal rhs)
        {
            CurrentOperator = rhs.CurrentOperator;
            AppendExecutableString = rhs.AppendExecutableString;
            HoldMark = rhs.HoldMark;
            TokenIndex = rhs.TokenIndex;
            connection = rhs.connection;
            AddEdgeOtherSource = rhs.AddEdgeOtherSource;
            dir = rhs.dir;
            RepeatSubstring = rhs.RepeatSubstring;
            LazyMark = rhs.LazyMark;
        }

        public GraphTraversal(ref GraphViewConnection pConnection)
        {
            CurrentOperator = null;
            AppendExecutableString = "";
            HoldMark = true;
            TokenIndex = new List<int>();
            connection = pConnection;
            dir = direction.Undefine;
            LazyMark = false;
        }

        public GraphTraversal(GraphTraversal rhs, string NewAES)
        {
            CurrentOperator = rhs.CurrentOperator;
            AppendExecutableString = NewAES;
            HoldMark = rhs.HoldMark;
            TokenIndex = rhs.TokenIndex;
            connection = rhs.connection;
            AddEdgeOtherSource = rhs.AddEdgeOtherSource;
            RepeatSubstring = rhs.RepeatSubstring;
            dir = rhs.dir;
            UnionString = rhs.UnionString;
            LazyMark = rhs.LazyMark;
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

        public static GraphTraversal _underscore()
        {
            GraphViewConnection NullConnection = new GraphViewConnection();
            GraphTraversal HeldPipe = new GraphTraversal(ref NullConnection);
            HeldPipe.HoldMark = false;
            HeldPipe.AppendExecutableString += "__.";
            HeldPipe.LazyMark = true;
            return HeldPipe;
        }
        public GraphTraversal V()
        {
            if (HoldMark == true) held = this;

            return new GraphTraversal(this, AppendExecutableString + "V().");
        }

        public GraphTraversal E()
        {
            if (HoldMark == true) held = this;

            return new GraphTraversal(this, AppendExecutableString + "E().");

        }

        public GraphTraversal next()
        {
            if (HoldMark == true) held = this;

            return new GraphTraversal(this, AppendExecutableString + "next().");

        }

        public GraphTraversal has(string name, string value)
        {
            if (HoldMark == true) held = this;

            return new GraphTraversal(this, AppendExecutableString + "has(\'" + name + "\', " + "\'" + value + "\').");

        }

        public GraphTraversal has(string name, Tuple<int, GraphViewGremlinParser.Keywords> ComparisonFunc)
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

            return new GraphTraversal(this, AES);
        }
        public GraphTraversal has(string name, Tuple<string[], GraphViewGremlinParser.Keywords> ComparisonFunc)
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

            return new GraphTraversal(this, AES);
        }

        public GraphTraversal Out(params string[] Parameters)
        {
            string AES = AppendExecutableString;
            if (Parameters.Length == 0)
                AES += "out().";
            else
                AES += "out(\'"+string.Join("\', \'",Parameters)+"\').";
            if (HoldMark == true) held = this;

            return new GraphTraversal(this, AES);

        }

        public GraphTraversal In(params string[] Parameters)
        {
            string AES = AppendExecutableString;
            if (Parameters.Length == 0)
                AES += "in().";
            else
                AES += "in(\'" + string.Join("\', \'", Parameters) + "\').";
            if (HoldMark == true) held = this;

            return new GraphTraversal(this, AES);

        }

        public GraphTraversal outE(params string[] Parameters)
        {
            string AES = AppendExecutableString;
            if (Parameters.Length == 0)
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

            return new GraphTraversal(this, AES);

        }

        public GraphTraversal inE(params string[] Parameters)
        {
            string AES = AppendExecutableString;
            if (Parameters.Length == 0)
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

            return new GraphTraversal(this, AES);
        }

        public GraphTraversal inV()
        {
            if (HoldMark == true) held = this;

            return new GraphTraversal(this, AppendExecutableString + "inV().");

        }

        public GraphTraversal outV()
        {
            if (HoldMark == true) held = this;

            return new GraphTraversal(this, AppendExecutableString + "outV().");

        }
        public GraphTraversal As(string alias)
        {
            if (HoldMark == true) held = this;

            return new GraphTraversal(this, AppendExecutableString + "as(\'" + alias + "\').");

        }

        public GraphTraversal select(params string[] Parameters)
        {
            string AES = AppendExecutableString;
            if (Parameters == null)
                AES += "select().";
            else
                AES += "select(\'"+ string.Join("\',\'",Parameters)+"\').";
            if (HoldMark == true) held = this;
            return new GraphTraversal(this,AES);

        }

        public GraphTraversal addV(params string[] Parameters)
        {

            if (!LazyMark)
            {
                GraphViewGremlinParser parser = new GraphViewGremlinParser();
                parser.Parse(CutTail(AppendExecutableString + "addV(\'" + string.Join("\',\'", Parameters) + "\')."))
                    .Generate(connection)
                    .Next();
            }

            if (HoldMark == true) held = this;

            return new GraphTraversal(this, AppendExecutableString + "addV(\'" + string.Join("\',\'", Parameters) + "\').");

        }

        public GraphTraversal addV(List<string> Parameters)
        {

            if (!LazyMark)
            {
                GraphViewGremlinParser parser = new GraphViewGremlinParser();
                parser.Parse(CutTail(AppendExecutableString + "addV(\'" + string.Join("\',\'", Parameters) + "\')."))
                    .Generate(connection)
                    .Next();
            }

            if (HoldMark == true) held = this;

            return new GraphTraversal(this, AppendExecutableString + "addV(\'" + string.Join("\',\'", Parameters) + "\').");

        }

        public GraphTraversal addOutE(params string[] Parameters)
        {
            if (!LazyMark)
            {
                GraphViewGremlinParser parser = new GraphViewGremlinParser();
                parser.Parse(CutTail(AppendExecutableString + "addOutE(\'" + string.Join("\',\'", Parameters) + "\')."))
                    .Generate(connection)
                    .Next();
            }
            if (HoldMark == true) held = this;


            return new GraphTraversal(this, AppendExecutableString + "addOutE(\'" + string.Join("\',\'", Parameters) + "\').");

        }

        public GraphTraversal addInE(params string[] Parameters)
        {
            if (!LazyMark)
            {
                GraphViewGremlinParser parser = new GraphViewGremlinParser();
                parser.Parse(CutTail(AppendExecutableString + "addInE(\'" + string.Join("\',\'", Parameters) + "\')."))
                    .Generate(connection)
                    .Next();
            }
            if (HoldMark == true) held = this;

            return new GraphTraversal(this, AppendExecutableString + "addInE(\'" + string.Join("\',\'", Parameters) + "\').");
        }

        public GraphTraversal values(string name)
        {
            if (HoldMark == true) held = this;

            return new GraphTraversal(this, AppendExecutableString + "values(\'" + name + "\').");

        }

        public GraphTraversal where(Tuple<string, GraphViewGremlinParser.Keywords> ComparisonFunc)
        {

            string AES = AppendExecutableString;
            if (HoldMark == true) held = this;

            if (ComparisonFunc.Item2 == GraphViewGremlinParser.Keywords.eq)
                AES += "where(eq(\'" + ComparisonFunc.Item1 + "\')).";
            if (ComparisonFunc.Item2 == GraphViewGremlinParser.Keywords.neq)
                AES += "where(neq(\'" + ComparisonFunc.Item1 + "\')).";

            return new GraphTraversal(this,AES);
        }

        public GraphTraversal where(GraphTraversal pipe)
        {
            string AES = AppendExecutableString;
            AES = AES + "where(" + CutTail(pipe.AppendExecutableString) + "))";
            return new GraphTraversal(this,AES);
        }

        public GraphTraversal match(params GraphTraversal[] pipes)
        {
            string AES = AppendExecutableString;
            List<string> StringList = new List<string>();
            foreach (var x in pipes) StringList.Add(x.AppendExecutableString);
            AES += "match(\'" + String.Join(",", StringList) + ").";
            return new GraphTraversal(this,AES);
        }

        public GraphTraversal aggregate(string name)
        {
            if (HoldMark == true) held = this;

            return new GraphTraversal(this);
        }

        public GraphTraversal and(params GraphTraversal[] pipes)
        {

            List<string> PipeString = new List<string>();
            foreach(var x in pipes) PipeString.Add(x.AppendExecutableString);
            return new GraphTraversal(this, AppendExecutableString + "and(" + String.Join(",", PipeString) + ").");
        }

        public GraphTraversal or(params GraphTraversal[] pipes)
        {
            List<string> PipeString = new List<string>();
            foreach (var x in pipes) PipeString.Add(x.AppendExecutableString);
            return new GraphTraversal(this, AppendExecutableString + "or(" + String.Join(",", PipeString) + ").");
        }

        public GraphTraversal drop()
        {
            if (HoldMark == true) held = this;

            return new GraphTraversal(this, AppendExecutableString + "drop().");
        }

        public GraphTraversal Is(Tuple<string, GraphViewGremlinParser.Keywords> ComparisonFunc)
        {
            string AES = AppendExecutableString;
            if (HoldMark == true) held = this;

            if (ComparisonFunc.Item2 == GraphViewGremlinParser.Keywords.eq)
                AES += "is(eq(\'" + ComparisonFunc.Item1 + "\')).";
            if (ComparisonFunc.Item2 == GraphViewGremlinParser.Keywords.neq)
                AES += "is(neq(\'" + ComparisonFunc.Item1 + "\')).";

            return new GraphTraversal(this);
        }

        public GraphTraversal Limit(int i)
        {
            if (HoldMark == true) held = this;

            return new GraphTraversal(this);
        }

        public GraphTraversal repeat(GraphTraversal pipe)
        {
            GraphTraversal Tra = new GraphTraversal(this,AppendExecutableString + "repeat().");
            Tra.RepeatSubstring = "V()." + pipe.AppendExecutableString;
            return Tra;
        }

        public GraphTraversal until(GraphTraversal pipe)
        {
            GraphTraversal Tra = new GraphTraversal(this);
            return new GraphTraversal(this,AppendExecutableString + pipe.AppendExecutableString);
        }
        public GraphTraversal times(int i)
        {
            return new GraphTraversal(this, AppendExecutableString + "times(" + i + ").");
        }

        public GraphTraversal choose(GraphTraversal pipe)
        {
            return new GraphTraversal(this, AppendExecutableString + "choose(" + CutTail(pipe.AppendExecutableString) + ").");
        }

        public GraphTraversal option(string name, GraphTraversal pipe)
        {
            return new GraphTraversal(this, AppendExecutableString + "option(\'" + name + "\'" + CutTail(pipe.AppendExecutableString) + ").");
        }

        public GraphTraversal coalesce(params GraphTraversal[] pipes)
        {
            List<string> StringList = new List<string>();
            foreach(var x in pipes) StringList.Add(CutTail(x.AppendExecutableString));
            return new GraphTraversal(this, AppendExecutableString + "coalesce(" + String.Join(",", StringList) + ").");
        }

        public GraphTraversal addE(params string[] Parameters)
        {
            if (HoldMark == true) held = this;

            return new GraphTraversal(this, AppendExecutableString + "addE(\'" + string.Join("\',\'", Parameters) + "\').");
        }

        public GraphTraversal addE(List<string> Parameters)
        {
            if (HoldMark == true) held = this;

            return new GraphTraversal(this, AppendExecutableString + "addE(\'" + string.Join("\',\'", Parameters) + "\').");
        }

        public GraphTraversal from(GraphTraversal OtherSource)
        {
            GraphTraversal NewTraversal = new GraphTraversal(this);
            if (OtherSource != null)
            {
                NewTraversal.AddEdgeOtherSource = CutTail(OtherSource.AppendExecutableString);
                NewTraversal.dir = direction.In;
            }
            if (NewTraversal.AddEdgeOtherSource != null)
            {
                GraphViewGremlinParser ExtendParser1 = new GraphViewGremlinParser();
                ExtendParser1.Parse(CutTail(NewTraversal.AppendExecutableString));
                GraphViewGremlinParser ExtendParser2 = new GraphViewGremlinParser();
                ExtendParser2.Parse(NewTraversal.AddEdgeOtherSource);
                var X = new WInsertEdgeFromTwoSourceSpecification(ExtendParser1.SqlTree, ExtendParser2.SqlTree, dir);
                CurrentOperator = X.Generate(connection);
                while (CurrentOperator.Status()) CurrentOperator.Next();
            }
            return new GraphTraversal(NewTraversal);
        }

        public GraphTraversal to(GraphTraversal OtherSource)
        {
            GraphTraversal NewTraversal = new GraphTraversal(this);
            if (OtherSource != null)
            {
                NewTraversal.AddEdgeOtherSource = CutTail(OtherSource.AppendExecutableString);
                NewTraversal.dir = direction.Out;
            }
            if (NewTraversal.AddEdgeOtherSource != null)
            {
                GraphViewGremlinParser ExtendParser1 = new GraphViewGremlinParser();
                ExtendParser1.Parse(CutTail(NewTraversal.AppendExecutableString));
                GraphViewGremlinParser ExtendParser2 = new GraphViewGremlinParser();
                ExtendParser2.Parse(NewTraversal.AddEdgeOtherSource);
                var X = new WInsertEdgeFromTwoSourceSpecification(ExtendParser1.SqlTree, ExtendParser2.SqlTree, dir);
                CurrentOperator = X.Generate(connection);
                while (CurrentOperator.Status()) CurrentOperator.Next();
            }
            return new GraphTraversal(NewTraversal);
        }

        public GraphTraversal order()
        {
            return new GraphTraversal(this, AppendExecutableString + "order().");
        }

        public GraphTraversal by(string bywhat,string order ="")
        {
            if (order == "" && bywhat =="incr")
                return new GraphTraversal(this, AppendExecutableString + "by(incr).");
            if (order == "" && bywhat == "decr")
                return new GraphTraversal(this, AppendExecutableString + "by(decr).");
            return new GraphTraversal(this, AppendExecutableString + "by(\'" + bywhat + "\', " + order + ").");
        }

        public GraphTraversal max()
        {
            return new GraphTraversal(this, AppendExecutableString + "max().");
        }
        public GraphTraversal count()
        {
            return new GraphTraversal(this, AppendExecutableString + "count().");
        }
        public GraphTraversal min()
        {
            return new GraphTraversal(this, AppendExecutableString + "min().");
        }
        public GraphTraversal mean()
        {
            return new GraphTraversal(this, AppendExecutableString + "mean().");
        }
        internal string CutTail(string some)
        {
            if (some.Length < 1) return null;
            return some.Substring(0, some.Length - 1);
        }

        internal GraphTraversal union(params GraphTraversal[] pipes)
        {
            GraphTraversal tra = new GraphTraversal(this);
            tra.UnionString = new List<string>();
            foreach(var pipe in pipes) tra.UnionString.Add(AppendExecutableString+pipe.AppendExecutableString);
            return tra;
        }

        internal GraphTraversal path()
        {
            return new GraphTraversal(this, AppendExecutableString + "path().");
        }
    }
    
}
