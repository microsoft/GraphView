using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GraphView;

namespace GraphViewFlowTest
{
    public class PatternMatchTest
    {
        public void run(GraphViewConnection con)
        {
            System.Console.WriteLine( "---------------" + this.GetType().Name + "begin!" + "----------------" );
            using (var res = con.ExecuteReader(getQueryString()) )
            {
                int cnt = 0;
                while (res.Read())
                {
                    ++cnt;
                    for (int i = 0; i < res.FieldCount; ++i)
                    {
                        if (i > 0) Console.Write(",");
                        Console.Write(res.GetName(i) + ":" + res[i] );
                    }
                    Console.WriteLine();
                }
                Console.WriteLine("RowCount:" + cnt.ToString());
            }
        }
        virtual public string getQueryString()
        {
            Console.WriteLine("This is a virtual method. It has no t-sql statement");
            return "";
        }
    }
    
}
