using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra;

namespace TransactionBenchmarkTest.TPCC
{
    class CassandraTest
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Start...");

            var cluster = Cluster.Builder().AddContactPoints("127.0.0.1").Build();
            ISession session = cluster.Connect("test");
            var rs = session.Execute("DESC TABLES");
            foreach(var row in rs)
            {
                Console.WriteLine(row.GetValue<string>(0));
            }

            Console.ReadLine();
            Console.WriteLine("Done");
        }

    }
}
