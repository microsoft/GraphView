using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphViewFlowTest
{
    class TriangleMatchTest:PatternMatchTest
    {
        public override string getQueryString()
        {
            return @"set transaction isolation level read uncommitted;
                SELECT count(*)
                FROM 
					Patent_NT as A, 
					Patent_NT as B, 
					Patent_NT as C
                MATCH A-[adjacencyList as E1]->B,
                      A-[adjacencyList as E2]->C,
                      B-[adjacencyList as E3]->C
                WHERE
	                A.gyear = 1990
                and B.gyear > 1985
                and C.gyear > 1986";
        }
    }
}
