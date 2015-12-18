using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphViewFlowTest
{
    class TripleTriangleMatchTest:PatternMatchTest
    {
        public override string getQueryString()
        {
            return @"set transaction isolation level read uncommitted;
                SELECT count(*)
                FROM 
                    Patent_NT as A, 
                    Patent_NT as B, 
                    Patent_NT as C, 
                    Patent_NT as D,
                    Patent_NT as S
                MATCH 
                    S-[adjacencyList as E1]->A,
                    S-[adjacencyList as E2]->B,
                    S-[adjacencyList as E3]->C,
                    S-[adjacencyList as E4]->D,
                    A-[adjacencyList as E5]->B-[adjacencyList as E6]->C-[adjacencyList as E7]->D         
                WHERE
	                S.gyear = 1990
                and A.gyear > 1985
                and B.gyear > 1985
                and C.gyear > 1985
                and D.gyear > 1985";
        }
    }
}
