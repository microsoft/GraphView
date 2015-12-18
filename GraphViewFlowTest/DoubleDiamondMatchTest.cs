using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphViewFlowTest
{
    class DoubleDiamondMatchTest : PatternMatchTest
    {
        public override string getQueryString()
        {
            return @" set transaction isolation level read uncommitted;
                SELECT count(*)
                FROM 
                    Patent_NT as A, 
                    Patent_NT as B, 
                    Patent_NT as C, 
                    Patent_NT as D,
                    Patent_NT as En,
                    Patent_NT as S1,
                    Patent_NT as S2
                MATCH 
                    S1-[adjacencyList as E1]->A-[adjacencyList as E2]->En,
                    S1-[adjacencyList as E3]->B-[adjacencyList as E4]->En,
                    S2-[adjacencyList as E5]->C-[adjacencyList as E6]->En,
                    S2-[adjacencyList as E7]->D-[adjacencyList as E8]->En                   
                WHERE
	                S1.gyear = 1990
	            and S2.gyear = 1990
                and S1.patentid != S2.patentid
                and A.gyear = 1990
                and B.gyear = 1990
                and C.gyear = 1990
                and D.gyear = 1990
                and En.gyear = 1990";
        }
    }
}
