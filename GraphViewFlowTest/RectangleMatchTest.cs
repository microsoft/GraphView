using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphViewFlowTest
{
    class RectangleMatchTest:PatternMatchTest
    {
        public override string  getQueryString()
        {
            return @" set transaction isolation level read uncommitted;
                SELECT count(*)
                FROM 
                    Patent_NT as A, 
                    Patent_NT as B, 
                    Patent_NT as C, 
                    Patent_NT as D
                MATCH 
                    A-[adjacencyList as E1]->B,
                    B-[adjacencyList as E2]->C,
                    C-[adjacencyList as E3]->D,
                    A-[adjacencyList as E4]->D
                WHERE
	                A.gyear = 1990
                and B.gyear > 1985
                and C.gyear > 1986
                and D.gyear > 1985 ";
        }
    }
}
