using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphViewFlowTest
{
    class InfluxMatchTest:PatternMatchTest
    {
        public override string getQueryString()
        {
            return @"set transaction isolation level read uncommitted;
                    SELECT 
					A.patentid, 
					B.patentid, 
					C.patentid, 
					D.patentid, 
					E.patentid
                FROM 
					Patent_NT as A, 
					Patent_NT as B, 
					Patent_NT as C, 
					Patent_NT as D, 
					Patent_NT as E
                MATCH A-[adjacencyList]->B,
                      A-[adjacencyList]->C,
                      B-[adjacencyList]->C,
                      D-[adjacencyList]->C,          
                      E-[adjacencyList]->B
                WHERE
                    A.gyear = 1990
                and B.gyear = 1990
                and C.gyear = 1990
                and D.gyear = 1990
                and E.gyear = 1990";
        }
    }
}
