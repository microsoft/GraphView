----------------------- Rectangle -----------------------
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
                and D.gyear > 1985 
                GO
----------------------- Three Leaves to Leaves -----------------------
				SELECT count(*)
                FROM 
                    Patent_NT as A, 
                    Patent_NT as B, 
                    Patent_NT as C, 
                    Patent_NT as D,
                    Patent_NT as E
                MATCH 
                    A-[adjacencyList as E1]->B,
                    A-[adjacencyList as E2]->C,
                    A-[adjacencyList as E3]->D,
                    E-[adjacencyList as E4]->B,
                    E-[adjacencyList as E5]->C,
                    E-[adjacencyList as E6]->D,                    
                WHERE
	                A.gyear = 1990
	            and E.gyear = 1990
                and B.gyear = 1990 and B.patentid != C.patentid
                and C.gyear = 1990 and C.patentid != D.patentid
                and D.gyear = 1990 and D.patentid != B.patentid
                and A.patentid != E.patentid
GO
----------------------- Double Diamond -----------------------
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
                and En.gyear = 1990
GO
----------------------- InFlux -----------------------
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
                and E.gyear = 1990
GO
----------------------- Triple Triangle -----------------------
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
                and D.gyear > 1985
GO

----------------------- Petagon -----------------------
	            SELECT count(*)
                FROM 
                    Patent_NT as A, 
                    Patent_NT as B, 
                    Patent_NT as C, 
                    Patent_NT as D,
                    Patent_NT as E
                MATCH 
                    A-[adjacencyList as E1]->B-[adjacencyList as E2]->C-[adjacencyList as E3]->D,
                    A-[adjacencyList as E4]->E-[adjacencyList as E5]->D
                WHERE
	                A.gyear = 1990
                and B.gyear > 1985
                and C.gyear > 1986
                and E.gyear > 1985
                and D.gyear = 1990
GO

----------------------- Triangle -----------------------
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
                and C.gyear > 1986
GO

----------------------- Long Chains -----------------------
	            SELECT count(*)
                FROM 
                    Patent_NT as A, 
                    Patent_NT as B, 
                    Patent_NT as C, 
                    Patent_NT as D,
                    Patent_NT as E,
                    Patent_NT as F
                MATCH 
                    A-[adjacencyList as E1]->B-[adjacencyList as E2]->C-[adjacencyList as E3]->D-[adjacencyList as E4]->E-[adjacencyList as E5]->F
                WHERE
	                A.gyear = 1990
                and B.gyear > 1985
                and C.gyear > 1986
                and D.gyear > 1985
                and E.gyear > 1985
                and F.gyear = 1990
                and A.patentid != B.patentid and B.patentid != C.patentid and C.patentid != D.patentid and D.patentid != E.patentid and E.patentid != F.patentid
