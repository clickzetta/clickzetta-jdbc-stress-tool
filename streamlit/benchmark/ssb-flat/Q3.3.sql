SELECT C_CITY,S_CITY,Year(LO_ORDERDATE) AS year,sum(LO_REVENUE) AS revenue FROM lineorder_flat WHERE (C_CITY = 'UNITED KI1' OR C_CITY = 'UNITED KI5') AND (S_CITY = 'UNITED KI1' OR S_CITY = 'UNITED KI5') GROUP BY C_CITY,S_CITY,year HAVING year >= 1992 AND year <= 1997 ORDER BY year ASC,revenue DESC;