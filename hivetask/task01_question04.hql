-- Find five most busy airports in US during Jun 01 - Aug 31. (Screenshot#3)

SELECT airport, 
       Sum(total_flights) AS total_flights 
FROM   (SELECT origin        AS airport, 
               Count(origin) AS total_flights 
        FROM   y2007 
        WHERE  month IN ( 6, 7, 8 ) 
        GROUP  BY origin 
        UNION ALL 
        SELECT dest, 
               Count(dest) 
        FROM   y2007 
        WHERE  month IN ( 6, 7, 8 ) 
        GROUP  BY dest) a 
GROUP  BY airport 
ORDER  BY total_flights DESC 
LIMIT  5; 
