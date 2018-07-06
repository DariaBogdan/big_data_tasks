-- The total number of flights served in Jun 2007 by NYC (all airports, use join with Airports)(Screenshot#2)

SELECT Count(*) as total_flights
FROM   y2007 
       JOIN airports 
         ON y2007.origin = airports.iata 
WHERE  airports.city = 'New York' 
       AND y2007.month = 6; 
