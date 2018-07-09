-- Find all carriers who cancelled more than 1 flights during 2007, order them from biggest to lowest by number of cancelled flights -- and list in each record all departure cities where cancellation happened. (Screenshot #1)

SELECT uniquecarrier, 
       Count(*) AS cancelled_flights, 
       Collect_set(city) 
FROM   y2007 
       LEFT JOIN airports 
              ON y2007.origin = airports.iata 
WHERE cancelled = 1
GROUP  BY uniquecarrier 
HAVING cancelled_flights > 1 
ORDER  BY cancelled_flights DESC;
