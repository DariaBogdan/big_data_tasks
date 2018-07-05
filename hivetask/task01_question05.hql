-- Find the carrier who served the biggest number of flights (Screenshot#3)

SELECT uniquecarrier, 
       Count(*) AS total_flights 
FROM   y2007 
GROUP  BY uniquecarrier 
ORDER  BY total_flights DESC 
LIMIT  1; 
