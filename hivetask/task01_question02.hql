-- Count total number of flights per carrier in 2007 (Screenshot#1)

SELECT uniquecarrier, 
       Count(*) AS total_flights 
FROM   y2007 
GROUP  BY uniquecarrier; 
