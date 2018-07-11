-- find most popular device, browser, OS for each city.

set hive.execution.engine=mr;

-- add my transform file
ADD FILE /home/raj_ops/parse_user_agent.py;

-- create temporary table to call transform only once
create temporary table temp as
SELECT TRANSFORM(city_id, user_agent)
USING 'parse_user_agent.py'
AS city_id, device, os, browser
FROM logs;

-- table for most popular os by city
CREATE TEMPORARY TABLE top_os_by_city (city_id INT, os STRING);
INSERT INTO TABLE top_os_by_city
	SELECT First_value(sub.city_id)
					 OVER (partition BY city_id) as city_id, 
				   First_value(sub.os)
					 OVER (partition BY city_id) as os 
			FROM   (SELECT city_id, 
						   os, 
						   Count(*) AS os_count 
					FROM   temp 
					GROUP  BY city_id, 
							  os 
					ORDER  BY city_id, 
							  os_count DESC) AS sub;
-- table for most popular browser by city
CREATE TEMPORARY TABLE top_browser_by_city (city_id INT, browser STRING);
INSERT INTO TABLE top_browser_by_city
	SELECT First_value(sub.city_id)
					 OVER (partition BY city_id) as city_id, 
				   First_value(sub.browser)
					 OVER (partition BY city_id) as browser 
			FROM   (SELECT city_id, 
						   browser, 
						   Count(*) AS browser_count 
					FROM   temp 
					GROUP  BY city_id, 
							  browser 
					ORDER  BY city_id, 
							  browser_count DESC) AS sub;
						
-- table for most popular device by city	  
CREATE TEMPORARY TABLE top_device_by_city (city_id INT, device STRING);
INSERT INTO TABLE top_device_by_city
	SELECT First_value(sub.city_id)
					 OVER (partition BY city_id) as city_id, 
				   First_value(sub.device)
					 OVER (partition BY city_id) as device 
			FROM   (SELECT city_id, 
						   device, 
						   Count(*) AS device_count 
					FROM   temp 
					GROUP  BY city_id, 
							  device 
					ORDER  BY city_id, 
							  device_count DESC) AS sub;
-- main select							  
SELECT 
    distinct top_os_by_city.city_id,
    top_os_by_city.os, 
    top_browser_by_city.browser,
    top_device_by_city.device
FROM
     top_os_by_city 
       LEFT JOIN top_browser_by_city 
              ON top_os_by_city.city_id = top_browser_by_city.city_id 
       LEFT JOIN top_device_by_city 
              ON top_os_by_city.city_id = top_device_by_city.city_id; 
							  
