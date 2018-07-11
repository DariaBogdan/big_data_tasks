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

-- joining 3 subqueries
-- each consists of city and the most popular device/browser/OS for each city
-- the first value is taken in subquery because queries are ordered desc
SELECT distinct sub_os.city_id, 
       sub_os.os, 
       sub_browser.browser,
	   sub_device.device
FROM   (SELECT First_value(sub.city_id)
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
                          os_count DESC) AS sub) AS sub_os 
		 JOIN (SELECT First_value(sub.city_id)
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
								 browser_count DESC) AS sub) AS sub_browser 
		   ON sub_os.city_id = sub_browser.city_id

		 JOIN (SELECT First_value(sub.city_id)
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
								 device_count DESC) AS sub) AS sub_device 
		   ON sub_os.city_id = sub_device.city_id; 		 
