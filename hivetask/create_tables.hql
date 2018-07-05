-- creating tables airports, carriers and y2007 and filling them with given files.

create table carriers (  
  code string,  
  description string  
) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES 
(
    "separatorChar" = ",",
    "quoteChar"     = "\""
)  
STORED AS TEXTFILE 
location '/carriers' 
tblproperties("skip.header.line.count"="1");

load data inpath 'hdfs://sandbox-hdp.hortonworks.com:8020/user/raj_ops/hivetask/carriers.csv' overwrite into table carriers;

create table airports (  
  iata string,
  airport string,
  city string,
  state string,
  country string,
  lat float,
  lon float
 
) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES 
(
    "separatorChar" = ",",
    "quoteChar"     = "\""
)  
STORED AS TEXTFILE 
location '/airports' 
tblproperties("skip.header.line.count"="1");

load data inpath 'hdfs://sandbox-hdp.hortonworks.com:8020/user/raj_ops/hivetask/airports.csv' overwrite into table airports;

create table y2007 (  
  Year int,
  Month int,
  DayofMonth int,
  DayOfWeek int,
  DepTime int,
  CRSDepTime int,
  ArrTime int,
  CRSArrTime int,
  UniqueCarrier string,
  FlightNum int,
  TailNum string,
  ActualElapsedTime int,
  CRSElapsedTime int,
  AirTime int,
  ArrDelay int,
  DepDelay int,
  Origin string,
  Dest string,
  Distance int,
  TaxiIn int,
  TaxiOut int,
  Cancelled int,
  CancellationCode int,
  Diverted int,
  CarrierDelay int,
  WeatherDelay int,
  NASDelay int,
  SecurityDelay int, 
  LateAircraftDelay int

) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES 
(
    "separatorChar" = ",",
    "quoteChar"     = "\""
)  
STORED AS TEXTFILE 
location '/y2007' 
tblproperties("skip.header.line.count"="1");

load data inpath 'hdfs://sandbox-hdp.hortonworks.com:8020/user/raj_ops/hivetask/2007.csv' overwrite into table y2007;
