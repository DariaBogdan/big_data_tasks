-- create table logs

create external table logs (  
  bid_id string,  
  timestamp_ string,
  log_type int,
  ipinyouid int,
  user_agent string,
  ip string,
  region_id int,
  city_id int,
  ad_exchange int,
  domain string,
  url string,
  anonim_url string,
  ad_slot_id int,
  ad_slot_width int,
  ad_slot_heigth int,
  ad_slot_visibility string,
  ad_slot_format string,
  ad_slot_floor_price int,
  creative_id string,
  bidding_price int,
  pauing_price int,
  landing_page_url string,
  advertiser_id int,
  user_profile_ids string
) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES 
(
    "separatorChar" = "\t",
    "quoteChar"     = "\""
)  
STORED AS TEXTFILE 
location 'hdfs://sandbox-hdp.hortonworks.com:8020/task3' 
tblproperties("skip.header.line.count"="1");

