CREATE EXTERNAL TABLE IF NOT EXISTS TABLENAME(
  person_id integer, 
  first_name string, 
  last_name string, 
  email string, 
  date_of_birth string, 
  address string, 
  bank_balance integer,
  city string, 
  state string, 
  country string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 'S3LOCATION'
TBLPROPERTIES ('classification'='csv', 
               'skip.header.line.count'='1');