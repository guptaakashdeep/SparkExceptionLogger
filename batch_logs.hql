CREATE EXTERNAL TABLE `control_db.batch_logs`(
  `process_name` string, 
  `sub_process` string, 
  `script_name` string, 
  `cluster_id` string, 
  `application_id` string, 
  `status` string, 
  `start_ts` timestamp, 
  `end_ts` timestamp, 
  `error` string, 
  `error_desc` string, 
  `time_taken` string)
PARTITIONED BY ( 
  `execution_dt` date)
STORED AS PARQUET
LOCATION
  's3://my-data-bucket/control_db/batch_logs';
