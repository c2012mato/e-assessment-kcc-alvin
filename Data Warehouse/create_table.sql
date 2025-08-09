/*
    CREATE TABLE for storing social media events.
*/
CREATE TABLE `test-01-468006.production.tb_socmed_events` (
  event_id STRING,
  event_name STRING,
  user_id STRING,
  event_timestamp TIMESTAMP,
  received_timestamp TIMESTAMP,
  is_valid BOOL
)
PARTITION BY DATE(event_timestamp)