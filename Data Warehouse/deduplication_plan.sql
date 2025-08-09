/*  
    This is better handled in Dataform/dbt orchestrations, 
    but for the sake of this example, we will use a 
    BigQuery SQL script to deduplicate the events table.
*/
CREATE OR REPLACE TABLE `test-01-468006.production.tb_socmed_events_dedup` 
PARTITION BY DATE(event_timestamp)
AS
SELECT * EXCEPT(RN)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY received_timestamp DESC) AS RN
  FROM `test-01-468006.production.tb_socmed_events`
)
WHERE RN = 1