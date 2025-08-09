/*  
    This is better handled in Dataform/dbt orchestrations as well.
    However, for the sake of this example, we will use a 
    BigQuery SQL script to deduplicate the events table.
*/
SELECT 
    * 
FROM 
    `test-01-468006.production.tb_socmed_events_dedup`
WHERE 
    is_valid = TRUE
ORDER BY 
    received_timestamp DESC
