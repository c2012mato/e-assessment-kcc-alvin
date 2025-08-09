SELECT 
    * 
FROM 
    `test-01-468006.production.tb_socmed_events_dedup`
WHERE 
    is_valid = FALSE
ORDER BY 
    received_timestamp DESC
