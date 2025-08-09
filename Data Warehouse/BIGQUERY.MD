## Bigquery Table Config and Schema
- **test-01-468006.production.tb_socmed_events**
    - **schema**
    ```json
    [
        {
            "name": "event_id","type": "STRING","mode": "NULLABLE",
        },
        {
            "name": "event_name","type": "STRING","mode": "NULLABLE",
        },
        {
            "name": "user_id","type": "STRING","mode": "NULLABLE",
        },
        {
            "name": "event_timestamp","type": "TIMESTAMP","mode": "NULLABLE",
        },
        {
            "name": "received_timestamp","type": "TIMESTAMP","mode": "NULLABLE",
        },
        {
            "name": "is_valid","type": "BOOLEAN","mode": "NULLABLE",
        }
    ]
- **test-01-468006.production.tb_socmed_events_dedup**
    - **schema**
    ```json
    [
        {
            "name": "event_id","type": "STRING","mode": "NULLABLE",
        },
        {
            "name": "event_name","type": "STRING","mode": "NULLABLE",
        },
        {
            "name": "user_id","type": "STRING","mode": "NULLABLE",
        },
        {
            "name": "event_timestamp","type": "TIMESTAMP","mode": "NULLABLE",
        },
        {
            "name": "received_timestamp","type": "TIMESTAMP","mode": "NULLABLE",
        },
        {
            "name": "is_valid","type": "BOOLEAN","mode": "NULLABLE",
        }
    ]
## 
- **Create table** \
    Hadles creation of destination table
- **Deduplication plan** \
    Handles deduplication but better set up in proper transformation framework
- **Exclude failed events** \
    Handles de-selection/exclusion of rows for final output, better handled in proper transformation framework 