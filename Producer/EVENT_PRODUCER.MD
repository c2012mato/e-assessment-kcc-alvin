## Event producer configuration 

- **Job name** \
    Event-mock-firing-socmed
- **Region** \
    asia-southeast1
- **Environment variables**
    - BQ project 
    - Pub/Sub topic
    - API_KEY

## Pub/Sub Topic
Topic where events from Producer will be stored, a buffer before Dataflow steam data to BigQuery to ensure no data loss

- **Topic name** \
    projects/test-01-468006/topics/socmed-events
- **Topic subscription** \
    projects/test-01-468006/topics/socmed-events-sub
    

