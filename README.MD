# E_ASSESSMENT Repository

This repository contains code and resources for the E_ASSESSMENT project. Below is documentation for the main objects and components included.

## Table of Contents
- [Overview](#overview)
- [Objects](#objects)
    - [Data Warehouse](#Data-Warehouse)
    - [Examples](#Examples)
    - [Ingestion](#Ingestion)
    - [Producer](#Producer)
- [Usage](#usage)
---

## Overview

This is engineering assessment foor Data Engineer position in KCC Holdings Inc.

## Objects

### Data-Warehouse
#### Overview
Bigquery is the chosen data warehouse to store data because it is GCP native.
#### Contents
- **create_table_sql** \
    Creates destination table.
- **deduplication_plan** \
    Creates table and do deduplication logic.

### Examples
#### Overview
Example event data in different csvs. Also contains SQL queries.
#### Contents
- **all_events.csv** \
    Select and extract to csv all events.
- **all_events.sql**  \
    Select statement of `all_events.csv`
- **events_dedup.csv**  \
    Select and extract to csv deduplicated events.
- **events_dedup.sql** \
    Select statement of `events_dedup.csv`
- **exclude_failed_events.csv** \
    Select and extract to csv all not failed/invalid events.
- **exclude_failed_events.sql** \
    Select statement of `exclude_failed_events.csv`
- **failed_events.csv** \
    Select and extract to csv all failed/invalid events.
- **failed_events.sql** \
    Select statement of `failed_events.csv`

### Ingestion
#### Overview
Dataflow is used to ingest pubsub messages from producer.
#### Contents
- **DATAFLOW.MD** \
    Dataflow configuration.

### Producer
#### Overview
Cloud Run Function is used to populate Pub/Sub using publish() to simulate event messages.
#### Contents
- **fire_event.py** \
    Python script in vloud run function.
- **EVENT_PRODUCER.MD** \
    Producer details and configuration.
- **Producer_flow_chart.MD** \
    Logic flow of producer script.
- **requirements.txt** \
    Libraries required during cloud build.
---

## Usage and Example triggers
- send messages to pubsub topic :
    ```json
    {
    "event_id": "12345",
    "event_name": "user_signup",
    "user_id": "67890",
    "event_timestamp": "2025-08-10T12:34:56Z",
    "received_timestamp": "2025-08-10T12:35:00Z",
    "is_valid": true
    }
- trigger producer
    - Notes :
        APP_URL - cloud run function app URL trigger
        APIKEY - cloud run function APIKEY 
        MAX duration - 20 minutes
    ```bash
    curl -X POST "APP_URL" \
    -H "Authorization: bearer $(gcloud auth print-identity-token)" \
    -H "Content-Type: application/json" \
    -H "Api-Key: APIKEY" \
    -d '{
    "action": "Start",
    "duration": 15 
    }'
