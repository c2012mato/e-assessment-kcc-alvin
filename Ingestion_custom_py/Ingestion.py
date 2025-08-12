import os
import json
from google.cloud import pubsub_v1
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

class EventIngestor:
    TABLE_SCHEMA = [
        bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_name", "STRING"),
        bigquery.SchemaField("user_id", "STRING"),
        bigquery.SchemaField("event_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("received_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("is_valid", "BOOL"),
        bigquery.SchemaField("is_deleted", "BOOL"),
    ]

    PARAM_TYPES = {
        "event_id": "STRING",
        "event_name": "STRING",
        "user_id": "STRING",
        "event_timestamp": "TIMESTAMP",
        "received_timestamp": "TIMESTAMP",
        "is_valid": "BOOL",
        "is_deleted": "BOOL"
    }

    def __init__(self, project_id, subscription_id, table_a, table_b):
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.table_a = table_a
        self.table_b = table_b
        self.bq_client = bigquery.Client(project=self.project_id)
        self.subscriber = pubsub_v1.SubscriberClient()
        self.sub_path = self.subscriber.subscription_path(self.project_id, self.subscription_id)

    def create_table_if_not_exists(self, table_id, partition_field):
        try:
            self.bq_client.get_table(table_id)
            print(f"Table {table_id} already exists.")
        except NotFound:
            print(f"Table {table_id} not found, creating...")
            table = bigquery.Table(table_id, schema=self.TABLE_SCHEMA)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            )
            self.bq_client.create_table(table)
            print(f"Created table {table_id} partitioned by {partition_field}.")

    def ensure_tables(self):
        self.create_table_if_not_exists(self.table_a, "event_timestamp")
        self.create_table_if_not_exists(self.table_b, "event_timestamp")

    def upsert_table_a(self, event):
        event_id = event["event_id"]
        # Fill missing fields with None
        for field in self.PARAM_TYPES:
            if field not in event:
                event[field] = None
        # Always set is_deleted for clarity
        is_valid = event.get("is_valid", True)
        event["is_deleted"] = not is_valid

        set_fields = [f"{k} = @{k}" for k in event.keys()]
        insert_fields = ', '.join(event.keys())
        insert_values = ', '.join([f"@{k}" for k in event.keys()])
        sql = f"""
        MERGE `{self.table_a}` T
        USING (SELECT
            @event_id AS event_id,
            @event_name AS event_name,
            @user_id AS user_id,
            @event_timestamp AS event_timestamp,
            @received_timestamp AS received_timestamp,
            @is_valid AS is_valid,
            @is_deleted AS is_deleted
        ) S
        ON T.event_id = S.event_id
        WHEN MATCHED AND (T.received_timestamp IS NULL OR S.received_timestamp > T.received_timestamp) THEN
          UPDATE SET {', '.join(set_fields)}
        WHEN NOT MATCHED THEN
          INSERT ({insert_fields}) VALUES ({insert_values})
        """
        params = []
        for k, v in event.items():
            if k in self.PARAM_TYPES:
                params.append(bigquery.ScalarQueryParameter(k, self.PARAM_TYPES[k], v))
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        self.bq_client.query(sql, job_config=job_config).result()

    def insert_table_b(self, event):
        self.bq_client.insert_rows_json(self.table_b, [event])

    def handle_tombstone(self, event):
        # If only event_id is present, treat as delete/tombstone
        if set(event.keys()) == {"event_id"}:
            tombstone_event = {
                "event_id": event["event_id"],
                "event_name": None,
                "user_id": None,
                "event_timestamp": None,
                "received_timestamp": None,
                "is_valid": False,
                "is_deleted": True,
            }
            self.upsert_table_a(tombstone_event)
            self.insert_table_b(tombstone_event)
            return True
        return False

    def process_message(self, message):
        try:
            event = json.loads(message.data.decode("utf-8"))
            # Handle tombstone event
            if self.handle_tombstone(event):
                print(f"Processed tombstone event for event_id={event['event_id']}")
                message.ack()
                return
            self.upsert_table_a(event)
            if event.get("is_valid") is False:
                self.insert_table_b(event)
            print(f"Processed event_id={event['event_id']} (is_valid={event['is_valid']})")
            message.ack()
        except Exception as e:
            print(f"Error processing message: {e}")
            message.nack()

    def run(self):
        self.ensure_tables()
        print("Listening for messages on:", self.sub_path)
        streaming_pull_future = self.subscriber.subscribe(self.sub_path, callback=self.process_message)
        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()

def main():
    PROJECT_ID = os.getenv("PROJECT_ID")
    SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID")
    TABLE_A = os.getenv("VALID_TABLE")
    TABLE_B = os.getenv("INVALID_TABLE")

    ingestor = EventIngestor(PROJECT_ID, SUBSCRIPTION_ID, TABLE_A, TABLE_B)
    ingestor.run()

if __name__ == "__main__":
    main()
