flowchart TD
    A[Start] --> B[Initialize EventIngestor]
    B --> C[Ensure Tables Exist]
    C --> D[Listen for Pub/Sub Messages]
    D --> E{Message Received}
    E -->|Yes| F[Decode JSON Event]
    F --> G{Is Tombstone?}
    G -->|Yes| H[Create Tombstone Event]
    H --> I[Upsert into Table A]
    H --> J[Insert into Table B]
    G -->|No| K[Upsert into Table A]
    K --> L{Is Valid?}
    L -->|No| M[Insert into Table B]
    L -->|Yes| N[No action]
    N --> O[Acknowledge Message]
    M --> O
    I --> O
    J --> O
    O --> D
    E -->|No| P[End or wait for next message]
