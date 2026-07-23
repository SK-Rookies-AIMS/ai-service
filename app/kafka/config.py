# kafka/config.py
RAW_TOPIC = "factory.manufacturing.raw"
ANALYSIS_TOPIC = "factory.manufacturing.analysis"
# ES sync events reuse the existing analysis topic to avoid requiring an extra Kafka topic.
ANALYSIS_SYNC_TOPIC = ANALYSIS_TOPIC
RAW_CONSUMER_GROUP_ID = "ai-analysis-consumer-group"
ANALYSIS_SYNC_CONSUMER_GROUP_ID = "ai-analysis-sync-consumer-group"
RAW_CONSUMER_CONCURRENCY = 2
AUTO_OFFSET_RESET = "earliest"

ENABLE_AUTO_COMMIT = False
CONSUMER_TIMEOUT_MS = 1000

SESSION_TIMEOUT_MS = 30000

HEARTBEAT_INTERVAL_MS = 10000

MAX_POLL_RECORDS = 1

MAX_POLL_INTERVAL_MS = 900000
PRODUCER_RETRIES = 3
PRODUCER_LINGER_MS = 10
