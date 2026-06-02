import os

class Config:
    KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
    CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "boxedsnake-workers")
    TASKS_TOPIC = os.getenv("TASKS_TOPIC", "tasks")
    RESULTS_TOPIC = os.getenv("RESULTS_TOPIC", "task-results")
    
    # Execution constraints
    DEFAULT_TIMEOUT_SECONDS = int(os.getenv("DEFAULT_TIMEOUT_SECONDS", "300"))
    MAX_OUTPUT_SIZE_BYTES = int(os.getenv("MAX_OUTPUT_SIZE_BYTES", str(10 * 1024 * 1024))) # 10 MB

config = Config()
