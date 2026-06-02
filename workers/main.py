import sys
import json
import logging
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from config import config
import executor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_consumer() -> Consumer:
    conf = {
        'bootstrap.servers': config.KAFKA_BROKERS,
        'group.id': config.CONSUMER_GROUP,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    return Consumer(conf)

def create_producer() -> Producer:
    conf = {
        'bootstrap.servers': config.KAFKA_BROKERS
    }
    return Producer(conf)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def process_message(msg_value: bytes, producer: Producer):
    try:
        payload = json.loads(msg_value.decode('utf-8'))
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode message payload: {e}")
        return

    task_id = payload.get('task_id')
    if not task_id:
        logger.error("Message missing 'task_id'")
        return

    script_content = payload.get('script_content')
    if not script_content:
        logger.error(f"Task {task_id} missing 'script_content'")
        return

    env_vars = payload.get('env_vars', {})
    timeout_seconds = payload.get('timeout_seconds', config.DEFAULT_TIMEOUT_SECONDS)

    logger.info(f"Starting execution of task {task_id}")
    
    result = executor.execute_task(
        task_id=task_id,
        script_content=script_content,
        env_vars=env_vars,
        timeout_seconds=timeout_seconds
    )
    
    # Send result to results topic
    try:
        producer.produce(
            config.RESULTS_TOPIC,
            key=str(task_id).encode('utf-8'),
            value=json.dumps(result).encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)
    except Exception as e:
        logger.error(f"Failed to publish result for task {task_id}: {e}")

def main():
    logger.info("Starting Boxed Snake Worker daemon")
    
    try:
        consumer = create_consumer()
        producer = create_producer()
    except Exception as e:
        logger.error(f"Failed to initialize Kafka clients: {e}")
        sys.exit(1)

    consumer.subscribe([config.TASKS_TOPIC])
    logger.info(f"Subscribed to topic: {config.TASKS_TOPIC}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    import time
                    logger.warning("Topic not available yet, waiting...")
                    time.sleep(2)
                    continue
                else:
                    logger.error(msg.error())
                    raise KafkaException(msg.error())

            process_message(msg.value(), producer)
            
            # Commit offset after successful processing and publishing result
            consumer.commit(asynchronous=False)
            
    except KeyboardInterrupt:
        logger.info("Aborted by user")
    finally:
        logger.info("Closing Kafka consumer")
        consumer.close()
        producer.flush(timeout=5.0)

if __name__ == "__main__":
    main()
