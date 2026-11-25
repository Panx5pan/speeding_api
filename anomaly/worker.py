import json
import logging
import logging.config
import os
import signal
import time
from datetime import datetime, timezone

import yaml
from confluent_kafka import Consumer, KafkaException, Producer

from detect import THRESHOLDS, check_default

with open("log_conf.yaml", "r") as f:
    logging.config.dictConfig(yaml.safe_load(f.read()))

logger = logging.getLogger("anomaly.worker")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
EVENTS_TOPIC = os.getenv("KAFKA_TOPIC", "events")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "anomalies")
GROUP_ID = os.getenv("GROUP_ID", "anomaly_worker_group")


def handle_sigterm(signum, frame):
    raise KeyboardInterrupt


signal.signal(signal.SIGTERM, handle_sigterm)


def utc_now_iso():
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="microseconds") + "Z"


def publish_with_retry(producer, key, payload):
    delay = 1
    while True:
        outcome = {}

        def on_delivery(err, msg):
            outcome["err"] = err

        try:
            producer.produce(DLQ_TOPIC, value=payload, key=key, on_delivery=on_delivery)
            producer.flush(10)
        except (BufferError, KafkaException) as e:
            outcome["err"] = e

        if "err" in outcome and outcome["err"] is None:
            return

        logger.warning(f"Could not publish to '{DLQ_TOPIC}', retrying in {delay}s")
        time.sleep(delay)
        delay = min(delay * 2, 30)


def handle_message(msg, producer):
    try:
        event = json.loads(msg.value().decode("utf-8"))
        reason = check_default(event)
    except (ValueError, UnicodeDecodeError, AttributeError) as e:
        logger.warning(f"Skipping invalid message at offset {msg.offset()}: {type(e).__name__}")
        return

    if reason is None:
        return

    trace_id = event.get("trace_id")
    payload = {
        "type": event.get("type"),
        "trace_id": trace_id,
        "reason": reason,
        "detected_at": utc_now_iso(),
        "event": event,
    }
    publish_with_retry(producer, str(trace_id), json.dumps(payload).encode("utf-8"))
    logger.info(f"Anomaly sent to '{DLQ_TOPIC}': {reason} | trace_id: {trace_id}")


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "enable.idempotence": True,
    })
    consumer.subscribe([EVENTS_TOPIC])
    logger.info(
        f"Anomaly worker listening on '{EVENTS_TOPIC}' (group '{GROUP_ID}'), "
        f"publishing to '{DLQ_TOPIC}'; thresholds: {THRESHOLDS}"
    )

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().fatal():
                    raise KafkaException(msg.error())
                logger.warning(f"Kafka error: {msg.error()}")
                continue

            handle_message(msg, producer)

            try:
                consumer.commit(message=msg, asynchronous=False)
            except KafkaException as ke:
                logger.warning(f"Could not commit offset {msg.offset()}: {ke}")

    except KafkaException as ke:
        logger.error(f"Fatal Kafka error: {ke}")
        raise
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        logger.info("Closing Kafka connection...")
        producer.flush(5)
        consumer.close()


if __name__ == "__main__":
    main()