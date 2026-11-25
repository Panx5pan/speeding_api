import json
import logging
import logging.config
import os
import signal
import time
from datetime import datetime, timezone

import yaml
from confluent_kafka import Consumer, KafkaException
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, InterfaceError, OperationalError

from engine import make_session, ENGINE
from models import Speeding, Violation, Base

with open("log_conf.yaml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("storage.worker")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")

MYSQL_DUPLICATE_ENTRY = 1062


def handle_sigterm(signum, frame):
    raise KeyboardInterrupt


signal.signal(signal.SIGTERM, handle_sigterm)


def wait_for_db():
    delay = 1
    while True:
        try:
            with ENGINE.connect() as conn:
                conn.execute(text("SELECT 1"))
            Base.metadata.create_all(ENGINE)
            logger.info("Database is reachable")
            return
        except (OperationalError, InterfaceError):
            logger.warning(f"Database not reachable, retrying in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, 30)


def to_utc_naive(value):
    dt = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def build_event(value):
    event_type = value.get("type")
    if event_type == "speed":
        return Speeding(
            device_id=value["device_id"],
            vehicle_plate=value["vehicle_plate"],
            speed_mph=value["speed_mph"],
            location=value["location"],
            timestamp=to_utc_naive(value["timestamp"]),
            trace_id=value["trace_id"],
        )
    if event_type == "violation":
        return Violation(
            station_id=value["station_id"],
            vehicle_plate=value["vehicle_plate"],
            speed_overlimit_mph=value["speed_overlimit_mph"],
            location=value["location"],
            timestamp=to_utc_naive(value["timestamp"]),
            trace_id=value["trace_id"],
        )
    raise ValueError(f"unknown event type: {event_type!r}")


def is_duplicate(error):
    orig = getattr(error, "orig", None)
    return bool(orig is not None and orig.args and orig.args[0] == MYSQL_DUPLICATE_ENTRY)


def save_to_db(event):
    session = make_session()
    try:
        session.add(event)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def store_with_retry(value, offset):
    trace_id = value["trace_id"]
    delay = 1
    while True:
        try:
            save_to_db(build_event(value))
            logger.debug(
                f"Stored event '{value['type']}' with a trace id of {trace_id} (offset {offset})"
            )
            return
        except IntegrityError as e:
            if is_duplicate(e):
                logger.info(f"Duplicate trace_id {trace_id}, already stored (offset {offset})")
            else:
                logger.error(
                    f"Integrity error, skipping message at offset {offset} "
                    f"| trace_id: {trace_id}"
                )
            return
        except (OperationalError, InterfaceError):
            logger.warning(
                f"Database unavailable, retrying offset {offset} in {delay}s"
            )
            time.sleep(delay)
            delay = min(delay * 2, 30)
        except Exception as e:
            logger.error(
                f"Permanent failure ({type(e).__name__}), skipping message at "
                f"offset {offset} | trace_id: {trace_id}"
            )
            return


def handle_message(msg):
    try:
        value = json.loads(msg.value().decode("utf-8"))
        build_event(value)
        value["trace_id"]
    except (json.JSONDecodeError, UnicodeDecodeError, KeyError, ValueError, AttributeError, TypeError) as e:
        logger.warning(
            f"Skipping invalid message at offset {msg.offset()}: "
            f"{type(e).__name__}: {e} | raw={msg.value()!r}"
        )
        return

    store_with_retry(value, msg.offset())


def main():
    wait_for_db()

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "storage_worker_group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC])
    logger.info(f"Storage worker listening on topic '{KAFKA_TOPIC}' at {KAFKA_BROKER}")

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

            handle_message(msg)

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
        consumer.close()


if __name__ == "__main__":
    main()