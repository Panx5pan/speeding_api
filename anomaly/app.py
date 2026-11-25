import json
import logging
import logging.config
import os
import time
import uuid

import connexion
import yaml
from confluent_kafka import Consumer, KafkaException, TopicPartition

from detect import check

with open("log_conf.yaml", "r") as f:
    logging.config.dictConfig(yaml.safe_load(f.read()))

logger = logging.getLogger("anomaly")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
EVENTS_TOPIC = os.getenv("KAFKA_TOPIC", "events")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "anomalies")
READ_TIMEOUT = float(os.getenv("READ_TIMEOUT_SEC", "15"))


class KafkaUnavailable(Exception):
    pass


def read_all(topic, tail=None):
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": f"anomaly_api_{uuid.uuid4().hex}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    try:
        try:
            metadata = consumer.list_topics(topic, timeout=5)
        except KafkaException as e:
            raise KafkaUnavailable(str(e))

        info = metadata.topics.get(topic)
        if info is None or info.error is not None:
            return [], 0

        remaining = {}
        starts = []
        total = 0
        for p in info.partitions:
            try:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=5)
            except KafkaException as e:
                raise KafkaUnavailable(str(e))
            if high > low:
                start = low if tail is None else max(low, high - tail)
                starts.append(TopicPartition(topic, p, start))
                remaining[p] = high - 1
                total += high - low

        if not remaining:
            return [], 0

        consumer.assign(starts)
        events = []
        deadline = time.monotonic() + READ_TIMEOUT

        while remaining and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().fatal():
                    raise KafkaUnavailable(str(msg.error()))
                continue
            try:
                events.append(json.loads(msg.value().decode("utf-8")))
            except (ValueError, UnicodeDecodeError):
                pass
            last = remaining.get(msg.partition())
            if last is not None and msg.offset() >= last:
                del remaining[msg.partition()]

        if remaining:
            raise KafkaUnavailable("timed out reading the topic")
        return events, total
    finally:
        consumer.close()


def get_anomalies(limit=100):
    try:
        messages, total = read_all(DLQ_TOPIC, tail=limit * 2)
    except KafkaUnavailable as e:
        logger.warning(f"Reading '{DLQ_TOPIC}' did not finish in time or Kafka is unreachable: {e}")
        return {"message": "Message broker unavailable"}, 503

    unique = {}
    for m in messages:
        if isinstance(m, dict):
            unique[(m.get("type"), m.get("trace_id"))] = m
    anomalies = sorted(unique.values(), key=lambda m: m.get("detected_at") or "", reverse=True)
    logger.debug(f"'{DLQ_TOPIC}' holds {total} messages, returning the newest {min(limit, len(anomalies))}")
    return {"count": total, "anomalies": anomalies[:limit]}, 200


def scan(type, above=None, below=None, limit=100):
    if above is None and below is None:
        return {"message": "Provide at least one of 'above' or 'below'"}, 400

    try:
        events, _ = read_all(EVENTS_TOPIC)
    except KafkaUnavailable as e:
        logger.warning(f"Reading '{EVENTS_TOPIC}' did not finish in time or Kafka is unreachable: {e}")
        return {"message": "Message broker unavailable"}, 503

    matches = []
    for event in events:
        if isinstance(event, dict) and event.get("type") == type:
            reason = check(event, above=above, below=below)
            if reason:
                matches.append({**event, "reason": reason})

    logger.info(f"Scan type={type} above={above} below={below}: {len(matches)} of {len(events)} events matched")
    return {"count": len(matches), "anomalies": matches[::-1][:limit]}, 200


def get_status():
    return {"status": "ok"}, 200


app = connexion.AsyncApp(__name__, specification_dir=".")
app.add_api("anomaly.yaml", strict_validation=True)