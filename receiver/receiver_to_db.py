import asyncio
import json
import logging
import logging.config
import os
import time
from contextlib import asynccontextmanager

import connexion
import yaml
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError, KafkaError
from connexion import NoContent, request

with open("log_conf.yaml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("receiver")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")


async def start_producer():
    delay = 1
    while True:
        producer = AIOKafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            enable_idempotence=True,
            request_timeout_ms=10_000,
            retry_backoff_ms=250,
            linger_ms=5,
        )
        try:
            logger.info(f"Connecting to Kafka at {KAFKA_BROKER}...")
            await producer.start()
            logger.info("Kafka producer started")
            return producer
        except (KafkaConnectionError, KafkaError, OSError):
            logger.warning(f"Kafka not reachable, retrying in {delay}s")
            try:
                await producer.stop()
            except Exception:
                pass
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


@asynccontextmanager
async def lifespan(app):
    producer = await start_producer()
    try:
        yield {"kafka": producer}
    finally:
        logger.info("Stopping Kafka producer...")
        await producer.stop()


async def send_to_kafka(event_type, body, trace_id):
    payload = {"type": event_type, **body, "trace_id": trace_id}
    try:
        await asyncio.wait_for(
            request.state.kafka.send_and_wait(
                KAFKA_TOPIC, json.dumps(payload).encode("utf-8")
            ),
            timeout=5,
        )
    except (KafkaError, asyncio.TimeoutError) as e:
        logger.warning(
            f"Kafka unavailable, rejecting '{event_type}' event "
            f"({type(e).__name__}) | trace_id: {trace_id}"
        )
        return {"message": "Message broker unavailable"}, 503
    logger.debug(f"Produced '{event_type}' event to topic '{KAFKA_TOPIC}' | trace_id: {trace_id}")
    return NoContent, 201


async def report_speed(body):
    trace_id = time.time_ns()
    logger.info(f"Received 'speed' event - speed_mph: {body['speed_mph']} | trace_id: {trace_id}")
    return await send_to_kafka("speed", body, trace_id)


async def report_violation(body):
    trace_id = time.time_ns()
    logger.info(
        f"Received 'violation' event - speed_overlimit_mph: {body['speed_overlimit_mph']} | trace_id: {trace_id}"
    )
    return await send_to_kafka("violation", body, trace_id)


def get_status():
    return {"status": "ok"}, 200


app = connexion.AsyncApp(__name__, specification_dir="", lifespan=lifespan)
app.add_api("speeding.yaml", strict_validation=True, validate_responses=True)