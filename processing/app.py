import os
import json
import time
import asyncio
import logging
import logging.config
from pathlib import Path
from datetime import datetime, timezone
from contextlib import asynccontextmanager
import yaml
import httpx
import connexion

with open("log_conf.yaml", "r") as f:
    logging.config.dictConfig(yaml.safe_load(f.read()))
logger = logging.getLogger("processing")

STORAGE_URL = os.getenv("STORAGE_URL", "http://localhost:12345")
STATS_FILE = Path(os.getenv("STATS_FILE", "stats.json"))
INTERVAL = int(os.getenv("PROCESSING_INTERVAL", "5"))

EPOCH = "1970-01-01T00:00:00.000000Z"
DEFAULT_STATS = {
    "num_speed_readings": 0,
    "max_speed_mph": 0.0,
    "avg_speed_mph": 0.0,
    "num_violations": 0,
    "max_speed_overlimit_mph": 0.0,
    "last_speed_recorded": EPOCH,
    "last_violation_recorded": EPOCH,
    "last_updated": EPOCH,
}

storage_available = True


def utc_now_iso():
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="microseconds") + "Z"


def load_stats():
    if STATS_FILE.exists():
        with STATS_FILE.open() as f:
            return json.load(f)
    return dict(DEFAULT_STATS)


def save_stats(stats):
    STATS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATS_FILE.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(stats, f, indent=2)
    tmp.replace(STATS_FILE)


def describe_error(error):
    if isinstance(error, httpx.HTTPStatusError):
        return f"HTTP {error.response.status_code}"
    return type(error).__name__


async def fetch_events(client, path, after):
    r = await client.get(f"{STORAGE_URL}{path}", params={"after": after})
    r.raise_for_status()
    return r.json()


async def update_stats(client):
    global storage_available
    start = time.perf_counter()
    logger.info("Processing round started")

    stats = await asyncio.to_thread(load_stats)

    try:
        speeds = await fetch_events(client, "/vehicle/speed", stats["last_speed_recorded"])
        violations = await fetch_events(client, "/vehicle/violation", stats["last_violation_recorded"])
    except (httpx.HTTPError, ValueError) as e:
        if storage_available:
            logger.warning(
                f"Storage at {STORAGE_URL} unavailable ({describe_error(e)}), "
                f"skipping processing rounds until it recovers; statistics are kept"
            )
            storage_available = False
        else:
            logger.debug(f"Storage still unavailable ({describe_error(e)}), skipping round")
        return

    if not storage_available:
        logger.info("Storage is available again, resuming statistics")
        storage_available = True

    logger.info(f"Received {len(speeds)} speed readings and {len(violations)} violations from storage")

    if speeds:
        values = [s["speed_mph"] for s in speeds]
        old_n = stats["num_speed_readings"]
        new_n = old_n + len(values)
        stats["avg_speed_mph"] = (stats["avg_speed_mph"] * old_n + sum(values)) / new_n
        stats["num_speed_readings"] = new_n
        stats["max_speed_mph"] = max(stats["max_speed_mph"], max(values))
        stats["last_speed_recorded"] = max(s["recorded_timestamp"] for s in speeds)

    if violations:
        values = [v["speed_overlimit_mph"] for v in violations]
        stats["num_violations"] += len(values)
        stats["max_speed_overlimit_mph"] = max(stats["max_speed_overlimit_mph"], max(values))
        stats["last_violation_recorded"] = max(v["recorded_timestamp"] for v in violations)

    stats["last_updated"] = utc_now_iso()
    await asyncio.to_thread(save_stats, stats)

    logger.info(f"Updated statistics: {stats}")
    logger.info(f"Processing round finished in {(time.perf_counter() - start) * 1000:.1f} ms")


async def update_loop():
    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            try:
                await update_stats(client)
            except Exception:
                logger.exception("Processing round failed")
            await asyncio.sleep(INTERVAL)


@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(update_loop())
    logger.info(f"Background processing started (every {INTERVAL}s, storage at {STORAGE_URL}, stats in {STATS_FILE})")
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        logger.info("Background processing stopped")


def get_stats():
    stats = load_stats()
    logger.debug(f"Serving statistics: {stats}")
    return stats, 200


def get_status():
    return {"status": "ok"}, 200


app = connexion.AsyncApp(__name__, specification_dir=".", lifespan=lifespan)
app.add_api("stats.yaml", strict_validation=True, validate_responses=True)