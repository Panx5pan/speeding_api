import functools
import logging
import logging.config
import threading
import time
from datetime import datetime, timezone
import connexion
import yaml
from sqlalchemy import select, text
from sqlalchemy.exc import InterfaceError, OperationalError, SQLAlchemyError
from engine import make_session, ENGINE
from models import Speeding, Violation, Base

with open("log_conf.yaml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("storage")


def ensure_schema():
    delay = 1
    while True:
        try:
            with ENGINE.connect() as conn:
                conn.execute(text("SELECT 1"))
            Base.metadata.create_all(ENGINE)
            logger.info("Database is reachable and schema is ready")
            return
        except (OperationalError, InterfaceError):
            logger.warning(f"Database not reachable, retrying in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, 30)


threading.Thread(target=ensure_schema, daemon=True).start()


def use_db_session(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        session = make_session()
        try:
            return func(session, *args, **kwargs)
        except SQLAlchemyError as e:
            logger.warning(
                f"Database unavailable, returning 503 for {func.__name__} ({type(e).__name__})"
            )
            return {"message": "Database unavailable"}, 503
        finally:
            try:
                session.close()
            except SQLAlchemyError:
                pass

    return wrapper


def to_utc_naive(value):
    dt = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


@use_db_session
def get_speed_readings(session, after=None):
    query = select(Speeding)
    if after:
        query = query.where(Speeding.recorded_timestamp > to_utc_naive(after))
    rows = session.scalars(query).all()
    logger.debug(f"Found {len(rows)} speed readings after {after}")
    return [r.to_json() for r in rows], 200


@use_db_session
def get_violation_readings(session, after=None):
    query = select(Violation)
    if after:
        query = query.where(Violation.recorded_timestamp > to_utc_naive(after))
    rows = session.scalars(query).all()
    logger.debug(f"Found {len(rows)} violations after {after}")
    return [r.to_json() for r in rows], 200


def get_status():
    return {"status": "ok"}, 200


app = connexion.AsyncApp(__name__, specification_dir=".")
app.add_api("speeding.yaml", strict_validation=True)