from datetime import datetime, timezone
from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy import Integer, String, Float, BigInteger
from sqlalchemy.dialects.mysql import DATETIME


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def iso_utc(dt):
    return dt.isoformat(timespec="microseconds") + "Z"


class Base(DeclarativeBase):
    pass


class Speeding(Base):
    __tablename__ = "speeding"
    id = mapped_column(Integer, primary_key=True)
    device_id = mapped_column(String(36))
    vehicle_plate = mapped_column(String(6))
    speed_mph = mapped_column(Float)
    location = mapped_column(String(100))
    timestamp = mapped_column(DATETIME(fsp=6))
    recorded_timestamp = mapped_column(DATETIME(fsp=6), default=utc_now)
    trace_id = mapped_column(BigInteger, nullable=False, unique=True)

    def to_json(self):
        return {
            "device_id": self.device_id,
            "vehicle_plate": self.vehicle_plate,
            "speed_mph": self.speed_mph,
            "location": self.location,
            "timestamp": iso_utc(self.timestamp),
            "recorded_timestamp": iso_utc(self.recorded_timestamp),
            "trace_id": self.trace_id,
        }


class Violation(Base):
    __tablename__ = "violation"
    id = mapped_column(Integer, primary_key=True)
    station_id = mapped_column(String(36))
    vehicle_plate = mapped_column(String(6))
    speed_overlimit_mph = mapped_column(Float)
    location = mapped_column(String(100))
    timestamp = mapped_column(DATETIME(fsp=6))
    recorded_timestamp = mapped_column(DATETIME(fsp=6), default=utc_now)
    trace_id = mapped_column(BigInteger, nullable=False, unique=True)

    def to_json(self):
        return {
            "station_id": self.station_id,
            "vehicle_plate": self.vehicle_plate,
            "speed_overlimit_mph": self.speed_overlimit_mph,
            "location": self.location,
            "timestamp": iso_utc(self.timestamp),
            "recorded_timestamp": iso_utc(self.recorded_timestamp),
            "trace_id": self.trace_id,
        }