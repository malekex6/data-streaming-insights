"""Small helpers to move between Beam timestamps, datetimes and BigQuery strings."""

from datetime import datetime, timedelta, timezone

from apache_beam.utils.timestamp import Timestamp

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_rfc3339(dt: datetime) -> str:
    """2026-11-12T10:03:00.123456Z — accepted by BigQuery TIMESTAMP columns."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def parse_rfc3339(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def beam_ts_to_datetime(ts: Timestamp) -> datetime:
    return _EPOCH + timedelta(microseconds=ts.micros)


def beam_ts_to_rfc3339(ts: Timestamp) -> str:
    return to_rfc3339(beam_ts_to_datetime(ts))
