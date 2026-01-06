import os
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


# -------------------------
# Config (via env vars)
# -------------------------
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "bigdata-dev-token")
INFLUX_ORG = os.getenv("INFLUX_ORG", "bigdata")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "bigdata_bucket")


def _clean_usid(x) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def _parse_iso(dt_str: str) -> datetime:
    """
    Parse ISO timestamps (with timezone) and return UTC.
    Fallback: now(UTC) if missing or invalid.
    """
    if not dt_str:
        return datetime.now(timezone.utc)
    try:
        # handle "Z" suffix
        dt_str = dt_str.replace("Z", "+00:00")
        return datetime.fromisoformat(dt_str).astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def get_client() -> InfluxDBClient:
    return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)


def ping_influx() -> bool:
    """Simple connectivity check."""
    with get_client() as client:
        return bool(client.ping())




# -------------------------
# ORF write
# -------------------------
def write_orf_articles(items: list[dict]) -> int:
    """
    Measurement: orf_article
    Tags: category, usid
    Fields: title, link
    Time: ORF dc:date (stored in UTC)
    """
    points = []

    for it in items:
        usid = _clean_usid(it.get("usid"))
        if not usid:
            continue

        dt = _parse_iso(it.get("date") or "")
        category = str(it.get("oewaCategory") or "unknown")

        p = (
            Point("orf_article")
            .tag("category", category)
            .tag("usid", usid)
            .field("title", str(it.get("title") or ""))
            .field("link", str(it.get("link") or ""))
            .time(dt)
        )
        points.append(p)

    if not points:
        return 0

    with get_client() as client:
        client.write_api(write_options=SYNCHRONOUS).write(
            bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points
        )
    return len(points)

# -------------------------
# Reddit write
# -------------------------
def write_reddit_matches(rows: list[dict]) -> int:
    """
    Measurement: reddit_post
    Tags: usid, source
    Fields: reddit_id, title, permalink, url, checked_word_count, group_matches_in_window
    Time: saved_at_utc (fallback: now UTC)
    """
    points = []
    now = datetime.now(timezone.utc)

    for r in rows:
        usid = _clean_usid(r.get("article_usid"))
        reddit_id = _clean_usid(r.get("reddit_id"))
        if not usid or not reddit_id:
            continue

        dt_raw = r.get("saved_at_utc") or ""
        dt = _parse_iso(dt_raw) if dt_raw else now

        p = (
            Point("reddit_post")
            .tag("usid", usid)
            .tag("source", str(r.get("source") or ""))
            .field("reddit_id", reddit_id)
            .field("title", str(r.get("reddit_title") or ""))
            .field("permalink", str(r.get("reddit_permalink") or ""))
            .field("url", str(r.get("post_url") or ""))
            .field("checked_word_count", int(r.get("checked_word_count") or 0))
            .field("group_matches_in_window", int(r.get("group_matches_in_window") or 0))
            .time(dt)
        )
        points.append(p)

    if not points:
        return 0

    with get_client() as client:
        client.write_api(write_options=SYNCHRONOUS).write(
            bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points
        )
    return len(points)