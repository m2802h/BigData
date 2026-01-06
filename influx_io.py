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


def _parse_iso(dt_str: str) -> datetime:
    """
    ORF uses ISO timestamps like: 2026-01-06T13:40:58+01:00
    We store everything in UTC.
    """
    try:
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
    Tags: category
    Fields: usid, title, link
    Time: ORF dc:date

    InfluxDB identifies a point by (measurement + tag set + timestamp).
    If you write the same point again, it merges/overwrites fields (intended). :contentReference[oaicite:1]{index=1}
    """
    points = []
    for it in items:
        usid = it.get("usid")
        if not usid:
            continue

        dt = _parse_iso(it.get("date") or "")
        category = str(it.get("oewaCategory") or "unknown")

        p = (
            Point("orf_article")
            .tag("category", category)
            .field("usid", str(usid))
            .field("title", str(it.get("title") or ""))
            .field("link", str(it.get("link") or ""))
            .time(dt)
        )
        points.append(p)

    if not points:
        return 0

    with get_client() as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)

    return len(points)


# -------------------------
# Reddit write
# -------------------------
def write_reddit_matches(rows: list[dict]) -> int:
    """
    Measurement: reddit_post
    Tags: article_usid, source
    Fields: reddit_id, title, permalink, url, checked_word_count, group_matches_in_window
    Time: saved_at_utc (fallback: now)
    """
    points = []
    now = datetime.now(timezone.utc)

    for r in rows:
        article_usid = r.get("article_usid")
        reddit_id = r.get("reddit_id")
        if not article_usid or not reddit_id:
            continue

        dt_raw = (r.get("saved_at_utc") or "").replace("Z", "+00:00")
        dt = _parse_iso(dt_raw) if dt_raw else now

        p = (
            Point("reddit_post")
            .tag("article_usid", str(article_usid))
            .tag("source", str(r.get("source") or ""))
            .field("reddit_id", str(reddit_id))
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
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)

    return len(points)
