import os
from datetime import datetime, timezone
from typing import Iterable
import pandas as pd
# InfluxDB Python Client
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


# -------------------------
# Configuration (via env vars)
# -------------------------
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "bigdata-dev-token")
INFLUX_ORG = os.getenv("INFLUX_ORG", "bigdata")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "bigdata_bucket")


def _clean_usid(x) -> str | None:
    """
    Normalisiert eine USID:
    - None bleibt None
    - Whitespace wird entfernt
    - leere Strings werden verworfen
    """
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def _parse_iso(dt_str: str) -> datetime:
    """
    Parst ISO-8601 Zeitstempel und gibt UTC zurück.
    - unterstützt 'Z' Suffix
    - Fallback auf aktuelle Zeit bei Fehlern
    """
    if not dt_str:
        return datetime.now(timezone.utc)
    try:
        dt_str = str(dt_str).replace("Z", "+00:00")
        return datetime.fromisoformat(dt_str).astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def _parse_unix_utc_seconds(x) -> datetime | None:
    """
    Parse unix timestamp (seconds) -> UTC datetime.
    Returns None if missing/invalid.
    Uses int seconds to avoid float jitter causing duplicate points.
    """
    if x is None or x == "":
        return None
    try:
        return datetime.fromtimestamp(int(float(x)), tz=timezone.utc)
    except Exception:
        return None


def get_client() -> InfluxDBClient:
    """Erstellt einen neuen InfluxDB client."""
    return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)


def ping_influx() -> bool:
    """Simple connectivity check."""
    with get_client() as client:
        return bool(client.ping())


def _clip(s: str, n: int = 8000) -> str:
    """
    Kürzt lange Texte (z.B. Reddit Selftext).
    """
    s = s or ""
    return s if len(s) <= n else s[:n] + "…"


# -------------------------
# Generic helpers
# -------------------------
def write_points(points: list[Point]) -> int:
    """Write a list of points synchronously. Returns number written."""
    if not points:
        return 0
    with get_client() as client:
        client.write_api(write_options=SYNCHRONOUS).write(
            bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points
        )
    return len(points)


# -------------------------
# ORF write
# -------------------------
def write_orf_articles(items: list[dict]) -> int:
    """
    Schreibt ORF Artikel als Time-Series-Daten.

    Measurement: orf_article
    Tags: category, usid
    Fields: title, link
    Time: ORF dc:date (stored in UTC)
    """
    points: list[Point] = []

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

    return write_points(points)


# -------------------------
# ORF read (added)
# -------------------------
def load_orf_articles_from_influx(lookback: str = "1h") -> list[dict]:
    """
    Pull ORF articles (measurement 'orf_article') within lookback.
    Returns deduped list by usid with fields: time, usid, title, link, date.
    """
    with get_client() as client:
        query_api = client.query_api()
        flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{lookback})
  |> filter(fn: (r) => r._measurement == "orf_article")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "usid", "title", "link", "date"])
"""
        tables = query_api.query(flux, org=INFLUX_ORG)

    rows: list[dict] = []
    for t in tables:
        for rec in t.records:
            d = rec.values
            rows.append(
                {
                    "time": d.get("_time"),
                    "usid": d.get("usid"),
                    "title": d.get("title"),
                    "link": d.get("link"),
                    "date": d.get("date"),
                }
            )

    dedup: dict[str, dict] = {}
    for r in rows:
        u = _clean_usid(r.get("usid"))
        if u and u not in dedup:
            dedup[u] = r
    return list(dedup.values())


# -------------------------
# Reddit write (existing)
# -------------------------
def write_reddit_matches(rows: list[dict]) -> int:
    """
    Speichert Reddit-Posts, die zu Artikeln passen.

    Measurement: reddit_post
    Tags: usid, source
    Fields: reddit_id, title, permalink, url, checked_word_count, group_matches_in_window
    Time: saved_at_utc (fallback: now UTC)
    """
    points: list[Point] = []
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
            .field("selftext", _clip(str(r.get("reddit_selftext") or ""), 8000))
            .field("stance_label", str(r.get("stance_label") or ""))
            .field("stance_conf", float(r.get("stance_conf") or 0.0))
            .time(dt)
        )
        points.append(p)

    return write_points(points)


def write_reddit_stance_updates(rows: list[dict]) -> int:
    """
    Aktualisiert NUR die Stance-Felder bestehender Reddit-Posts.

    WICHTIG:
    - gleiche Tags (usid, source)
    - gleicher Timestamp (_time)
    sonst erzeugt InfluxDB einen neuen Datenpunkt
    """
    points: list[Point] = []

    for r in rows:
        usid = _clean_usid(r.get("article_usid") or r.get("usid"))
        if not usid:
            continue

        dt_raw = r.get("saved_at_utc") or r.get("_time") or ""
        dt = _parse_iso(str(dt_raw))

        source = str(r.get("source") or "")

        p = (
            Point("reddit_post")
            .tag("usid", usid)
            .tag("source", source)
            .field("stance_label", str(r.get("stance_label") or ""))
            .field("stance_conf", float(r.get("stance_conf") or 0.0))
            .time(dt)
        )
        points.append(p)

    return write_points(points)


# -------------------------
# Reddit dedup read (added)
# -------------------------
def load_existing_reddit_ids_for_usid(usid: str, lookback: str = "365d") -> set[str]:
    """
    Fetch already-written reddit_id values for a given usid to avoid duplicates.
    Note: reddit_id is stored as a FIELD in your schema, so we filter _field == "reddit_id".
    """
    usid = _clean_usid(usid)
    if not usid:
        return set()

    with get_client() as client:
        query_api = client.query_api()
        flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{lookback})
  |> filter(fn: (r) => r._measurement == "reddit_post")
  |> filter(fn: (r) => r.usid == "{usid}")
  |> filter(fn: (r) => r._field == "reddit_id")
  |> keep(columns: ["_value"])
  |> distinct(column: "_value")
"""
        tables = query_api.query(flux, org=INFLUX_ORG)

    out: set[str] = set()
    for t in tables:
        for rec in t.records:
            v = rec.values.get("_value")
            if v is not None:
                out.add(str(v))
    return out


# -------------------------
# Reddit write for "matcher rows" (added)
# -------------------------
def reddit_rows_to_points(rows: Iterable[dict]) -> list[Point]:
    """
    Converts rows from your Reddit search/matcher cell into Points.

    Expected row keys (from your matcher):
      usid, source, reddit_id, reddit_title, reddit_permalink, post_url,
      reddit_selftext, checked_word_count, group_matches_in_window, created_utc

    Timestamp:
      uses created_utc (unix seconds) deterministically (int seconds).
      rows without created_utc are skipped (otherwise you create new points each run).
    """
    points: list[Point] = []

    for r in rows:
        usid = _clean_usid(r.get("usid"))
        reddit_id = _clean_usid(r.get("reddit_id"))
        if not usid or not reddit_id:
            continue

        dt = _parse_unix_utc_seconds(r.get("created_utc"))
        if dt is None:
            # skip to keep writes idempotent; otherwise now() creates duplicates on reruns
            continue

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
            .field("selftext", _clip(str(r.get("reddit_selftext") or ""), 8000))
            .time(dt)
        )
        points.append(p)

    return points


def write_reddit_posts(rows: list[dict]) -> int:
    """
    Writes reddit posts for the matcher cell row format (see reddit_rows_to_points()).
    Dedup should be done outside (e.g., using load_existing_reddit_ids_for_usid).
    """
    points = reddit_rows_to_points(rows)
    return write_points(points)

def load_unlabeled_reddit_posts_from_influx(lookback: str = "30d", limit: int = 500) -> pd.DataFrame:
    """
    Pull reddit_post points where stance_label is missing or empty.
    Returns a dataframe with: _time, usid, source, title, selftext, stance_label
    """
    with get_client() as client:
        query_api = client.query_api()
        flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{lookback})
  |> filter(fn: (r) => r._measurement == "reddit_post")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time","usid","source","title","selftext","stance_label"])
  |> filter(fn: (r) => (not exists r.stance_label) or r.stance_label == "")
  |> limit(n: {int(limit)})
"""
        tables = query_api.query(flux, org=INFLUX_ORG)

    rows = []
    for t in tables:
        for rec in t.records:
            v = rec.values
            rows.append({
                "_time": v.get("_time"),
                "usid": v.get("usid"),
                "source": v.get("source"),
                "title": v.get("title") or "",
                "selftext": v.get("selftext") or "",
                "stance_label": v.get("stance_label") or "",
            })

    return pd.DataFrame(rows)

# -------------------------
# Analytics reads (for notebooks / evaluation)
# -------------------------
def query_df(flux: str) -> pd.DataFrame:
    """Run a Flux query and return a single concatenated DataFrame."""
    with get_client() as client:
        df = client.query_api().query_data_frame(flux, org=INFLUX_ORG)
    if isinstance(df, list):
        df = pd.concat(df, ignore_index=True) if df else pd.DataFrame()
    return df


def load_orf_titles_df(lookback: str = "30d", limit: int = 5000) -> pd.DataFrame:
    """
    Loads ORF articles (measurement 'orf_article').

    Returns df with columns:
      usid, orf_time, orf_title, category, link
    """
    flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{lookback})
  |> filter(fn: (r) => r._measurement == "orf_article")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time","usid","title","category","link"])
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: {int(limit)})
"""
    df = query_df(flux)
    if df.empty:
        return df

    df = df.rename(columns={"_time": "orf_time", "title": "orf_title"})
    df["usid"] = df["usid"].astype(str).str.strip()
    df["orf_title"] = df["orf_title"].fillna("").astype(str)

    # Some older points may have missing category/link; keep columns stable
    for col in ["category", "link"]:
        if col not in df.columns:
            df[col] = ""

    return df[["usid", "orf_time", "orf_title", "category", "link"]].drop_duplicates(subset=["usid"])


def load_reddit_posts_df(lookback: str = "30d", limit: int = 20000) -> pd.DataFrame:
    """
    Loads reddit posts (measurement 'reddit_post').

    Returns df with columns:
      usid, reddit_time, reddit_id, reddit_title, selftext, source, stance_label, stance_conf
    """
    flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{lookback})
  |> filter(fn: (r) => r._measurement == "reddit_post")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time","usid","source","reddit_id","title","selftext","stance_label","stance_conf"])
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: {int(limit)})
"""
    df = query_df(flux)
    if df.empty:
        return df

    df = df.rename(columns={"_time": "reddit_time", "title": "reddit_title"})
    df["usid"] = df["usid"].astype(str).str.strip()
    df["reddit_id"] = df.get("reddit_id", "").fillna("").astype(str)
    df["reddit_title"] = df.get("reddit_title", "").fillna("").astype(str)
    df["selftext"] = df.get("selftext", "").fillna("").astype(str)
    df["source"] = df.get("source", "").fillna("").astype(str)
    df["stance_label"] = df.get("stance_label", "").fillna("").astype(str)
    df["stance_conf"] = pd.to_numeric(df.get("stance_conf", 0.0), errors="coerce").fillna(0.0)

    # Drop rows without reddit_id (shouldn't happen, but avoids weird joins)
    df = df[df["reddit_id"].str.len() > 0].copy()
    return df


def load_reddit_posts_with_labels_df(lookback: str = "30d", limit: int = 10000) -> pd.DataFrame:
    """
    Loads reddit posts that already have a non-empty stance_label.

    Returns df with columns (at least):
      reddit_time, usid, source, reddit_id, reddit_title, selftext, stance_label, stance_conf
    """
    df = load_reddit_posts_df(lookback=lookback, limit=limit)
    if df.empty:
        return df
    # treat missing as empty and filter
    df["stance_label"] = df["stance_label"].fillna("").astype(str)
    return df[df["stance_label"].str.len() > 0].copy()
