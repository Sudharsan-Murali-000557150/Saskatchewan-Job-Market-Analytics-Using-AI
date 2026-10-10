"""MongoDB storage for the Saskatchewan job scraper.

Turned on by setting the MONGODB_URI environment variable. Works the same for a local
server (mongodb://localhost:27017) and MongoDB Atlas (mongodb+srv://...).

Collections
-----------
postings         One document per posting, _id = Record_ID. Upserted every run.
                 first_seen is set once; last_seen moves forward each run the posting is
                 still listed; status flips to "closed" when it disappears from its site.
raw_postings     Latest raw scrape of each posting (before cleaning), _id = Record_ID.
                 Lets the cleaning step be re-run without re-scraping.
posting_changes  One document whenever a tracked field (salary, closing date, ...) changes
                 on a posting that is still open.
runs             One summary document per pipeline run.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
from datetime import date, datetime, timezone

import pandas as pd
from pymongo import ASCENDING, MongoClient, UpdateOne

log = logging.getLogger(__name__)

DEFAULT_DB = "sk_jobs"

# Fields whose changes are logged to posting_changes.
TRACKED_FIELDS = [
    "Job_Title", "Salary_Range", "Salary_Min", "Salary_Max", "Closing_Date",
    "Employment_Type_Standardized", "Number_of_Openings", "City",
]

# A source's closures are only recorded when this run listed at least this share of the
# postings the database currently has open for it. Guards against a partial crawl
# (site hiccup, pagination cut short) wrongly closing hundreds of live postings.
MIN_LISTED_SHARE = 0.5
MIN_OPEN_FOR_GUARD = 10


# ---- helpers ---------------------------------------------------------------------
def _plain(value):
    """Convert pandas/numpy values into types MongoDB can store."""
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, dict):
        return {_safe_key(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        value = value.item()  # numpy scalar -> Python scalar
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _safe_key(key) -> str:
    key = str(key).replace(".", "_")
    return "_" + key[1:] if key.startswith("$") else key


def _rows(df: pd.DataFrame) -> list[dict]:
    return [{_safe_key(k): _plain(v) for k, v in row.items()} for row in df.to_dict("records")]


def _hash(doc: dict) -> str:
    return hashlib.sha1(json.dumps(doc, sort_keys=True, default=str).encode()).hexdigest()


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---- store -------------------------------------------------------------------------
class MongoStore:
    def __init__(self, uri: str, db_name: str = DEFAULT_DB):
        self.client = MongoClient(uri, serverSelectionTimeoutMS=20000, tz_aware=True)
        self.client.admin.command("ping")  # fail fast with a clear error if unreachable
        self.db = self.client[db_name]
        self.postings = self.db["postings"]
        self.raw = self.db["raw_postings"]
        self.changes = self.db["posting_changes"]
        self.runs = self.db["runs"]
        self._ensure_indexes()
        log.info("MongoDB: connected to database '%s'", db_name)

    @classmethod
    def from_env(cls, test_run: bool = False) -> "MongoStore | None":
        uri = os.environ.get("MONGODB_URI")
        if not uri:
            return None
        db_name = os.environ.get("MONGODB_DB", DEFAULT_DB)
        if test_run:
            db_name += "_test"  # test runs never touch the real data
        return cls(uri, db_name)

    def _ensure_indexes(self) -> None:
        self.postings.create_index([("status", ASCENDING), ("Source", ASCENDING)])
        self.postings.create_index([("last_seen", ASCENDING)])
        self.postings.create_index([("first_seen", ASCENDING)])
        self.changes.create_index([("record_id", ASCENDING), ("changed_on", ASCENDING)])
        self.runs.create_index([("started_at", ASCENDING)])

    # -- raw layer --
    def save_raw(self, source: str, records: list[tuple[str, dict]], today: str) -> int:
        ops = [
            UpdateOne(
                {"_id": rid},
                {"$set": {"source": source, "scraped_date": today, "data": _plain(rec)}},
                upsert=True,
            )
            for rid, rec in records
        ]
        if ops:
            self.raw.bulk_write(ops, ordered=False)
        return len(ops)

    # -- clean layer --
    def sync(self, open_df: pd.DataFrame, closed_df: pd.DataFrame, sources_ok: set[str],
             today: str, close_missing: bool = True) -> dict:
        """Upsert this run's postings and record closures. Returns counts for the run log."""
        stats = {"new": 0, "updated": 0, "unchanged": 0, "reopened": 0,
                 "closed_missing": 0, "closed_by_site": 0, "changes_logged": 0,
                 "closure_skipped_sources": []}
        now = _now()
        docs = _rows(open_df)
        closed_docs = _rows(closed_df)
        ids = [d["Record_ID"] for d in docs]
        closed_ids = [d["Record_ID"] for d in closed_docs]

        existing = {
            d["_id"]: d
            for d in self.postings.find(
                {"_id": {"$in": ids + closed_ids}},
                {"content_hash": 1, "status": 1, **{f: 1 for f in TRACKED_FIELDS}},
            )
        }

        ops, change_docs = [], []
        for doc in docs:
            rid = doc["Record_ID"]
            doc.pop("Collection_Date", None)  # replaced by first_seen / last_seen
            content_hash = _hash(doc)
            old = existing.get(rid)
            if old is None:
                stats["new"] += 1
            elif old.get("status") == "closed":
                stats["reopened"] += 1
            elif old.get("content_hash") != content_hash:
                stats["updated"] += 1
                diffs = {f: {"old": old.get(f), "new": doc.get(f)}
                         for f in TRACKED_FIELDS if old.get(f) != doc.get(f)}
                if diffs:
                    change_docs.append({"record_id": rid, "Source": doc.get("Source"),
                                        "changed_on": today, "changes": diffs})
            else:
                stats["unchanged"] += 1

            ops.append(UpdateOne(
                {"_id": rid},
                {
                    "$set": {**doc, "status": "open", "last_seen": today,
                             "content_hash": content_hash, "updated_at": now},
                    "$setOnInsert": {"first_seen": today, "created_at": now},
                    "$unset": {"closed_date": "", "closed_reason": ""},
                },
                upsert=True,
            ))

        # Postings the site itself lists as closed (e.g. SaskWater's archive).
        # Ones already recorded as closed are left alone so their closed_date stays put.
        for doc in closed_docs:
            rid = doc["Record_ID"]
            if existing.get(rid, {}).get("status") == "closed":
                continue
            doc.pop("Collection_Date", None)
            ops.append(UpdateOne(
                {"_id": rid},
                {
                    "$set": {**doc, "status": "closed", "last_seen": today,
                             "closed_date": today, "closed_reason": "Listed as closed by site",
                             "content_hash": _hash(doc), "updated_at": now},
                    "$setOnInsert": {"first_seen": today, "created_at": now},
                },
                upsert=True,
            ))
            stats["closed_by_site"] += 1

        if ops:
            self.postings.bulk_write(ops, ordered=False)
        if change_docs:
            self.changes.insert_many(change_docs, ordered=False)
            stats["changes_logged"] = len(change_docs)

        if close_missing:
            listed = set(ids) | set(closed_ids)
            for source in sorted(sources_ok):
                stats["closed_missing"] += self._close_missing(source, listed, open_df, today,
                                                               now, stats)
        return stats

    def _close_missing(self, source: str, listed: set[str], open_df: pd.DataFrame,
                       today: str, now: datetime, stats: dict) -> int:
        open_in_db = self.postings.count_documents({"Source": source, "status": "open"})
        listed_now = int((open_df["Source"] == source).sum()) if not open_df.empty else 0
        if open_in_db >= MIN_OPEN_FOR_GUARD and listed_now < MIN_LISTED_SHARE * open_in_db:
            log.warning("MongoDB: %s listed %d postings vs %d open in the database; "
                        "looks like a partial crawl, so no closures recorded for it this run",
                        source, listed_now, open_in_db)
            stats["closure_skipped_sources"].append(source)
            return 0
        result = self.postings.update_many(
            {"Source": source, "status": "open", "_id": {"$nin": list(listed)}},
            {"$set": {"status": "closed", "closed_date": today,
                      "closed_reason": "No longer listed on site", "updated_at": now}},
        )
        return result.modified_count

    def log_run(self, summary: dict) -> None:
        self.runs.insert_one({k: _plain(v) for k, v in summary.items()})

    def close(self) -> None:
        self.client.close()


def today_iso() -> str:
    return date.today().isoformat()
