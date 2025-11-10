"""
MongoDB to Aiven Postgres CDC (Users)
-------------------------------------
Syncs users from MongoDB Atlas to analytics.raw_users on Aiven Postgres
using hash-based change detection.
"""

import os
import json
import hashlib
import logging
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - mongodb_cdc - %(levelname)s - %(message)s"
)
logger = logging.getLogger("mongodb_cdc")


def get_mongo_client():
    """Return a MongoDB client using environment variables."""
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise ValueError("MONGO_URI not set")
    return MongoClient(uri)


def get_pg_connection():
    """Connect to Aiven Postgres."""
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "require"),
    )


def calculate_record_hash(document: dict) -> str:
    """Return MD5 hash of document excluding _id."""
    doc_copy = {k: v for k, v in document.items() if k != "_id"}
    doc_string = json.dumps(doc_copy, sort_keys=True, default=str)
    return hashlib.md5(doc_string.encode()).hexdigest()


def fetch_existing_hashes(conn) -> dict:
    """Return {uid: record_hash} from analytics.raw_users."""
    with conn.cursor() as cur:
        cur.execute("SELECT uid, record_hash FROM analytics.raw_users")
        rows = cur.fetchall()
    return {row[0]: row[1] for row in rows}


def fetch_mongo_users():
    """Fetch all users from MongoDB."""
    client = get_mongo_client()
    db = client[os.getenv("MONGO_DB", "nomba_users")]
    coll = db[os.getenv("MONGO_COLLECTION", "nomba")]
    users = list(coll.find())
    client.close()
    logger.info("Fetched %d users from MongoDB", len(users))
    return users


def sync_users():
    """Insert new, update changed, skip unchanged, and update metadata."""
    start_time = datetime.now()
    logger.info("Starting MongoDB to Postgres CDC")

    users = fetch_mongo_users()
    if not users:
        logger.info("No users found in MongoDB.")
        return

    conn = get_pg_connection()
    conn.autocommit = False

    try:
        existing_hashes = fetch_existing_hashes(conn)
        inserts, updates, unchanged = [], [], 0

        for doc in users:
            uid = doc.get("Uid")
            if not uid:
                continue
            current_hash = calculate_record_hash(doc)
            first_name = doc.get("firstName")
            last_name = doc.get("lastName")
            occupation = doc.get("occupation")
            state = doc.get("state")

            if uid not in existing_hashes:
                inserts.append(
                    (uid, first_name, last_name, occupation, state,
                     current_hash, datetime.utcnow(), datetime.utcnow())
                )
            elif existing_hashes[uid] != current_hash:
                updates.append(
                    (uid, first_name, last_name, occupation, state,
                     current_hash, datetime.utcnow(), datetime.utcnow())
                )
            else:
                unchanged += 1

        with conn.cursor() as cur:
            if inserts or updates:
                all_records = inserts + updates
                logger.info("Upserting %d users into analytics.raw_users", len(all_records))
                execute_values(
                    cur,
                    """
                    INSERT INTO analytics.raw_users (
                        uid, first_name, last_name, occupation, state,
                        record_hash, extracted_at, updated_at
                    )
                    VALUES %s
                    ON CONFLICT (uid)
                    DO UPDATE SET
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        occupation = EXCLUDED.occupation,
                        state = EXCLUDED.state,
                        record_hash = EXCLUDED.record_hash,
                        updated_at = EXCLUDED.updated_at;
                    """,
                    all_records,
                )

            cur.execute(
                """
                UPDATE analytics.cdc_metadata
                SET last_extracted_timestamp = %s,
                    last_extraction_status = %s,
                    records_extracted = %s,
                    updated_at = %s
                WHERE source_name = %s
                """,
                (
                    datetime.utcnow(),
                    "success",
                    len(users),
                    datetime.utcnow(),
                    "mongodb_users",
                ),
            )

        conn.commit()
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(
            "CDC complete: %d inserted/updated, %d unchanged in %.2fs",
            len(inserts) + len(updates),
            unchanged,
            elapsed,
        )
    except Exception as exc:
        conn.rollback()
        logger.error("MongoDB CDC failed: %s", exc, exc_info=True)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sync_users()
