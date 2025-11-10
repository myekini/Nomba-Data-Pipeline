"""
Aiven Postgres to Analytics Schema CDC
--------------------------------------
Reads changes from public tables and upserts them into analytics schema
using updated_at and deleted_at as CDC signals.
"""

import os
import logging
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - postgres_cdc - %(levelname)s - %(message)s"
)
logger = logging.getLogger("postgres_cdc")


def get_connection():
    """Connect to Aiven Postgres."""
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "require"),
    )


TABLES_CONFIG = {
    "savings_plan": {
        "source_table": "public.savings_plan",
        "target_table": "analytics.raw_savings_plan",
        "primary_key": "plan_id",
        "metadata_name": "postgres_savings_plan",
    },
    "savingstransaction": {
        "source_table": "public.savingstransaction",
        "target_table": "analytics.raw_savingstransaction",
        "primary_key": "txn_id",
        "metadata_name": "postgres_savingstransaction",
    },
}


def get_last_extraction_ts(conn, metadata_name: str) -> datetime:
    """Get last_extracted_timestamp from analytics.cdc_metadata."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_extracted_timestamp
            FROM analytics.cdc_metadata
            WHERE source_name = %s
            """,
            (metadata_name,),
        )
        row = cur.fetchone()

    if row and row[0]:
        logger.info("Last extraction for %s: %s", metadata_name, row[0])
        return row[0]

    logger.info("No previous extraction for %s. Starting from epoch.", metadata_name)
    return datetime(1970, 1, 1)


def extract_changes(conn, source_table: str, last_ts: datetime):
    """Extract rows changed since last_ts based on updated_at or deleted_at."""
    query = f"""
        SELECT *
        FROM {source_table}
        WHERE
            (updated_at IS NOT NULL AND updated_at > %s)
            OR
            (deleted_at IS NOT NULL AND deleted_at > %s)
        ORDER BY COALESCE(updated_at, deleted_at)
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (last_ts, last_ts))
        rows = cur.fetchall()

    logger.info("Extracted %d changed rows from %s", len(rows), source_table)
    return rows


def upsert_into_raw(conn, target_table: str, primary_key: str, records: list):
    """Upsert changed records into analytics raw tables."""
    if not records:
        return 0

    columns = list(records[0].keys())
    columns_str = ", ".join(columns + ["extracted_at"])
    pk = primary_key

    update_cols = [c for c in columns if c != pk]
    update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols] + ["extracted_at = EXCLUDED.extracted_at"])

    values = []
    now = datetime.utcnow()
    for r in records:
        row_values = [r.get(c) for c in columns]
        row_values.append(now)
        values.append(tuple(row_values))

    insert_sql = f"""
        INSERT INTO {target_table} ({columns_str})
        VALUES %s
        ON CONFLICT ({pk})
        DO UPDATE SET {update_str}
    """
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, values)

    logger.info("Upserted %d rows into %s", len(records), target_table)
    return len(records)


def update_metadata(conn, metadata_name: str, record_count: int):
    """Update analytics.cdc_metadata with job stats."""
    with conn.cursor() as cur:
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
                record_count,
                datetime.utcnow(),
                metadata_name,
            ),
        )


def run_postgres_cdc():
    """Run Postgres CDC for configured tables."""
    start = datetime.now()
    logger.info("Starting Postgres CDC job")

    conn = get_connection()
    conn.autocommit = False

    try:
        total_processed = 0
        for name, cfg in TABLES_CONFIG.items():
            logger.info("Processing %s", name)

            last_ts = get_last_extraction_ts(conn, cfg["metadata_name"])
            changed = extract_changes(conn, cfg["source_table"], last_ts)
            count = upsert_into_raw(conn, cfg["target_table"], cfg["primary_key"], changed)
            update_metadata(conn, cfg["metadata_name"], count)
            total_processed += count

        conn.commit()
        elapsed = (datetime.now() - start).total_seconds()
        logger.info(
            "Postgres CDC complete. Total rows processed: %d in %.2fs",
            total_processed,
            elapsed,
        )
    except Exception as exc:
        conn.rollback()
        logger.error("Postgres CDC failed: %s", exc, exc_info=True)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run_postgres_cdc()
