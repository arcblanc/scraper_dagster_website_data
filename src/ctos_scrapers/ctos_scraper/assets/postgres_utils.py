import os

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")

DSN = f"dbname={PG_DATABASE} user={PG_USER} password={PG_PASSWORD} host={PG_HOST} port={PG_PORT}"

ALLOWED_TABLE_NAMES = {
    "rob_url_master",
    "roc_url_master",
    "rob_tracking",
    "roc_tracking",
    "rob_registration",
    "roc_registration",
}


def validate_table_name(table_name: str) -> str:
    if table_name not in ALLOWED_TABLE_NAMES:
        msg = f"Invalid table name: {table_name}"
        raise ValueError(msg)
    return table_name


def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(DSN)


def ensure_url_master_table(table_name: str) -> None:
    table_name = validate_table_name(table_name)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                name TEXT,
                url TEXT PRIMARY KEY
            )
        """)
        conn.commit()


def ensure_tracking_table(table_name: str) -> None:
    table_name = validate_table_name(table_name)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                prefix TEXT,
                page INTEGER,
                status TEXT,
                url TEXT,
                retry_count INTEGER DEFAULT 0,
                last_retry TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (prefix, page, status)
            )
        """)
        conn.commit()


def ensure_registration_table(table_name: str) -> None:
    table_name = validate_table_name(table_name)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id TEXT,
                url TEXT,
                business_name TEXT,
                registration_number_old TEXT,
                registration_number TEXT PRIMARY KEY,
                business_type TEXT,
                business_commenced TEXT,
                state TEXT
            )
        """)
        # Add index on url for faster joins
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_url ON {table_name} (url)"
        )
        conn.commit()


def insert_urls(table_name: str, rows: list[dict]) -> None:
    table_name = validate_table_name(table_name)
    if not rows:
        return
    with get_conn() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {table_name} (name, url)
            VALUES %s
            ON CONFLICT (url) DO NOTHING
            """,  # noqa: S608
            [(row["name"], row["url"]) for row in rows],
        )
        conn.commit()


def fetch_all_urls(table_name: str) -> set:
    table_name = validate_table_name(table_name)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT url FROM {table_name}")  # noqa: S608
        return {row[0] for row in cur.fetchall()}


def insert_tracking(
    table_name: str, prefix: str, page: int, status: str, url: str = ""
) -> None:
    table_name = validate_table_name(table_name)
    with get_conn() as conn, conn.cursor() as cur:
        # Table name is validated and whitelisted, so this is safe
        cur.execute(
            f"""
            INSERT INTO {table_name} (prefix, page, status, url)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (prefix, page, status) DO NOTHING
            """,  # noqa: S608
            (prefix, page, status, url),
        )
        conn.commit()


def fetch_tracking_scraped(table_name: str) -> set:
    table_name = validate_table_name(table_name)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT prefix, page FROM {table_name} WHERE status='scraped'")  # noqa: S608
        return {(row[0], row[1]) for row in cur.fetchall()}


def fetch_tracking_errors(table_name: str) -> list:
    table_name = validate_table_name(table_name)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT prefix, page, url FROM {table_name} WHERE status='error'")  # noqa: S608
        return [(row[0], row[1], row[2]) for row in cur.fetchall()]


def fetch_failed_pages_with_retry_info(table_name: str) -> list:
    table_name = validate_table_name(table_name)
    with get_conn() as conn, conn.cursor() as cur:
        # Get all error entries that don't have corresponding 'scraped' entries
        cur.execute(f"""
            SELECT e.prefix, e.page, e.url
            FROM {table_name} e
            WHERE e.status = 'error'
            AND NOT EXISTS (
                SELECT 1 FROM {table_name} s
                WHERE s.prefix = e.prefix
                AND s.page = e.page
                AND s.status = 'scraped'
            )
        """)  # noqa: S608
        return [(row[0], row[1], row[2]) for row in cur.fetchall()]


def get_retry_statistics(table_name: str) -> dict:
    table_name = validate_table_name(table_name)
    with get_conn() as conn, conn.cursor() as cur:
        # Get total failed pages
        cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE status='error'")  # noqa: S608
        result = cur.fetchone()
        total_failed = result[0] if result else 0

        # Get total scraped pages
        cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE status='scraped'")  # noqa: S608
        result = cur.fetchone()
        total_scraped = result[0] if result else 0

        # Get failed pages that can be retried (no corresponding scraped entry)
        cur.execute(f"""
            SELECT COUNT(*)
            FROM {table_name} e
            WHERE e.status = 'error'
            AND NOT EXISTS (
                SELECT 1 FROM {table_name} s
                WHERE s.prefix = e.prefix
                AND s.page = e.page
                AND s.status = 'scraped'
            )
        """)  # noqa: S608
        result = cur.fetchone()
        retryable_failed = result[0] if result else 0

        return {
            "total_failed": total_failed,
            "total_scraped": total_scraped,
            "retryable_failed": retryable_failed,
            "remaining_retries": retryable_failed,
        }


def upsert_registration(table_name: str, rows: list[dict]) -> None:
    table_name = validate_table_name(table_name)
    if not rows:
        return
    with get_conn() as conn, conn.cursor() as cur:
        # Deduplication logic: only update if new row has registration_number_old and existing does not
        for row in rows:
            cur.execute(
                f"""
                INSERT INTO {table_name} (
                    id, url, business_name, registration_number_old,
                    registration_number, business_type, business_commenced, state
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (registration_number) DO UPDATE SET
                    id = EXCLUDED.id,
                    url = EXCLUDED.url,
                    business_name = EXCLUDED.business_name,
                    registration_number_old = CASE
                        WHEN {table_name}.registration_number_old IS NULL
                             OR {table_name}.registration_number_old = ''
                             OR {table_name}.registration_number_old = '-'
                        THEN EXCLUDED.registration_number_old
                        ELSE {table_name}.registration_number_old
                    END,
                    business_type = EXCLUDED.business_type,
                    business_commenced = EXCLUDED.business_commenced,
                    state = EXCLUDED.state
                WHERE (
                    EXCLUDED.registration_number_old IS NOT NULL
                    AND EXCLUDED.registration_number_old <> ''
                    AND EXCLUDED.registration_number_old <> '-'
                )
            """,
                (
                    row.get("id"),
                    row.get("url"),
                    row.get("business_name"),
                    row.get("registration_number_old"),
                    row.get("registration_number"),
                    row.get("business_type"),
                    row.get("business_commenced"),
                    row.get("state"),
                ),
            )
        conn.commit()


def upsert_registration_batch(table_name: str, rows: list[dict]) -> None:
    table_name = validate_table_name(table_name)
    if not rows:
        return

    # Prepare data tuples for execute_values
    data_tuples = [
        (
            row.get("id"),
            row.get("url"),
            row.get("business_name"),
            row.get("registration_number_old"),
            row.get("registration_number"),
            row.get("business_type"),
            row.get("business_commenced"),
            row.get("state"),
        )
        for row in rows
    ]

    with get_conn() as conn, conn.cursor() as cur:
        # Use execute_values for batch insert with conflict resolution
        execute_values(
            cur,
            f"""
            INSERT INTO {table_name} (
                id, url, business_name, registration_number_old,
                registration_number, business_type, business_commenced, state
            )
            VALUES %s
            ON CONFLICT (registration_number) DO UPDATE SET
                id = EXCLUDED.id,
                url = EXCLUDED.url,
                business_name = EXCLUDED.business_name,
                registration_number_old = CASE
                    WHEN {table_name}.registration_number_old IS NULL
                         OR {table_name}.registration_number_old = ''
                         OR {table_name}.registration_number_old = '-'
                    THEN EXCLUDED.registration_number_old
                    ELSE {table_name}.registration_number_old
                END,
                business_type = EXCLUDED.business_type,
                business_commenced = EXCLUDED.business_commenced,
                state = EXCLUDED.state
            WHERE (
                EXCLUDED.registration_number_old IS NOT NULL
                AND EXCLUDED.registration_number_old <> ''
                AND EXCLUDED.registration_number_old <> '-'
            )
            """,
            data_tuples,
            template=None,
            page_size=1000,  # Process in chunks of 1000 for optimal performance
        )
        conn.commit()


def fetch_unscraped_urls(url_table: str, reg_table: str) -> list[str]:
    url_table = validate_table_name(url_table)
    reg_table = validate_table_name(reg_table)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"""
            SELECT m.url FROM {url_table} m
            LEFT JOIN {reg_table} r ON m.url = r.url
            WHERE r.url IS NULL
        """)  # noqa: S608
        return [row[0] for row in cur.fetchall()]
