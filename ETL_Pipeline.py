import json
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from Config import (
    RAW_FILE_PATH,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def read_raw_logs(file_path: str) -> list[dict]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Expected raw_logs.json to contain a list of records")

    return data


def normalize_timestamp(ts: str) -> tuple[datetime | None, str | None]:
    if not ts:
        return None, None

    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        iso_ts = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        return dt, iso_ts
    except Exception:
        return None, None


def load_raw_to_postgres(conn, raw_data: list[dict], source_file_name: str):
    if not raw_data:
        return

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE raw_user_logs;")

        raw_rows = [
            (
                record.get("user_id"),
                record.get("timestamp"),
                record.get("action_type"),
                json.dumps(record.get("metadata")) if record.get("metadata") is not None else None,
                source_file_name,
            )
            for record in raw_data
        ]

        execute_values(
            cur,
            """
            INSERT INTO raw_user_logs
            (user_id, timestamp_raw, action_type, metadata, source_file_name)
            VALUES %s
            """,
            raw_rows,
            page_size=1000,
        )

def transform_logs_efficient(raw_data: list[dict], source_file_name: str):
    raw_record_count = len(raw_data)

    cleaned_records = []
    rejected_records = []
    seen_events = set()

    user_dimension_set = set()
    action_dimension_set = set()

    rejected_missing_user_or_action = 0
    rejected_invalid_timestamp = 0
    rejected_duplicates = 0

    for record in raw_data:
        user_id = record.get("user_id")
        action_type = record.get("action_type")
        timestamp_raw = record.get("timestamp")
        metadata = record.get("metadata") or {}

        if not user_id or not action_type:
            rejected_missing_user_or_action += 1
            rejected_records.append(
                {
                    "user_id": record.get("user_id"),
                    "timestamp_raw": record.get("timestamp"),
                    "action_type": record.get("action_type"),
                    "metadata": record.get("metadata"),
                    "rejection_reason": "missing_user_id_or_action_type",
                    "source_file_name": source_file_name,
                }
            )
            continue

        user_id = str(user_id).strip()
        action_type = str(action_type).strip().lower()

        if not user_id or not action_type:
            rejected_missing_user_or_action += 1
            rejected_records.append(
                {
                    "user_id": record.get("user_id"),
                    "timestamp_raw": record.get("timestamp"),
                    "action_type": record.get("action_type"),
                    "metadata": record.get("metadata"),
                    "rejection_reason": "blank_user_id_or_action_type",
                    "source_file_name": source_file_name,
                }
            )
            continue

        event_dt, event_timestamp = normalize_timestamp(timestamp_raw)
        if event_dt is None:
            rejected_invalid_timestamp += 1
            rejected_records.append(
                {
                    "user_id": record.get("user_id"),
                    "timestamp_raw": record.get("timestamp"),
                    "action_type": record.get("action_type"),
                    "metadata": record.get("metadata"),
                    "rejection_reason": "invalid_timestamp",
                    "source_file_name": source_file_name,
                }
            )
            continue

        device = metadata.get("device") if isinstance(metadata, dict) else None
        location = metadata.get("location") if isinstance(metadata, dict) else None

        if device is not None:
            device = str(device).strip()
        if location is not None:
            location = str(location).strip()

        duplicate_key = (user_id, action_type, event_timestamp)
        if duplicate_key in seen_events:
            rejected_duplicates += 1
            rejected_records.append(
                {
                    "user_id": record.get("user_id"),
                    "timestamp_raw": record.get("timestamp"),
                    "action_type": record.get("action_type"),
                    "metadata": record.get("metadata"),
                    "rejection_reason": "duplicate_event",
                    "source_file_name": source_file_name,
                }
            )
            continue

        seen_events.add(duplicate_key)

        cleaned_records.append(
            {
                "user_id": user_id,
                "action_type": action_type,
                "event_timestamp": event_timestamp,
                "event_date": event_dt.date(),
                "device": device,
                "location": location,
                "action_count": 1,
            }
        )

        user_dimension_set.add((user_id, device, location))
        action_dimension_set.add(action_type)

    cleaned_record_count = len(cleaned_records)
    rejected_record_count = len(rejected_records)

    dim_users_list = sorted(
        user_dimension_set,
        key=lambda x: (x[0], str(x[1]), str(x[2]))
    )
    dim_actions_list = sorted(action_dimension_set)

    dim_users = [
        {
            "user_id": user_id,
            "device": device,
            "location": location,
        }
        for user_id, device, location in dim_users_list
    ]

    dim_actions = [
        {
            "action_type": action_type,
        }
        for action_type in dim_actions_list
    ]

    fact_user_actions = [
        {
            "user_id": record["user_id"],
            "device": record["device"],
            "location": record["location"],
            "action_type": record["action_type"],
            "event_timestamp": record["event_timestamp"],
            "event_date": record["event_date"],
            "action_count": record["action_count"],
        }
        for record in cleaned_records
    ]

    dim_users_df = pd.DataFrame(dim_users)
    dim_actions_df = pd.DataFrame(dim_actions)
    fact_user_actions_df = pd.DataFrame(fact_user_actions)
    rejected_records_df = pd.DataFrame(rejected_records)

    count_summary = {
        "raw_record_count": raw_record_count,
        "cleaned_record_count": cleaned_record_count,
        "rejected_record_count": rejected_record_count,
        "rejected_missing_user_or_action": rejected_missing_user_or_action,
        "rejected_invalid_timestamp": rejected_invalid_timestamp,
        "rejected_duplicates": rejected_duplicates,
    }

    return (
        dim_users_df,
        dim_actions_df,
        fact_user_actions_df,
        rejected_records_df,
        count_summary,
    )


def load_quarantine_to_postgres(conn, rejected_records_df: pd.DataFrame):
    if rejected_records_df.empty:
        return

    with conn.cursor() as cur:
        quarantine_rows = [
            (
                row.user_id if pd.notna(row.user_id) else None,
                row.timestamp_raw if pd.notna(row.timestamp_raw) else None,
                row.action_type if pd.notna(row.action_type) else None,
                json.dumps(row.metadata) if pd.notna(row.metadata) else None,
                row.rejection_reason,
                row.source_file_name if pd.notna(row.source_file_name) else None,
            )
            for row in rejected_records_df.itertuples(index=False)
        ]

        execute_values(
            cur,
            """
            INSERT INTO quarantine_user_logs
            (user_id, timestamp_raw, action_type, metadata, rejection_reason, source_file_name)
            VALUES %s
            """,
            quarantine_rows,
            page_size=1000,
        )


def run_quality_checks(
    dim_users: pd.DataFrame,
    dim_actions: pd.DataFrame,
    fact_user_actions: pd.DataFrame,
    count_summary: dict,
) -> dict:
    checks = {
        "raw_record_count": count_summary["raw_record_count"],
        "cleaned_record_count": count_summary["cleaned_record_count"],
        "rejected_record_count": count_summary["rejected_record_count"],
        "rejected_missing_user_or_action": count_summary["rejected_missing_user_or_action"],
        "rejected_invalid_timestamp": count_summary["rejected_invalid_timestamp"],
        "rejected_duplicates": count_summary["rejected_duplicates"],
        "dim_users_null_user_id": int(dim_users["user_id"].isna().sum()) if not dim_users.empty else 0,
        "dim_actions_null_action_type": int(dim_actions["action_type"].isna().sum()) if not dim_actions.empty else 0,
        "fact_null_event_timestamp": int(fact_user_actions["event_timestamp"].isna().sum()) if not fact_user_actions.empty else 0,
        "fact_duplicate_events": int(
            fact_user_actions.duplicated(
                subset=["user_id", "device", "location", "action_type", "event_timestamp"]
            ).sum()
        ) if not fact_user_actions.empty else 0,
        "count_reconciliation_ok": (
            count_summary["raw_record_count"]
            == count_summary["cleaned_record_count"] + count_summary["rejected_record_count"]
        ),
    }

    failed = {}

    for key in [
        "dim_users_null_user_id",
        "dim_actions_null_action_type",
        "fact_null_event_timestamp",
        "fact_duplicate_events",
    ]:
        if checks[key] > 0:
            failed[key] = checks[key]

    if not checks["count_reconciliation_ok"]:
        failed["count_reconciliation_ok"] = checks["count_reconciliation_ok"]

    if failed:
        raise ValueError(f"Data quality checks failed: {failed}")

    return checks


def load_to_postgres(
    conn,
    dim_users: pd.DataFrame,
    dim_actions: pd.DataFrame,
    fact_user_actions: pd.DataFrame,
):
    with conn.cursor() as cur:
        dim_users_rows = [
            (
                row.user_id,
                row.device if pd.notna(row.device) else None,
                row.location if pd.notna(row.location) else None,
            )
            for row in dim_users.itertuples(index=False)
        ]

        dim_actions_rows = [
            (row.action_type,)
            for row in dim_actions.itertuples(index=False)
        ]

        if dim_users_rows:
            execute_values(
                cur,
                """
                INSERT INTO dim_users (user_id, device, location)
                VALUES %s
                ON CONFLICT (user_id, device, location) DO NOTHING
                """,
                dim_users_rows,
                page_size=1000,
            )

        if dim_actions_rows:
            execute_values(
                cur,
                """
                INSERT INTO dim_actions (action_type)
                VALUES %s
                ON CONFLICT (action_type) DO NOTHING
                """,
                dim_actions_rows,
                page_size=1000,
            )

        cur.execute(
            """
            SELECT user_key, user_id, device, location
            FROM dim_users
            """
        )
        user_map = {
            (user_id, device, location): user_key
            for user_key, user_id, device, location in cur.fetchall()
        }

        cur.execute(
            """
            SELECT action_key, action_type
            FROM dim_actions
            """
        )
        action_map = {
            action_type: action_key
            for action_key, action_type in cur.fetchall()
        }

        fact_rows = [
            (
                user_map[
                    (
                        row.user_id,
                        row.device if pd.notna(row.device) else None,
                        row.location if pd.notna(row.location) else None,
                    )
                ],
                action_map[row.action_type],
                pd.to_datetime(row.event_timestamp).to_pydatetime(),
                row.event_date,
                int(row.action_count),
            )
            for row in fact_user_actions.itertuples(index=False)
        ]

        if fact_rows:
            execute_values(
                cur,
                """
                INSERT INTO fact_user_actions
                (user_key, action_key, event_timestamp, event_date, action_count)
                VALUES %s
                ON CONFLICT (user_key, action_key, event_timestamp) DO NOTHING
                """,
                fact_rows,
                page_size=1000,
            )


def main():
    source_file_name = Path(RAW_FILE_PATH).name
    raw_data = read_raw_logs(RAW_FILE_PATH)

    (
        dim_users,
        dim_actions,
        fact_user_actions,
        rejected_records_df,
        count_summary,
    ) = transform_logs_efficient(raw_data, source_file_name)

    checks = run_quality_checks(
        dim_users,
        dim_actions,
        fact_user_actions,
        count_summary,
    )
    print("Data quality checks passed:", checks)

    conn = get_connection()
    try:
        with conn:
            load_raw_to_postgres(conn, raw_data, source_file_name)

        if not rejected_records_df.empty:
            try:
                with conn:
                    load_quarantine_to_postgres(conn, rejected_records_df)
            except Exception as e:
                print(f"Warning: failed to load quarantine records: {e}")

        with conn:
            load_to_postgres(conn, dim_users, dim_actions, fact_user_actions)

    finally:
        conn.close()

    print("Data loaded successfully into PostgreSQL")
    if not rejected_records_df.empty:
        print(f"Rejected records moved to quarantine table: {len(rejected_records_df)}")


if __name__ == "__main__":
    main()