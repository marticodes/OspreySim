import json
import os
import sqlite3
import time
from typing import Any, Dict, Optional


def get_env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f'Missing required environment variable: {name}')
    return value


def connect_sqlite(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def build_record(row: sqlite3.Row, *, id_column: str, timestamp_column: str) -> Dict[str, Any]:
    # Map your SQLite schema into the expected Kafka JSON for Osprey
    # Dataset fields: post_id, parent_id, user_id, content, topic, media_type,
    # media_url, timestamp, duration, visibility, comm_id, hashtag
    action_name = os.getenv('PRODUCER_ACTION_NAME', 'create_post')
    event_type = os.getenv('PRODUCER_EVENT_TYPE', 'create_post')

    # Optional IP column; if not present, use a neutral placeholder
    ip_column = os.getenv('SQLITE_IP_COLUMN', '')
    ip_value: str = '0.0.0.0'
    if ip_column and ip_column in row.keys():  # type: ignore[attr-defined]
        ip_value = str(row[ip_column])

    post_obj: Dict[str, Any] = {
        'text': row['content'],
    }

    # Include optional fields if present - all the things we have in our sim database more but not needed (for now ) for OSPREY
    for extra in [
        'parent_id',
        'topic',
        'media_type',
        'media_url',
        'duration',
        'visibility',
        'comm_id',
        'hashtag',
    ]:
        if extra in row.keys():  # type: ignore[attr-defined]
            post_obj[extra] = row[extra]

    return {
        'send_time': row[timestamp_column],  # e.g., 2025-09-26T07:13:45.345Z
        'data': {
            'action_id': row[id_column],
            'action_name': action_name,
            'data': {
                'user_id': row['user_id'],
                'ip_address': ip_value,
                'event_type': event_type,
                'post': post_obj,
            },
        },
    }


def main() -> None:
    db_path = get_env('SQLITE_DB_PATH')
    table = get_env('SQLITE_TABLE', 'posts')
    id_column = get_env('SQLITE_ID_COLUMN', 'post_id')
    timestamp_column = get_env('SQLITE_TIMESTAMP_COLUMN', 'timestamp')
    kafka_broker = get_env('KAFKA_BROKER')
    kafka_topic = get_env('KAFKA_TOPIC', 'osprey.actions_input')
    poll_interval = float(os.getenv('POLL_INTERVAL_SECONDS', '1.0'))

    # Lazy import to avoid adding dependencies to worker image
    from kafka import KafkaProducer  # type: ignore

    producer = KafkaProducer(bootstrap_servers=kafka_broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    conn = connect_sqlite(db_path)
    cur = conn.cursor()

    # Resume from last seen id (stateless fallback: start at 0)
    last_seen_id = int(os.getenv('START_FROM_ID', '0'))

    while True:
        cur.execute(
            f'SELECT * FROM {table} WHERE {id_column} > ? ORDER BY {id_column} ASC',
            (last_seen_id,),
        )
        rows = cur.fetchall()
        for row in rows:
            payload = build_record(row, id_column=id_column, timestamp_column=timestamp_column)
            producer.send(kafka_topic, value=payload)
            last_seen_id = max(last_seen_id, int(row[id_column]))
        producer.flush()
        time.sleep(poll_interval)


if __name__ == '__main__':
    main()
