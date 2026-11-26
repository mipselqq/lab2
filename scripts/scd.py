"""Декодирование Debezium и построение полуоткрытых интервалов SCD Type 2."""

import json
from datetime import datetime, timedelta, timezone
from itertools import groupby
from operator import itemgetter

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def decode(message):
    envelope = message.value().decode()
    record = json.loads(envelope)
    event = record.get("payload", record)
    operation = event["op"]
    if operation not in {"r", "c", "u", "d"}:
        raise ValueError(f"Неизвестная операция CDC: {operation}")
    account = event["before"] if operation == "d" else event["after"]
    account_id = account["account_id"]
    source = event["source"]
    micros = source.get("ts_us")
    if not micros:
        micros = (source.get("ts_ms") or event["ts_ms"]) * 1000

    if operation == "r":
        event_id = f"snapshot:{account_id}"
    else:
        tx = event["transaction"]
        event_id = f"{account_id}:{source['lsn']}:{tx['id']}:{tx['total_order']}"

    return {
        "kafka_offset": message.offset(),
        "account_id": account_id,
        "operation": operation,
        "source_time": EPOCH + timedelta(microseconds=int(micros)),
        "source_event_id": event_id,
        "owner_name": account["owner_name"] if operation != "d" else None,
        "status": account["status"] if operation != "d" else None,
        "envelope": envelope,
    }


def account_timeline(events):
    unique = {}
    for event in sorted(events, key=itemgetter("kafka_offset")):
        unique.setdefault(event["source_event_id"], event)
    baseline = next((row for row in unique.values() if row["operation"] == "r"), None)
    changes = [row for row in unique.values() if row["operation"] != "r"]
    if baseline:
        baseline_time = min([baseline["source_time"], *(row["source_time"] for row in changes)])
        changes.append({**baseline, "source_time": baseline_time})
    # Снимок — исходное состояние. У изменений с равным временем побеждает более позднее сообщение.
    return sorted(changes, key=lambda row: (row["source_time"], row["operation"] != "r", row["kafka_offset"]))


def history(events):
    versions = []
    accounts = groupby(sorted(events, key=itemgetter("account_id")), key=itemgetter("account_id"))
    for account_id, account_events in accounts:
        timeline = groupby(account_timeline(account_events), key=itemgetter("source_time"))
        current = None
        for timestamp, changes in timeline:
            latest = list(changes)[-1]
            deleted = latest["operation"] == "d"
            if current:
                same_state = (current["owner_name"], current["status"]) == (latest["owner_name"], latest["status"])
                if not deleted and same_state:
                    continue
                current.update(valid_to=timestamp, is_current=False, closed_by_offset=latest["kafka_offset"])
                current = None
            if not deleted:
                current = {
                    "account_id": account_id,
                    "owner_name": latest["owner_name"],
                    "status": latest["status"],
                    "valid_from": timestamp,
                    "valid_to": None,
                    "is_current": True,
                    "opened_by_offset": latest["kafka_offset"],
                    "closed_by_offset": None,
                }
                versions.append(current)
    return versions
