"""Один потребитель Kafka, Bronze → Silver → подтверждение offset."""

import os
import signal

from confluent_kafka import Consumer, OFFSET_BEGINNING, TopicPartition
from pyiceberg.expressions import In

from scripts.scd import decode, history
from scripts.settings import BRONZE, BROKER, GROUP, SILVER, TOPIC, catalog
from scripts.tables import BRONZE_SCHEMA, SILVER_SCHEMA, arrow_rows


def write_bronze(lakehouse, events):
    bronze = lakehouse.load_table(BRONZE)
    received = {event["kafka_offset"] for event in events}
    existing = bronze.scan(row_filter=In("kafka_offset", received), selected_fields=("kafka_offset",))
    stored = set(existing.to_arrow()["kafka_offset"].to_pylist())
    new = [event for event in events if event["kafka_offset"] not in stored]
    if new:
        bronze.append(arrow_rows(new, BRONZE_SCHEMA))
    return bronze, len(new)


def materialize(client, lakehouse, messages):
    events = [decode(message) for message in messages]
    bronze, inserted = write_bronze(lakehouse, events)
    if os.environ.get("FAIL_AFTER_BRONZE") == "1":
        raise SystemExit("Контрольный сбой: Bronze сохранён, Silver и offset ещё не записаны")

    accounts = {event["account_id"] for event in events}
    predicate = In("account_id", accounts)
    rows = bronze.scan(
        row_filter=predicate,
        selected_fields=(
            "account_id", "kafka_offset", "operation", "source_time", "source_event_id", "owner_name", "status",
        ),
    ).to_arrow().to_pylist()
    lakehouse.load_table(SILVER).overwrite(arrow_rows(history(rows), SILVER_SCHEMA), overwrite_filter=predicate)
    next_offset = messages[-1].offset() + 1
    for partition in client.commit(offsets=[TopicPartition(TOPIC, 0, next_offset)], asynchronous=False):
        if partition.error:
            raise RuntimeError(partition.error)
    print(f"offset={messages[0].offset()}..{next_offset - 1} bronze_new={inserted} silver_keys={len(accounts)} committed={next_offset}")


def assign(client, partitions):
    if len(partitions) != 1 or partitions[0].partition != 0:
        raise RuntimeError("Стенд рассчитан на один экземпляр потребителя и партицию 0")
    committed = client.committed(partitions, timeout=15)[0]
    if committed.error:
        raise RuntimeError(committed.error)
    low, high = client.get_watermark_offsets(partitions[0], timeout=15)
    if (committed.offset < 0 and low != 0) or (committed.offset >= 0 and not low <= committed.offset <= high):
        raise RuntimeError(f"Потеря позиции Kafka: сохранено {committed.offset}, доступно [{low}, {high}]")
    partitions[0].offset = committed.offset if committed.offset >= 0 else OFFSET_BEGINNING
    client.assign(partitions)
    print(f"Назначена {TOPIC}[0], start={partitions[0].offset}, high={high}")


def main():
    lakehouse = catalog()
    client = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": GROUP,
        "auto.offset.reset": "error",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "max.poll.interval.ms": 300_000,
    })
    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    client.subscribe([TOPIC], on_assign=assign)
    try:
        while running:
            messages = client.consume(num_messages=100, timeout=2.0)
            for message in messages:
                if message.error():
                    raise RuntimeError(message.error())
            if messages:
                materialize(client, lakehouse, messages)
    finally:
        client.close()


if __name__ == "__main__":
    main()
