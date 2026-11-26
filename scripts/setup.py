"""Идемпотентная инициализация каталога, бакета и CDC-коннектора."""

import json
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from scripts.settings import (
    BROKER, BUCKET, CONNECT, LAKEKEEPER, S3_ACCESS, S3_ENDPOINT, S3_SECRET, TOPIC,
    WAREHOUSE, catalog,
)
from scripts.tables import create_tables


def request(url, method="GET", payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    query = Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urlopen(query, timeout=20) as response:
        return json.loads(body) if (body := response.read()) else None


def retry(label, action):
    deadline = time.monotonic() + 150
    while True:
        try:
            return action()
        except (HTTPError, URLError, TimeoutError, EndpointConnectionError) as error:
            if isinstance(error, HTTPError) and error.code not in (404, 502, 503, 504):
                raise RuntimeError(f"{label}: HTTP {error.code}: {error.read().decode()}") from error
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Таймаут ожидания: {label}") from error
            time.sleep(2)


def create_bucket():
    client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS,
        aws_secret_access_key=S3_SECRET,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, connect_timeout=3, read_timeout=5),
    )
    try:
        client.head_bucket(Bucket=BUCKET)
    except ClientError as error:
        if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
            raise
        client.create_bucket(Bucket=BUCKET)


def setup_lakehouse():
    retry("Lakekeeper", lambda: request(f"{LAKEKEEPER}/health"))
    retry("S3", create_bucket)
    try:
        request(f"{LAKEKEEPER}/management/v1/bootstrap", "POST", {"accept-terms-of-use": True})
    except HTTPError as error:
        body = error.read().decode()
        if "CatalogAlreadyBootstrapped" not in body:
            raise RuntimeError(f"Ошибка bootstrap: {error.code} {body}") from error
    url = f"{LAKEKEEPER}/management/v1/warehouse"
    existing = request(url)["warehouses"]
    if not any(warehouse["name"] == WAREHOUSE for warehouse in existing):
        request(url, "POST", {
            "warehouse-name": WAREHOUSE,
            "project-id": "00000000-0000-0000-0000-000000000000",
            "storage-profile": {
                "type": "s3",
                "bucket": BUCKET,
                "key-prefix": "",
                "endpoint": S3_ENDPOINT,
                "region": "us-east-1",
                "path-style-access": True,
                "flavor": "s3-compat",
                "sts-enabled": False,
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "access-key-id": S3_ACCESS,
                "secret-access-key": S3_SECRET,
            },
        })
    create_tables(catalog())


def setup_cdc():
    admin = AdminClient({"bootstrap.servers": BROKER})
    topic = NewTopic(TOPIC, 1, 1, config={"cleanup.policy": "delete", "retention.ms": "-1"})
    try:
        admin.create_topics([topic], request_timeout=20)[TOPIC].result()
    except KafkaException as error:
        if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
    topic_metadata = admin.list_topics(timeout=15).topics.get(TOPIC)
    if topic_metadata is None or len(topic_metadata.partitions) != 1:
        raise RuntimeError(f"Топик {TOPIC} должен содержать ровно одну партицию")

    retry("Kafka Connect", lambda: request(f"{CONNECT}/connectors"))
    url = f"{CONNECT}/connectors/accounts-cdc"
    config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "cdc",
        "database.password": "cdc-local",
        "database.dbname": "banking",
        "topic.prefix": "bank",
        "plugin.name": "pgoutput",
        "slot.name": "accounts_slot",
        "publication.name": "accounts_publication",
        "publication.autocreate.mode": "disabled",
        "table.include.list": "public.accounts",
        "snapshot.mode": "initial",
        "provide.transaction.metadata": "true",
        "tombstones.on.delete": "false",
    }
    try:
        existing = request(f"{url}/config")
    except HTTPError as error:
        if error.code != 404:
            raise
        existing = {}
    if any(existing.get(key) != value for key, value in config.items()):
        request(f"{url}/config", "PUT", config)

    deadline = time.monotonic() + 100
    while time.monotonic() < deadline:
        try:
            status = request(f"{url}/status")
        except HTTPError as error:
            if error.code != 404:
                raise
        else:
            states = [status["connector"], *status["tasks"]]
            for state in states:
                if state["state"] == "FAILED":
                    raise RuntimeError(state.get("trace", state))
            if status["tasks"] and all(state["state"] == "RUNNING" for state in states):
                return
        time.sleep(2)
    raise RuntimeError("Коннектор accounts-cdc не перешёл в статус RUNNING")


if __name__ == "__main__":
    setup_lakehouse()
    setup_cdc()
    print("Готовы S3, warehouse history, таблицы Iceberg и коннектор accounts-cdc")
