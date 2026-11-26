"""Таблицы Iceberg и преобразование строк к схеме Arrow."""

import pyarrow as pa
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType, LongType, NestedField, StringType, TimestamptzType

from scripts.settings import BRONZE, SILVER

BRONZE_SCHEMA = Schema(
    NestedField(1, "kafka_offset", LongType(), required=True),
    NestedField(2, "account_id", LongType(), required=True),
    NestedField(3, "operation", StringType(), required=True),
    NestedField(4, "source_time", TimestamptzType(), required=True),
    NestedField(5, "source_event_id", StringType(), required=True),
    NestedField(6, "owner_name", StringType()),
    NestedField(7, "status", StringType()),
    NestedField(8, "envelope", StringType(), required=True),
    identifier_field_ids=[1],
)

SILVER_SCHEMA = Schema(
    NestedField(1, "account_id", LongType(), required=True),
    NestedField(2, "owner_name", StringType(), required=True),
    NestedField(3, "status", StringType(), required=True),
    NestedField(4, "valid_from", TimestamptzType(), required=True),
    NestedField(5, "valid_to", TimestamptzType()),
    NestedField(6, "is_current", BooleanType(), required=True),
    NestedField(7, "opened_by_offset", LongType(), required=True),
    NestedField(8, "closed_by_offset", LongType()),
    identifier_field_ids=[1, 4],
)


def create_tables(catalog):
    catalog.create_namespace_if_not_exists("bank")
    catalog.create_table_if_not_exists(BRONZE, BRONZE_SCHEMA)
    catalog.create_table_if_not_exists(SILVER, SILVER_SCHEMA)


def arrow_rows(rows, schema):
    return pa.Table.from_pylist(rows, schema=schema_to_pyarrow(schema))
