"""Имена и конфигурация закрытой сети локального стенда."""

from pyiceberg.catalog import load_catalog

BROKER = "kafka:9092"
TOPIC = "bank.public.accounts"
GROUP = "accounts-iceberg-v1"
CONNECT = "http://connect:8083"
LAKEKEEPER = "http://lakekeeper:8181"
S3_ENDPOINT = "http://minio:9000"
S3_ACCESS = "lab2storage"
S3_SECRET = "lab2-s3-password"
BUCKET = "lab2-history"
WAREHOUSE = "history"
BRONZE = "bank.accounts_cdc"
SILVER = "bank.accounts_history"


def catalog():
    return load_catalog(
        "history",
        type="rest",
        uri=f"{LAKEKEEPER}/catalog",
        warehouse=WAREHOUSE,
        **{
            "s3.endpoint": S3_ENDPOINT,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.access-key-id": S3_ACCESS,
            "s3.secret-access-key": S3_SECRET,
            "s3.region": "us-east-1",
            "s3.force-virtual-addressing": "false",
            "header.X-Iceberg-Access-Delegation": "client-managed",
        },
    )
