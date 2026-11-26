#!/usr/bin/env bash
# Без флага выводит план. С --apply удаляет контейнеры, сеть и тома проекта.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

if [[ $# == 0 ]]; then
    echo 'План: docker compose --project-name lab2-cdc --file docker-compose.yml down --volumes --remove-orphans'
    echo 'Будут удалены данные PostgreSQL, Kafka, Lakekeeper и S3 этого проекта.'
    echo 'Для выполнения: bash scripts/cleanup.sh --apply'
    exit 0
fi

if [[ $# != 1 || $1 != --apply ]]; then
    echo 'Использование: bash scripts/cleanup.sh [--apply]' >&2
    exit 2
fi

docker compose --project-name lab2-cdc --file docker-compose.yml down --volumes --remove-orphans
