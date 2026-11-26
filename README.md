# Практическая №2. История изменений банковских счетов

Вариант 1, Илья Семёнов, ОТИ-11. PostgreSQL -> Debezium -> Kafka -> Iceberg SCD Type 2 -> Trino.

- [Отчёт](отчет.md) с описанием архитектуры, результатами запуска и выводами.
- [Схема draw.io](architecture.drawio) и [PNG](architecture.png).
- [Результаты проверки](results/) и [SQL-запросы](sql/queries.sql).

## Запуск

Понадобятся Docker Engine, Docker Compose v2, Bash и около 8 ГиБ памяти для стенда. При первом запуске Docker скачивает образы и собирает Python-контейнер. Учебные сервисы общаются в сети Compose. Из портов на хост опубликован только Trino на `127.0.0.1:18082`.

```bash
docker compose up -d --build
docker compose ps -a
bash scripts/demo.sh
docker compose exec -T -e TERM=dumb trino trino --output-format ALIGNED < sql/queries.sql
```

`demo.sh` ждёт начальный снимок, создаёт новый счёт и печатает его `account_id`. На чистом стенде это `1000`. При повторном запуске подставьте напечатанный ID в запросы. Скрипт проверяет смену владельца, смену статуса, обновление без изменений, удаление и повторное открытие.

Чтобы проверить восстановление после записи в Bronze, передайте свежий ID из `demo.sh`.

```bash
bash scripts/replay.sh 1000
```

Тома сохраняются между запусками. [Скрипт очистки](scripts/cleanup.sh) без аргументов печатает план. С флагом `--apply` он удаляет контейнеры, сеть и тома этого Compose-проекта.
