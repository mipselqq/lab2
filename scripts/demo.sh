#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

pg() {
    docker compose exec -T postgres psql -XqAt -v ON_ERROR_STOP=1 -U banking -d banking "$@"
}

query() {
    docker compose exec -T -e LC_ALL=C.UTF-8 -e TERM=dumb trino trino \
        --client-request-timeout 10s --output-format TSV --execute "$1" </dev/null
}

# Снимок нужен в результатах до проверки новых изменений.
ready=
for ((attempt=1; attempt<=30; attempt++)); do
    if [[ $(query "SELECT count(*) FROM history.bank.accounts_cdc WHERE operation = 'r' AND account_id IN (101, 102)" 2>/dev/null) == 2 ]]; then
        ready=1
        break
    fi
    sleep 2
done
if [[ -z $ready ]]; then
    echo 'Trino или начальный снимок CDC не готовы. Проверьте docker compose logs.' >&2
    exit 1
fi

account_id=$(pg -c "INSERT INTO accounts (owner_name, status) VALUES ('Мария Орлова', 'OPEN') RETURNING account_id")
[[ $account_id =~ ^[0-9]+$ ]]
echo "account_id=$account_id"
pg <<SQL
UPDATE accounts SET owner_name = 'Иван Белов' WHERE account_id = $account_id;
UPDATE accounts SET status = status WHERE account_id = $account_id;
UPDATE accounts SET status = 'FROZEN' WHERE account_id = $account_id;
BEGIN;
UPDATE accounts SET status = 'CLOSED' WHERE account_id = $account_id;
UPDATE accounts SET status = 'FROZEN' WHERE account_id = $account_id;
COMMIT;
DELETE FROM accounts WHERE account_id = $account_id;
INSERT INTO accounts (account_id, owner_name, status) VALUES ($account_id, 'Олег Смирнов', 'OPEN');
SQL

bronze=history.bank.accounts_cdc
silver=history.bank.accounts_history
count_cdc="SELECT count(*), count(DISTINCT kafka_offset) FROM $bronze WHERE account_id = $account_id"
history_sql="SELECT owner_name, status, is_current FROM $silver WHERE account_id = $account_id ORDER BY valid_from"
expected=$(printf 'Мария Орлова\tOPEN\tfalse\nИван Белов\tOPEN\tfalse\nИван Белов\tFROZEN\tfalse\nОлег Смирнов\tOPEN\ttrue')
actual=
counts=
for ((attempt=1; attempt<=30; attempt++)); do
    counts=$(query "$count_cdc")
    actual=$(query "$history_sql")
    if [[ $counts == $'8\t8' && $actual == "$expected" ]]; then
        break
    fi
    sleep 2
done
if [[ $counts != $'8\t8' || $actual != "$expected" ]]; then
    printf 'Некорректный результат\nCDC: %s\nSCD: %s\n' "$counts" "$actual" >&2
    exit 1
fi

# Проверяем включение начала, исключение конца и разрыв после удаления.
boundaries=$(query "
WITH changes AS (
    SELECT min(source_time) FILTER (WHERE operation = 'u' AND owner_name = 'Иван Белов') AS transfer_at,
           min(source_time) FILTER (WHERE operation = 'd') AS deleted_at,
           max(source_time) FILTER (WHERE operation = 'c') AS recreated_at
    FROM $bronze WHERE account_id = $account_id
), moments(label, moment) AS (
    SELECT '1-transfer', transfer_at FROM changes UNION ALL
    SELECT '2-delete', deleted_at FROM changes UNION ALL
    SELECT '3-recreate', recreated_at FROM changes
)
SELECT label, coalesce(owner_name, '<отсутствует>')
FROM moments LEFT JOIN $silver ON account_id = $account_id
    AND valid_from <= moment AND (valid_to > moment OR valid_to IS NULL)
ORDER BY label")
expected_boundaries=$(printf '1-transfer\tИван Белов\n2-delete\t<отсутствует>\n3-recreate\tОлег Смирнов')
if [[ $boundaries != "$expected_boundaries" ]]; then
    printf 'Некорректные границы SCD:\n%s\n' "$boundaries" >&2
    exit 1
fi

echo 'Bronze: 8 записей, 8 уникальных offsets'
echo 'Silver: 4 версии, 1 текущая'
query "SELECT owner_name, status, valid_from, valid_to, is_current FROM $silver WHERE account_id = $account_id ORDER BY valid_from"
printf '%s\n' "$boundaries"
echo 'Проверки пройдены'
