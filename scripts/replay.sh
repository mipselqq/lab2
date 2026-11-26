#!/usr/bin/env bash
# Воспроизведение сбоя после записи Bronze. Передать свежий account_id из demo.sh.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
if [[ $# != 1 || ! $1 =~ ^[0-9]+$ ]]; then
    echo 'Использование: bash scripts/replay.sh ACCOUNT_ID' >&2
    exit 2
fi
account_id=$1
probe="lab2-cdc-replay-$account_id"

query() {
    docker compose exec -T -e LC_ALL=C.UTF-8 -e TERM=dumb trino trino \
        --client-request-timeout 10s --output-format TSV --execute "$1" </dev/null
}

counts() {
    query "SELECT (SELECT count(*) FROM history.bank.accounts_cdc WHERE account_id = $account_id),
        (SELECT count(DISTINCT kafka_offset) FROM history.bank.accounts_cdc WHERE account_id = $account_id),
        (SELECT count(*) FROM history.bank.accounts_history WHERE account_id = $account_id),
        (SELECT max(status) FROM history.bank.accounts_history WHERE account_id = $account_id AND is_current)"
}

[[ $(counts) == $'8\t8\t4\tOPEN' ]] || {
    echo 'Укажите свежий счёт из успешно завершённого demo.sh.' >&2
    exit 1
}

docker compose stop materialize
trap 'docker compose start materialize >&2' EXIT
docker compose exec -T postgres psql -Xq -v ON_ERROR_STOP=1 -U banking -d banking \
    -c "UPDATE accounts SET status = 'CLOSED' WHERE account_id = $account_id"

set +e
output=$(timeout --foreground 75s docker compose run -T --no-deps --name "$probe" \
    -e FAIL_AFTER_BRONZE=1 materialize 2>&1)
code=$?
set -e
printf '%s\n' "$output"
if [[ $code == 124 ]]; then
    docker stop "$probe"
fi
[[ $code == 1 && $output == *'Контрольный сбой: Bronze сохранён'* ]]

after_bronze=$(counts)
printf 'После сбоя (Bronze, уникальные offsets, Silver, текущий статус): %s\n' "$after_bronze"
[[ $after_bronze == $'9\t9\t4\tOPEN' ]]

docker compose start materialize
trap - EXIT
after_replay=
for ((attempt=1; attempt<=30; attempt++)); do
    after_replay=$(counts)
    [[ $after_replay == $'9\t9\t5\tCLOSED' ]] && break
    sleep 2
done
printf 'После повторного чтения: %s\n' "$after_replay"
[[ $after_replay == $'9\t9\t5\tCLOSED' ]]
echo 'Проверки восстановления пройдены'
