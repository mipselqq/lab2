-- Сырые события и значения времени изменения в PostgreSQL.
SELECT kafka_offset, operation, source_time, owner_name, status
FROM history.bank.accounts_cdc
WHERE account_id = 1000
ORDER BY kafka_offset;

-- Полная история счёта и offsets, открывшие и закрывшие версии.
SELECT account_id, owner_name, status, valid_from, valid_to,
       is_current, opened_by_offset, closed_by_offset
FROM history.bank.accounts_history
WHERE account_id = 1000
ORDER BY valid_from;

-- Владелец и статус ровно в момент заморозки.
-- Для произвольной даты задайте CTE так:
-- WITH point(as_of) AS (VALUES (TIMESTAMP 'YYYY-MM-DD HH:MM:SS UTC'))
WITH point AS (
    SELECT min(source_time) AS as_of
    FROM history.bank.accounts_cdc
    WHERE account_id = 1000 AND operation = 'u' AND status = 'FROZEN'
)
SELECT account_id, owner_name, status, as_of
FROM history.bank.accounts_history CROSS JOIN point
WHERE account_id = 1000
    AND valid_from <= as_of AND (valid_to > as_of OR valid_to IS NULL);

-- Инварианты. Оба запроса должны вернуть 0 строк.
SELECT kafka_offset, count(*) AS copies
FROM history.bank.accounts_cdc
GROUP BY kafka_offset
HAVING count(*) > 1;

WITH ordered AS (
    SELECT *,
        lead(valid_from) OVER (PARTITION BY account_id ORDER BY valid_from) AS next_from,
        count_if(is_current) OVER (PARTITION BY account_id) AS current_versions
    FROM history.bank.accounts_history
)
SELECT account_id, valid_from, valid_to
FROM ordered
WHERE current_versions > 1
    OR is_current <> (valid_to IS NULL)
    OR (valid_to IS NOT NULL AND valid_to <= valid_from)
    OR (next_from IS NOT NULL AND (valid_to IS NULL OR valid_to > next_from));
