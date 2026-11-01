-- Fixed conversion case: the 2025-01-15 03:00 UTC instant must be 22:00 of the
-- previous day in America/Lima (UTC-5, no DST). It detects a timezone()
-- argument order mistake.
select
    timezone(
        'America/Lima',
        timezone('UTC', timestamp '2025-01-15 03:00:00')
    ) as local_ts
where timezone(
        'America/Lima',
        timezone('UTC', timestamp '2025-01-15 03:00:00')
    ) is distinct from timestamp '2025-01-14 22:00:00'
