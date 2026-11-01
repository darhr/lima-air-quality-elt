-- One row per enabled pollutant, with its canonical unit and physical range.
select
    pollutant,
    display_name,
    category,
    canonical_unit,
    min_valid,
    max_valid
from {{ ref('pollutant_units') }}
