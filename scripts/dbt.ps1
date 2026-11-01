# Runs dbt from the repository root with absolute local paths.
$root = Split-Path -Parent $PSScriptRoot
$env:RAW_ROOT = Join-Path $root 'data/raw'
$env:DUCKDB_PATH = Join-Path $root 'data/warehouse.duckdb'
$env:DBT_PROFILES_DIR = Join-Path $root 'dbt'

Push-Location $root
try {
    uv run dbt @args --project-dir (Join-Path $root 'dbt') --profiles-dir (Join-Path $root 'dbt')
}
finally {
    Pop-Location
}