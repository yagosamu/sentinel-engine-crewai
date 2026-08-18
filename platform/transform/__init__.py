"""C3 transform (dbt project). Sole writer of bronze/silver/gold schemas.

dbt-duckdb medallion over ``raw.raw_*``: bronze (views) -> silver (incremental,
with ``*_rejects`` quarantine tables) -> gold (tables). dbt NEVER reads Postgres.
The dbt project files live under this directory.
"""
