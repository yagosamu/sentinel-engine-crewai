"""C2 ingestion (Dagster). SOLE writer of the ``raw`` schema in DuckDB.

Reads Postgres read-only and lands ``raw.raw_*`` tables (mirroring source 1:1,
defects intact) via timestamp-incremental dual-watermark extraction. Consumes
``platform.warehouse`` for the connection; never creates the warehouse path.
"""
