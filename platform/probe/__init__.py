"""C8 active freshness probe (AC-3 gate).

Measures REAL end-to-end pipeline latency: inject a uniquely-identifiable beacon
order into the SOURCE Postgres, run C2 ingestion and ``dbt build``, then poll
``gold.gold_orders_obt`` (read-only) until the beacon is queryable. The lag from
source COMMIT to gold-visibility is the AC-3 statistic.

AC-3 gate: the MEDIAN sample lag must be <= 5 min (300 s); the CLI exits
non-zero on breach. ``--ci`` runs a single-shot sample. A DB-down condition is a
SKIP (exit 77), not a failure.
"""
