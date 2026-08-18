"""C4h peak-load harness (AC-1 gate).

Drives 75k-orders/day-equivalent OLTP load on Postgres and proves zero
analytics-attributable (application_name='dagster_ingest') lock-wait. AC-1 is the
only criterion this package measures; AC-2 (gold query p95 <= 5s) is measured
separately by ``platform/evals/eval_ac2.sh``, not here.
"""
