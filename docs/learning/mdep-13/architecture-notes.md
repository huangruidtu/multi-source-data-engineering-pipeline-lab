# MDEP-13 architecture notes

MDEP-13 changes no data-plane ownership. Airflow remains the batch Bronze owner; Spark remains batch/reference Silver owner; Flink remains CDC/event Bronze and Silver owner; Snowflake/dbt remains Gold owner. The new validation plane is deliberately file-based and local: it observes the existing architecture through scripts, evidence directories, and reconciliation queries rather than adding an observability platform.

The key boundary is semantic reconciliation. Bronze history can contain duplicates and replays, Silver should represent one current row per business key, and Gold may aggregate or deliberately preserve warning-level exceptions. A single cross-layer row-count equality assertion would therefore be incorrect.

The matrix uses `BLOCKED` when a required environment is missing and `NOT_RUN` when an executable stage is intentionally not invoked. A `PASSED` record requires a log/evidence path from the current run.
