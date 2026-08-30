# Solutions — 06 Snowflake and dbt
## SD-01
Reject duplication: Spark/Flink own Silver; Snowflake registers external Iceberg access; dbt owns native Gold. Copying changes ownership and adds an unnecessary competing Silver representation.
## SD-02
Merge upserts but does not remove absent source rows; `fct_orders` post-hook deletes targets no longer in enriched source. Payments intentionally rebuild to make delete/relink correctness simple at current scale. Runtime dbt execution is deferred.
## SD-03
Missing rate stays null with `missing_dkk_rate=true`; payment left join preserves orphan evidence; relationship is warning severity. Wrong answer: zero conversion or inner join away the orphan.
