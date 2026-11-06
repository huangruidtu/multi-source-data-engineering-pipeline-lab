# AWS-04 interview explanation (30-60 seconds)

“AWS-04 adds an isolated dbt Gold project without changing MDEP V1. In Redshift,
the bootstrap maps the existing AWS-03 Glue Iceberg database read-only, while
dimensions, facts and marts are native Redshift tables. AWS-05 commerce Silver
does not exist yet, so I made that gap explicit: DuckDB substitutes bounded
fixtures at the source boundary but runs the same SQL and real materializations.

The order fact uses a deterministic-key MERGE and business-version-first
ordering. EUR is converted to DKK by business date using only positive rates;
missing rates stay null and flagged, and orphan customers and payments survive
left joins. Normal runs never infer deletes. A separate macro requires verified
full-snapshot evidence and rejects an excessive delete fraction. The integration
test proves replay, stale updates, FX corrections and deletion behavior. AWS is
still deferred: Redshift is disabled by default, private, capped at 8 RPUs and
query-deactivated at 16 monthly RPU-hours.”
