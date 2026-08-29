# MDEP-7 Architecture Notes — Source Behavior Before Pipelines

## Implemented decisions

- Use local Docker Compose for the two reproducible runtime sources: PostgreSQL 16 and a small Python REST service.
- Keep REST data in code and use standard-library HTTP tooling. This makes records, pagination, and failure responses deterministic without an external dependency.
- Model four PostgreSQL entities from the MDEP-6 CDC contract with source-side keys and referential constraints.
- Seed intentionally imperfect data and provide explicit mutation/failure files rather than making defects accidental.
- Configure PostgreSQL logical replication prerequisites now, but defer Debezium connector configuration and event verification.

## Why this design

Later ingestion work needs stable inputs that can be reset after a failure exercise. A fixed REST API lets a developer test page traversal, retry, rate-limit, and timeout handling without depending on a vendor API. Source-side foreign keys and checks demonstrate failures at the right boundary: before a pipeline receives an invalid relational record.

The files distinguish three different cases often collapsed in simple demos: duplicate content under another filename, syntactically malformed JSON, and JSON that parses but violates the logical contract. Each needs different diagnostics downstream.

## Alternatives and trade-offs

| Alternative | Reason not used | Trade-off |
| --- | --- | --- |
| Public API | Non-deterministic availability, data, and rate limits | Local API does not model auth or real vendor changes |
| Full application framework | Extra dependency/tool coverage | Standard library has minimal routing and observability features |
| Random/generated seed data | Harder to compare runs | Small static dataset has limited volume/distribution realism |
| Immediate Debezium setup | Outside this Story and source prerequisites must be understood first | CDC behavior remains unverified/deferred |

## Ownership and failure boundaries

PostgreSQL owns relational constraint enforcement. The REST source owns its HTTP response contract. The source fixtures own their deliberate defect definitions. Future Bronze ingestion owns capture, while Silver processing owns contract validation, deduplication, and quarantine—consistent with MDEP-6.

An invalid foreign-key insert should fail in PostgreSQL. A malformed JSON file cannot be parsed; semantically invalid JSON parses but should be rejected by a future schema/reference check. A 503 or 429 is an HTTP response, not a data-quality error, and needs retry/rate-limit logic rather than quarantine.

## Scalability, reliability, and consistency implications

The lab is deliberately small, but it teaches reliable interfaces: stable keys, deterministic reset, bounded page size, `Retry-After`, and idempotent-looking seed restoration. Real production systems would need authentication, TLS, secret management, source backups, rate budgets, real pagination cursors, API observability, and stronger schema tooling.

The current relational delete is only a source mutation file. Its downstream representation—physical delete or tombstone in Silver—is deferred to the streaming design. Likewise, `wal_level=logical` is configuration evidence, not proof that CDC captures all required events.

## Relationship to Bronze/Silver/Gold, batch, and CDC

MDEP-7 provides the raw source side of the target architecture. Airflow/Spark batch work will use the REST/file/PostgreSQL inputs to create Bronze and batch-owned Silver references. Debezium/Kafka/Flink will later turn the documented database insert/update/delete into CDC events and current-state Silver records. Nothing in this Story writes Bronze, Silver, Gold, S3, Iceberg, Snowflake, or dbt.

## Assumptions and deferred decisions

Assumptions: Docker-capable development environment, PostgreSQL default initialization behavior, and fixed test-scale records. Deferred: connector slot/publication config, source permissions, API auth/checkpoint persistence, content-hash policy, full logical replication runtime verification, data volume, and production recovery objectives.
