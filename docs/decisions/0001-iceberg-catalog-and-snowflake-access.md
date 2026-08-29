# ADR-0001: Use an S3-backed HadoopCatalog for V1 Iceberg metadata

**Status:** Accepted for V1 planning and contract implementation

**Story:** MDEP-6

## Context

Spark and Flink must refer to the same Iceberg table metadata, while Snowflake must read the resulting Silver tables. Iceberg requires a catalog to atomically map a table name to its current metadata. The approved V1 scope names S3, Iceberg, Spark, Flink, Snowflake, and dbt, but deliberately avoids adding technology merely for coverage.

## Decision

Use an S3-backed **HadoopCatalog** as the shared V1 Iceberg catalog. The Iceberg warehouse path is `s3://<bucket>/iceberg/<namespace>/<table>/`. Spark and Flink will use that same catalog/table location when their Stories are implemented.

Snowflake will receive read access to Silver Iceberg tables through a Snowflake external volume and object-storage catalog integration that points at the Iceberg metadata in S3. Snowflake/dbt owns Gold models; it does not write back into externally managed Silver Iceberg tables in V1.

## Consequences

- No Glue catalog, metastore service, or additional data platform is introduced.
- Table locations and namespaces must be deterministic and documented before Spark/Flink writers are configured.
- Silver metadata refresh/access in Snowflake is a deliberate integration boundary to validate in MDEP-7.
- Automatic discovery, multi-writer warehouse workflows, and catalog-linked databases are out of scope for V1.

## Alternatives considered

- **AWS Glue Data Catalog:** technically viable, but adds a service not required for the smallest approved V1.
- **Iceberg REST catalog:** viable but introduces another service boundary without adding required concept coverage.
- **Snowflake-managed Iceberg tables:** conflicts with the V1 ownership rule that Spark/Flink share the externally managed Silver tables.

## Validation boundary

This is an implemented design decision, not an operational integration claim. The physical S3 catalog, Spark/Flink configuration, and Snowflake access will be validated by their assigned later Stories.
