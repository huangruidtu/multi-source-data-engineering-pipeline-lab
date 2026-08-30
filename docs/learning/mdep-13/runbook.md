# MDEP-13 runbook

1. Run `./scripts/preflight-mdep-13.ps1` and save its concise capability report.
2. Run `./scripts/validate-mdep-13-e2e.ps1` first. It safely runs Python tests when available and records runtime paths as NOT_RUN unless explicitly enabled.
3. On a disposable runtime host, provide `-RunRuntime`; supply `-BronzeRoot` and `-Warehouse` for Spark and `-Bucket` for Flink/S3. Do not put secrets on the command line or evidence logs.
4. For each completed stage, copy the actual result into the matching matrix/evidence entry. For blocked stages, retain the blocker and do not transition historical debt.
5. Execute the key-level anti-joins and quality checks in `validation/reconciliation/`, save outputs under the run id, and investigate exceptions before declaring reconciliation successful.

Expected evidence layout is documented in `validation/evidence/README.md`. The runner is resumable by choosing a new run id; it does not delete prior evidence.
