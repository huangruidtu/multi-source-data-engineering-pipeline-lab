# MDEP-13 implementation guide

## Implemented

MDEP-13 adds `validation/mdep-13-validation-matrix.yml`, the 20-case failure catalogue, quality gates, reconciliation SQL templates, and `scripts/preflight-mdep-13.ps1` plus `scripts/validate-mdep-13-e2e.ps1`. The runner creates a local ignored evidence directory and records each stage as PASSED, FAILED, BLOCKED, or NOT_RUN; it never turns an unavailable integration into PASSED.

## How to use it

```powershell
.scriptspreflight-mdep-13.ps1
.scriptsalidate-mdep-13-e2e.ps1
.scriptsalidate-mdep-13-e2e.ps1 -RunRuntime -BronzeRoot '<uri>' -Warehouse '<uri>' -Bucket '<bucket>'
```

Use `-RunRuntime` only in a disposable environment with the required endpoints and credentials. Inspect `validation/evidence/<run-id>/validation-summary.json`, logs, and saved reconciliation output. Runtime evidence is ignored by Git to avoid secrets and unstable machine-specific artifacts.

## Files changed by this Story

- `validation/`: matrix, failures, gates, evidence contract, and reconciliation templates.
- `scripts/`: capability preflight and master validator.
- `docs/project-evidence.md`: factual status table and final Mermaid architecture.
- `docs/interview/`: short interview pack.
- `tests/test_mdep13_validation_framework.py`: static framework contracts.

## Deferred

This implementation does not execute Docker, Spark, Flink, S3, Snowflake, or dbt. The matrix carries MDEP-8 through MDEP-12 debt forward rather than declaring it resolved.
