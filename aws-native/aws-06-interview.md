# AWS-06 interview explanation

## 45–60 seconds

“I turned the pipeline into a small paved road instead of adding another
framework. A source owner adds one JSON registry entry with an approved contract,
safe path or endpoint environment variable, record limits and ownership. The
runner validates that config, then calls the existing extractor, contract
validator, Parquet serializer and immutable Bronze publisher. In the demo, the
devices file lands two real Parquet rows and a verified commit manifest; replay
is a no-op, a failed manifest write recovers with the same UUID, bad input is
quarantined, and changed content under that UUID is rejected. Every real
boundary emits the same PII-safe JSON event. Optional CloudWatch filters alarm
on failed or conflicting statuses with no high-cardinality dimensions. IAM and
the immutable environment OIDC subject are unchanged, paid services still
default off, and the cleanup command is preview-only—so this improves developer
self-service without weakening the data or deployment controls.”

## Authentic demo

```text
uv run --project aws-native --locked python aws-native/scripts/run_platform_demo.py
uv run --project aws-native --locked python aws-native/scripts/platform_inventory.py --config aws-native/config/sandbox-preview.json
```

The first command prints `AWS06_PLATFORM_EVIDENCE`; the second prints a
non-destructive configuration inventory, not observed billing.

## Code anchors

- `config/source-registry.json` and `mdep_platform/source-registry.schema.json`
- `mdep_platform/registry.py`, `observability.py` and `recovery.py`
- `mdep_ingestion/pipeline.py` public completion/rejection seams
- `terraform/observability.tf` and `terraform/tests/security.tftest.hcl`
- `tests/test_aws06_platform.py`
