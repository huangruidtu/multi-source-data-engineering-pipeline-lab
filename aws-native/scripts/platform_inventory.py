"""Non-destructive configuration inventory; never calls AWS or deletes resources."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


FLAGS = {
    "create_github_oidc_provider": True,
    "enable_ingestion_lambda": False,
    "enable_glue_silver_job": False,
    "enable_redshift_serverless": False,
    "enable_dms_cdc": False,
    "enable_platform_observability": False,
}
PAID = {
    "enable_ingestion_lambda": "usage-based Lambda and CloudWatch logs",
    "enable_glue_silver_job": "bounded Glue G.1X workers",
    "enable_redshift_serverless": "8-RPU Serverless workgroup with hard usage limit",
    "enable_dms_cdc": "DMS instance, one Kinesis shard, Firehose and logs",
    "enable_platform_observability": "three custom metrics and two alarms",
}


def preview(path: Path) -> dict:
    supplied = json.loads(path.read_text(encoding="utf-8"))
    unknown = sorted(set(supplied) - set(FLAGS))
    if unknown or any(not isinstance(value, bool) for value in supplied.values()):
        raise ValueError(f"invalid preview flags: {unknown or 'values must be booleans'}")
    flags = {**FLAGS, **supplied}
    enabled = [name for name, value in flags.items() if value]
    disabled = [name for name, value in flags.items() if not value]
    return {
        "mode": "NON_DESTRUCTIVE_CONFIGURATION_PREVIEW",
        "observed_billing": False,
        "always_managed": ["encrypted versioned S3 bucket", "KMS key", "AWS Budget", "Glue database", "GitHub plan role"],
        "enabled_flags": enabled,
        "disabled_flags": disabled,
        "cost_assumptions": {name: description for name, description in PAID.items() if flags[name]},
        "cleanup_order": [
            "stop producers and scheduled/manual invocations",
            "disable opt-in compute and streaming flags in a reviewed plan",
            "export evidence and verify exact account, region and resource identities",
            "retain S3, Iceberg, KMS, Terraform state and evidence until separate approval",
            "remove shared or account-global OIDC only after ownership review",
        ],
        "deletion_performed": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    print("AWS06_RESOURCE_PREVIEW=" + json.dumps(preview(args.config), sort_keys=True))


if __name__ == "__main__":
    main()
