"""Validate the Terraform-built Lambda source archive without deploying it."""

from __future__ import annotations

import sys
import zipfile
from pathlib import Path


AWS_ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = AWS_ROOT / "terraform" / ".terraform" / "bronze_ingestion.zip"
REQUIRED = {
    "bronze_ingestion.py",
    "mdep_ingestion/__init__.py",
    "mdep_ingestion/extractors.py",
    "mdep_ingestion/models.py",
    "mdep_ingestion/object_store.py",
    "mdep_ingestion/pipeline.py",
    "mdep_ingestion/publication.py",
    "mdep_ingestion/runtime.py",
    "mdep_ingestion/serialization.py",
}


def main() -> int:
    if not ARCHIVE.is_file():
        raise SystemExit(f"Lambda archive does not exist; run terraform test first: {ARCHIVE}")
    with zipfile.ZipFile(ARCHIVE) as archive:
        names = set(archive.namelist())
        missing = sorted(REQUIRED.difference(names))
        if missing:
            raise SystemExit(f"Lambda archive is missing: {missing}")
        for name in sorted(REQUIRED):
            compile(archive.read(name), name, "exec")
    print(f"PASS: {ARCHIVE.name} contains {len(names)} source files; required modules compile.")
    print("NOTE: PyArrow and psycopg binaries are deployment-gated Lambda layer dependencies.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
