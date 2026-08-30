# Module 04 — Code walkthrough

Reading order: `source-data/postgres/cdc-init.sql` → `ingestion/cdc/debezium-postgres-connector.json` → `ingestion/cdc/contracts.py` → `tests/test_cdc_contracts.py` → `scripts/validate-mdep-10-cdc.ps1`.

The JSON is the actual connector contract: class, `pgoutput`, publication/slot, `snapshot.mode=initial`, tombstones, exact four-table include list and `topic.prefix=mdep`. `contracts.py` defines expected entity/topic shape without duplicating JSON constants. Its tests read the JSON, protecting the reviewed transaction metadata correction. The script describes registration/mutation evidence but is not a completed run. Notice that MDEP-10 stops at Kafka; MDEP-11 owns applying the envelope.
