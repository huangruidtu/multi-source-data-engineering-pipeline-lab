# Solutions — 03 CDC transport and Debezium
## CT-01
Publication must already exist; disabled auto-create prevents hidden source-scope changes. Preserved slot supports resume but can retain WAL and pressure disk. Monitor slot/LSN/disk in production; live observation is deferred.
## CT-02
`primary_key` returns order ID; `classify_envelope` returns delete and requires null after. Debezium delete is business mutation; enabled tombstone is later Kafka compaction marker, not second deletion.
## CT-03
Likely inactive/lagging replication slot prevents WAL reclamation. Check connector state, slot progress/current WAL LSN, disk, and recovery plan. MDEP config exists; actual connector/slot diagnostics are runtime deferred.
