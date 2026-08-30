import unittest
from processing.flink.cdc_model import apply_current_state, parse_debezium, parse_lsn

def event(op="u", lsn="0/10", offset=1, value=None):
    payload={"op":op,"before":{"customer_id":"c1"},"after": value if value is not None else ({"customer_id":"c1","preferred_language":"en"} if op!="d" else None),"source":{"lsn":lsn,"txId":7,"ts_ms":"2025-01-01T00:00:00Z"}}
    import json; return parse_debezium(json.dumps({"customer_id":"c1"}),json.dumps(payload),"mdep.commerce.customers",0,offset)

class FlinkCdcTests(unittest.TestCase):
 def test_lsn_and_envelope(self): self.assertGreater(parse_lsn("0/11"),parse_lsn("0/10")); self.assertEqual("c1",event().primary_key)
 def test_upsert_duplicate_stale_and_delete(self):
  state={}; versions={}; self.assertEqual("upserted",apply_current_state(state,versions,event()))
  self.assertEqual("stale_or_duplicate_ignored",apply_current_state(state,versions,event()))
  self.assertEqual("stale_or_duplicate_ignored",apply_current_state(state,versions,event(lsn="0/F",offset=9)))
  self.assertEqual("deleted",apply_current_state(state,versions,event(op="d",lsn="0/11",offset=2))); self.assertNotIn("c1",state)
 def test_tombstone_and_bad_delete(self):
  self.assertEqual("tombstone_ignored",apply_current_state({}, {}, None))
  with self.assertRaises(ValueError): event(op="d", value={"customer_id":"c1"})
