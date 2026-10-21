"""Explicitly synthetic qualification contracts; never actual runtime evidence."""
import copy
import hashlib
import json
from pathlib import Path
import unittest

import webr_community_prelease_runtime_proof as P


def fixture():
    inventory = {"schema": P.INVENTORY_SCHEMA, "reader_inventory_revision": 7, "readers": []}
    source = {"data_source_sha": "a" * 40, "sql_commit_sha": "b" * 40,
              "workflow_sha256": "c" * 64, "materializer_sha256": "d" * 64}
    contract = {"schema": P.CONTRACT_SCHEMA,
                "producer": {"schema": "fixture.readonly-runtime-qualification.v1",
                             "source_proof_sha256": "e" * 64, "entry_sha256": "f" * 64},
                "peer": {"schema": "fixture.independent-runtime-peer.v1", "verdict": "FIXTURE_PASS_ONLY",
                         "source_sha256": "1" * 64}, "qualifications": {}}
    mapped, qualifications = {}, {}
    for ordinal, host in enumerate(("testgo", "www"), 1):
        instance = host + ":community"
        endpoint = "https://" + host + ".web-r.org/internal/community-publication/inventory"
        inventory["readers"].append({"app_service": "web-r", "instance_id": instance, "inventory_endpoint": endpoint})
        row = {"app_service": "web-r", "inventory_endpoint_sha256": hashlib.sha256(endpoint.encode()).hexdigest(),
               "reader_epoch_uuid": "00000000-0000-4000-8000-00000000000" + str(ordinal),
               "source_commit_sha": str(ordinal) * 40, "image_id": "sha256:" + str(ordinal + 2) * 64,
               "binary_sha256": str(ordinal + 4) * 64}
        receipt = {"schema": contract["producer"]["schema"],
                   "producer_source_proof_sha256": contract["producer"]["source_proof_sha256"],
                   "producer_entry_sha256": contract["producer"]["entry_sha256"], "reader_instance": instance,
                   **row, "native_proof_sha256": "7" * 64, "runtime_proof_sha256": "8" * 64,
                   "at": "2026-10-22T00:00:00.123456789Z"}
        peer = {"schema": contract["peer"]["schema"], "verdict": contract["peer"]["verdict"],
                "peer_source_sha256": contract["peer"]["source_sha256"],
                "producer_source_proof_sha256": contract["producer"]["source_proof_sha256"],
                "producer_receipt_sha256": P.canonical_sha256(receipt), "reader_instance": instance,
                "reader_epoch_uuid": row["reader_epoch_uuid"]}
        pair = {"producer_receipt": receipt, "independent_peer": peer}
        digest = P.canonical_sha256(pair)
        row["qualified_runtime_proof_sha256"] = digest
        qualifications[digest] = pair
        contract["qualifications"][digest] = {"producer_receipt_sha256": P.canonical_sha256(receipt),
                                            "independent_peer_sha256": P.canonical_sha256(peer)}
        mapped[instance] = row
    bundle = {"schema": P.BUNDLE_SCHEMA, "reader_inventory_revision": 7, "source_binding": source,
              "qualification_contract": contract, "readers": mapped, "qualifications": qualifications}
    raw = P.canonical_bytes(bundle) + b"\n"
    selected = {"bundle_sha256": hashlib.sha256(raw).hexdigest(), "source_binding": copy.deepcopy(source),
                "qualification_contract": copy.deepcopy(contract)}
    return bundle, raw, selected, inventory


class Controls(unittest.TestCase):
    def test_source_pinned_bundle_and_provenance(self):
        bundle, raw, selected, inventory = fixture()
        self.assertIsNone(P.PRELEASE_RUNTIME_SOURCE_SELECTION)
        self.assertEqual(P.validate_runtime_proof_bundle(raw, expected_selection=selected, inventory=inventory), bundle)
        # Unbound production selection rejects before inspecting the raw operand.
        with self.assertRaises(ValueError):
            P.validate_runtime_proof_bundle(object(), expected_selection=None, inventory=inventory)
        self.assertEqual(P.canonical_bytes({"html": "<&>\u2028\u2029", "utf8": "한글"}),
                         b'{"html":"\\u003c\\u0026\\u003e\\u2028\\u2029","utf8":"' + "한글".encode() + b'"}')
        for kind in ("immutable_bytes", "source", "inventory_revision", "reader_keyset", "endpoint", "epoch",
                     "peer_verdict", "producer_source", "proof_digest", "success_boolean", "extra_pair"):
            with self.subTest(kind=kind):
                changed = copy.deepcopy(bundle)
                if kind == "immutable_bytes":
                    bad = raw + b" "
                else:
                    first = next(iter(changed["readers"]))
                    digest = changed["readers"][first]["qualified_runtime_proof_sha256"]
                    pair = changed["qualifications"][digest]
                    if kind == "source": changed["source_binding"]["sql_commit_sha"] = "9" * 40
                    if kind == "inventory_revision": changed["reader_inventory_revision"] = True
                    if kind == "reader_keyset": changed["readers"].pop(first)
                    if kind == "endpoint": changed["readers"][first]["inventory_endpoint_sha256"] = "9" * 64
                    if kind == "epoch": changed["readers"][first]["reader_epoch_uuid"] = "00000000-0000-4000-8000-000000000009"
                    if kind == "peer_verdict": pair["independent_peer"]["verdict"] = "CALLER_TRUE"
                    if kind == "producer_source": pair["producer_receipt"]["producer_entry_sha256"] = "9" * 64
                    if kind == "proof_digest": changed["readers"][first]["qualified_runtime_proof_sha256"] = "9" * 64
                    if kind == "success_boolean": pair["producer_receipt"]["qualified"] = True
                    if kind == "extra_pair": changed["qualifications"]["9" * 64] = pair
                    bad = P.canonical_bytes(changed)
                # Give the immutable-transport gate the changed SHA except the
                # byte-mutation case, so deeper tuple/provenance gates run too.
                expected = copy.deepcopy(selected)
                if kind != "immutable_bytes": expected["bundle_sha256"] = hashlib.sha256(bad).hexdigest()
                with self.assertRaises(ValueError):
                    P.validate_runtime_proof_bundle(bad, expected_selection=expected, inventory=inventory)
        for bad in (raw.replace(b'"schema":', b'"schema":null,"schema":', 1), b'{"x":NaN}', b'{"x":1.5}', b'{"x":"\xff"}'):
            expected = copy.deepcopy(selected); expected["bundle_sha256"] = hashlib.sha256(bad).hexdigest()
            with self.assertRaises(ValueError):
                P.validate_runtime_proof_bundle(bad, expected_selection=expected, inventory=inventory)
        with self.assertRaises(ValueError):
            P._timestamp("2026-10-22T00:00:00.1٢Z")
        self.assertEqual(selected["qualification_contract"], bundle["qualification_contract"])


if __name__ == "__main__":
    unittest.main()
