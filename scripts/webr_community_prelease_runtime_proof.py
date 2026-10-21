"""Pure, default-off validation of a source-pinned public runtime qualification.

No qualification producer or actual runtime evidence is supplied by this module.
Production callers must select the immutable bundle/contract in reviewed source
before reading a bundle. A file path or owner manifest cannot supply that trust.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import uuid
from urllib.parse import urlsplit


PRELEASE_RUNTIME_SOURCE_SELECTION = None
MAX_BUNDLE_BYTES = 65536
BUNDLE_SCHEMA = "webr.community.prelease-runtime-proof-bundle.v1"
CONTRACT_SCHEMA = "webr.community.prelease-runtime-qualification-contract.v1"
INVENTORY_SCHEMA = "web-r.community.reader-inventory.v1"
SOURCE_FIELDS = frozenset(("data_source_sha", "sql_commit_sha", "workflow_sha256", "materializer_sha256"))
READER_FIELDS = frozenset(("app_service", "inventory_endpoint_sha256", "reader_epoch_uuid",
                           "source_commit_sha", "image_id", "binary_sha256", "qualified_runtime_proof_sha256"))
RECEIPT_FIELDS = frozenset(("schema", "producer_source_proof_sha256", "producer_entry_sha256",
                            "reader_instance", "app_service", "inventory_endpoint_sha256",
                            "reader_epoch_uuid", "source_commit_sha", "image_id", "binary_sha256",
                            "native_proof_sha256", "runtime_proof_sha256", "at"))
PEER_FIELDS = frozenset(("schema", "verdict", "peer_source_sha256", "producer_source_proof_sha256",
                         "producer_receipt_sha256", "reader_instance", "reader_epoch_uuid"))
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SHA1 = re.compile(r"[0-9a-f]{40}\Z")
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,159}\Z")


def _need(value):
    if value is not True:
        raise ValueError("prelease_runtime_proof_refused")


def _fields(value, keys):
    _need(type(value) is dict and set(value) == set(keys))


def _hash(value):
    _need(type(value) is str and _SHA256.fullmatch(value) is not None)
    return value


def _commit(value):
    _need(type(value) is str and _SHA1.fullmatch(value) is not None)
    return value


def _name(value):
    _need(type(value) is str and _NAME.fullmatch(value) is not None)
    return value


def _epoch(value):
    _need(type(value) is str)
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError):
        raise ValueError("prelease_runtime_proof_refused") from None
    _need(str(parsed) == value and parsed.int != 0)
    return value


def canonical_bytes(value):
    """Canonical Go JSON maps, integers and strings; no floats or non-string keys."""
    def check(v, depth=0):
        _need(depth <= 32)
        if v is None or type(v) in (bool, int):
            return
        if type(v) is str:
            try:
                v.encode("utf-8", "strict")
            except UnicodeError:
                raise ValueError("prelease_runtime_proof_refused") from None
            return
        if type(v) is dict:
            _need(all(type(k) is str for k in v))
            for k, item in v.items():
                check(k, depth + 1)
                check(item, depth + 1)
            return
        if type(v) is list:
            for item in v:
                check(item, depth + 1)
            return
        _need(False)
    check(value)
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    for literal, escaped in (("&", "\\u0026"), ("<", "\\u003c"), (">", "\\u003e"),
                             ("\u2028", "\\u2028"), ("\u2029", "\\u2029")):
        text = text.replace(literal, escaped)
    return text.encode("utf-8")


def canonical_sha256(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _source(value):
    _fields(value, SOURCE_FIELDS)
    _commit(value["data_source_sha"])
    _commit(value["sql_commit_sha"])
    _hash(value["workflow_sha256"])
    _hash(value["materializer_sha256"])


def _contract(value):
    _fields(value, ("schema", "producer", "peer", "qualifications"))
    _need(value["schema"] == CONTRACT_SCHEMA)
    producer = value["producer"]
    peer = value["peer"]
    _fields(producer, ("schema", "source_proof_sha256", "entry_sha256"))
    _name(producer["schema"])
    _hash(producer["source_proof_sha256"])
    _hash(producer["entry_sha256"])
    _fields(peer, ("schema", "verdict", "source_sha256"))
    _name(peer["schema"])
    _name(peer["verdict"])
    _hash(peer["source_sha256"])
    qualifications = value["qualifications"]
    _need(type(qualifications) is dict and 1 <= len(qualifications) <= 2)
    for digest, refs in qualifications.items():
        _hash(digest)
        _fields(refs, ("producer_receipt_sha256", "independent_peer_sha256"))
        _hash(refs["producer_receipt_sha256"])
        _hash(refs["independent_peer_sha256"])


def validate_source_selection(value):
    """Validate trusted source operands, never populate them from a bundle/path."""
    _fields(value, ("bundle_sha256", "source_binding", "qualification_contract"))
    _hash(value["bundle_sha256"])
    _source(value["source_binding"])
    _contract(value["qualification_contract"])
    # Return an independent canonical value to prevent later caller mutation.
    return json.loads(canonical_bytes(value))


def _inventory(value):
    _fields(value, ("schema", "reader_inventory_revision", "readers"))
    _need(value["schema"] == INVENTORY_SCHEMA and type(value["reader_inventory_revision"]) is int
          and value["reader_inventory_revision"] > 0)
    rows = value["readers"]
    _need(type(rows) is list and len(rows) == 2)
    result = {}
    for row in rows:
        _fields(row, ("app_service", "instance_id", "inventory_endpoint"))
        _need(row["app_service"] == "web-r")
        instance = _name(row["instance_id"])
        _need(instance not in result)
        endpoint = row["inventory_endpoint"]
        _need(type(endpoint) is str and 0 < len(endpoint.encode("utf-8")) <= 2048)
        parts = urlsplit(endpoint)
        _need(parts.scheme == "https" and bool(parts.hostname) and not parts.username
              and not parts.password and not parts.query and not parts.fragment)
        result[instance] = {"app_service": row["app_service"],
                            "inventory_endpoint_sha256": hashlib.sha256(endpoint.encode()).hexdigest()}
    return result


def _timestamp(value):
    _need(type(value) is str and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?Z", value) is not None)
    try:
        dt.datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        raise ValueError("prelease_runtime_proof_refused") from None


def _qualification(pair, digest, refs, contract, instance, reader):
    _fields(pair, ("producer_receipt", "independent_peer"))
    receipt, peer = pair["producer_receipt"], pair["independent_peer"]
    _fields(receipt, RECEIPT_FIELDS)
    _fields(peer, PEER_FIELDS)
    _need(canonical_sha256(pair) == digest)
    receipt_hash = canonical_sha256(receipt)
    _need(receipt_hash == refs["producer_receipt_sha256"]
          and canonical_sha256(peer) == refs["independent_peer_sha256"])
    producer = contract["producer"]
    reviewer = contract["peer"]
    _need(receipt["schema"] == producer["schema"]
          and receipt["producer_source_proof_sha256"] == producer["source_proof_sha256"]
          and receipt["producer_entry_sha256"] == producer["entry_sha256"])
    _need(peer["schema"] == reviewer["schema"] and peer["verdict"] == reviewer["verdict"]
          and peer["peer_source_sha256"] == reviewer["source_sha256"]
          and peer["producer_source_proof_sha256"] == producer["source_proof_sha256"]
          and peer["producer_receipt_sha256"] == receipt_hash)
    _need(receipt["reader_instance"] == peer["reader_instance"] == instance
          and receipt["reader_epoch_uuid"] == peer["reader_epoch_uuid"] == reader["reader_epoch_uuid"])
    for key in ("app_service", "inventory_endpoint_sha256", "source_commit_sha", "image_id", "binary_sha256"):
        _need(receipt[key] == reader[key])
    _hash(receipt["native_proof_sha256"])
    _hash(receipt["runtime_proof_sha256"])
    _timestamp(receipt["at"])


def validate_runtime_proof_bundle(raw, *, expected_selection, inventory):
    """Return a checked public bundle using caller's trusted source selection.

    This validates immutable source/producer/peer references and exact reader
    tuple joins. Original guarded qualification must first produce those bytes;
    this parser performs no qualification, activation, freshness waiver or IO.
    """
    selected = validate_source_selection(expected_selection)
    readers = _inventory(inventory)
    _need(type(raw) is bytes and 0 < len(raw) <= MAX_BUNDLE_BYTES
          and hashlib.sha256(raw).hexdigest() == selected["bundle_sha256"])
    def pairs(items):
        value = {}
        for key, item in items:
            _need(key not in value)
            value[key] = item
        return value
    def invalid_number(_):
        _need(False)
    try:
        bundle = json.loads(raw.decode("utf-8", "strict"), object_pairs_hook=pairs,
                            parse_constant=invalid_number, parse_float=invalid_number)
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise ValueError("prelease_runtime_proof_refused") from None
    _fields(bundle, ("schema", "reader_inventory_revision", "source_binding", "qualification_contract", "readers", "qualifications"))
    _need(bundle["schema"] == BUNDLE_SCHEMA and type(bundle["reader_inventory_revision"]) is int
          and bundle["reader_inventory_revision"] == inventory["reader_inventory_revision"])
    _need(bundle["source_binding"] == selected["source_binding"]
          and bundle["qualification_contract"] == selected["qualification_contract"])
    mapped = bundle["readers"]
    qualifications = bundle["qualifications"]
    _need(type(mapped) is dict and set(mapped) == set(readers)
          and type(qualifications) is dict)
    referenced = set()
    for instance, reader in mapped.items():
        _fields(reader, READER_FIELDS)
        _need(all(reader[k] == v for k, v in readers[instance].items()))
        _epoch(reader["reader_epoch_uuid"])
        _commit(reader["source_commit_sha"])
        _hash(reader["binary_sha256"])
        image = reader["image_id"]
        _need(type(image) is str and image.startswith("sha256:"))
        _hash(image[7:])
        digest = _hash(reader["qualified_runtime_proof_sha256"])
        _need(digest in qualifications and digest in selected["qualification_contract"]["qualifications"])
        _qualification(qualifications[digest], digest, selected["qualification_contract"]["qualifications"][digest],
                       selected["qualification_contract"], instance, reader)
        referenced.add(digest)
    _need(set(qualifications) == set(selected["qualification_contract"]["qualifications"]) == referenced)
    canonical_bytes(bundle)  # Reject invalid Unicode/unsupported scalar shapes.
    return bundle
