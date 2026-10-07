import base64
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

import verify_community_generation_cdn as verifier
from export_community_cdn import (
    GENERATION_MANIFEST_SCHEMA,
    GENERATION_PROOF_SCHEMA,
    encrypt_document,
    go_canonical_json_bytes,
)


class FakeResponse:
    def __init__(self, url: str, body: bytes, commit: str) -> None:
        self.url = url
        self.body = body
        self.status = 200
        self.offset = 0
        self.headers = {"x-jsd-version": commit, "x-jsd-version-type": "commit"}

    def __enter__(self):  # noqa: ANN001
        return self

    def __exit__(self, *args):  # noqa: ANN002, ANN001
        return False

    def read(self, limit: int) -> bytes:
        chunk = self.body[self.offset:self.offset + limit]
        self.offset += len(chunk)
        return chunk

    def geturl(self) -> str:
        return self.url


class FakeOpener:
    def __init__(self, bodies: dict[str, bytes], commit: str) -> None:
        self.bodies = bodies
        self.commit = commit

    def open(self, request, timeout: int):  # noqa: ANN001
        url = request.full_url
        return FakeResponse(url, self.bodies[url], self.commit)


class CommunityGenerationCDNVerifyTest(unittest.TestCase):
    def test_encrypted_v2_index_and_proof_require_exact_immutable_remote_bytes(self) -> None:
        generation = "2026-09-27 00:00:00.123456"
        uuid = "00000000-0000-0000-0000-000000000001"
        items = {uuid: {"uuid": uuid, "title": "<notice>&", "path": "community/ko/rcommunity/x.json"}}
        proof = {
            "schema": GENERATION_PROOF_SCHEMA,
            "generation": generation,
            "complete": True,
            "item_count": 1,
            "identity_hash": hashlib.sha256(uuid.encode()).hexdigest(),
            "content_hash": hashlib.sha256(go_canonical_json_bytes(items)).hexdigest(),
            "withdrawal_revision": 1,
            "account_authority_revision": 2,
            "category_authority_revision": 3,
        }
        manifest = {
            "schema": GENERATION_MANIFEST_SCHEMA,
            "language": "ko",
            **{key: value for key, value in proof.items() if key != "schema"},
            "generated_at": "2026-09-27T00:00:00+00:00",
            "items": items,
        }
        key = b"x" * 32
        path = "community/ko/index.json"
        encrypted = encrypt_document(manifest, key, path, "ko", "")
        nonce = base64.urlsafe_b64decode(encrypted["nonce"] + "==")
        ciphertext = base64.urlsafe_b64decode(encrypted["ciphertext"] + "==")
        self.assertEqual(
            json.loads(AESGCM(key).decrypt(nonce, ciphertext, path.encode())),
            manifest,
        )

        proof_bytes = json.dumps(proof, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        index_bytes = json.dumps(encrypted, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        commit = "a" * 40
        base = f"https://cdn.jsdelivr.net/gh/statground/web-r_CDN2_community@{commit}"
        proof_url = base + "/community/ko/generation-proof.json"
        index_url = base + "/" + path
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            proof_path = root / "generation-proof.json"
            index_path = root / "index.json"
            proof_path.write_bytes(proof_bytes)
            index_path.write_bytes(index_bytes)
            with mock.patch.object(
                verifier.urllib.request,
                "build_opener",
                return_value=FakeOpener({proof_url: proof_bytes, index_url: index_bytes}, commit),
            ):
                result = verifier.verify(base, proof_path, [(path, index_path)], 1, 0)
            self.assertEqual(result["status"], "verified")
            self.assertEqual(result["asset_count"], 1)

            corrupted_index = index_bytes.replace(b'"ciphertext":"', b'"ciphertext":"X', 1)
            with mock.patch.object(
                verifier.urllib.request,
                "build_opener",
                return_value=FakeOpener({proof_url: proof_bytes, index_url: corrupted_index}, commit),
            ):
                with self.assertRaisesRegex(ValueError, "immutable CDN asset differs"):
                    verifier.verify(base, proof_path, [(path, index_path)], 1, 0)

            corrupted_proof = json.dumps(
                {**proof, "content_hash": "c" * 64}, sort_keys=True, separators=(",", ":")
            ).encode() + b"\n"
            with mock.patch.object(
                verifier.urllib.request,
                "build_opener",
                return_value=FakeOpener({proof_url: corrupted_proof, index_url: index_bytes}, commit),
            ):
                with self.assertRaisesRegex(ValueError, "immutable community proof verification failed"):
                    verifier.verify(base, proof_path, [(path, index_path)], 1, 0)


class WorkshopCompletePreflightTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.key = b'w' * 32
        self.commit = 'b' * 40
        self.base = f'https://cdn.jsdelivr.net/gh/statground/web-r_CDN2_community@{self.commit}'
        self.items = {}
        self.posts = {}
        for i in range(41):
            identity = f'rconf-source-{i}'
            self.items[identity] = {
                'uuid': identity, 'slug': identity, 'board_key': identity, 'language': 'ko',
                'title': f'공개 Workshop {i}', 'path': f'community/ko/workshop/curated/{identity}.json',
                'base_url': '', 'active': True, 'capacity': 0,
            }
            self.posts[identity] = [{'uuid': f'post-{i}', 'workshop_key': identity, 'title': '공개', 'active': True}]
        self.write_catalog()
        self.proof = self.root / 'generation-proof.json'
        self.proof.write_text(json.dumps({
            'schema': GENERATION_PROOF_SCHEMA, 'generation': '2026-09-27 00:00:00.123456',
            'complete': True, 'item_count': 1, 'identity_hash': 'a' * 64, 'content_hash': 'c' * 64,
            'withdrawal_revision': 1, 'account_authority_revision': 2, 'category_authority_revision': 3,
        }))
        index = self.root / 'community/ko/index.json'
        index.parent.mkdir(parents=True, exist_ok=True)
        index.write_bytes(b'{"existing":"index"}')
        self.assets = [('community/ko/index.json', index),
                       ('community/ko/workshop/index.json', self.root / 'community/ko/workshop/index.json')]
        self.refresh_remote()

    def write_document(self, path: str, plain: dict, identity: str = '') -> None:
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(encrypt_document(plain, self.key, path, 'ko', identity), ensure_ascii=False))

    def write_catalog(self) -> None:
        from export_community_cdn import WORKSHOP_CONTENT_SCHEMA, WORKSHOP_MANIFEST_SCHEMA, workshop_export_proof
        for identity, item in self.items.items():
            self.write_document(item['path'], {'schema': WORKSHOP_CONTENT_SCHEMA, 'workshop': item,
                                               'posts': self.posts[identity]}, identity)
        proof = workshop_export_proof(self.items, self.posts)
        self.write_document('community/ko/workshop/index.json', {
            'schema': WORKSHOP_MANIFEST_SCHEMA, 'language': 'ko', 'generated_at': '2026-10-07T00:00:00Z',
            'catalog_token': proof['catalog_token'], 'items': self.items,
        })

    def refresh_remote(self) -> None:
        self.remote = {self.base + '/' + path.relative_to(self.root).as_posix(): path.read_bytes()
                       for path in self.root.rglob('*.json')}
        self.remote[self.base + '/community/ko/generation-proof.json'] = self.proof.read_bytes()

    def run_verify(self, opener=None) -> dict:
        import inspect
        # The same causal fixture runs against the original index-only verifier.
        kwargs = {'workshop_root': self.root, 'workshop_key': self.key} if 'workshop_root' in inspect.signature(verifier.verify).parameters else {}
        with mock.patch.object(verifier.urllib.request, 'build_opener', return_value=opener or FakeOpener(self.remote, self.commit)):
            return verifier.verify(self.base, self.proof, self.assets, 1, 0, **kwargs)

    def test_index_success_cannot_hide_missing_unchanged_detail(self) -> None:
        import urllib.error
        missing = self.base + '/' + self.items['rconf-source-7']['path']
        del self.remote[missing]
        owner = self
        class MissingDetail(FakeOpener):
            def open(self, request, timeout):
                if request.full_url == missing:
                    raise urllib.error.URLError('synthetic unavailable detail')
                return super().open(request, timeout)
        with self.assertRaisesRegex(ValueError, 'complete immutable inventory'):
            self.run_verify(MissingDetail(owner.remote, owner.commit))

    def test_complete_catalog_verifies_every_object_with_four_joined_workers(self) -> None:
        import threading
        import time
        owner = self
        class ConcurrentDetails(FakeOpener):
            def __init__(self):
                super().__init__(owner.remote, owner.commit)
                self.active = self.maximum = 0
                self.paths = []
                self.lock = threading.Lock()
            def open(self, request, timeout):
                detail = '/workshop/curated/' in request.full_url
                with self.lock:
                    self.paths.append(request.full_url)
                    self.active += int(detail)
                    self.maximum = max(self.maximum, self.active)
                try:
                    if detail:
                        time.sleep(0.02)
                    return super().open(request, timeout)
                finally:
                    with self.lock:
                        self.active -= int(detail)
        opener = ConcurrentDetails()
        result = self.run_verify(opener)
        self.assertTrue(result['workshop_complete'])
        self.assertEqual(result['workshop_item_count'], len(self.items))
        self.assertEqual(result['workshop_verified_objects'], len(self.items) + 1)
        self.assertEqual(sum('/workshop/curated/' in path for path in opener.paths), len(self.items))
        self.assertEqual(opener.maximum, 4)
        self.assertEqual(opener.active, 0)
        self.assertEqual(verifier.VerificationBudget(8, 5).seconds, 825)

    def test_remote_detail_body_header_and_slow_fetch_fail_closed(self) -> None:
        import urllib.error
        path = self.base + '/' + self.items['rconf-source-0']['path']
        for mode in ('body', 'version', 'type', 'redirect', 'slow'):
            owner = self
            class Drift(FakeOpener):
                def open(self, request, timeout):
                    if request.full_url == path and mode == 'slow':
                        raise TimeoutError('synthetic original transport timeout')
                    response = super().open(request, timeout)
                    if request.full_url == path:
                        if mode == 'body': response.body = b'{}'
                        if mode == 'version': response.headers['x-jsd-version'] = 'c' * 40
                        if mode == 'type': response.headers['x-jsd-version-type'] = 'branch'
                        if mode == 'redirect': response.url = owner.base + '/other.json'
                    return response
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                self.run_verify(Drift(self.remote, self.commit))

    def test_descriptor_crypto_content_and_type_denials_before_network(self) -> None:
        import copy
        original = copy.deepcopy(self.items)
        for mode in ('uri', 'traversal', 'duplicate', 'uuid', 'type', 'base'):
            self.items = copy.deepcopy(original)
            item = self.items['rconf-source-0']
            if mode == 'uri': item['path'] = 'https://example.com/private.json'
            if mode == 'traversal': item['path'] = 'community/ko/workshop/curated/../../escape.json'
            if mode == 'duplicate': item['path'] = self.items['rconf-source-1']['path']
            if mode == 'uuid': item['uuid'] = 'other'
            if mode == 'type': item['capacity'] = True
            if mode == 'base': item['base_url'] = self.base.replace(self.commit, 'c' * 40)
            # Do not write unsafe paths; only the authenticated descriptor is changed.
            from export_community_cdn import WORKSHOP_MANIFEST_SCHEMA, workshop_export_proof
            self.write_document('community/ko/workshop/index.json', {
                'schema': WORKSHOP_MANIFEST_SCHEMA, 'language': 'ko', 'generated_at': '2026-10-07T00:00:00Z',
                'catalog_token': workshop_export_proof(self.items, self.posts)['catalog_token'], 'items': self.items,
            })
            opener = mock.Mock()
            with self.subTest(mode=mode), self.assertRaises(ValueError): self.run_verify(opener)
            opener.open.assert_not_called()
        self.items = original
        self.write_catalog()
        path = self.root / self.items['rconf-source-0']['path']
        doc = json.loads(path.read_text())
        doc['ciphertext'] = 'invalid'
        path.write_text(json.dumps(doc))
        with self.assertRaisesRegex(ValueError, 'decryption'): self.run_verify()

    def test_changed_board_posts_count_token_and_runtime_size_are_denied(self) -> None:
        from export_community_cdn import WORKSHOP_CONTENT_SCHEMA, WORKSHOP_MANIFEST_SCHEMA
        identity = 'rconf-source-0'
        item = self.items[identity]
        for mode in ('identity', 'post_board', 'token', 'size', 'detail_active_type', 'detail_capacity_type'):
            self.write_catalog()
            if mode == 'identity':
                self.write_document(item['path'], {'schema': WORKSHOP_CONTENT_SCHEMA, 'workshop': {**item, 'board_key': 'other'}, 'posts': self.posts[identity]}, identity)
            if mode.startswith('detail_'):
                bad = {**item, 'active': 1} if mode == 'detail_active_type' else {**item, 'capacity': False}
                self.write_document(item['path'], {'schema': WORKSHOP_CONTENT_SCHEMA, 'workshop': bad, 'posts': self.posts[identity]}, identity)
            if mode == 'post_board':
                self.write_document(item['path'], {'schema': WORKSHOP_CONTENT_SCHEMA, 'workshop': item, 'posts': [{**self.posts[identity][0], 'workshop_key': 'other'}]}, identity)
            if mode == 'token':
                self.write_document('community/ko/workshop/index.json', {'schema': WORKSHOP_MANIFEST_SCHEMA, 'language': 'ko', 'generated_at': 'now', 'catalog_token': '0' * 64, 'items': self.items})
            if mode == 'size': (self.root / item['path']).write_bytes(b'x' * (verifier.MAX_WORKSHOP_BYTES + 1))
            self.refresh_remote()
            opener = mock.Mock(wraps=FakeOpener(self.remote, self.commit))
            with self.subTest(mode=mode), self.assertRaises(ValueError): self.run_verify(opener)
            opener.open.assert_not_called()

    def test_shared_budget_and_cancellation_never_return_partial_success(self) -> None:
        from export_community_cdn import WORKSHOP_CONTENT_SCHEMA
        inventory, _ = verifier.workshop_inventory(self.root, self.key, self.base)
        for mode in ('cancel', 'expired'):
            budget = verifier.VerificationBudget(1, 0)
            if mode == 'cancel': budget.cancelled.set()
            else: budget.deadline = 0
            opener = mock.Mock()
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                verifier.verify_workshop_inventory(opener, self.base, self.commit, inventory, 1, 0, budget)
            opener.open.assert_not_called()
        # Successful slow chunks must not defer cancellation to end-of-body.
        import time
        path, body = inventory[0]
        response = FakeResponse(self.base + '/' + path, body, self.commit)
        calls = []
        def drip(limit):
            calls.append(limit)
            time.sleep(0.008)
            return response.read(1)
        response.read1 = drip
        opener = mock.Mock()
        opener.open.return_value = response
        budget = verifier.VerificationBudget(1, 0)
        budget.deadline = time.monotonic() + 0.03
        with self.assertRaises(ValueError):
            verifier.verify_workshop_inventory(opener, self.base, self.commit, [inventory[0]], 1, 0, budget)
        self.assertTrue(calls)
        self.assertLess(len(calls), len(body))
        joined_calls = len(calls)
        time.sleep(0.01)
        self.assertEqual(len(calls), joined_calls)

    def test_workflow_requires_full_preflight_before_native_reader_mutation(self) -> None:
        workflow = (Path(__file__).resolve().parents[1] / '.github/workflows/r-project-all.yml').read_text()
        gate = workflow.index('--workshop-root web-r_CDN2_community')
        self.assertLess(gate, workflow.index('- name: Preflight exact application reader transition'))
        self.assertLess(gate, workflow.index('- name: Publish and reopen exact application readers'))
        block = workflow[workflow.rfind('- name:', 0, gate):gate]
        self.assertIn('R_ECOSYSTEM_CONTENT_KEY: ${{ secrets.R_ECOSYSTEM_CONTENT_KEY }}', block)
        # The untouched historical bootstrap remains a stdlib-only caller.
        import builtins
        import importlib.util
        original_import = builtins.__import__
        def no_crypto(name, *args, **kwargs):
            if name.startswith('cryptography') or name == 'export_community_cdn':
                raise ImportError('legacy caller has no optional crypto dependencies')
            return original_import(name, *args, **kwargs)
        spec = importlib.util.spec_from_file_location('legacy_verifier_compatibility', Path(verifier.__file__))
        module = importlib.util.module_from_spec(spec)
        with mock.patch('builtins.__import__', side_effect=no_crypto):
            spec.loader.exec_module(module)
            with mock.patch.object(module.urllib.request, 'build_opener', return_value=FakeOpener(self.remote, self.commit)):
                result = module.verify(self.base, self.proof, self.assets, 1, 0)
        self.assertEqual(result['status'], 'verified')


if __name__ == "__main__":
    unittest.main()
