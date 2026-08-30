from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).with_name("generate_webr_notebook_daily.py")
sys.path.insert(0, str(SCRIPT.parent))
SPEC = importlib.util.spec_from_file_location("generate_webr_notebook_daily", SCRIPT)
assert SPEC and SPEC.loader
notebook = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = notebook
SPEC.loader.exec_module(notebook)


def row_from_spec(spec: dict) -> dict:
    return {
        "title": spec["title"],
        "description": spec["description"],
        "data_markdown": json.dumps([cell for cell in spec["cells"] if cell["mode"] == "markdown"], ensure_ascii=False),
        "data_rcode": json.dumps([cell for cell in spec["cells"] if cell["mode"] == "r"], ensure_ascii=False),
        "data_meta": json.dumps(
            {
                "topic": spec["topic"]["key"],
                "style": spec["style"]["key"],
                "blueprint": spec["blueprint"],
            },
            ensure_ascii=False,
        ),
    }


class NotebookDiversityTest(unittest.TestCase):
    def test_clickhouse_history_read_retries_url_error(self) -> None:
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b'{"value":1}\n'
        env = {
            "CH_HOST": "clickhouse.test",
            "CH_PORT": "8123",
            "CH_USER": "app",
            "CH_PASSWORD": "secret",
            "WEBR_NOTEBOOK_DAILY_READ_ATTEMPTS": "2",
            "WEBR_NOTEBOOK_DAILY_READ_BACKOFF_SECONDS": "0",
        }

        with mock.patch.object(
            notebook.urllib.request,
            "urlopen",
            side_effect=[urllib.error.URLError("temporary timeout"), response],
        ) as urlopen:
            rows = notebook.clickhouse_json_each_row(env, "SELECT 1 FORMAT JSONEachRow")

        self.assertEqual([{"value": 1}], rows)
        self.assertEqual(2, urlopen.call_count)

    def test_clickhouse_history_read_retries_builtin_timeout(self) -> None:
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b'{"value":1}\n'
        env = {
            "CH_HOST": "clickhouse.test",
            "CH_PORT": "8123",
            "CH_USER": "app",
            "CH_PASSWORD": "secret",
            "WEBR_NOTEBOOK_DAILY_READ_ATTEMPTS": "2",
            "WEBR_NOTEBOOK_DAILY_READ_BACKOFF_SECONDS": "0",
        }

        with mock.patch.object(
            notebook.urllib.request,
            "urlopen",
            side_effect=[TimeoutError("timed out"), response],
        ) as urlopen:
            rows = notebook.clickhouse_json_each_row(env, "SELECT 1 FORMAT JSONEachRow")

        self.assertEqual([{"value": 1}], rows)
        self.assertEqual(2, urlopen.call_count)

    def test_clickhouse_history_read_does_not_retry_type_contract_error(self) -> None:
        detail = "Code: 386. DB::Exception: There is no supertype for types String, UUID. (NO_COMMON_TYPE)"
        self.assertFalse(notebook.is_retryable_clickhouse_insert_error(500, detail))

    def test_data_rpackage_compatibility_keeps_package_names_only(self) -> None:
        self.assertEqual(
            ["Matrix", "jsonlite"],
            notebook.notebook_package_names(
                [
                    {"package": "Matrix", "version": "1.7-4"},
                    {"package": "jsonlite", "version": "2.0.0"},
                    {"package": "Matrix", "version": "1.7-4"},
                    "",
                ]
            ),
        )

    def test_batch_history_is_excluded_before_distributed_visibility(self) -> None:
        fingerprint = "a" * 64
        result = {
            "title": "이미 생성된 배치 제목",
            "series_date": "2026-07-12",
            "blueprint_fingerprint": fingerprint,
            "data_design": "clustered-sample",
            "validation_lens": "bootstrap-stability",
            "visual_grammar": "calibration-curve",
            "narrative_frame": "communication",
            "webr_package": "jsonlite",
            "webr_package_profile": "jsonlite-roundtrip",
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "batch.jsonl"
            path.write_text(json.dumps(result, ensure_ascii=False) + "\n", encoding="utf-8")
            titles: set[str] = set()
            recent: list[dict[str, object]] = []
            fingerprints: set[str] = set()
            count = notebook.merge_batch_history_exclusions(
                path,
                existing_titles=titles,
                recent_rows=recent,
                published_blueprints=fingerprints,
            )

        self.assertEqual(1, count)
        self.assertIn(result["title"], titles)
        self.assertIn(fingerprint, fingerprints)
        meta = json.loads(str(recent[0]["data_meta"]))
        self.assertEqual("jsonlite-roundtrip", meta["blueprint"]["package_profile"]["key"])

    def test_blueprint_space_covers_one_daily_post_for_one_hundred_years(self) -> None:
        blueprint = notebook.build_diversity_blueprint(notebook.TOPICS[0], notebook.STYLES[0], 12345, 0)
        self.assertGreaterEqual(blueprint["space_size_per_topic"], 36525)
        self.assertRegex(blueprint["fingerprint"], r"^[0-9a-f]{64}$")

    def test_five_same_day_specs_have_distinct_materialized_blueprints(self) -> None:
        titles: set[str] = set()
        recent_rows: list[dict] = []
        fingerprints: set[str] = set()
        specs: list[dict] = []
        dimension_values = {
            key: set()
            for key in ("data_design", "validation_lens", "visual_grammar", "narrative_frame", "package_profile")
        }
        for _ in range(5):
            spec = notebook.build_notebook_spec(
                "2026-07-12",
                titles,
                recent_rows,
                topic_pool=notebook.TOPICS,
                force_new=True,
                published_blueprints=fingerprints,
            )
            fingerprint = spec["blueprint"]["fingerprint"]
            self.assertNotIn(fingerprint, fingerprints)
            self.assertEqual(3, sum(cell["mode"] == "r" for cell in spec["cells"]))
            self.assertIn(spec["blueprint"]["validation_lens"]["label"], spec["cells"][4]["source"])
            self.assertEqual([spec["blueprint"]["package_profile"]["package"]], spec["required_packages"])
            self.assertIn("package calculation:", spec["cells"][5]["source"])
            fingerprints.add(fingerprint)
            for dimension, values in dimension_values.items():
                values.add(spec["blueprint"][dimension]["key"])
            titles.add(spec["title"])
            recent_rows.insert(0, row_from_spec(spec))
            specs.append(spec)
        self.assertEqual(5, len({spec["title"] for spec in specs}))
        self.assertEqual(5, len(fingerprints))
        for dimension, values in dimension_values.items():
            self.assertEqual(5, len(values), dimension)

    def test_recent_dimension_guards_are_applied_before_candidate_sampling(self) -> None:
        recent_keys = {
            "data_design": {item.key for item in notebook.DATA_DESIGNS[:5]},
            "validation_lens": {item.key for item in notebook.VALIDATION_LENSES[:5]},
            "visual_grammar": {item.key for item in notebook.VISUAL_GRAMMARS[:5]},
            "narrative_frame": {item.key for item in notebook.NARRATIVE_FRAMES[:5]},
            "package_profile": {item.key for item in notebook.WEBR_PACKAGE_PROFILES[:8]},
        }
        pools = notebook.eligible_blueprint_dimension_pools(recent_keys)

        for dimension, excluded in recent_keys.items():
            self.assertTrue(pools[dimension], dimension)
            self.assertTrue({item.key for item in pools[dimension]}.isdisjoint(excluded), dimension)

        blueprint = notebook.build_diversity_blueprint(
            notebook.TOPICS[0],
            notebook.STYLES[0],
            12345,
            0,
            dimension_pools=pools,
        )
        for dimension, excluded in recent_keys.items():
            self.assertNotIn(blueprint[dimension]["key"], excluded, dimension)

    def test_notebook_spec_skips_recent_blueprint_dimensions_without_exhausting_attempts(self) -> None:
        recent_dimension_keys = {
            "data_design": [item.key for item in notebook.DATA_DESIGNS[:5]],
            "validation_lens": [item.key for item in notebook.VALIDATION_LENSES[:5]],
            "visual_grammar": [item.key for item in notebook.VISUAL_GRAMMARS[:5]],
            "narrative_frame": [item.key for item in notebook.NARRATIVE_FRAMES[:5]],
            "package_profile": [item.key for item in notebook.WEBR_PACKAGE_PROFILES[:8]],
        }
        recent_rows = []
        for index in range(8):
            blueprint = {
                dimension: {"key": keys[index % len(keys)]}
                for dimension, keys in recent_dimension_keys.items()
            }
            recent_rows.append(
                {
                    "title": f"history {index}",
                    "description": "",
                    "data_markdown": "",
                    "data_rcode": "",
                    "data_meta": json.dumps({"blueprint": blueprint}),
                }
            )

        spec = notebook.build_notebook_spec(
            "2026-08-07",
            set(),
            recent_rows,
            topic_pool=notebook.TOPICS,
            force_new=True,
        )

        for dimension, excluded in recent_dimension_keys.items():
            self.assertNotIn(spec["blueprint"][dimension]["key"], excluded, dimension)
        self.assertTrue(spec["similarity_guard"]["accepted"])

    def test_curated_topics_are_searched_after_source_context_novelty_exhaustion(self) -> None:
        dynamic_topic = notebook.Topic(
            **{
                **notebook.TOPICS[0].__dict__,
                "key": "source-context-regression",
                "source_context": {"context_kind": "unit_test"},
            }
        )

        def similarity_by_topic(spec: dict, _rows: list[dict]) -> tuple[float, str]:
            if spec["topic"].get("source_context"):
                return 0.99, "repeated source context"
            return 0.1, ""

        with (
            mock.patch.object(notebook, "MAX_CANDIDATE_ATTEMPTS", 2),
            mock.patch.object(notebook, "max_recent_similarity", side_effect=similarity_by_topic),
        ):
            spec = notebook.build_notebook_spec(
                "2026-08-07",
                set(),
                [{"title": "history"}],
                topic_pool=[dynamic_topic, notebook.TOPICS[1]],
                force_new=True,
            )

        self.assertIsNone(spec["topic"]["source_context"])
        self.assertEqual("curated_static", spec["similarity_guard"]["topic_pool_kind"])
        self.assertEqual(3, spec["similarity_guard"]["attempt"])
        self.assertTrue(spec["similarity_guard"]["accepted"])

    def test_all_dimension_combinations_build_base_r_validation_code(self) -> None:
        topic = notebook.TOPICS[0]
        style = notebook.STYLES[0]
        for attempt in range(960):
            blueprint = notebook.build_diversity_blueprint(topic, style, 987654321, attempt)
            code = notebook.build_blueprint_validation_r_code(
                seed=attempt + 1,
                topic=topic,
                blueprint=blueprint,
                plot_path="/tmp/notebook-diversity-test.svg",
            )
            self.assertIn("validation summary:", code)
            self.assertIn(blueprint["fingerprint"], code)


if __name__ == "__main__":
    unittest.main()
