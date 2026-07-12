from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path


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
