from __future__ import annotations

import ast
import pathlib
import re
import unittest

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"


def evaluate(expression: str, scope: str, run_id: int, **values):
    """Evaluate the small expression subset used by actual queue/needs rules."""
    context = {
        "inputs.scope": scope,
        "inputs.r_package_job": "all",
        "inputs.r_package_reverse_shard_index": "0",
        "github.run_id": run_id,
        **values,
    }
    source = expression.strip()
    if source.startswith("${{"):
        source = source[3:-2].strip()
    source = re.sub(
        r"\b(?:inputs\.[a-z_]+|github\.run_id|needs\.[a-z-]+\.result)\b",
        lambda match: repr(context[match.group()]),
        source,
    )
    source = source.replace("&&", " and ").replace("||", " or ").replace("!cancelled()", "not cancelled()")

    def read(node):
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            return not read(node.operand)
        if isinstance(node, ast.BoolOp):
            result = None
            for child in node.values:
                result = read(child)
                if isinstance(node.op, ast.And) and not result:
                    break
                if isinstance(node.op, ast.Or) and result:
                    break
            return result
        if isinstance(node, ast.Compare) and len(node.ops) == 1:
            left, right = read(node.left), read(node.comparators[0])
            if isinstance(node.ops[0], ast.Eq):
                return left == right
            if isinstance(node.ops[0], ast.NotEq):
                return left != right
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id == "always" and not node.args and not node.keywords:
                return True
            if node.func.id == "cancelled" and not node.args and not node.keywords:
                return values.get("cancelled", False)
            if node.func.id == "format" and not node.keywords:
                pattern, *args = [read(arg) for arg in node.args]
                return pattern.format(*args)
        raise AssertionError(f"Unsupported workflow expression: {ast.dump(node)}")

    return read(ast.parse(source, mode="eval").body)


class WorkflowTargetConcurrencyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = yaml.safe_load((WORKFLOWS / "r-project-all.yml").read_text())
        cls.jobs = cls.workflow["jobs"]

    def group(self, job: str | None, scope: str, run_id: int, **values) -> str:
        owner = self.workflow if job is None else self.jobs[job]
        group = owner["concurrency"]["group"]
        return evaluate(group, scope, run_id, **values) if "${{" in group else group

    def test_waiting_publisher_does_not_hold_next_daily_cdn_export(self):
        # The previous export finished; its downstream dedicated runner is
        # still unassigned. Keep that workflow and publication owner held.
        held = {
            self.group(None, "cdn", 100),
            self.group("community-publication", "cdn", 100),
        }
        for scope, run_id in (("cdn", 101), ("all", 102)):
            with self.subTest(scope=scope):
                outer = self.group(None, scope, run_id)
                self.assertNotIn(outer, held)
                held.add(outer)
                collect = self.group("collect", scope, run_id)
                self.assertNotIn(collect, held)
                held.add(collect)
                held.remove(collect)  # Intake can complete independently.
                export = self.group("cdn-export", scope, run_id)
                self.assertNotIn(export, held)
                held.add(export)
                held.remove(export)  # Next export is admitted after completion.
                held.remove(outer)
        self.assertIn(self.group("community-publication", "cdn", 100), held)

    def test_all_and_cdn_exports_have_one_owner_without_parent_self_deadlock(self):
        export = self.group("cdn-export", "all", 100)
        self.assertEqual(export, self.group("cdn-export", "cdn", 101))
        publication = self.group("community-publication", "all", 100)
        self.assertNotEqual(export, publication)
        for scope in ("all", "cdn", "publication"):
            self.assertEqual(publication, self.group("community-publication", scope, 101))
            self.assertNotIn(self.group(None, scope, 101), (export, publication))
        siblings = [WORKFLOWS / "r-project-community-bootstrap.yml"]
        notice_workflow = WORKFLOWS / "web-r-notice-cdn.yml"
        if notice_workflow.exists():
            siblings.append(notice_workflow)
        for path in siblings:
            sibling = yaml.safe_load(path.read_text())
            self.assertEqual(sibling["concurrency"]["group"], publication)

    def test_youtube_writers_remain_serial_across_all_social_and_availability(self):
        groups = {self.group("collect", scope, 100 + index) for index, scope in
                  enumerate(("all", "social", "youtube-availability"))}
        self.assertEqual(groups, {"r-project-youtube-ingest"})
        self.assertFalse(self.jobs["collect"]["concurrency"]["cancel-in-progress"])

    def test_failed_intake_or_cdn_export_cannot_publish(self):
        publisher = self.jobs["community-publication"]
        self.assertEqual(publisher["needs"], ["collect", "cdn-export"])
        cases = (
            ("publication", "success", "skipped", True),
            ("publication", "failure", "skipped", False),
            ("cdn", "success", "success", True),
            ("all", "success", "success", True),
            ("cdn", "success", "failure", False),
            ("all", "success", "cancelled", False),
            ("all", "failure", "skipped", False),
            ("community", "success", "skipped", False),
        )
        for scope, intake, export, allowed in cases:
            with self.subTest(scope=scope, intake=intake, export=export):
                result = evaluate(publisher["if"], scope, 100, **{
                    "needs.collect.result": intake, "needs.cdn-export.result": export,
                })
                self.assertEqual(result, allowed)
        self.assertEqual(self.jobs["cdn-export"]["needs"], "collect")
        for scope, export in (("publication", "skipped"), ("all", "success"), ("cdn", "success")):
            with self.subTest(scope=scope, cancelled=True):
                self.assertFalse(evaluate(publisher["if"], scope, 100, **{
                    "needs.collect.result": "success", "needs.cdn-export.result": export,
                    "cancelled": True,
                }))

    def test_export_job_regates_its_exact_write_target_before_mutation(self):
        steps = self.jobs["cdn-export"]["steps"]
        gate_index = next(index for index, step in enumerate(steps)
                          if step.get("name") == "Gate CDN release writes on storage pressure")
        gate = steps[gate_index]
        self.assertEqual(gate["run"], "python3 scripts/clickhouse_pressure_gate.py")
        self.assertEqual(gate["env"]["CLICKHOUSE_PRESSURE_GATE_TARGETS"],
                         "replica:Data_R_Community_Service.web_r_cdn_release_log_local")
        self.assertEqual(self.workflow["env"]["CLICKHOUSE_PRESSURE_GATE_MAX_IOWAIT_NORMALIZED"], "0.50")
        for index, step in enumerate(steps):
            if any(verb in step.get("name", "") for verb in
                   ("Ensure Web-R", "Commit and push", "Record Web-R")):
                self.assertGreater(index, gate_index)
        publisher = self.jobs["community-publication"]
        self.assertEqual(publisher["runs-on"], ["self-hosted", "linux", "x64", "webr-community-publisher"])
        self.assertEqual(publisher["environment"], "web-r-community-publication")

    def test_moved_export_steps_keep_all_job_local_dependencies(self):
        for job_name, job in self.jobs.items():
            ids = {step["id"] for step in job["steps"] if "id" in step}
            references = set(re.findall(r"steps\.([a-zA-Z0-9_-]+)\.", yaml.safe_dump(job)))
            self.assertTrue(references <= ids, (job_name, references - ids))
        collect_names = {step.get("name") for step in self.jobs["collect"]["steps"]}
        self.assertNotIn("Export encrypted Web-R CDN2 content", collect_names)

    def test_reusable_package_shards_do_not_cancel_each_other(self):
        first = self.group(None, "package", 100, **{
            "inputs.r_package_job": "cran-reverse-dependencies",
            "inputs.r_package_reverse_shard_index": "0",
        })
        second = self.group(None, "package", 100, **{
            "inputs.r_package_job": "cran-reverse-dependencies",
            "inputs.r_package_reverse_shard_index": "1",
        })
        self.assertNotEqual(first, second)


if __name__ == "__main__":
    unittest.main()
