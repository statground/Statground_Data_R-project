"""Locate optional workspace checkouts while preserving standalone CI defaults."""

from pathlib import Path
import runpy


def workspace_repo(repo_key: str) -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    for workspace in repo_root.parents:
        resolver = workspace / "release_notes" / "scripts" / "workspace_layout.py"
        if resolver.is_file():
            return runpy.run_path(str(resolver))["resolve_repo"](workspace, repo_key)
    return repo_root.parent / repo_key
