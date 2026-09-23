import re
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

import pytest
import yaml
from packaging.requirements import Requirement

ROOT = Path(__file__).resolve().parent.parent

# The project's own lint and type gates, run with the versions pinned in the dev dependency group.
STATIC_CHECKS = {
    "ruff-check": ["ruff", "check", "--no-fix", "pumpe"],
    "ruff-format": ["ruff", "format", "--check", "pumpe"],
    "mypy": ["mypy", "pumpe"],
}


@pytest.mark.parametrize("command", STATIC_CHECKS.values(), ids=STATIC_CHECKS.keys())
def test_static_checks(command: list[str]) -> None:
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-m", *command],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def load_workflow(name: str) -> dict[Any, Any]:
    workflow: dict[Any, Any] = yaml.safe_load((ROOT / ".github/workflows" / name).read_text())
    return workflow


def test_release_workflow() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    jobs = load_workflow("publish-to-pypi.yml")["jobs"]

    assert jobs["test"]["uses"] == "./.github/workflows/test.yml"
    # YAML 1.1 reads the bare `on` key as boolean true.
    assert "workflow_call" in load_workflow("test.yml")[True]
    assert "test" in jobs["release-build"]["needs"]
    assert jobs["pypi-publish"]["environment"]["url"] == f"https://pypi.org/p/{project['name']}"

    minimum_python = project["requires-python"].removeprefix(">=")
    build_python = next(
        step["with"]["python-version"]
        for step in jobs["release-build"]["steps"]
        if step.get("uses", "").startswith("actions/setup-python")
    )
    assert tuple(map(int, build_python.split("."))) >= tuple(map(int, minimum_python.split(".")))


def test_workflow_action_refs_are_pinned() -> None:
    # astral-sh/setup-uv stopped publishing floating major tags after v7, so only exact versions or SHAs resolve.
    refs = [
        step["uses"].partition("@")[2]
        for name in ("test.yml", "publish-to-pypi.yml")
        for job in load_workflow(name)["jobs"].values()
        for step in job.get("steps", [])
        if step.get("uses", "").startswith("astral-sh/setup-uv@")
    ]

    assert refs
    for ref in refs:
        assert re.fullmatch(r"v\d+\.\d+\.\d+|[0-9a-f]{40}", ref), ref


def test_runtime_requires_sqlalchemy_asyncio() -> None:
    # SQLAlchemy installs greenlet, which AsyncSession needs, only on some architectures unless asked via the extra.
    dependencies = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["dependencies"]
    sqlalchemy = [r for r in map(Requirement, dependencies) if r.name == "sqlalchemy"]
    assert len(sqlalchemy) == 1
    assert "asyncio" in sqlalchemy[0].extras

    lock = tomllib.loads((ROOT / "uv.lock").read_text())
    (pumpe,) = (package for package in lock["package"] if package["name"] == "pumpe")
    locked = [d for d in pumpe["dependencies"] if d["name"] == "sqlalchemy"]
    assert locked
    assert all("asyncio" in d.get("extra", []) for d in locked)
