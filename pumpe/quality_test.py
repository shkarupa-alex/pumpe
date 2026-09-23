import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
import yaml

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


def test_release_workflow() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    workflow = yaml.safe_load((ROOT / ".github/workflows/publish-to-pypi.yml").read_text())
    jobs = workflow["jobs"]

    assert jobs["test"]["uses"] == "./.github/workflows/test.yml"
    assert "test" in jobs["release-build"]["needs"]
    assert jobs["pypi-publish"]["environment"]["url"] == f"https://pypi.org/p/{project['name']}"

    minimum_python = project["requires-python"].removeprefix(">=")
    build_python = next(
        step["with"]["python-version"]
        for step in jobs["release-build"]["steps"]
        if step.get("uses", "").startswith("actions/setup-python")
    )
    assert tuple(map(int, build_python.split("."))) >= tuple(map(int, minimum_python.split(".")))
