#!/usr/bin/env python3
"""Buildkite dynamic pipeline generator for qdb-api-jni.

Step templates in steps/*.yml define nearly-complete Buildkite steps with
{placeholder} variables.  This script loads them, substitutes variables, and
overlays environment variables and the Docker plugin per platform.

Usage:
    python3 pipeline.py           # emit pipeline YAML to stdout
    python3 pipeline.py check     # validate without emitting
"""
from __future__ import annotations

import dataclasses
import sys
from pathlib import Path

from buildkite_sdk import CommandStep, Pipeline, GroupStep

sys.path.insert(0, str(Path(__file__).parent / "tools"))
from qdb_pipeline import (
    Platform,
    apply_docker,
    load_template,
    merge_env,
    select_platforms,
    validate_pipeline,
    get_git_ref,
    set_artifact_plugin_options,
)  # noqa: E402

STEPS_DIR = Path(__file__).parent / "steps"

# Quasardb-specific toolchain overlays on top of shared infrastructure platforms.
_LINUX = dict(
    c_compiler="$$QDB_CICD_AGENT_CC",
    cxx_compiler="$$QDB_CICD_AGENT_CXX",
    asm_compiler="$$QDB_CICD_AGENT_YASM",
    ccache="$$QDB_CICD_AGENT_CCACHE",
    docker_image="bureau14/builder:rhel7",
    docker_volumes=("/var/lib/ccache:/var/lib/ccache",),
)
_WIN = dict(
    asm_compiler="$$QDB_CICD_AGENT_YASM",
    ccache="$$QDB_CICD_AGENT_CCACHE",
)
_FREEBSD = dict(
    c_compiler="$$QDB_CICD_AGENT_CC",
    cxx_compiler="$$QDB_CICD_AGENT_CXX",
    ccache="$$QDB_CICD_AGENT_CCACHE",
)
_MACOS = dict(
    c_compiler="$$QDB_CICD_AGENT_CC",
    cxx_compiler="$$QDB_CICD_AGENT_CXX",
    ccache="$$QDB_CICD_AGENT_CCACHE",
)

_OS_OVERLAY = {"linux": _LINUX, "windows": _WIN, "freebsd": _FREEBSD, "macos": _MACOS}
PLATFORMS: list[Platform] = [
    dataclasses.replace(p, **_OS_OVERLAY.get(p.os, {}))
    for p in select_platforms(
        "freebsd-amd64-core2",
        "linux-amd64-core2",
        "windows-amd64-core2",
        "macos-aarch64",
    )
]

BUILD_TYPES = ["Release"]

# Environment variable layering: global → step → os → os+step → platform compilers.
GLOBAL_ENV: dict[str, str] = {
    "CMAKE_GENERATOR": "Ninja",
    "JAVA_PATH": "$$QDB_CICD_AGENT_JAVA_PATH",
    "JAVA_HOME": "$$QDB_CICD_AGENT_JAVA_HOME",
}

STEP_ENV: dict[str, dict[str, str]] = {}

OS_ENV: dict[str, dict[str, str]] = {
    "linux": {
    },
    "freebsd": {
    },
    "macos": {
    },
    "windows": {
        "WINDOWS_TARGET_ARCH": "win64",
    },
}

OS_STEP_ENV: dict[str, dict[str, str]] = {}

CPU_ENV: dict[str, dict[str, str]] = {
    "core2": {
        "QDB_CPU_ARCHITECTURE_CORE2": "ON",
    },
}


def _env(p: Platform, step_name: str, build_type: str) -> dict[str, str]:
    """Compose the full environment dict for one step."""
    return merge_env(
        GLOBAL_ENV,
        STEP_ENV.get(step_name, {}),
        OS_ENV.get(p.os, {}),
        OS_STEP_ENV.get(f"{p.os}/{step_name}", {}),
        CPU_ENV.get(p.cpu, {}),
        {"CMAKE_BUILD_TYPE": build_type},
        platform=p,
    )


def generate_pipeline() -> Pipeline:
    """Load templates, expand across platforms × build_types, overlay env and docker."""
    pipeline = Pipeline()
    git_ref = get_git_ref()
    group_steps = {}
    variants = []

    for p in PLATFORMS:
        for bt in BUILD_TYPES:
            slug = p.slug(bt.lower())
            variants.append(slug)

            # We want to use Release QuasarDB binaries when building Python API (debug and release)
            dependency_slug = p.slug("release")

            tvars = {
                "slug": slug,
                "queue": f"{p.queue_os}-{p.arch}",
                "name": slug.replace("-", " ").title(),
            }

            artifact_vars_per_step = {
                "upload": {"variant": slug, "git-ref": git_ref},
                "promote": {"variant": slug, "git-ref": git_ref},
                "download": {
                    "variant": dependency_slug,
                    "git-ref": git_ref,
                },
            }

            step = load_template(STEPS_DIR / "_build.yml", **tvars)
            env = _env(p, "test", bt)
            env.update(step.get("env") or {})
            step["env"] = env
            apply_docker(step, p.docker_image, p.docker_volumes)
            set_artifact_plugin_options(step, artifact_vars_per_step)

            # add step to group
            group_name = p.slug(bt.lower()).replace("-", " ").title()
            if group_name not in group_steps:
                group_steps[group_name] = []
            group_steps[group_name].append(step)

    # create groups and add to pipeline
    for group, steps in group_steps.items():
        group_step = GroupStep(group=group, steps=steps)
        pipeline.add_step(group_step)
    
    # Aggregate all test reports
    step = load_template(STEPS_DIR / "_test_report.yml", **tvars)
    step["depends_on"] = [f"build-{variant}" for variant in variants]
    pipeline.add_step(CommandStep.from_dict(step))


    return pipeline


def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "generate"

    try:
        pipeline = generate_pipeline()
    except Exception as e:
        print(f"[FAIL] Pipeline generation failed: {e}", file=sys.stderr)
        sys.exit(1)

    if command == "generate":
        print(pipeline.to_yaml())
    elif command == "check":
        errors = validate_pipeline(pipeline)
        if errors:
            for e in errors:
                print(f"[FAIL] {e}", file=sys.stderr)
            sys.exit(1)
        print(f"[OK] Pipeline valid: {len(pipeline.steps)} steps")
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        print("Usage: pipeline.py [generate|check]", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
