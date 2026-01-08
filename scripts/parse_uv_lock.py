#!/bin/env python3

"""
Parse uv.lock dependencies into JSON to be consumed by Terraform/OpenTofu.
"""

import json
import os
import tomllib


def parse_uv_lock():
    lock_path = "uv.lock"
    project_path = "pyproject.toml"

    if not os.path.exists(lock_path):
        return {"error": "uv.lock not found"}

    with open(lock_path, "rb") as f:
        lock_data = tomllib.load(f)

    with open(project_path, "rb") as f:
        project_data = tomllib.load(f)

    # 1. Get the list of names you actually asked for in pyproject.toml
    top_level_names = [
        req.split(">=")[0].split("==")[0].strip().lower()
        for req in project_data.get("project", {}).get("dependencies", [])
    ]

    # 2. Extract those specific versions from the lock file
    # uv.lock stores packages in a list of tables called [[package]]
    pypi_map = {}
    for pkg in lock_data.get("package", []):
        name = pkg.get("name", "").lower()
        if name in top_level_names:
            version = pkg.get("version")
            pypi_map[name] = f"=={version}"

    return pypi_map


if __name__ == "__main__":
    print(json.dumps(parse_uv_lock()))
