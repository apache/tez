#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
Compares Tez DAG counters between two ZIP files.

This module extracts DAG data from two provided zip archives, parses
the counters and other metrics, and outputs a formatted table showing
the values and the delta between them.
"""

import argparse
import json
import sys
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Optional

# Check for texttable dependency
try:
    from texttable import Texttable
except ImportError:
    print(
        "Could not import Texttable. Retry after 'pip install texttable'",
        file=sys.stderr,
    )
    sys.exit(1)


def extract_zip(filename: Path, target_dir: Path) -> Path:
    """Extracts a zip file to a specific directory."""
    file_stem = filename.stem
    extract_path = target_dir / file_stem

    if not extract_path.exists():
        extract_path.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(filename, "r") as zip_ref:
        zip_ref.extractall(extract_path)

    return extract_path


def load_dag_json(extracted_dir: Path) -> Dict[str, Any]:
    """
    Locates and loads the DAG JSON data from an extracted directory.
    Handles differences between Tez UI (dag.json) and DebugTool (TEZ_DAG).
    """
    dag_json_path = extracted_dir / "dag.json"
    tez_dag_path = extracted_dir / "TEZ_DAG"

    target_file: Optional[Path] = None
    is_ui_format = True

    if dag_json_path.is_file():
        target_file = dag_json_path
    elif tez_dag_path.is_file():
        target_file = tez_dag_path
        is_ui_format = False

    if not target_file:
        raise FileNotFoundError(
            f"Unable to find dag.json or TEZ_DAG inside {extracted_dir}"
        )

    with target_file.open("r", encoding="utf-8") as f:
        data = json.load(f)
        # Tez UI wraps content in a "dag" root node; DebugTool might not
        return data.get("dag", data) if is_ui_format else data


def _parse_metrics(data: Dict[str, Any]) -> Dict[str, Dict[str, int]]:
    """
    Helper function to parse counters and 'otherinfo' from DAG data
    into a structured dictionary for easy comparison.
    """
    metrics: Dict[str, Dict[str, int]] = {}

    # 1. Process Standard Counters
    counters = data.get("otherinfo", {}).get("counters", {})
    for group in counters.get("counterGroups", []):
        group_name = group["counterGroupName"]
        if group_name not in metrics:
            metrics[group_name] = {}
        for counter in group["counters"]:
            metrics[group_name][counter["counterName"]] = counter["counterValue"]

    # 2. Process 'Other Info' Metrics
    other_info = data.get("otherinfo", {})
    other_metrics_map = {
        "TIME_TAKEN": "timeTaken",
        "COMPLETED_TASKS": "numCompletedTasks",
        "SUCCEEDED_TASKS": "numSucceededTasks",
        "FAILED_TASKS": "numFailedTasks",
        "KILLED_TASKS": "numKilledTasks",
        "FAILED_TASK_ATTEMPTS": "numFailedTaskAttempts",
        "KILLED_TASK_ATTEMPTS": "numKilledTaskAttempts",
    }

    metrics["otherinfo"] = {}
    for display_key, json_key in other_metrics_map.items():
        metrics["otherinfo"][display_key] = other_info.get(json_key, 0)

    return metrics


def _compare_group(
    group_m1: Dict[str, int], group_m2: Dict[str, int]
) -> Dict[str, List[Any]]:
    """
    Compares counters between two dictionaries for a single group.
    Returns a dictionary mapping counter names to [val1, val2, delta_str].
    """
    group_diff = {}
    all_counters = set(group_m1.keys()) | set(group_m2.keys())

    for counter in all_counters:
        val1 = group_m1.get(counter, 0)
        val2 = group_m2.get(counter, 0)
        delta = val2 - val1
        delta_str = ("+" if delta > 0 else "") + str(delta)

        # Store in format expected by print_table: [val1, val2, delta]
        group_diff[counter] = [val1, val2, delta_str]

    return group_diff


def calculate_diff(file1: Path, file2: Path, temp_dir: Path) -> Dict[str, Any]:
    """Calculates the difference between counters in two ZIP files."""

    # Extract
    dir1 = extract_zip(file1, temp_dir)
    dir2 = extract_zip(file2, temp_dir)

    # Load and Parse
    # Combining load and parse to reduce local variables
    metrics1 = _parse_metrics(load_dag_json(dir1))
    metrics2 = _parse_metrics(load_dag_json(dir2))

    diff_table: Dict[str, Any] = {}

    # Identify all unique counter groups from both files
    all_groups = set(metrics1.keys()) | set(metrics2.keys())

    for group in all_groups:
        diff_table[group] = _compare_group(
            metrics1.get(group, {}), metrics2.get(group, {})
        )

    return diff_table


def print_table(
    diff_table: Dict[str, Any], name1: str, name2: str, detailed: bool = False
) -> None:
    """Formats and prints the difference table."""
    table = Texttable(max_width=0)
    table.set_cols_align(["l", "l", "l", "l", "l"])
    table.set_cols_valign(["m", "m", "m", "m", "m"])

    # Header
    table.add_row(["Counter Group", "Counter Name", name1, name2, "Delta"])

    for group_name in sorted(diff_table.keys()):
        # Filter internal task counters unless detailed view is requested
        if not detailed and ("_INPUT_" in group_name or "_OUTPUT_" in group_name):
            continue

        group_data = diff_table[group_name]

        # Prepare columns
        c_names = []
        c_val1 = []
        c_val2 = []
        c_delta = []

        for key, values in group_data.items():
            c_names.append(key)
            c_val1.append(str(values[0]))
            c_val2.append(str(values[1]))
            c_delta.append(str(values[2]))

        # Format Group Name (Shorten unless detailed)
        display_group_name = group_name if detailed else group_name.split(".")[-1]

        row = [
            display_group_name,
            "\n".join(c_names),
            "\n".join(c_val1),
            "\n".join(c_val2),
            "\n".join(c_delta),
        ]
        table.add_row(row)

    print(table.draw() + "\n")


def main() -> None:
    """
    Main entry point for the CLI tool.
    Parses arguments, executes comparison, and handles top-level errors.
    """
    parser = argparse.ArgumentParser(
        description="Compare TeZ counters between two DAG zip files."
    )
    parser.add_argument("file1", type=Path, help="Path to the first DAG zip file")
    parser.add_argument("file2", type=Path, help="Path to the second DAG zip file")
    parser.add_argument(
        "--detail", action="store_true", help="Show detailed task-specific counters"
    )

    args = parser.parse_args()

    if not args.file1.exists():
        print(f"Error: File '{args.file1}' does not exist.", file=sys.stderr)
        sys.exit(1)
    if not args.file2.exists():
        print(f"Error: File '{args.file2}' does not exist.", file=sys.stderr)
        sys.exit(1)

    # Use a temporary directory context manager for automatic cleanup
    with TemporaryDirectory() as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        try:
            diff_data = calculate_diff(args.file1, args.file2, temp_dir)
            print_table(
                diff_data, args.file1.stem, args.file2.stem, detailed=args.detail
            )
        except (OSError, json.JSONDecodeError, zipfile.BadZipFile, KeyError) as e:
            print(f"Error processing files: {e}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
