"""Run modules from the banking learning project by name."""

import argparse
import sys

MODULES = {
    "basic": "banking_learning_project.basic",
    "pandas_module": "banking_learning_project.pandas_module",
    "pandas_polars_module": "banking_learning_project.pandas_polars_module",
    "math_libs": "banking_learning_project.math_libs",
    "stats": "banking_learning_project.stats",
    "pyspark_module": "banking_learning_project.pyspark_module",
    "orchestration_module": "banking_learning_project.orchestration_module",
    "llm_module": "banking_learning_project.llm_module",
    "advanced_ai_module": "banking_learning_project.advanced_ai_module",
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a banking learning module")
    parser.add_argument("--module", required=True, choices=MODULES.keys(), help="Module to run")

    args = parser.parse_args()
    module_name = MODULES[args.module]

    try:
        module = __import__(module_name, fromlist=["*"])
    except ImportError as exc:
        print(f"ERROR: Could not import module {module_name}: {exc}", file=sys.stderr)
        return 2

    if not hasattr(module, "example"):
        print(f"ERROR: Module {module_name} has no example() function", file=sys.stderr)
        return 3

    try:
        module.example()
        return 0
    except Exception as exc:
        print(f"ERROR running {module_name}.example(): {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
