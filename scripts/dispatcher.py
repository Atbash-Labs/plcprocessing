#!/usr/bin/env python3
"""
PyInstaller entry-point dispatcher for the PLC Ontology Assistant.

When frozen (packaged), this executable replaces the system Python interpreter.
Usage:  dispatcher.exe <script_name.py> [script_args ...]

In development mode you would never call this directly – the Electron app
spawns ``python -u <script>.py ...`` as before.
"""

import os
import sys
import runpy


def _scripts_dir() -> str:
    """Return the directory that contains the bundled Python scripts."""
    if getattr(sys, "frozen", False):
        # PyInstaller puts data files relative to sys._MEIPASS (--onedir)
        return os.path.join(sys._MEIPASS, "scripts")
    else:
        # Running from source – scripts are in the same directory as this file
        return os.path.dirname(os.path.abspath(__file__))


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: dispatcher <script_name.py> [args ...]", file=sys.stderr)
        sys.exit(1)

    script_name = sys.argv[1]
    scripts_dir = _scripts_dir()
    script_path = os.path.join(scripts_dir, script_name)

    if not os.path.exists(script_path):
        print(f"Script not found: {script_path}", file=sys.stderr)
        sys.exit(1)

    # Shift argv so the target script sees its own name as argv[0]
    sys.argv = sys.argv[1:]

    # Make sure inter-script imports (e.g. ``from neo4j_ontology import ...``)
    # resolve against the scripts directory.
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)

    # If a DOTENV_PATH was passed by the Electron host, load it before the
    # target script calls load_dotenv() (which only searches cwd / parents).
    dotenv_path = os.environ.get("DOTENV_PATH")
    if dotenv_path and os.path.isfile(dotenv_path):
        try:
            from dotenv import load_dotenv
            load_dotenv(dotenv_path)
        except ImportError:
            pass  # dotenv not available – env vars must be set externally

    # Execute the target script in its own __main__ namespace.
    runpy.run_path(script_path, run_name="__main__")


if __name__ == "__main__":
    main()
