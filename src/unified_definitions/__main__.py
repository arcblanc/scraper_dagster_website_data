#!/usr/bin/env python3
"""Main entry point for the unified Dagster definitions."""

import sys
from pathlib import Path

# Add the src directory to the Python path
src_path = Path(__file__).parent.parent
sys.path.insert(0, str(src_path))

from unified_definitions import defs

if __name__ == "__main__":
    # This allows the module to be run directly
    print("Unified Dagster definitions loaded successfully")
    assets_list = list(defs.assets) if defs.assets else []
    jobs_list = list(defs.jobs) if defs.jobs else []
    schedules_list = list(defs.schedules) if defs.schedules else []
    print(f"Assets: {len(assets_list)}")
    print(f"Jobs: {len(jobs_list)}")
    print(f"Schedules: {len(schedules_list)}")
