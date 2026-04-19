import yaml
import os
import json

from adapters.duckdb_adapter import DuckDBAdapter
from profiler.profiler import Profiler

# Load config
with open("config.yaml") as f:
    config = yaml.safe_load(f)

# Setup
adapter = DuckDBAdapter(config["db_path"])
profiler = Profiler(adapter, config)

tables = adapter.list_tables()

# Resume support
progress_file = "output/progress.json"

if os.path.exists(progress_file):
    completed = json.load(open(progress_file))
else:
    completed = []

os.makedirs(config["output"]["path"], exist_ok=True)

# Run profiling
for table in tables:
    if config["resume"] and table in completed:
        print(f"Skipping {table}")
        continue

    print(f"Profiling {table}")

    profile = profiler.profile_table(table)

    with open(f"{config['output']['path']}/{table}.json", "w") as f:
        json.dump(profile, f, indent=2)

    completed.append(table)
    json.dump(completed, open(progress_file, "w"))

print("Done.")
