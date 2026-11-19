# Databricks notebook source
# MAGIC %md
# MAGIC ## Run the whole pipeline — one notebook
# MAGIC Runs every notebook in order: **setup → landing → bronze → silver → gold (spec + conventional) → outputs**.
# MAGIC
# MAGIC Widgets at the top:
# MAGIC - **run_setup** — `yes` the first time (creates catalog, schemas, volume), then `no`
# MAGIC - **start_from** — step number to restart from after a failure (earlier steps are skipped)
# MAGIC - **stop_after** — last step to run (e.g. `7` = stop after silver)

# COMMAND ----------

dbutils.widgets.dropdown("run_setup", "no", ["yes", "no"])
dbutils.widgets.text("start_from", "1")
dbutils.widgets.text("stop_after", "15")

run_setup = dbutils.widgets.get("run_setup") == "yes"
start_from = int(dbutils.widgets.get("start_from"))
stop_after = int(dbutils.widgets.get("stop_after"))

# COMMAND ----------

# MAGIC %md ### Pipeline steps (in dependency order)

# COMMAND ----------

TIMEOUT_SECONDS = 3600

SETUP = ("../one_off/create_catalog_schema_volume", {})

STEPS = [
    # 00 landing
    ("./notebooks/00_landing/ingest_yellow_trips",   {}),
    ("./notebooks/00_landing/ingest_lookup",         {}),
    # 01 bronze
    ("./notebooks/01_bronze/yellow_trips_raw",       {}),
    ("./notebooks/01_bronze/taxi_zone_lookup_raw",   {}),
    # 02 silver
    ("./notebooks/02_silver/taxi_zone_lookup",       {}),
    ("./notebooks/02_silver/data_profile",           {}),
    ("./notebooks/02_silver/fact_trips",             {}),
    # 03 gold - spec settings
    ("./notebooks/03_gold/window_grid",              {"settings": "spec"}),
    ("./notebooks/03_gold/zsi_windows",              {"settings": "spec"}),
    ("./notebooks/03_gold/zone_stress",              {"settings": "spec"}),
    # 03 gold - conventional settings (Q4)
    ("./notebooks/03_gold/window_grid",              {"settings": "conventional"}),
    ("./notebooks/03_gold/zsi_windows",              {"settings": "conventional"}),
    ("./notebooks/03_gold/zone_stress",              {"settings": "conventional"}),
    # 04 outputs
    ("./notebooks/04_outputs/export_outputs",        {}),
    ("./notebooks/04_outputs/answers",               {}),
]

for number, (path, params) in enumerate(STEPS, start=1):
    marker = "run " if start_from <= number <= stop_after else "skip"
    print(f"{marker}  {number:>2}. {path.replace('./notebooks/', '')} {params or ''}")

# COMMAND ----------

# MAGIC %md ### Run

# COMMAND ----------

import time

import pandas as pd

results = []


def run_step(label, path, params):
    """Run one notebook; record status and duration. Raises on failure so the pipeline stops."""
    started = time.time()
    print(f"▶ {label} ...", flush=True)
    try:
        dbutils.notebook.run(path, TIMEOUT_SECONDS, params)
    except Exception as error:
        seconds = round(time.time() - started)
        results.append((label, "FAILED", seconds))
        print(f"✗ {label} failed after {seconds}s")
        raise Exception(f"Pipeline stopped at '{label}'. Fix it, then re-run with start_from = this step.\n{error}")
    seconds = round(time.time() - started)
    results.append((label, "OK", seconds))
    print(f"✓ {label} — {seconds}s")


pipeline_start = time.time()

if run_setup:
    run_step("0. setup", *SETUP)

try:
    for number, (path, params) in enumerate(STEPS, start=1):
        if not start_from <= number <= stop_after:
            continue
        label = f"{number}. {path.replace('./notebooks/', '')}"
        if params:
            label += f" [{params['settings']}]"
        run_step(label, path, params)
finally:
    summary = pd.DataFrame(results, columns=["step", "status", "seconds"])
    display(summary)
    print(f"Total: {round(time.time() - pipeline_start)}s")

# COMMAND ----------

# MAGIC %md ### Check the outputs

# COMMAND ----------

# MAGIC %run ./config/config

# COMMAND ----------

display(dbutils.fs.ls(OUTPUT_DIR))
