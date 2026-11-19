# NYC Taxi Zone Stress Index — Databricks Medallion Pipeline

An end-to-end PySpark pipeline on Databricks that ingests one month of NYC TLC Yellow Taxi data (March 2025),
cleans it with ordered data-quality rules, and computes a **Zone Stress Index (ZSI)** for every physical taxi
zone: how much more often a zone sends taxis out than it receives them during the weekday morning rush.

---

## Architecture

```
TLC website
    │
    ▼
00_landing   raw files in a Unity Catalog volume            raw/yellow_tripdata_2025-03.parquet, raw/taxi_zone_lookup.csv
    │
    ▼
01_bronze    data exactly as delivered                       yellow_trips_raw, taxi_zone_lookup_raw
    │
    ▼
02_silver    profiled, cleaned, conformed                    taxi_zone_lookup, fact_trips, dq_exclusions
    │
    ▼
03_gold      business metrics (spec + conventional)          window_grid, zsi_windows, zone_stress (+ _conventional)
    │
    ▼
04_outputs   deliverables + answers                          outputs/*.parquet, *.csv, ANSWERS.md
```

- **Catalog:** `nyctaxi_zsi`
- **Schemas:** `00_landing`, `01_bronze`, `02_silver`, `03_gold`
- **Volume:** `/Volumes/nyctaxi_zsi/00_landing/nyc_taxi_stress/` with `raw/` and `outputs/` folders
- **Table format:** Delta
- **Engine:** PySpark on Databricks Runtime 15.4 LTS

---

## Project structure

```
nyctaxi_zsi_project/
├── one_off/
│   └── create_catalog_schema_volume.ipynb   # run once: catalog, schemas, volume
├── transformations/
│   ├── run_all.py                           # ONE notebook that runs the whole pipeline
│   ├── config/
│   │   └── config.ipynb                     # all parameters (loaded with %run)
│   ├── utils/
│   │   └── zsi_functions.ipynb              # shared functions: DST window grid, ZSI, Harrell-Davis
│   └── notebooks/
│       ├── 00_landing/
│       │   ├── ingest_yellow_trips.ipynb    # download trips -> raw/
│       │   └── ingest_lookup.ipynb          # download zone lookup -> raw/
│       ├── 01_bronze/
│       │   ├── yellow_trips_raw.ipynb       # -> 01_bronze.yellow_trips_raw
│       │   └── taxi_zone_lookup_raw.ipynb   # -> 01_bronze.taxi_zone_lookup_raw
│       ├── 02_silver/
│       │   ├── taxi_zone_lookup.ipynb       # -> 02_silver.taxi_zone_lookup (+ is_physical flag)
│       │   ├── data_profile.ipynb           # 17 profiling checks -> outputs/profile_report.md
│       │   └── fact_trips.ipynb             # 8 DQ rules -> 02_silver.fact_trips, 02_silver.dq_exclusions
│       ├── 03_gold/                         # widget: settings = spec | conventional
│       │   ├── window_grid.ipynb            # -> 03_gold.window_grid
│       │   ├── zsi_windows.ipynb            # -> 03_gold.zsi_windows
│       │   └── zone_stress.ipynb            # -> 03_gold.zone_stress, 03_gold.hd_weights
│       └── 04_outputs/
│           ├── export_outputs.ipynb         # -> outputs/ files
│           └── answers.ipynb                # -> outputs/ANSWERS.md
├── NOTES.md                                 # assumptions and design choices
└── README.md
```

---

## Quick start

### 1. Import the project
**Workspace → your folder → Create → Git folder** and paste the repository URL
(or **Import** the zip).

### 2. Attach a cluster
Classic compute, **Databricks Runtime 15.4 LTS**, single node is enough (16 GB+ memory),
access mode **Dedicated / Single user**.

> Do not use Serverless: the silver notebooks use `.cache()` / `.persist()`, which serverless does not support.

### 3. Run everything
Open `transformations/run_all` and set the widgets:

| Widget | First run | Later runs | Meaning |
|---|---|---|---|
| `run_setup` | `yes` | `no` | create catalog, schemas, volume |
| `start_from` | `1` | step number | restart from this step after a failure |
| `stop_after` | `15` | step number | last step to run |

Click **Run all**. Each step prints `✓ step — seconds`; a summary table is shown at the end.

> **No internet on the cluster?** Upload `yellow_tripdata_2025-03.parquet` and `taxi_zone_lookup.csv` to
> `/Volumes/nyctaxi_zsi/00_landing/nyc_taxi_stress/raw/` — the landing notebooks skip the download when the
> files already exist.

---

## Pipeline steps

| # | Notebook | Output |
|---|---|---|
| 0 | `one_off/create_catalog_schema_volume` | catalog, schemas, volume (only with `run_setup = yes`) |
| 1 | `00_landing/ingest_yellow_trips` | `raw/yellow_tripdata_2025-03.parquet` |
| 2 | `00_landing/ingest_lookup` | `raw/taxi_zone_lookup.csv` |
| 3 | `01_bronze/yellow_trips_raw` | `01_bronze.yellow_trips_raw` |
| 4 | `01_bronze/taxi_zone_lookup_raw` | `01_bronze.taxi_zone_lookup_raw` |
| 5 | `02_silver/taxi_zone_lookup` | `02_silver.taxi_zone_lookup` |
| 6 | `02_silver/data_profile` | `outputs/profile_report.md` |
| 7 | `02_silver/fact_trips` | `02_silver.fact_trips`, `02_silver.dq_exclusions` |
| 8 | `03_gold/window_grid` · spec | `03_gold.window_grid` |
| 9 | `03_gold/zsi_windows` · spec | `03_gold.zsi_windows` |
| 10 | `03_gold/zone_stress` · spec | `03_gold.zone_stress`, `03_gold.hd_weights` |
| 11 | `03_gold/window_grid` · conventional | `03_gold.window_grid_conventional` |
| 12 | `03_gold/zsi_windows` · conventional | `03_gold.zsi_windows_conventional` |
| 13 | `03_gold/zone_stress` · conventional | `03_gold.zone_stress_conventional` |
| 14 | `04_outputs/export_outputs` | files in `outputs/` |
| 15 | `04_outputs/answers` | `outputs/ANSWERS.md` |

---

## Running as a Databricks job

**Option A — one task:** Jobs & Pipelines → Create → Job → one Notebook task pointing to
`transformations/run_all` on a job cluster (15.4 LTS, single node).

**Option B — one task per notebook:** create the 15 tasks from the table above with these dependencies:

```
landing_yellow_trips ─► bronze_yellow_trips ─────────────┐
landing_lookup ───────► bronze_zone_lookup ─► silver_zone_lookup ─┴─► silver_data_profile ─► silver_fact_trips
                                                                                                 │
                     ┌───────────────────────────────────────────────────────────────────────────┤
                     ▼                                                                           ▼
       gold_window_grid_spec                                                      gold_window_grid_conv
       gold_zsi_windows_spec                                                      gold_zsi_windows_conv
       gold_zone_stress_spec                                                      gold_zone_stress_conv
                     └──────────────────────────────► export_outputs ◄──────────────────────────┘
                                                            │
                                                         answers
```

Gold tasks need the task parameter `settings` = `spec` (or `conventional` for the `_conv` tasks).

---

## Outputs

In `/Volumes/nyctaxi_zsi/00_landing/nyc_taxi_stress/outputs/`:

| File | Content |
|---|---|
| `fact_trips.parquet/` | clean trips: `trip_key`, `pickup_ts`, `dropoff_ts`, `duration_min`, zones, `operational_date`, all fee columns |
| `dq_exclusions.csv` | `rule_order`, `rule_id`, `description`, `rows_excluded`, `pct_of_raw` |
| `zone_stress.csv` | `LocationID`, `zone`, `borough`, `S` — one row per physical zone |
| `zsi_windows.parquet/` | `P`, `D`, `ZSI` for every zone × 210 morning windows |
| `profile_report.md` | the 17 profiling checks with their results |
| `ANSWERS.md` | Part C answers with the query behind each number |

---

## How the Zone Stress Index works

1. **Operational day** runs 04:30 → 04:30 (New York wall clock), split into 20-minute windows.
   A window exists only if at least one real instant falls in it — on 2025-03-09 clocks jump from
   02:00 to 03:00, so operational day 2025-03-08 has **70** windows instead of 72.
2. **Morning windows** = windows starting 06:50–09:50 on Monday–Friday operational days → **210** windows.
3. For each physical zone and morning window: **P** = pickups, **D** = dropoffs, and
   **ZSI = √P + √(P+1) − √(4D+3)**.
4. **S(z)** = Harrell-Davis 0.9 quantile of the zone's 210 ZSI values.

## Data-quality rules (silver)

Applied in order; each excluded row is counted under the first rule it fails.

| # | Rule | Why |
|---|---|---|
| 1 | `DUPLICATE_ROW` | exact copy of another row |
| 2 | `MISSING_TIMESTAMP` | pickup or dropoff is NULL |
| 3 | `NONEXISTENT_LOCAL_TIME` | timestamp inside the DST gap (02:00–02:59 on 2025-03-09) |
| 4 | `PICKUP_OUT_OF_PERIOD` | pickup outside March 2025 |
| 5 | `NON_POSITIVE_DURATION` | dropoff at or before pickup |
| 6 | `EXCESSIVE_DURATION` | longer than `MAX_DURATION_MIN` (360 min) |
| 7 | `INVALID_LOCATION_ID` | location NULL or not in the lookup |
| 8 | `NEGATIVE_AMOUNT` | negative fare or total (void / refund records) |

Check: raw rows = kept rows + Σ `rows_excluded`. Details and evidence: `NOTES.md`, `profile_report.md`.

---

## Configuration

Every parameter is in `transformations/config/config.ipynb` — no values are hard-coded in the notebooks.

| Setting | Default | Used by |
|---|---|---|
| `CATALOG` | `nyctaxi_zsi` | all |
| `PERIOD_START` / `PERIOD_END` | `2025-03-01 00:00` / `2025-04-01 04:30` | silver rules |
| `MAX_DURATION_MIN` | `360` | silver rules |
| `ZSI_SETTINGS["spec"]` | day start 04:30, 20-min windows, mornings 06:50–09:50, quantile 0.9, smoothing constant 3 | gold |
| `ZSI_SETTINGS["conventional"]` | day start 00:00, mornings 06:40–09:40 | gold (Q4) |

**To change a parameter:** edit `config`, then re-run `run_all` with `start_from = 8` (gold + outputs only).
If you change the window grid, set `expected_am_windows` to `None`.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `PERMISSION_DENIED` on `CREATE CATALOG` | use an existing catalog: change `CATALOG` in config and the names in the one-off notebook |
| Download fails / times out | upload the two files to `raw/` manually and re-run |
| `%run` path not found | keep the folder structure intact; `run_all.py` must stay in `transformations/` |
| `persist` / `cache` not supported | you are on serverless — switch to a classic cluster |
| A step failed | fix it, then re-run `run_all` with `start_from` = that step number |
| Conventional tables look identical to spec | the `_conv` job tasks are missing `settings = conventional` |
