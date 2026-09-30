# Nebula Wind Pipeline

A small, production-minded PySpark proof of concept for processing wind-turbine telemetry. The repository includes the three supplied CSVs representing 15 turbines for March 2022.

## What it does

The pipeline implements a simple Bronze → Silver → Gold flow:

1. **Bronze** – read the CSV input using an explicit schema and retain source-file metadata.
2. **Silver** – deduplicate measurements, reconstruct missing hourly rows, validate physically impossible values, identify measurement-level IQR outliers, and impute missing/outlier values using per-turbine robust statistics.
3. **Gold statistics** – calculate daily min/max/average/stddev power output plus data-quality completeness metrics for every turbine.
4. **Gold anomalies** – compare each turbine's daily average output with the fleet's average for the same day and flag values outside `fleet mean ± 2 × population standard deviation`.
5. Persist outputs as **Delta tables** using merge/upsert semantics so reruns are idempotent.

## Project layout

```text
nebula-wind-pipeline/
├── data/raw/                  # supplied CSV files
├── output/                    # generated Delta tables (gitignored)
├── src/nebula/
│   ├── anomalies.py
│   ├── cleaning.py
│   ├── config.py
│   ├── ingestion.py
│   ├── pipeline.py
│   ├── schemas.py
│   ├── spark.py
│   ├── statistics.py
│   └── storage.py
├── tests/
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Data assumptions

- Measurements are expected once per hour for each turbine.
- `(turbine_id, timestamp)` uniquely identifies a measurement.
- The supplied dataset contains five turbines per file and 15 turbines in total.
- The supplied files span `2022-03-01 00:00:00` through `2022-03-31 23:00:00`.
- Negative wind speed and negative power output are invalid.
- Wind direction must be in `[0, 359]` degrees.
- Missing rows are reconstructed by building an expected turbine × hourly timestamp grid.
- When exact run boundaries are not supplied, the observed minimum and maximum timestamps define the expected interval. In production, the scheduler should pass explicit daily boundaries so complete beginning/end-of-window outages are detectable.
- Missing/invalid wind speed and power output values, plus measurement-level IQR outliers, are imputed with the turbine median for the batch.
- Missing wind direction is imputed using the per-turbine **circular mean**, not a normal arithmetic mean.
- A daily operational anomaly is a turbine whose daily mean MW lies outside the same-day fleet mean ± 2 population standard deviations.
- Measurement-level sensor outliers and daily operational anomalies are deliberately separate concepts.
- The pipeline uses UTC by default.

## Why pi appears in the implementation

The statistical requirements do not inherently require π. Wind direction is angular data, however, so direction is converted between degrees and radians using Python's `math.pi`. Circular mean direction is computed from average sine/cosine components. This is the mathematically relevant use of π without artificially injecting it into unrelated calculations.

## Cleaning strategy

The pipeline avoids a blind `dropna()` approach because a completely missing sensor measurement has no row to drop. It first reconstructs the expected hourly grid for each turbine, then exposes a `measurement_missing` flag.

For observed rows:

- impossible physical values are nulled;
- wind speed and power output are checked for per-turbine IQR sensor outliers;
- null/invalid/outlier wind speed and power values are imputed using per-turbine medians;
- missing direction is imputed with a circular mean;
- quality flags are preserved for downstream auditing.

For a production system, turbine-specific rated capacity, manufacturer power curves, maintenance state, and local weather would be preferable to generic IQR rules.

## Setup

Requirements:

- Python 3.10–3.12
- Java 11 or 17 recommended

Create a virtual environment and install the project:

```bash
python -m venv .venv

# Linux/macOS
source .venv/bin/activate

# Windows PowerShell
.venv\Scripts\Activate.ps1

pip install -e ".[dev]"
```

Or install from the requirements file:

```bash
pip install -r requirements.txt
pip install -e . --no-deps
```

## Run

From the repository root:

```bash
python -m nebula.pipeline
```

Equivalent explicit command:

```bash
python -m nebula.pipeline \
  --input "data/raw/data_group_*.csv" \
  --output output \
  --timezone UTC
```

For an incremental daily run, explicitly set the expected processing window:

```bash
python -m nebula.pipeline \
  --input "data/raw/data_group_*.csv" \
  --output output \
  --expected-start "2022-03-31T00:00:00" \
  --expected-end "2022-03-31T23:00:00"
```

## Delta outputs

The run creates:

```text
output/
├── bronze/turbine_measurements
├── silver/turbine_measurements
└── gold/
    ├── turbine_daily_statistics
    └── turbine_daily_anomalies
```

The Delta merge keys are:

- Bronze/Silver: `(turbine_id, timestamp)`
- Gold: `(turbine_id, measurement_date)`

This keeps reruns idempotent instead of appending duplicate rows.

## Run tests

```bash
pytest
```

The tests exercise:

- reconstruction of a missing hourly record;
- physical validation;
- measurement-level IQR outlier handling;
- imputation;
- daily min/max/average calculations;
- data-quality counts;
- ±2σ turbine anomaly detection.

## Scalability notes

The code uses Spark transformations rather than local Pandas operations. Input files can be expanded by glob/path without changing the transformations. Daily Gold output is small, while detailed Silver data can remain partitioned by `measurement_date`.

For a larger production system I would additionally consider:

- Auto Loader / structured streaming or cloud object-store event ingestion;
- schema evolution and quarantine/dead-letter tables;
- Delta `OPTIMIZE`/compaction appropriate to the platform;
- turbine metadata and model-specific power curves;
- weather/location-aware expected-output models;
- late-data handling and watermarking;
- orchestration in Databricks Workflows, Airflow, or another scheduler;
- observability for row counts, missing percentages, source freshness, and anomaly volumes;
- CI that runs transformation tests and a small end-to-end Delta test.

## POC scope

This solution intentionally stays compact. The focus is on readable/testable Spark transformations and making assumptions explicit rather than building a large framework around three source files.
