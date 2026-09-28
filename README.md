# DineIQ Analytics

Restaurant intelligence for a 25-location restaurant chain. The project takes a generated year of operations data (about 1.07M order lines) through a Spark pipeline and an independent Python pipeline. The results are served in a role-based Streamlit web app: menu classification, customer segments, market-basket rules, demand forecasts, wastage risk, price and promotion analysis, anomalies, recommendations and a what-if simulator.

- **Report:** [`documentation/project_report.md`](documentation/project_report.md)
- **Diagrams** (architecture, ERD, DFD 0/1, use case, activity, sequence): [`documentation/diagrams.md`](documentation/diagrams.md)
- **Data dictionary:** [`documentation/data_dictionary.md`](documentation/data_dictionary.md)
- **Build log** (every step, decision and deviation): [`documentation/dev_log.md`](documentation/dev_log.md)
- **Technical blog:** [`documentation/technical_blog.md`](documentation/technical_blog.md)
- **Demo video script:** [`documentation/demo_video_script.md`](documentation/demo_video_script.md)
- **Deployment:** local install (see below). No hosted URL yet.

## Login credentials (evaluators)

Created by `database/init_db.py`. Passwords are stored as scrypt hashes; these plain values are demo credentials only.

| Role | Username | Password | Access |
|---|---|---|---|
| Administrator | `admin` | `Admin@123` | Everything, including Admin (users, master data, job monitoring, model registry, audit log) |
| Regional Manager | `regional` | `Regional@123` | All dashboards, Model Comparison, exports |
| Restaurant Manager | `manager` | `Manager@123` | All dashboards except Model Comparison, locked to LOC0001 |
| Analyst | `analyst` | `Analyst@123` | All dashboards, Model Comparison, exports |

---

## 1. Requirements

| | Tested with | Notes |
|---|---|---|
| OS | Ubuntu 26.04 LTS | Any Linux or macOS should work. On Windows, use WSL2. |
| Python | 3.14.4 | 3.11+ should work; the pins in `requirements.txt` are the tested versions |
| Java | OpenJDK 17 | Required by Spark |
| Spark | 4.2.0 via the `pyspark` pip package | No separate Spark download is needed |
| RAM | 8 GB | Spark's driver is set to 4 GB |
| Disk | ~1 GB | Data, Parquet and models are ~500 MB |

### Java

```bash
sudo apt install openjdk-17-jdk-headless
java -version                       # should print 17.x
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64   # add to ~/.bashrc
```

On macOS: `brew install openjdk@17` and set `JAVA_HOME` to its path.

### Spark and PySpark configuration

The `pyspark` package ships Spark itself, so `pip install -r requirements.txt` is the whole Spark installation. Every job builds its session through `spark_jobs/spark_utils.get_spark()`:

| Setting | Value | Why |
|---|---|---|
| `master` | `local[*]` | All local cores |
| `spark.driver.memory` | `4g` | The fact table has ~1M rows |
| `spark.sql.shuffle.partitions` | `16` | The default of 200 is far too many for local data |
| `spark.sql.session.timeZone` | `UTC` | Stable date and timestamp parsing |
| `spark.hadoop.fs.defaultFS` | `file:///` | Ignores any global `HADOOP_CONF_DIR` that points at HDFS; the project uses local files only |

To change memory or partitions, edit `get_spark()`.

## 2. Install

```bash
git clone https://github.com/MuzammilAsif/DineIQ-DataScience.git
cd DineIQ-DataScience
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

If `python3 -m venv` fails with "ensurepip is not available", install the venv package (`sudo apt install python3-venv`). You can also run `python3 -m venv --without-pip .venv`, then bootstrap pip with `get-pip.py`.

### Database setup

The app creates and seeds `database/app.db` automatically on first start if it is missing or incomplete (for example on a fresh clone or on Streamlit Community Cloud). To create it by hand instead:

```bash
.venv/bin/python database/init_db.py
```

This creates `database/app.db` (SQLite) from `database/schema.sql`:
- `users`: the four demo users above
- `audit_log`
- editable copies of `restaurants` and `menu_items`

It is idempotent, so running it again does not duplicate anything. Analytical data stays in Parquet; SQLite holds app state only.

## 3. Run the web app

The repository already contains the generated data, Parquet outputs and fitted models, so the app runs straight after install:

```bash
.venv/bin/streamlit run src/app.py
```

Open http://localhost:8501 and sign in with one of the credentials above. The first load of the Executive Dashboard reads the full order table and takes about 10 s. After that, pages load in 1-3 s.

## 4. Rebuild everything from scratch

Run every command from the project root. Each step reads the previous step's output. The times were measured on a 4-core, 8 GB VM; the full rebuild takes about 1.5 hours.

### 4.1 Generate the dataset (~1-2 min)

```bash
.venv/bin/python data_generator/generate_dataset.py              # scale 1.0, seed 42 -> raw_data/
.venv/bin/python data_generator/generate_dataset.py --scale 5.0  # ~5.3M order lines
.venv/bin/python data_generator/generate_dataset.py --clean      # no dirty-data injection
```

It writes 13 CSVs to `raw_data/`, plus these files in `reports/`:
- `dirty_injection_log.csv` (every corrupted row)
- `dirty_injection_summary.csv`
- `generation_row_counts.csv`

### 4.2 Spark ingestion, cleaning and Parquet generation (~9 min)

```bash
.venv/bin/python spark_jobs/01_ingest_and_validate.py   # explicit schemas, data-quality report
.venv/bin/python spark_jobs/02_clean.py                 # cleaning rules + quarantine -> processed_data/
.venv/bin/python spark_jobs/03_integrate_and_store.py   # Spark SQL joins -> parquet_data/
```

- `01` writes `reports/data_quality_report.md`.
- `02` writes `processed_data/<Table>.csv`, `processed_data/quarantine/`, `reports/cleaning_log.csv` and `reports/cleaning_row_counts.csv`.
- `03` writes `parquet_data/fact_order_line/`, partitioned by year and month, along with the orders, order items, inventory and wastage tables.

Each job logs its stages and durations to `reports/spark_execution_log_<job>.txt`.

### 4.3 Run Spark SQL

The integration queries live in `spark_sql/integration_queries.sql`: 10 joins plus the `fact_order_line` build, each marked `-- @name:`. `spark_jobs/03_integrate_and_store.py` registers the cleaned tables as views and runs them. To query the Parquet output yourself:

```bash
.venv/bin/python -c "
import sys; sys.path.insert(0, 'spark_jobs')
from spark_utils import get_spark
s = get_spark('adhoc')
s.read.parquet('parquet_data/fact_order_line').createOrReplaceTempView('f')
s.sql('SELECT category_name, ROUND(SUM(line_total)) AS revenue FROM f GROUP BY 1 ORDER BY 2 DESC').show()
"
```

### 4.4 Feature engineering and EDA (~4 min)

```bash
.venv/bin/python spark_jobs/04_feature_engineering.py
.venv/bin/jupyter nbconvert --to notebook --execute --inplace notebooks/01_eda.ipynb
```

- The job writes `parquet_data/features/{item,customer,location}_features/as_of_date=<date>/`.
- The notebook writes `reports/eda_report.md` and `reports/figures/`.

### 4.5 Menu analysis and Spark model training (~31 min)

```bash
.venv/bin/python spark_jobs/05_menu_classification.py          # rule-based classes (optional as_of_date arg)
.venv/bin/python spark_jobs/06_menu_classification_model.py    # MLlib LR / DT / RF, 5-fold CV (~28 min)
```

- **Menu analysis:** `parquet_data/menu_classification/` and `reports/menu_classification_report.md`. Every threshold is in `config/classification_thresholds.yaml`.
- **Spark model:** `models/spark/menu_classification/<version>/`, `reports/spark_mllib_menu_classification_report.md` and `reports/sample_predictions_menu_classification.csv`.

### 4.6 Analytics modules: segments, basket, wastage, price, promotions, anomalies (~25 min)

```bash
for j in 07_customer_segmentation 08_market_basket 09_wastage_analysis 10_price_intelligence \
         11_promotion_analysis 12_anomaly_detection 13_slow_moving_and_location 14_channel_and_churn; do
  .venv/bin/python spark_jobs/$j.py; done
```

| Job | What it does | Output |
|---|---|---|
| 07 | RFM + KMeans customer segments | `parquet_data/customer_segments/`, `models/spark/customer_segmentation/` |
| 08 | Market-basket analysis (FPGrowth) | `parquet_data/market_basket/{itemsets,rules}/` |
| 09 | Wastage analysis + wastage-risk RF model | `parquet_data/wastage_analysis/`, `parquet_data/wastage_risk/`, `models/spark/wastage_risk/` |
| 10 | Price elasticity and sensitivity classes | `parquet_data/price_sensitivity/` |
| 11 | Promotion effectiveness and traps | `parquet_data/promotion_effectiveness/` |
| 12 | Rating, sales, order-value, duplicate anomalies | `parquet_data/anomalies/` |
| 13 | Slow-moving items, location intelligence | `parquet_data/slow_moving/`, `parquet_data/location_intelligence/` |
| 14 | Channel analysis, churn risk | `parquet_data/channel_analysis/`, `parquet_data/churn_risk/` |

Each job rewrites its own section of `reports/restaurant_intelligence_report.md`. Thresholds are in `config/analytics_thresholds.yaml`. Job 14 reads job 07's output, so run 07 first.

### 4.7 Generate forecasts (~5.5 min)

```bash
.venv/bin/python spark_jobs/15_demand_forecasting.py
```

This trains an MLlib GBTRegressor at item x location x day grain and writes five roll-up views to `parquet_data/demand_forecast/`, plus `reports/forecast_report.md`. The horizon, lags and test length are in `config/forecast_config.yaml`.

### 4.8 Train the Python models and compare with Spark (~1 min)

```bash
.venv/bin/python -m python_pipeline.customer_segmentation_model   # scikit-learn KMeans
.venv/bin/python -m python_pipeline.menu_classification_model     # scikit-learn LR / DT / RF
.venv/bin/python -m python_pipeline.compare_pipelines             # record-level comparison
```

The Python pipeline reads only `processed_data/` and never imports Spark code (a test checks this). Models go to `models/python/`. The comparison writes `reports/dual_pipeline_comparison.csv` and `reports/dual_pipeline_comparison_report.md`.

### 4.9 Recommendations and what-if (~2 s)

```bash
.venv/bin/python -m python_pipeline.recommendation_engine        # -> parquet_data/recommendations/, reports/recommendations_report.md
.venv/bin/python -m python_pipeline.what_if_simulator ITEM0001    # sample: +10% price on one item, printed as JSON
```

### 4.10 Refresh the model registry

```bash
.venv/bin/python src/model_registry.py    # rebuilds models/model_registry.json from each model_version.txt
```

Then start the app as in section 3.

## 5. Using the app

| Task | Where |
|---|---|
| Log in / log out | Sign-in page; Log out is in the sidebar |
| Filter | The bar at the top of every page (date range, location, menu category, performance class). Filters persist across pages; each section's caption says which filters it applies |
| Access dashboards | Sidebar groups: Overview, Intelligence, Decisions & Actions, Platform |
| Menu analysis | Intelligence → **Menu Intelligence**: class counts, profitability vs demand map, item table, slow movers, ratings and wastage |
| View segments | Intelligence → **Customer Intelligence**: segment breakdown, RFM distribution, high-value / at-risk / promotion-sensitive lists |
| Basket analysis | The rules are in `reports/restaurant_intelligence_report.md` section 2; bundles appear as recommendations (type "Bundle frequently bought items") |
| Forecasts | Intelligence → **Demand Forecast**: history vs forecast vs baseline at chain, location, category or item level, accuracy table |
| Analyze wastage | Intelligence → **Wastage**: cost trend, worst items and locations, reasons, wastage-risk predictions |
| Anomalies | Decisions & Actions → **Anomalies** |
| View recommendations | Decisions & Actions → **Recommendations**: filter by priority, type or text; each card shows its evidence |
| What-if analysis | Decisions & Actions → **What-If Simulator**: pick an item and a scenario, set the input |
| Compare model results | Platform → **Model Comparison** (Spark vs Python agreement, disagreements) |
| Export reports | **Download CSV** under each table; **Download reports** at the bottom of the Executive Dashboard for any `reports/*.md`. Every export is written to the audit log |
| Admin | Platform → **Admin** (Administrator only): job monitoring, model registry, audit log, users, master data |

## 6. Run tests

```bash
.venv/bin/python -m pytest tests/ -v                  # full suite, ~17 min (Spark tests dominate)
.venv/bin/python -m pytest tests/test_app.py -v       # web app only, ~20 s
```

The last full run is saved in `reports/test_results.txt`. On an 8 GB machine, run the full suite with nothing else heavy open (see Troubleshooting).

## 7. Retake screenshots (optional)

Playwright isn't in `requirements.txt`, because only the screenshot script needs it.

```bash
.venv/bin/pip install playwright && .venv/bin/playwright install chromium
.venv/bin/streamlit run src/app.py --server.port 8599 &
.venv/bin/python screenshots/take_screenshots.py
```

## 8. Troubleshooting

| Problem | Fix |
|---|---|
| `JAVA_GATEWAY_EXITED` or "Java not found" | Install OpenJDK 17 and set `JAVA_HOME` (section 1) |
| `ConnectException: ... localhost:9000` | A global Hadoop config points at HDFS. `get_spark()` already forces `file:///`; if you build your own session, set `spark.hadoop.fs.defaultFS=file:///` |
| Spark job or test run dies, or the machine freezes | Out of memory. On 8 GB, don't run the Spark tests while Streamlit and a headless browser are also running. Lower `spark.driver.memory` in `spark_utils.py` if needed |
| `FutureWarning: PySpark does not yet fully support pandas >= 3.0.0` | Harmless; all tests pass with pandas 3.0.6 |
| "Login is unavailable: the user database could not be read" | Restart the app so it recreates the database, or run `.venv/bin/python database/init_db.py` |
| Port 8501 in use | `streamlit run src/app.py --server.port 8502` |
| Executive Dashboard slow on first load | Expected (~10 s); it reads the full order table once, then caches |
| A page says "No ... match the current filters" | Clear the filters in the top bar; menu and customer pages are snapshots and ignore the date range |
| `ensurepip is not available` | `sudo apt install python3-venv`, or see section 2 |

## 9. Repository layout

```
config/              thresholds for classification, analytics, forecasting (YAML)
data_generator/      synthetic dataset generator (13 tables, seed 42)
database/            SQLite schema + init script for users, audit log, master data
documentation/       report, diagrams, data dictionary, data-quality rules, dev log, blog
models/              Spark MLlib and scikit-learn models, model_registry.json
notebooks/           EDA notebook
parquet_data/        Parquet outputs (partitioned fact tables, features, analytics)
processed_data/      cleaned CSVs + quarantine
python_pipeline/     independent pandas/scikit-learn pipeline, recommendations, what-if
raw_data/            generated raw CSVs (with injected data-quality issues)
raw_data_parts/      multi-file ingestion demo (Order_Items split by month)
reports/             data-quality, EDA, classification, model, forecast, intelligence, comparison,
                     recommendation reports, Spark execution logs, test results
sample_data/         first 200 rows of each raw table, for a quick look
screenshots/         app screenshots + the Playwright script that takes them
spark_jobs/          Spark jobs 01-15 and shared helpers
spark_sql/           Spark SQL integration queries
src/                 Streamlit app (app.py, pages/, ui.py, style.css, auth, data loader)
tests/               pytest suite
```

## 10. License

MIT, see [LICENSE](LICENSE).
