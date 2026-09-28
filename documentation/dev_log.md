# Dev Log

## Step 1 — Schema design and dataset generation (2026-09-23)

**Built:**
- `data_generator/generate_dataset.py`: orchestrator. Builds the 13 tables in foreign-key order and writes `raw_data/<Table>.csv`.
- Supporting modules:
  - `reference_data.py`: locations, menu, item archetypes, promotion calendar
  - `dimensions.py`: restaurants, menu, pricing history, customers, promotions
  - `transactions.py`: orders and order items
  - `operations.py`: ratings, inventory, wastage
  - `dirty.py`: data-quality injection
  - `common.py`: shared helpers
- Schema documented in `documentation/data_dictionary.md`.

**Run:**
```
.venv/bin/python data_generator/generate_dataset.py --scale 1.0          # default, seed 42
.venv/bin/python data_generator/generate_dataset.py --scale 5.0          # ~5.3M order lines
.venv/bin/python data_generator/generate_dataset.py --clean              # skip dirty-data injection
```
- `--scale` multiplies the number of customers, orders, order lines and ratings.
- Inventory stays at one row per item per location per day, so its row count does not scale. Its quantities do, because they come from actual sales.
- Scale 1 runs in about 30–110 s, depending on the machine.
- Scale 5 took 88 s and peaked at about 3.5 GB of RAM.

**Setup note:** the machine had no pip or venv support (`python3.14-venv` is not installed). The project `.venv` was created with `--without-pip`, and pip was bootstrapped with `get-pip.py`. Dependencies are in `requirements.txt`.

**Assumptions:**
- The chain is set in Pakistan: 25 locations in 11 cities.
- Prices are in PKR, with provincial sales tax.
- The spec's Monsoon season and Fri–Sun weekend pointed to this setting.
- Ramadan (March 2025) moves order hours to the evening.
- The Eid days, Independence Day and year-end get demand spikes.

**Output at scale 1.0 (seed 42), including injected duplicates:**

| Table | Rows |
|---|---|
| Restaurants | 25 |
| Menu_Categories | 14 |
| Menu_Items | 160 |
| Pricing_History | 959 (2–5 chain-wide price points per item) |
| Customers | 55,000 |
| Promotions | 20 |
| Promotion_Items | 1,316 |
| Promotion_Locations | 424 |
| Orders | 196,547 (194,601 unique) |
| Order_Items | 1,070,114 (1,059,519 unique; 1,028,791 with every corrupted row removed) |
| Ratings | 105,000 |
| Inventory | 1,251,246 |
| Wastage | 60,290 |

**Patterns built into the data (none of them are labelled in the CSVs):**
- **Weekends:** Fri–Sun is about 17% of weekly orders per day, against about 12% on weekdays. Downtown locations are flatter.
- **Peak hours:** 12–14 and 19–22.
- **Seasonality:** monthly seasonality plus seasonal items (Summer, Winter, Monsoon).
- **Growth:** about 8% growth over the year.
- **Location spread:** order volume ranges about 4.5x between the quietest and busiest location. Each location also has its own sales mix and service quality, so the same item sells and rates differently by location.
- **Customer archetypes:** loyal high-value, frequent, promotion-driven, churned, new (October–December signups) and occasional. They show up in frequency, recency and spend. Median orders per customer is 2, the 99th percentile about 20, and about 7% of customers never ordered.
- **Item archetypes:**
  - High-volume, low-margin items: margin about 31% against a median of 59%.
  - High-margin, low-volume items: margin about 84%, volume rank around 130 of 160.
  - High-wastage popular items: the top 6 items by wastage cost.
  - Poorly rated items: average rating about 2.0 against 3.7 overall.
  - Promotion-dependent items: about 73% of their units sell on promotion orders, against 19% for all items.
- **Promotion trap:** "August BOGO Blast" (PROMO012). Compared with July, August has:
  - orders +58% and unique customers +38%
  - gross margin down from 57.4% to 53.9%
  - wastage cost about 3.5x, because kitchens over-prepare for the promotion
- **Price changes:** 7 items get a mid-year price rise of 18–28%, and their demand falls (e.g. Mutton Biryani, about 46 → 30 units a day). 3 items get a 15% cut, and their demand rises.
- **Cancellations:** about 4.2% of orders are cancelled, more on delivery channels. This is real data, not injected.

**Consistency checks on the clean build (`--clean`, no injection): zero violations.**
- Order subtotal, discount and total match the order's lines.
- Order line unit prices match the Pricing_History price in effect for that location and date.
- `closing_stock = opening + received − consumed − wasted` on every Inventory row.
- Wastage never exceeds prepared quantity.

**Injected dirtiness:**
- Full row-level log: `reports/dirty_injection_log.csv`
- Per-rule counts: `reports/dirty_injection_summary.csv`
- Each rule uses its own rows within a table.

| Table | Rule (column) | Rows |
|---|---|---|
| Customers | invalid_date (signup_date) | 275 |
| Customers | orphan_foreign_key (home_location_id) | 275 |
| Menu_Items | invalid_price (base_price) | 3 |
| Pricing_History | invalid_price (price) | 14 |
| Orders | missing_value (customer_id) | 1,946 |
| Orders | orphan_foreign_key (customer_id) | 973 |
| Orders | orphan_foreign_key (location_id) | 584 |
| Orders | invalid_date (order_datetime) | 973 |
| Orders | discount_exceeds_subtotal | 973 |
| Orders | duplicate_row | 1,946 |
| Order_Items | missing_value (item_id) | 5,298 |
| Order_Items | negative_quantity | 5,298 |
| Order_Items | invalid_price (unit_price) | 3,179 |
| Order_Items | orphan_foreign_key (order_id) | 3,179 |
| Order_Items | orphan_foreign_key (item_id) | 3,179 |
| Order_Items | duplicate_row | 10,595 |
| Ratings | missing_value (rating_value) | 1,575 |
| Ratings | rating_out_of_range | 1,050 |
| Ratings | invalid_date (rating_date) | 525 |
| Ratings | orphan_foreign_key (location_id) | 525 |
| Ratings | orphan_foreign_key (customer_id) | 315 |
| Inventory | inconsistent_unit | 12,512 |
| Inventory | orphan_foreign_key (item_id) | 3,754 |
| Wastage | wastage_exceeds_prepared | 603 |
| Wastage | inconsistent_unit | 1,206 |
| Wastage | invalid_date | 301 |
| Wastage | missing_value (item_id) | 301 |
| Wastage | orphan_foreign_key (location_id) | 301 |

**Known limitations:**
- Promotions are not tied to order channels. For example, the "App Exclusive Fest" can apply to dine-in orders.
- Corrupted rows keep the other columns' original values, so a negative-quantity line still has its original `line_total`.

**Next step:** Spark ingestion, schema validation, cleaning and Parquet conversion.

## Step 2 — Spark ingestion, data quality, cleaning, integration (2026-09-24)

**Built:**
- `spark_jobs/schemas.py`: explicit `StructType` per table (13/13), plus a raw all-string variant used for the type-validation check.
- `spark_jobs/spark_utils.py`: Spark session factory, `JobLogger` (console + `reports/spark_execution_log_<job>.txt`, with a `timer()` context manager for stage durations), single-file CSV/JSON writers.
- `spark_jobs/01_ingest_and_validate.py`: loads all 13 tables with explicit schemas; schema-inference demo (`Restaurants`, `inferSchema=True`, logged side by side with the declared schema); data-type validation (typed load vs. raw-string load diff on the 4 date/timestamp columns with injected malformed values); large-file load (`Order_Items` direct) plus a multi-file ingestion demo (joins `Order_Items` to `Orders` for the month, writes it partitioned by `order_month`, then reads that directory back as one DataFrame — 39 part files); referential-integrity orphan counts via left-anti join (not `.isin()` — see gotcha below); the full SRS Step-4 issue checklist. Writes `reports/data_quality_report.md` (+ a `.csv` twin for the tests) with a cross-check against `reports/dirty_injection_summary.csv`.
- `spark_jobs/02_clean.py`: applies `documentation/data_quality_rules.md` per table — dedupe (`Orders`, `Order_Items`), price correction from `Pricing_History` (`Menu_Items.base_price`, `Order_Items.unit_price`), unit normalization (`Inventory`/`Wastage`), and quarantine for everything else. Writes `processed_data/<Table>.csv`, `processed_data/quarantine/<Table>.csv` (with `quarantine_reason`), `reports/cleaning_log.csv`, `reports/cleaning_row_counts.csv`.
- `spark_sql/integration_queries.sql`: the 10 required joins plus `fact_order_line`, the order-line-grain base fact table (left join outward from `Order_Items`). Parsed and run by `spark_jobs/03_integrate_and_store.py` via `-- @name:` markers.
- `spark_jobs/03_integrate_and_store.py`: registers the cleaned tables as temp views, runs all 11 queries, writes `fact_order_line` to `parquet_data/fact_order_line/` partitioned by `order_year`/`order_month`; `Orders`, `Inventory`, `Wastage` to `parquet_data/<table>/` partitioned by `year`/`month`; `Order_Items` likewise (joined to `Orders` for the date). Writes `Menu_Categories`/`Promotions` to `processed_data/*.json` as the storage-format demo.
- `documentation/data_quality_rules.md`: the cleaning rules, written before implementation, per rule.
- `tests/`: `test_schema.py`, `test_data_quality.py`, `test_cleaning.py`, `test_joins.py`, `test_parquet_roundtrip.py` — 20 tests, all passing.

**Run (in order):**
```
.venv/bin/python spark_jobs/01_ingest_and_validate.py
.venv/bin/python spark_jobs/02_clean.py
.venv/bin/python spark_jobs/03_integrate_and_store.py
.venv/bin/python -m pytest tests/ -v
```
Scale-1 seed-42 dataset: ingest+validate ~75s, clean ~4 min, integrate ~3 min, tests ~85s.

**Row counts, before -> after cleaning (scale 1.0):**

| Table | Before | After | Removed |
|---|---|---|---|
| Pricing_History | 959 | 945 | 14 |
| Customers | 55,000 | 54,450 | 550 |
| Orders | 196,547 | 187,228 | 9,319 |
| Order_Items | 1,070,114 | 1,002,824 | 67,290 |
| Ratings | 105,000 | 97,121 | 7,879 |
| Inventory | 1,251,246 | 1,247,492 | 3,754 |
| Wastage | 60,290 | 58,787 | 1,503 |

Removed counts run higher than the Step 1 injection counts alone because quarantine cascades: e.g. a `Customers` row quarantined for a bad `signup_date` or invalid `home_location_id` makes every `Orders` row referencing it an orphan too, which in turn cascades to `Order_Items` and `Ratings`. This is intentional — it's what a real FK-aware cleaning pipeline does — and is called out in `reports/data_quality_report.md`'s cross-check section, which compares Step 2's detected counts against Step 1's injected counts on the *raw* data (where they match almost exactly, see below).

**Data quality cross-check:** on the raw data, Step 2's detected counts match Step 1's injected counts almost exactly (see `reports/data_quality_report.md`). The only gaps: `Order_Items` duplicate count runs +68 high (the (order_id, item_id) dedupe key also catches unrelated rows that share an order_id and both have a null item_id), `Wastage.wastage_exceeds_prepared` runs -3 low (a few Wastage rows have no matching Inventory row to compare against), and `Orders.cancelled_transaction_count` (8,262 rows, 4.2%) has no Step 1 counterpart by design — cancellations are real business data, not injected.

**Gotchas hit and fixed:**
- The machine's global `HADOOP_CONF_DIR` points `fs.defaultFS` at an HDFS namenode that isn't running (`hdfs://localhost:9000`), so any local-path read/write failed with `ConnectException`. Fixed by pinning `spark.hadoop.fs.defaultFS=file:///` in `get_spark()` — this project only touches local directories.
- Orphan-FK checks originally collected the parent table's keys into a Python list and used `.isin(list(...))`. With parent tables up to ~195k rows, the resulting IN-list expression made the driver choke (heartbeat timeouts, effectively hung). Rewrote as a left join against distinct parent keys (`with_orphan_flag()` in `02_clean.py`) — same result, no driver-side blowup.
- `02_clean.py` referenced `cleaned["Orders"]` etc. repeatedly across later table-cleaning blocks without caching, so Spark recomputed the entire upstream lineage (window functions and all) on every later action — one run took over 10 minutes before being fixed. Fixed by `.cache()` + an immediate `.count()` right after each `cleaned[table]` is finalized.
- Spark's CSV writer serializes `TimestampType` as ISO-8601 with a `T`/`Z` (`2025-01-01T00:12:52.000Z`) by default, which doesn't match the `yyyy-MM-dd HH:mm:ss` format the loader's `timestampFormat` option expects on reload — every `Orders.order_datetime` value came back null after a write/read round-trip, silently collapsing the year/month partitioning to a single `__HIVE_DEFAULT_PARTITION__` bucket. Fixed by pinning `dateFormat`/`timestampFormat` on write too (`_write_single_csv()`), so cleaned CSVs round-trip through the same schema they were written with.
- The unit-normalization map only covered the variants listed in the data dictionary's example (`kgs`, `KG`, `ltr`, ...) and missed variants actually present in the generated data (`Pcs`, `PCS`, `kilogram`, `Liters`) — caught by `tests/test_cleaning.py::test_units_normalized` failing with 4,656 un-normalized rows. Fixed by checking the raw value distribution directly and extending `UNIT_MAP`.

**Next step:** feature engineering (revenue, margin, RFM, etc.) on top of `parquet_data/fact_order_line/`.

## Step 3 — Feature engineering and EDA (2026-09-24)

**Built:**
- `spark_jobs/04_feature_engineering.py`: item, customer and location feature functions. Each takes an `as_of_date` and only uses orders, ratings, wastage and pricing dated on or before it; the whole `as_of_date` day is included. Also attaches the order-level `basket_size` to `parquet_data/fact_order_line/` by rewriting it through a temp directory. It is idempotent, so a rerun replaces the column.
- `parquet_data/features/{item,customer,location}_features/as_of_date=<date>/`: two snapshots, 2025-12-31 (dataset max) and 2025-09-30 (3 months earlier, to prove the parameter works). Item: 160 / 154 rows (items launched after the cutoff are excluded). Customer: 49,606 / 38,317. Location: 25 / 25.
- `tests/test_feature_engineering.py`: 18 tests on a hand-computed synthetic dataset. They cover every feature, as-of filtering at two cutoffs, the whole-day cutoff boundary, division-by-zero cases (unsold item, location with no orders), `rating_trend` null under 3 months, and peak-hour boundaries.
- `notebooks/01_eda.ipynb`: writes `reports/eda_report.md` and `reports/figures/*.png`. The report numbers are computed in the notebook, so re-executing it regenerates the report.

**Run:**
```
.venv/bin/python spark_jobs/04_feature_engineering.py        # ~1.5 min
.venv/bin/jupyter nbconvert --to notebook --execute --inplace notebooks/01_eda.ipynb   # ~2 min
.venv/bin/python -m pytest tests/ -v                         # 38 tests
```

**Definitions checked against the generator:**
- Weekend is Fri–Sun, matching `DOW_FACTORS` in `transactions.py`.
- Peak hours are the half-open windows [12:00, 14:00) and [19:00, 22:00). These match the `HOUR_BASE` maxima at 12–13 and 19–21. Hours 14 and 22 are elevated shoulder hours but fall outside the windows.
- During Ramadan (March) the generator moves the peak to 18:00 and lunch nearly vanishes. The EDA reports this. The fixed peak windows under-count March "peak" orders; they are not month-aware.

**Decisions / deviations from the spec:**
- **Wastage units.** `Wastage.quantity_wasted` is in kg, liters or pcs, while `total_quantity_sold` is in portions, so the spec formula would mix units. Wasted quantity is converted to portions as `wastage_cost / avg unit cost of the item's sales`, falling back to `Menu_Items.base_cost` for unsold items. The generator sets `cost_of_waste = units / portion * unit_cost`, so this recovers portions without using the generator's portion sizes. `wastage_cost` is also output.
- **`price_change_percentage`** uses chain-wide Pricing_History rows only (`location_id` null). The location-premium rows are one-off overrides, not a price trajectory.
- **Ratios are fractions (0–1)** where the spec's formula has no ×100: `wastage_percentage`, `promotion_dependency`, `discount_percentage`, repeat rates. `profit_percentage` and `price_change_percentage` are ×100, as specified.
- **Customer features cover only customers with at least one non-cancelled order by `as_of_date`.** Recency is undefined for the rest.
- **`channel_preference` ties** go to the alphabetically first channel, for deterministic reruns.
- **Grain.** Item grain only; the optional item × location grain was not built.
- **Cutoffs.** Ratings are cut by `rating_date` and items by `launch_date`, as well as orders by `order_datetime`.
- **Unmatched orders.** 41 orders have no surviving order lines after cleaning. They have null `basket_size` and are left out of `basket_size_avg`.

**Findings summary:** see `reports/eda_report.md`. The planted Step 1 patterns all come through: high-volume/low-margin staples (~30% margin vs 59% median), high-margin beverages and desserts (~84%), five poorly rated dishes (1.87–2.21), promotion-dependent items (up to 75% promo revenue), the August BOGO trap (orders +58%, margin 57.4% → 53.9%, wastage cost 3.6x July), and the 4.2x location revenue spread.

**Next step:** menu performance classification and customer segmentation on the feature tables.

## Step 4 — Menu profitability and performance classification (2026-09-24)

**Built:**
- `config/classification_thresholds.yaml`: every threshold (tier cutoffs, new-item days, wastage floor, promotion/weekend/rating cutoffs, location-check minimums). The job and the tests both read it; editing one value changes the output with no code change. A copy of the file used is written next to each output as `_thresholds.yaml`.
- `spark_jobs/04_feature_engineering.py`: two new item features. `weekend_order_ratio` is Fri–Sun quantity over total quantity. `sales_trend` is the slope of monthly quantity sums, null under 3 months. Both slopes now share `monthly_slope()`. Also new: `item_location_features()`, the metrics classification needs at item × location grain. The main job doesn't persist it; `05` calls it.
- `spark_jobs/05_menu_classification.py`: `classify()` computes percentiles, tiers and category; `add_flags()` adds the tricky-case flags; `location_consistency()` reruns `classify()` with percentiles partitioned by location. `build()` wires them together. Output:
  - `parquet_data/menu_classification/as_of_date=2025-12-31/`: 156 rows. Each row has the category, all 10 flags, 4 percentiles and tiers, raw metrics, and `location_categories` (a map of location to category).
  - `parquet_data/menu_classification_by_location/as_of_date=2025-12-31/`: 3,518 item × location rows with per-location percentiles, kept for Step 34.
  - `reports/menu_classification_report.md`, regenerated on every run.
- `tests/test_menu_classification.py`: 25 tests. Each target item is ranked against 10 evenly spaced filler items. The tests cover every category, every tricky case, zero sales, the new-item cutoff on both sides, and two config edits that each flip a boundary item's category. Boundary values come from the loaded config, so the tests keep passing after a threshold change. `tests/test_feature_engineering.py` gained 3 tests for the new features; its ratings fixture now has `location_id`.

**Run:**
```
.venv/bin/python spark_jobs/04_feature_engineering.py     # rerun first for the new columns
.venv/bin/python spark_jobs/05_menu_classification.py     # ~2.5 min; optional as_of_date arg
.venv/bin/python -m pytest tests/test_menu_classification.py -v
```
`pyyaml` added to `requirements.txt`.

**Result (2025-12-31):** Profit Driver 12, Volume Driver 38, Hidden Opportunity 43, Low Performer 57, Insufficient History 6.

**Decisions / deviations from the spec:**
- **Population.** Only active items are classified (`Menu_Items.is_active`), so 4 inactive items are dropped. `is_active` is the dataset-end status, not as-of-aware. Unsold items and items under the new-item cutoff are also left out of the percentile ranking, so their partial numbers don't shift established items' ranks. New items get null percentiles, and the percentile-based flags are false for them.
- **`quality_score`** = mean of `average_rating / 5` and `repeat_purchase_rate`. If only one of the two exists, that one is used.
- **Inclusive vs. strict comparisons** follow the spec table: rating `>=` 4.3 and `<=` 3.0, location mismatch share `>=` 25%, and everything else strict.
- **`location_categories`** is stored for every item sold at ≥ 3 locations, not only flagged items, because Step 34 needs the consistent items too.

**Known gaps in the real data (reported, not fabricated):**
- `loss_making`: 0 items. The lowest contribution margin is +343k (Tandoori Roti).
- `high_rating_low_profit`: 0 items. The highest average rating is 4.27, under the 4.3 threshold.
- `weekend_skewed` has just 1 item (Chicken Mac and Cheese at 55.4%), and `low_rating_high_sales` has 1 (Mutton Pulao, 2.21, demand percentile 0.72).

**Notes for later steps:**
- `location_inconsistent` fires for 48 of 156 items (median mismatch share 12%). A 25% mismatch is easy to reach because an item near a tier boundary flips category at a location on small per-location noise. If Step 34 finds this too noisy, raise `location.inconsistent_share` in the config.
- `excessive_wastage` fires for 35 items, but only 1 of them (Seekh Kabab, 19.5% wastage) lost Profit Driver status because of it. Most are already low-demand.

## Step 5 — Spark MLlib menu classification model (2026-09-25)

**Built:**
- `spark_jobs/06_menu_classification_model.py`: predicts the Step 4 category from the 16 raw Step 3 item metrics. Insufficient History items are dropped, leaving 150. The job makes a stratified `sampleBy` 80/20 split (seed 42) and runs 5-fold `CrossValidator` on the 80%. It compares Logistic Regression (multinomial, scaled), Decision Tree and Random Forest, and picks the final model by macro F1 on the held-out set.
- `models/spark/menu_classification/<version>/`: the fitted `PipelineModel` plus `model_version.txt` (algorithm, params, snapshot, metrics). `LATEST` holds the current version. Every row in `reports/sample_predictions_menu_classification.csv` carries `model_version`.
- `reports/spark_mllib_menu_classification_report.md`, regenerated on every run.
- `tests/test_menu_classification_model.py`: covers the end-to-end run on synthetic items, leakage (the assembler's inputs against the percentile, tier, flag and category columns), macro F1 against a hand-computed confusion matrix, and a save/reload that must reproduce the same predictions.

**Run:** `.venv/bin/python spark_jobs/06_menu_classification_model.py` (~28 min, almost all of it CV overhead on tiny data).

**Result (2025-12-31 snapshot, 28 test items):**

| Model | CV macro F1 | Test accuracy | Test macro F1 |
|---|---|---|---|
| Logistic Regression (regParam 0.01) | 0.707 | 0.821 | 0.812 |
| Decision Tree (maxDepth 3) | 0.884 | 0.786 | 0.798 |
| **Random Forest (30 trees, depth 10)** | 0.822 | **0.929** | **0.914** |

Both SRS targets are met (accuracy ≥ 0.85, macro F1 ≥ 0.80).

**Decisions / deviations:**
- **Macro F1** comes from `pyspark.mllib.evaluation.MulticlassMetrics.fMeasure(label)`, averaged over the classes present in the actual labels. The ML evaluator's `"f1"` is weighted F1. A custom `Evaluator` makes CrossValidator tune on macro F1 too. `MulticlassMetrics` throws for a class that is predicted but never actual. That can't happen here because every class is in every test fold.
- **Stratified folds.** Folds are dealt round-robin per class via `foldCol`, because random folds could leave one with no Profit Driver (only 8 in training).
- **Balanced class weights** (`weightCol`) because macro F1 weights the 12 Profit Drivers the same as the 57 Low Performers.
- **Nulls.** `price_change_percentage`, `rating_trend` and `sales_trend` nulls are set to 0 (no change / no trend). A training-median `Imputer` stays in the pipeline as a fallback.
- `spark.sql.shuffle.partitions` is set to 1 for this job; with 16 partitions, loading 150 rows took 68s.

**Known weaknesses (in the report):**
- Every test class has fewer than 10 items (Hidden Opportunity 5, Profit Driver 4, Volume Driver 7), so per-class metrics are indicative only.
- Selecting on the test set makes the winner's score optimistic. The Decision Tree had the best CV score but the worst test score.
- The label is a percentile function of raw metrics that are also features, so high scores are expected; the report says so.

## Step 6 — Core analytics bundle (2026-09-25)

**Built:** `spark_jobs/07`–`14`, one module per SRS area, plus `spark_jobs/analytics_common.py`, which holds the config loader, the shared report writer and promotion coverage.
- Every threshold is in `config/analytics_thresholds.yaml`.
- Each module writes its own table under `parquet_data/` and rewrites only its own section of `reports/restaurant_intelligence_report.md`. Sections are delimited by HTML comments and kept sorted. A Limitations section is always kept at the end.
- `tests/test_analytics.py` has 11 core-formula tests.

**Run** (order matters only for 14, which reads 07's segments):
```
for j in 07_customer_segmentation 08_market_basket 09_wastage_analysis 10_price_intelligence \
         11_promotion_analysis 12_anomaly_detection 13_slow_moving_and_location 14_channel_and_churn; do
  .venv/bin/python spark_jobs/$j.py; done          # ~25 min total; 09 ~10 min, 08 ~5 min
.venv/bin/python -m pytest tests/test_analytics.py -v
```

**Results (2025-12-31):**

| Module | Output | Headline |
|---|---|---|
| 07 Segmentation | `customer_segments/`, `models/spark/customer_segmentation/` | 49,606 customers in 6 KMeans clusters, one label each. High-Value Loyal is 6,602 (mean spend 107k, 12 orders); Promotion-Driven is 8,886 (88% promo orders). |
| 08 Market basket | `market_basket/{itemsets,rules}/` | 449 rules at 0.005 / 0.1. Max lift is 1.48; only 4 of the top 20 clear 1.2. |
| 09 Wastage | `wastage_analysis/`, `wastage_risk/`, `models/spark/wastage_risk/` | Karahi & Handi and Salads waste 12–13% of cost; promotion days waste 6.8% vs 4.3%. The RF risk model on the Oct–Dec test (318k item-days, 4.2% positive) scores ROC AUC 0.76 and PR AUC 0.23, with precision 0.23 and recall 0.46. |
| 10 Price | `price_sensitivity/{events,items}/` | 120 of 284 changes are evaluable: 13 Highly, 15 Moderately and 15 Low sensitivity, 53 Inconclusive, 60 Not Evaluated. |
| 11 Promotions | `promotion_effectiveness/` | 15 of 20 flagged, 8 of them on margin or post-promo drop. PROMO012 August BOGO: orders +87%, margin rate 60.9% → 47.0%, wastage cost 5.5x. |
| 12 Anomalies | `anomalies/` | Rating, sales, order-value and duplicate flags; 0 duplicate transactions. The busiest flag dates are promotion launches and holidays. |
| 13 Slow-moving / location | `slow_moving/`, `location_intelligence/` | 6 slow-moving items. Location revenue spread is 4.2x. |
| 14 Channel / churn | `channel_analysis/`, `churn_risk/` | Channel margins are within 0.3 pts; ThirdPartyDelivery cancels most (8.1%). 7,893 customers (15.9%) are churn-risk. |

**Decisions / deviations from the spec (each also in the report):**
- **Segment labels are rank-based.** Each cluster gets one label, by highest spend, promo share, recency, lowest tenure, then frequency. Fixed cutoffs (recency > 90 days) labelled 4 of 6 clusters At-Risk, because the typical recency is ~100 days. Frequency and spend are log-transformed before scaling.
- **Market basket thresholds.** 0.01 / 0.3 produced no rules, because the highest pair confidence in the data is 0.20. Lowered to 0.005 / 0.1. Rules under lift 1.2 are reported as weak, not as bundles.
- **Wastage-risk label.** Wasted / prepared > 0.10, not a top quartile: 95% of item-days have no waste and the 75th percentile of the rest is 1.0. The split is time-based (test from 2025-10-01), and `item_popularity` comes from the 2025-09-30 snapshot.
- **Leakage exclusions for wastage risk:** the day's waste, wastage %, consumed and closing stock, and item wastage features. The only history feature is `recent_demand`, the previous 7 days, excluding the day itself.
- **Wastage rate is cost-based**, wastage cost / (wastage cost + COGS), because wasted units (kg/l/pcs) and sold portions don't mix.
- **Price elasticity**:
  - adjusted by the rest of the item's category over the same windows, to take out shared seasonality;
  - changes under 5% are skipped;
  - positive elasticities are Inconclusive rather than forced into a class. Without the control and the sign rule, seasonal items (Haleem, pakoras, lassi) came out as the most price-sensitive.
- **Promotion windows** use the promotion's own items and locations and all orders, not only those that used the code. Pre and post windows have the same length as the promotion. `wastage_up` fires for most promotions (kitchens over-prepare for all of them), so the report separates the margin-based traps.
- **Churn rule:** recency > 2x the average inter-order gap (57.4 days → 115 days), and fewer orders in the last 90 days than in the 90 before.
- **Spark 4 ANSI mode** raises on division by zero, so ratio columns are guarded.

**Notes for later steps:**
- The dual-pipeline comparison should use `customer_segments` (49,606 rows) as planned. The saved KMeans pipeline is in `models/spark/customer_segmentation/as_of_date=2025-12-31/`.
- The wastage-risk model mostly learns item identity (item features carry 84% of the importance). Demand forecasting could feed it a better day-level signal.

## Step 7 — Independent Python pipeline and dual-pipeline comparison (2026-09-25)

**Built:**
- `python_pipeline/features.py`: pandas re-implementation of the Step 3 item and customer features, reading only the cleaned CSVs in `processed_data/`. It also adds `promo_order_share` and `signup_days` for segmentation.
- `python_pipeline/segment_rules.py`: the rule-based "Actual" customer segment (R/F/M tertiles plus promo share and signup date, thresholds in `config/analytics_thresholds.yaml` → `segment_rules`), the crc32 holdout mask and majority-vote cluster mapping.
- `python_pipeline/customer_segmentation_model.py`: scikit-learn KMeans (k=6), with the same five inputs as Spark's module 07.
- `python_pipeline/menu_classification_model.py`: scikit-learn LR/DT/RF, with the same grids, balanced weights and 5-fold stratified CV as Step 5.
- `python_pipeline/compare_pipelines.py`: the record-level comparison CSV and the report.
- Models go to `models/python/{customer_segmentation,menu_classification}/<version>/` (joblib + `model_version.txt` + `LATEST`), and segments to `models/python/customer_segments.parquet`.
- `tests/test_python_pipeline.py`: 12 tests. Four are AST checks that the pipeline files import nothing Spark-side and never reference Spark's features or segments. The rest cover a feature spot-check on hand-built tables, agreement % on a hand-built example, the segment rules, and crc32 split parity with Spark.
- `scikit-learn` added to `requirements.txt`.

**Spark-side change:** `07_customer_segmentation.py` now fits KMeans only on customers with `crc32(customer_id) % 5 != 0`. It assigns the rest (9,899 holdout customers) and outputs `split` and `distance_to_centroid`. The distance is computed with native array functions, because a Python UDF failed to unpickle in the worker.

**Run:**
```
.venv/bin/python spark_jobs/07_customer_segmentation.py       # after any config change
.venv/bin/python -m python_pipeline.customer_segmentation_model   # ~15 s
.venv/bin/python -m python_pipeline.menu_classification_model     # ~20 s
.venv/bin/python -m python_pipeline.compare_pipelines             # ~25 s
```

**Results:**

| Task | Records | Spark vs Actual | Python vs Actual | Spark vs Python |
|---|---|---|---|---|
| Customer segmentation (holdout) | 9,899 | 64.0% | 64.7% | 98.9% (ARI 0.969) |
| Menu classification (RF, held-out) | 28 | 92.9% | 92.9% | 100.0% |

- **Feature parity:** Python and Spark agree on every item and customer feature. The largest absolute difference is 1.5e-6, in item cost sums (relative difference under 1e-13), and there are no null mismatches.

**Decisions / deviations:**
- **Menu test set.** The Python menu model uses Spark's 28 held-out item IDs instead of its own `train_test_split`, so both pipelines are scored on the same unseen records. Only the IDs are shared.
- **Like-for-like algorithm.** The record-level menu comparison uses Python's RandomForest, the same algorithm as Spark's final model. Python's own held-out selection picked unpenalized Logistic Regression (macro F1 0.935 vs RF 0.914, one item's difference), even though it had the worst CV score (0.652).
- **Regularisation mapping.** Spark regParam is mapped to sklearn `C = 1 / (regParam * n_train)`; regParam 0 becomes `C = inf`.
- **Segment rule order.** High-Value Loyal, New, At-Risk, Promotion-Driven, Frequent, Occasional. At-Risk comes before Frequent so lapsed frequent customers count as At-Risk.
- **Cluster mapping.** Both pipelines' clusters are mapped by majority vote over their own training customers, not by module 07's rank-based labels, so the two are mapped the same way.

**Known limits (in the report):**
- No cluster on either side maps to New or Frequent, so those 1,567 holdout customers are misses for both pipelines. New depends on signup date, which isn't a clustering input.
- Agreement with Actual (~64%) measures how well unsupervised clusters line up with a business rule. It is not a supervised accuracy.

## Step 8 — Demand forecasting (2026-09-25)

**Built:**
- `spark_jobs/15_demand_forecasting.py`: one Spark MLlib `GBTRegressor` at item x location x day grain, rolled up afterwards to item, category, location and chain-total views.
- `config/forecast_config.yaml`: horizon (default 7), lags, rolling windows, test length and GBT settings.
- Outputs: `parquet_data/demand_forecast/{item_location_day,item_day,category_day,location_day,overall_day}/` (test period; actual, forecast and baseline), `models/spark/demand_forecast/<version>/` and `reports/forecast_report.md`.
- `tests/test_demand_forecasting.py`: 4 tests covering exact lag values, gap handling, horizon shifting, the chronological split and hand-computed metrics.

**Run:** `.venv/bin/python spark_jobs/15_demand_forecasting.py` (~5.5 min; GBT training ~3 min).

**Design:**
- **Grid.** The grid is the Inventory rows, i.e. the days each item was stocked at each location. A stocked day with no sales is a real 0. Stocked days cover 99.7% of sold quantity.
- **Rows.** A row is a forecast made on `origin_date` with data up to the day before. `lag_k` and `rolling_mean_w` use days before the origin. The target is `origin + horizon - 1`. Calendar, promotion and price features describe the target date; they are known in advance.
- **Exact offsets.** Lags join on exact calendar offsets, and rolling windows must be complete, so a gap drops the row instead of borrowing a neighbouring day. 126,080 of 1,247,492 rows are dropped, 2,438 of them for having no price on or before the target date.
- **Split.** Chronological on target date: train targets run to 2025-11-16, test targets 2025-11-17 to 2025-12-31 (45 days, 149,128 rows).
- **Baseline.** Same weekday last week (`7 * ceil(h / 7)` days before the target), which is `lag_7` when h = 1.
- **Price.** A location override applies when its window covers the date; otherwise the latest chain price that started on or before the date, carried over gaps between Pricing_History rows.

**Result (horizon 7):**

| Level | Baseline MAE | Model MAE | Baseline RMSE | Model RMSE | Model R² |
|---|---|---|---|---|---|
| item x location x day | 1.59 | 1.35 | 2.79 | 2.14 | 0.47 |
| item x day | 10.70 | 8.40 | 17.75 | 13.02 | 0.92 |
| category x day | 74.3 | 51.6 | 118.5 | 76.3 | 0.93 |
| location x day | 67.6 | 51.2 | 87.5 | 65.8 | 0.62 |
| chain x day | 928 | 512 | 1,226 | 667 | 0.80 |

The model beats the baseline on MAE and RMSE at every level.

**Known weakness (in the report):**
- Item-day MAPE is worse than the baseline's (64.1% vs 55.3%). Item-days selling 1–5 units are always over-forecast (MAPE 328% vs 203%). From 21 units up the model wins (22% vs 32%), and the median APE favours the model (24.7% vs 33.3%).
- MAPE at the base grain excludes 77,219 zero-actual rows (52%).

## Step 9 — Recommendation engine and what-if simulator (2026-09-25)

**Built:**
- `python_pipeline/recommendation_engine.py`: pandas only. It reads the Step 4-8 outputs and produces 9 recommendation types. Every recommendation has at least 2 evidence lines with computed values, an `estimated_impact_value` and its basis. Priority is the rank percentile of the impact across all recommendations: top 10% Critical, next 20% High, next 40% Medium, rest Low (config `recommendations.priority`).
- Outputs: `parquet_data/recommendations/recommendations.parquet` and `reports/recommendations_report.md` (SRS example format, grouped Critical first).
- `python_pipeline/what_if_simulator.py`: the 7 scenarios as pure functions on a `Baseline`, plus `simulate(item_id, scenario, **params)`, which loads the baseline from existing outputs. The first call takes 0.7s and later calls ~6ms. Every result carries `"note": "Simulated estimate, not an actual result"` and reports revenue, contribution margin, demand, wastage and profitability as baseline, simulated and change.
- `tests/test_recommendations_whatif.py`: 12 tests.
  - Recommendations: evidence on every real recommendation, rank-based buckets on synthetic impacts.
  - What-if: one hand-computed test per scenario, and the estimate note on real items.
- Config: `recommendations` and `what_if` sections in `config/analytics_thresholds.yaml`.
- `15_demand_forecasting.py` now also writes `recent_average` (the 14-day rolling mean at the origin) in every forecast view; the stock-before-peak trigger needs it. Metrics are unchanged.

**Run:** `.venv/bin/python -m python_pipeline.recommendation_engine` (~2 s).

**Result:** 208 recommendations, of which 20 are Critical, 43 High, 82 Medium and 63 Low.

| Type | Count |
|---|---|
| Promote Hidden Opportunity | 37 |
| Increase stock before peak | 53 |
| Reduce prep (high wastage) | 38 |
| Remove or redesign Low Performer | 36 |
| Review promotion | 15 |
| Price-sensitive dish | 13 |
| Target segment | 6 |
| Anomalous location | 6 |
| Bundle | 4 |

**Decisions / deviations:**
- **Bundle impact** is `(lift - 1) x combined item revenue`, not `lift x`. A lift of 1 means no association, so only the part above chance counts.
- **Persistent Low Performer** means Low Performer at 75% or more of its locations. At 50%, 52 of 57 Low Performers qualified.
- **Anomalous location** counts only location-specific flags (dates flagged at fewer than 3 locations) and triggers at 6 or more. Chain-wide spikes are holidays and promotion launches.
- **Promotion impact** is the margin given up (the larger of the total margin drop and the per-order margin drop times orders) plus the extra wastage cost.
- **Reduce prep:** the cut comes out of waste first, and any cut beyond current waste comes out of sales (flagged as stockout). A stockout risk is also flagged when daily prep falls below the item's forecast daily demand. Shrinking waste proportionally, as the spec suggested, would shrink sales by the same share.
- **Promotion frequency:** only the extra promoted revenue gets the typical promotion margin effect, the median change in margin rate across evaluated promotions. So a factor of 1 returns the baseline.
- **Elasticity:** the measured value is used when the item has a negative measured elasticity, capped at |3.0|. Otherwise the config default applies; Inconclusive and Not Evaluated items use `unclassified`, -0.7.

**Known limitation (in the report):** impact proxies are in different units. Segment spend (tens to hundreds of millions of PKR) is far larger than an item's wastage, so all 6 segment recommendations rank Critical. Compare within a type as well as across types.

## Step 10 — Web app and dashboards (2026-09-25)

**Built:**
- `src/app.py`: login and home page.
- `src/auth.py`: werkzeug scrypt hashes, login and logout on the session, `require(roles)` page gate.
- `src/db.py`: SQLite users, audit log and master data.
- `src/data_loader.py`: one `st.cache_data` reader per Parquet source, plus the job-log parser.
- `src/ui.py`: global sidebar filters that persist across pages, takeaways, CSV download with audit, a `guard()` that shows a plain error instead of a stack trace, and the red/grey chart style.
- `src/model_registry.py`: builds `models/model_registry.json` from the saved `model_version.txt` files; nothing is retrained.
- `src/pages/1-9`:
  - Executive, Menu, Customer, Wastage, Forecast and Dual-Pipeline dashboards.
  - Recommendations (SRS Action/Reason format).
  - What-If simulator: calls `python_pipeline/what_if_simulator.py` and shows the "Simulated estimate" note prominently.
  - Admin, with job monitoring, the model registry, the audit log, users, and master data for Restaurants and Menu Items.
- `.streamlit/config.toml`: white/red theme, `showErrorDetails = false`, no custom CSS or animation.
- `database/schema.sql` and `database/init_db.py` create `database/app.db`: one demo user per role, plus Restaurants and Menu_Items copied from `processed_data/`.
- `screenshots/take_screenshots.py` (Playwright, headless Chromium) and `screenshots/*.png`: login, home and all 9 pages.
- `tests/test_app.py`: 19 tests.
  - Passwords are hashed.
  - Login succeeds and sets the role; a wrong password fails and is audited.
  - The Admin page stops the three non-admin roles, and pages stop before login (Streamlit `AppTest`).
  - Each page's loaders return the expected columns from the real Parquet files.
- `requirements.txt`: added streamlit, plotly, werkzeug.

**Run:**
```
.venv/bin/python database/init_db.py        # once; idempotent
.venv/bin/python src/model_registry.py      # after retraining any model
.venv/bin/streamlit run src/app.py          # from the project root
```

**Demo logins:**

| Role | Username | Password |
|---|---|---|
| Administrator | `admin` | `Admin@123` |
| Regional Manager | `regional` | `Regional@123` |
| Restaurant Manager (LOC0001) | `manager` | `Manager@123` |
| Analyst | `analyst` | `Analyst@123` |

**Access:**
- Every role sees the six dashboards, Recommendations and What-If, except that Dual-Pipeline is limited to Administrator, Regional Manager and Analyst.
- Admin is Administrator-only.
- A Restaurant Manager's location filter is locked to their assigned location.

**Filters:** date range, location, menu category and performance class, applied globally. Each section's caption says which of them it applies. Snapshot pages (Menu, Customer) ignore the date range; Customer applies location through the customer's home location.

**Decisions / scope:**
- **Analytical data stays in Parquet.** SQLite holds only users, the audit log and editable master-data copies.
- **Master-data CRUD** covers Restaurants and Menu_Items only (add, edit, deactivate/reactivate, audited). Edits live in `app.db`; the pipeline reads `processed_data/`, so they reach the dashboards only on a pipeline run fed from them. Promotions, Pricing, Customers, Orders, Ratings, Inventory and Wastage have no CRUD screens. Those tables are pipeline-managed, and the SRS uses "should" for these screens.
- **Audit log** records logins (success and failure), logouts, CSV and report exports, what-if runs (on input change) and every admin action.
- **Job monitoring** parses `reports/spark_execution_log_*.txt`. A job is Completed when every `START` stage has a matching `END`; otherwise it shows as Incomplete with the open stage.
- **"Next-period forecast"** on the Executive page is the latest forecast window, i.e. the model's 45-day held-out test period. The page says so.
- **Charts** set the Plotly template and colors on each figure (`ui.chart`), because Streamlit's chart theme overrides the Plotly defaults.

**Known limitations:**
- The first load of the Executive Dashboard takes ~10 s, because it reads the 1M-line fact table to compute order margins. After that it is cached and pages load in 1-3 s.
- The home page appears as "app" in the sidebar navigation (Streamlit's name for the entry script).

## Step 10b — Frontend visual polish (2026-09-27)

Restyle only; no analytical change.

**Built:**
- `src/style.css`, loaded by `ui.css()` on every page: KPI tiles, priority badges, recommendation/anomaly cards, source footers, active nav item in red, white bordered cards on a #FAFAFA page. All CSS transitions are off.
- `.streamlit/config.toml`: page background #FAFAFA, border #E5E5E5, white sidebar. Red stays #C8102E.
- `src/app.py` now uses `st.navigation` with grouped sections. Pages a role cannot open are left out of its menu (the pages still check access themselves). Before login, only the sign-in page exists. The home page is gone; after login the app opens on the Executive Dashboard.
  - Overview: Executive Dashboard
  - Intelligence: Menu Intelligence, Customer Intelligence, Demand Forecast, Wastage
  - Decisions & Actions: Anomalies, Recommendations, What-If Simulator
  - Platform: Model Comparison (dual pipeline), Admin
- Filters moved from the sidebar to a bar at the top of each page (`ui.filter_bar`). The logic and persistence are unchanged. The sidebar keeps the user, the data period and Log out.
- `ui.kpis`, `ui.badge`, `ui.card`, `ui.hbar`, `ui.source`, `ui.anomaly_card` replace `st.metric` and plain lists on every page.
- New page `src/pages/10_Anomalies.py`: the anomaly flags from `spark_jobs/12_anomaly_detection.py` as severity counts, a weekly trend, and cards.
- Charts: white card, light grid, legend above the plot, axis titles with units, and hover templates with exact values. Rankings are horizontal bars. The customer pie became a bar of spend share, and the wastage trend is an area chart.
- Categorical palette is red, blue, amber, teal, purple, pink in that fixed order. It passes the colour-blind separation check for adjacent pairs. Gray is kept for baselines and "Insufficient History".
- Every section has a footer naming its source file and the file's last-modified time, plus the as_of_date where there is one.
- `screenshots/`: all pages retaken. The files are renamed to the new page names, and the old home page shot was removed.
- `tests/test_app.py`: 21 tests (added anomaly severity and an Anomalies page render).

**Decisions:**
- **Anomaly severity** is not in the data, so it comes from the z-score using a fixed rule. Every flag already has |z| >= 3 (config `anomaly.z_threshold`). Critical is |z| >= 6, High >= 4.5, Medium below that. High-value orders (a multiple of the IQR cutoff) and identical-rating weeks (a rating count) have no z-score and are labelled "Review", not given a priority. The rule is printed on the Anomalies page.
- Recommendations keep Step 9's own priority.
- **Not built:** Basket & Bundles, Pricing & Promotions and Reports & Export pages (the report download stays on the Executive Dashboard), a channel filter, and food images. These were left out because the first two pages and the channel filter would be new analysis, not restyling. Images would need a manual download and licence check.

## Step 11 — Packaging and submission (2026-09-27)

**Built:**
- `README.md`: requirements (OS, Python, Java, Spark, PySpark config), install, database setup, run commands for every stage (generation, Spark, Spark SQL, analytics, forecast, Python pipeline, comparison, recommendations), an app task guide, tests, troubleshooting, the repository layout, and evaluator credentials at the top.
- `requirements.txt`: every package is pinned to the version the tests passed with.
- `LICENSE`: MIT.
- `documentation/diagrams.md`: Mermaid diagrams.
  - architecture
  - ERD of all 13 tables with PKs and FKs
  - DFD level 0 and level 1
  - use cases for the four roles
  - activity diagram of a what-if run
  - sequence diagram of login, dashboard load and filtering
- `documentation/project_report.md`: background, scope, architecture, methodology per subsystem, results (every number from `reports/`), testing, security, limitations and future work.
- `documentation/technical_blog.md` (about 3,500 words).
- `documentation/demo_video_script.md`: a shot list covering every required demo item, plus the contradictory case.
- `sample_data/`: the first 200 rows of each raw table.
- `static/` and `templates/`: placeholder READMEs.
- `tests/test_analytics.py`: 2 new tests for SRS difficult cases that had no direct test.
  - `test_price_sensitive_item_classes`: price-sensitivity classes, including Inconclusive and Not Evaluated.
  - `test_rating_anomaly_flags_rating_drop_and_identical_week`: an average-rating drop and an identical-rating week.
- `reports/test_results.txt`: output of the last full run.

**On hold:** `AI_USAGE.md`, waiting on confirmation from faculty.

**Not done here:** the demo video (to be recorded from the script), deployment (no hosted URL; the README's local instructions are the fallback).
