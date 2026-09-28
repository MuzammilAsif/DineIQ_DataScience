# Demo Video Script

One continuous recording, about 18–22 minutes. The slow parts (dataset generation and the full Spark run) are narrated over the code and the outputs already on disk; don't re-run them on camera. The app is demonstrated live.

**Before recording**
- Run `.venv/bin/streamlit run src/app.py` and open the app once, so the Executive Dashboard cache is warm.
- Open the editor on the project root.
- Open these files in tabs: `reports/data_quality_report.md`, `reports/restaurant_intelligence_report.md`, `reports/dual_pipeline_comparison_report.md`, `reports/forecast_report.md`, `reports/menu_classification_report.md`.
- Clear the filters in the app's top bar.

| # | Time | Show | Say (key points) | SRS item |
|---|---|---|---|---|
| 1 | 0:00 | README top, repo tree | DineIQ: 25 restaurants, 160 dishes, one year of data. A Spark pipeline, an independent Python pipeline, and a role-based web app. | Intro |
| 2 | 0:45 | App sign-in page → log in as `admin` | Passwords are scrypt-hashed and every login is audited. Four roles; the menu only shows what each role can open. | Login |
| 3 | 1:30 | `data_generator/generate_dataset.py`, `reference_data.py`; `reports/generation_row_counts.csv` | 13 related tables in FK order, seed 42, `--scale` for 5x. Patterns are planted but not labelled. Then 28 dirty-data rules; every corrupted row is logged in `reports/dirty_injection_log.csv`. | Dataset generation |
| 4 | 3:00 | `spark_jobs/schemas.py`, `01_ingest_and_validate.py`; `reports/spark_execution_log_ingest.txt` | Explicit StructTypes for 13 tables, schema inference shown side by side, the 1M-row Order_Items load, and multi-file ingestion from 39 partitioned part files. | Big Data ingestion, Spark processing |
| 5 | 4:00 | `reports/data_quality_report.md` (Issues found + Cross-check) | Every issue type, with counts and % of table. Detected counts match the injected ones on the raw data. Cancellations (4.2%) are real data, not defects. | Data-quality analysis |
| 6 | 5:00 | `documentation/data_quality_rules.md`, `02_clean.py`, `reports/cleaning_row_counts.csv`, `processed_data/quarantine/` | Rules were written before the code. Dedupe, price correction, unit normalisation, quarantine with a reason. Quarantine cascades along foreign keys. | Cleaning |
| 7 | 6:00 | `spark_sql/integration_queries.sql`, `03_integrate_and_store.py`, the `parquet_data/fact_order_line/order_year=2025/` folders | 10 joins plus `fact_order_line`, written as Parquet partitioned by year and month. | Spark SQL, Parquet |
| 8 | 7:00 | `04_feature_engineering.py`; `parquet_data/features/item_features/` (two `as_of_date` folders) | Item, customer and location features, as of any date, using no future records. | Feature engineering |
| 9 | 7:45 | App → **Menu Intelligence** | 12 Profit Drivers, 38 Volume Drivers, 43 Hidden Opportunities, 57 Low Performers, 6 new items. Point at the performance map. | Menu profitability + classification |
| 10 | 8:45 | Menu Intelligence item table, sorted by margin; `reports/menu_classification_report.md` | **Contradictory case:** Chicken Tikka Pizza (Medium) sells 35,362 units (demand percentile 0.95) and has one of the largest contributions, but its margin is only 29.5%, the second-lowest on the menu. So it's a **Volume Driver, not a Profit Driver**. Profit Driver needs high profitability, not high volume. Also: Seekh Kabab is popular and profitable but lost Profit Driver to 19.5% wastage, and Mutton Pulao is rated 2.21 but is still a Profit Driver. | Difficult case |
| 11 | 10:00 | App → **Customer Intelligence** | 6 KMeans segments. High-Value Loyal is 13% of customers and 50% of spend. RFM score distribution. The at-risk tab uses the churn rule: 7,893 customers (15.9%). | Customer segmentation, RFM |
| 12 | 11:00 | `reports/restaurant_intelligence_report.md` section 2 | FPGrowth: 449 rules. The best lift is 1.48 (pakoras together), and only 4 rules clear 1.2. Associations are weak and we say so. | Market basket |
| 13 | 11:45 | Executive Dashboard sales trend; EDA report section 3 | Fri–Sun carries 44% more orders a day; 56% of orders fall in the 12–14 and 19–22 windows. | Peak period |
| 14 | 12:15 | App → **Demand Forecast**; switch level to Location, pick a location | A GBT at item × location × day grain, beating same-weekday-last-week on MAE and RMSE at every level. Chain MAE is 512 against 928. Honest weakness: MAPE on tiny-volume item-days. | Demand forecasting |
| 15 | 13:15 | App → **Wastage** | Karahi & Handi wastes 16.3M PKR. The Reasons chart names the largest cause. The risk model's precision and recall are on the page. | Wastage analysis |
| 16 | 14:00 | Intelligence report section 4 (price table) | Elasticity with a category control. Double Patty Burger: +6.7% price, −60.6% demand. Positive elasticities are Inconclusive, not forced into a class. | Price intelligence |
| 17 | 14:45 | Intelligence report section 5; PROMO012 row | The August BOGO: orders +87%, but margin rate fell from 60.9% to 47.0% and wastage cost went from 1.07M to 5.84M. A promotion trap. | Promotion analysis |
| 18 | 15:30 | App → **Anomalies** | Rolling z-scores on sales and ratings, IQR order outliers, identical-rating weeks. Severity rule shown on the page. Cards show value vs baseline. | Anomaly detection |
| 19 | 16:15 | `models/spark/menu_classification/LATEST`, `reports/spark_mllib_menu_classification_report.md`, `reports/sample_predictions_menu_classification.csv` | LR, DT and RF, 5-fold CV on macro F1, leakage-free features. RF wins: accuracy 0.929, macro F1 0.914. | Spark model prediction |
| 20 | 17:00 | `python_pipeline/`, `models/python/` | Separate pandas + scikit-learn pipeline, reading cleaned CSVs only; tests check it imports nothing from Spark. | Python model prediction |
| 21 | 17:30 | App → **Model Comparison** (Platform) | 9,927 unseen records, 98.9% agreement, ARI 0.969. Disagreements sit on cluster boundaries. Download the disagreements CSV. | Dual-pipeline comparison |
| 22 | 18:15 | App → **Recommendations** | 208 recommendations, each with evidence and an impact-based priority. Open REC-009 (PROMO012). | Recommendation engine |
| 23 | 19:00 | App → **What-If Simulator**: an item, +10% price, then Reduce prep | Formula-based, uses measured elasticity, and is always labelled "Simulated estimate". Show the stockout flag on a large prep cut. | What-if scenario |
| 24 | 19:45 | Executive Dashboard: change the location filter, then Download reports → `restaurant_intelligence_report.md`; Admin → Audit log shows the export | Filters persist across pages. Every export is audited. The source footers show which file each section reads. | Dashboard, report generation |
| 25 | 20:30 | Terminal: `reports/test_results.txt` tail | 135 tests, including one per SRS difficult case. | Testing |
| 26 | 21:00 | README Limitations link | What we cut and why, from `documentation/project_report.md` section 11. | Close |
