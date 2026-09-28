# AI Usage Declaration

This document declares the use of AI tooling during the development of DineIQ Analytics, in accordance with the competition's integrity requirements.

**Tool used:** Claude (Anthropic's Claude Code CLI), used as the primary AI development environment for the project.

**How it was used, overall:** AI was used to implement, test, debug, and refine the selected development stages described below. The AI performed the implementation work for these stages based on the project's requirements and specifications. The resulting work was reviewed, run, and verified by us before being considered complete.

## 1. Menu profitability analysis and performance classification

**Purpose:** Classify every menu item into Profit Driver / Volume Driver / Hidden Opportunity / Low Performer using a percentile-based, multi-signal decision rule, and flag the ten documented tricky business cases.

**Type of AI assistance:** AI implemented the fully specified decision tree and flag logic, including the exact rule ordering and thresholds, with all thresholds externalized to a config file.

**Files/modules affected:** `spark_jobs/05_menu_classification.py`, `config/classification_thresholds.yaml`, `parquet_data/menu_classification/`

**Review and verification:** We reviewed the resulting classification distribution against a scatter plot of demand percentile versus profitability percentile to confirm that the categories separated according to the intended logic.

**Testing completed:** Dedicated tests for each of the ten tricky cases, zero-sales and insufficient-history edge cases, and a test confirming that a config threshold change changes the output.

**Verified by:** Muzammil Mughal

## 2. Independent Python pipeline and dual-pipeline comparison

**Purpose:** Independently reproduce menu classification and customer segmentation in Pandas/Scikit-learn without reading Spark's derived output, and produce the formal Spark-vs-Python comparison report.

**Type of AI assistance:** AI implemented the independent feature engineering, model training in Python, and comparison-report logic according to the project requirements.

**Files/modules affected:** `python_pipeline/`, `models/python/`, `reports/dual_pipeline_comparison_report.md`, `reports/dual_pipeline_comparison.csv`

**Review and verification:** We confirmed that the Python pipeline did not import from or read the Spark pipeline's derived feature output, ensuring that the comparison remained independent.

**Testing completed:** Independence check and agreement-percentage calculation check against a hand-built example.

**Verified by:** Muzammil Mughal

## 3. Demand forecasting

**Purpose:** Perform time-aware demand forecasting at item/category/location grain and evaluate the results against a naive baseline.

**Type of AI assistance:** AI implemented the chronological train/test split, lag and rolling feature construction, and the GBTRegressor-versus-baseline comparison.

**Files/modules affected:** `spark_jobs/15_demand_forecasting.py`, `config/forecast_config.yaml`, `parquet_data/demand_forecast/`

**Review and verification:** We verified that the train/test split was chronological, with the maximum training date preceding the minimum test date, and that lag features did not leak information across the split.

**Testing completed:** Chronological-split test, lag-feature leakage test, and metric calculation test.

**Verified by:** Muzammil Mughal

## 4. Web application and dashboards

**Purpose:** Develop the Streamlit web application with authentication and role-based access, six dashboards, recommendations and what-if pages, and administrative tooling such as the model registry, audit log, and job monitoring.

**Type of AI assistance:** AI implemented the application structure, authentication/RBAC, dashboard pages, data loading, layouts, and visual styling according to the project requirements.

**Files/modules affected:** `src/`, `.streamlit/config.toml`, `database/`

**Review and verification:** We logged into the application under each seeded role and manually verified role-gated page access. We also checked all dashboard pages to confirm that they loaded real data.

**Testing completed:** Login/authentication tests, role-gating test, and per-page data-loader tests.

**Verified by:** Muzammil Mughal

## 5. Final packaging

**Purpose:** Prepare the project's diagrams, project report, README, GitHub housekeeping, and AI usage declaration.

**Type of AI assistance:** AI provided the implementation and drafting assistance for the project documentation and packaging materials, based on the project's generated reports, specifications, and completed work.

**Files/modules affected:** `README.md`, `documentation/`, project report, `AI_USAGE.md`

**Review and verification:** We reviewed the figures and metrics quoted in the project report against the project's actual report outputs before including them.

**Testing completed:** N/A (documentation and packaging)

**Verified by:** Muzammil Mughal

## Statement

The selected development stages documented above were implemented with AI assistance using Claude Code. AI performed the implementation work for menu profitability analysis and performance classification, the independent Python pipeline and dual-pipeline comparison, demand forecasting, the web application and dashboards, and final project packaging/documentation.

The final restaurant analytics, predictions, classifications, recommendations, and forecasts are produced by the project's own Spark, Python, Data Science, and Machine Learning code. No external generative-AI decision API is called at runtime by the application. AI assistance was used during development, as described above, and the resulting work was reviewed, tested, and verified by us.
