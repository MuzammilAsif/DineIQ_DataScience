# Busy Isn't Profitable: Building a Restaurant Intelligence Platform with Spark and a Second Pipeline to Check It

*DineIQ Analytics, Techwiz 7 Data Science track*

A restaurant's point-of-sale system is very good at one question: how much did we sell? It is much worse at the questions a manager loses sleep over:

- Which dishes make money, and which just keep the kitchen busy?
- Why did last month's big promotion feel like a success, yet the margin went down?
- Which regulars have quietly stopped coming?
- How much Mutton Karahi should we prepare for Saturday?

DineIQ is our attempt to answer those questions end to end. It covers a chain of 25 restaurants across 11 Pakistani cities, with 160 dishes and a full year of orders. This post walks through how we built it, what we found, and where it falls short.

## The business problem

Our fictional chain has the usual mix of formats: mall food courts, high-street branches, a motorway service area and dine-in family restaurants. Its data lives in 13 tables:

- **Reference data:** restaurants, menu categories, menu items and a price history per item per location
- **Customers and promotions:** customers, promotions, and the two bridge tables that say which items and locations each promotion covers
- **Transactions:** orders and order lines
- **Operations:** customer ratings, daily inventory and daily wastage

The brief was to turn that into decisions:
- classify every dish;
- segment customers;
- find items that sell together;
- forecast demand;
- predict wastage;
- judge prices and promotions;
- flag anomalies;
- then wrap it all in recommendations a manager can act on, and a what-if tool for testing ideas before trying them.

Two requirements shaped everything. First, the heavy lifting had to happen in Apache Spark. Second, a completely independent Python pipeline had to reproduce the key results so we could compare them record by record. The second one turned out to be the most useful constraint in the project.

## Background: why generate the data?

Real restaurant data with costs, wastage and ratings is not public, and a toy dataset of a few thousand rows would not exercise Spark. So we wrote a generator (`data_generator/`) that builds all 13 tables for calendar year 2025 in foreign-key order. It is seeded (42) and scalable. At scale 1 it produces 196,547 orders and 1,070,114 order lines, plus 1.25M inventory rows. At scale 5 it produces about 5.3M order lines in under 90 seconds.

A generated dataset is only worth analysing if it has structure, so we planted patterns without labelling them:
- **Calendar:** Friday–Sunday weekends, lunch and dinner peaks, and Ramadan pulling orders into the evening.
- **Growth and spread:** seasonal dishes, about 8% growth over the year, and a large volume spread between the busiest and quietest branch.
- **Customer archetypes:** loyal big spenders, promotion chasers, customers who churn, and late-year newcomers.
- **Item archetypes:** cheap high-volume staples with thin margins, expensive desserts and coffees that rarely sell, a handful of dishes that waste a lot of food, and five dishes customers dislike.
- **A promotion trap:** an "August BOGO Blast" that doubles traffic and quietly eats the margin.

Then we made the data dirty on purpose: 28 injection rules across 8 tables. They cover missing IDs, orphan foreign keys, malformed and out-of-range dates, negative quantities, impossible prices, ratings outside the 1–5 scale, duplicate rows, discounts larger than the bill, and a zoo of inconsistent units (`kgs`, `KG`, `Pcs`, `Liters`). Every corrupted row is logged. That log became the answer key for our data-quality checks.

## The proposed solution and its architecture

The system has three layers:

1. **Spark pipeline** (`spark_jobs/01`–`15`): ingestion, validation, cleaning, integration, feature engineering, classification, eight analytics modules, a Spark MLlib classifier and a demand forecaster. Everything lands in Parquet.
2. **Independent Python pipeline** (`python_pipeline/`): pandas and scikit-learn, reading only the cleaned CSVs. It recomputes the features and trains its own models.
3. **Decision layer and web app:** a recommendation engine, a what-if simulator, and a Streamlit app with four roles.

The data moves `raw_data/` → `processed_data/` (clean CSVs plus a quarantine folder) → `parquet_data/` (partitioned fact table, features, analytics outputs) → `models/` → the app. The full diagrams, including the ERD, data-flow diagrams, use cases and a sequence diagram, are in `documentation/diagrams.md`.

## Data quality: finding what we broke

The ingestion job loads every table with an explicit `StructType` rather than letting Spark guess. It also runs schema inference on one table, side by side, to show why: inference quietly turns IDs and dates into whatever it likes. To catch malformed dates, we load each table twice, once typed and once as raw strings, and diff them. A value that exists as a string but comes back null when typed was malformed. Orphan foreign keys are counted with left-anti joins.

The result, `reports/data_quality_report.md`, lists every issue with its count and share of the table. It then cross-checks the counts against the generator's injection log. They match almost exactly, and each gap has an explanation. The biggest "issue", 8,262 cancelled orders (4.2%), isn't an issue at all: cancellations are real business data, and we kept them.

We wrote the cleaning rules down before writing the cleaning code (`documentation/data_quality_rules.md`). The cleaning job:
- removes duplicates;
- corrects impossible prices from the price history;
- normalises units;
- quarantines everything else, each row with a reason.

Quarantine cascades along foreign keys: a customer with a corrupt signup date makes their orders orphans, and those orders' lines and ratings go too. That is why 67,290 order lines were removed when fewer than half as many were corrupted directly. A cleaning pipeline that doesn't cascade leaves you with order lines pointing at nothing.

## Spark, PySpark, Spark SQL and Parquet

The integration step is plain Spark SQL. `spark_sql/integration_queries.sql` holds ten joins and one fact-table query, each marked with a `-- @name:` comment that the job parses and runs. The fact table, `fact_order_line`, has one row per order line with everything attached: order, customer, location, item, category and promotion. It is written to Parquet partitioned by year and month. Every later stage reads it instead of re-joining the CSVs.

Spark bit us a few times, and the fixes are worth sharing:

- **Phantom HDFS.** The machine had a global Hadoop config pointing at an HDFS namenode that wasn't running, so every local read failed with a connection error. We pinned `spark.hadoop.fs.defaultFS=file:///` in our session factory.
- **The `.isin()` trap.** Our first orphan check collected 195k parent keys into a Python list and used `.isin()`. The driver choked. A join against the distinct keys does the same thing without the blow-up.
- **Recomputed lineage.** The cleaning job referenced cleaned tables again and again without caching them, so Spark recomputed window functions on every action. One run took over ten minutes. `.cache()` plus an immediate `.count()` fixed it.
- **Timestamps that vanished.** Spark writes timestamps in ISO format with a `T` by default. Our reader expected a space, so every order date came back null and the year/month partitioning collapsed into one default bucket. Pinning the timestamp format on write fixed it.

## Feature engineering

`04_feature_engineering.py` computes features for items, customers and locations as of any date, using only records up to and including that day. We saved two snapshots, the end of the year and three months earlier, to prove the parameter actually works.

Item features include revenue, contribution margin, profit %, units sold, popularity, repeat-purchase rate, rating and rating trend, wastage %, promotion dependency, price change, weekend share and a sales trend. Customer features are the classic recency, frequency and monetary value, plus average order value and channel preference.

The subtle problem was wastage. Wasted quantity is recorded in kilograms, litres or pieces, while sales are counted in portions, so "wastage %" from the spec would divide kilograms by plates. We converted wastage to portions through cost instead: waste cost divided by the item's average unit cost.

## Menu intelligence: busy isn't profitable

We rank every active dish on four percentiles: profitability, demand, quality (rating and repeat purchase) and wastage. Each percentile is cut into low, medium and high tiers. The tiers decide one of five classes: **Profit Driver**, **Volume Driver**, **Hidden Opportunity**, **Low Performer**, or **Insufficient History** for dishes under 90 days old. Every threshold lives in a YAML file, and our tests prove that editing it changes the output without touching code.

Across 156 active dishes we got 12 Profit Drivers, 38 Volume Drivers, 43 Hidden Opportunities, 57 Low Performers and 6 new items.

The interesting part is the difficult cases, where the obvious reading is wrong:

- **Chicken Tikka Pizza (Medium)** sold 35,362 units, in the top 5% by volume, and it's one of the biggest contributors in absolute rupees. Its margin is 29.5%, the second-lowest on the menu. It's a **Volume Driver, not a Profit Driver**. It keeps the lights on, but a price or recipe review is worth more here than another promotion.
- **Cafe Latte** has an 85.8% margin and almost nobody orders it (demand percentile 0.11). That makes it a **Hidden Opportunity**: promote it and it earns.
- **Seekh Kabab** is popular and profitable enough to be a Profit Driver, but 19.5% of what the kitchen prepares ends up in the bin. Excessive wastage demotes it.
- **Mutton Pulao** is rated 2.21 out of 5 and is still a **Profit Driver**, because people keep ordering it. It also behaves differently by branch: its class differs from the chain-wide one at 72.7% of the 22 locations that sell it.

Two of the difficult cases never fire in our data: a loss-making dish (every item sells above cost overall) and a highly rated, low-profit dish (the best rating is 4.27, under the 4.3 threshold). We report them as gaps and cover both rules with synthetic tests. We did not fudge the data to make them appear.

## Customer segmentation and RFM

Every customer gets RFM quintiles (a score from 3 to 15). Then Spark MLlib's KMeans (k=6) runs on standardised recency, log frequency, log spend, average order value and the share of orders that used a promotion. The log matters: most customers order once or twice, and a handful order 20+ times.

Our first labelling attempt used fixed cutoffs like "recency over 90 days is At-Risk". Four of six clusters came out At-Risk, because the typical customer's last order was about 100 days ago. We switched to ranking the cluster means and giving each label to one cluster. The result:

| Segment | Customers | Profile |
|---|---|---|
| High-Value Loyal | 6,502 | 12.3 orders, about 108k PKR spend, 22 days since the last order; 50% of all spend |
| Promotion-Driven | 8,870 | Used a promotion on 88% of orders |
| New, Frequent, At-Risk, Occasional | 34,234 | The rest |

A separate churn rule flags 7,893 customers (15.9%) whose last order is more than twice the average gap between orders (115 days) and who ordered less this quarter than last.

## Market-basket analysis

We ran Spark MLlib's FPGrowth over 179,283 completed orders. The spec's thresholds (support 0.01, confidence 0.3) returned nothing, because the strongest pair in the data only reaches 0.20 confidence. We lowered them to 0.005 / 0.1 and got 449 rules.

The best lift is 1.48: Aloo Pakora with Chicken Pakora. Only 4 of the top 20 rules clear 1.2, the level we'd call a bundle. The honest conclusion is that this menu has weak associations, and the strongest ones are probably seasonal items bought in the same months. We still turned the four strong pairs into bundle recommendations and labelled the rest as weak.

## Demand forecasting

The forecaster is one Spark MLlib GBTRegressor at item × location × day grain, trained over every day an item was stocked, so a stocked day with no sales is a real zero. Its inputs:
- **History:** quantity 1, 7 and 14 days before, and 7- and 14-day rolling means. These are joined on exact calendar offsets, so a gap drops the row instead of borrowing a neighbouring day.
- **Known in advance:** calendar, promotion and price for the target date.

The test period is the last 45 days of the year, never seen in training. The baseline is "same weekday last week".

The model beats the baseline on MAE and RMSE at every level. At the chain level, daily MAE is 512 units against 928. At the base grain it's 1.35 against 1.59. It loses on one metric: item-day MAPE, 64.1% against 55.3%. We dug in. On days an item sells 1–5 units, the smoothed model always over-forecasts, and percentage error explodes on tiny denominators. From 21 units a day up, the model wins clearly (22% vs 32%), and the median percentage error favours it too. We left the loss in the report rather than dropping MAPE.

## Wastage prediction

Wastage is concentrated: the Karahi & Handi category alone wastes 16.3M PKR, with its three biggest dishes throwing away about a fifth of what they cost. We trained a Spark RandomForest to flag item-location-days where more than 10% of preparation is wasted. A top-quartile label was impossible, because 95% of days have no waste at all. The split is by time, and anything computed from the day's own waste or consumption is excluded; a test checks this.

On the October–December test period the model scores ROC AUC 0.76 and PR AUC 0.23, against a 4.2% base rate, with precision 0.23 and recall 0.46. Feature importance shows it mostly learned *which items* waste, not *which days*. It's a useful watch list, not a daily prep planner, and that's how we present it.

## Pricing and promotion intelligence

For every price change of at least 5% with a full month of data on each side, we compare demand before and after. We then divide by the change in the rest of the item's category over the same weeks, which takes out shared seasonality. Without that control and the sign rule, seasonal dishes (haleem, pakoras, lassi) came out as the most price-sensitive items on the menu. Positive elasticities are labelled Inconclusive instead of being forced into a class.

Of 284 changes, 120 were evaluable. The standout is Double Patty Burger: a 6.7% price rise and demand fell 60.6%.

Promotions are measured on their own items at their own branches, in three equal windows: before, during and after. The August BOGO Blast is the textbook trap:
- orders on its items up 87% and customers up 71%;
- margin rate down from 60.9% to 47.0%;
- wastage cost up from 1.07M to 5.84M PKR, because kitchens over-prepared.

It looked like the best month of the year on the sales report. It wasn't.

## Spark MLlib and the Python pipeline

The Spark classifier predicts a dish's class from its 16 raw metrics. We made sure none of the percentiles, tiers or flags used to build the label leaked in; a test inspects the feature vector.

We compared Logistic Regression, Decision Tree and Random Forest with 5-fold cross-validation. Two details mattered:
- **Macro F1 as the tuning metric.** Spark's built-in "f1" is weighted, which flatters the big classes, so we wrote a small custom evaluator.
- **Stratified folds.** With only 8 Profit Drivers in training, random folds could leave one with none, so we dealt the folds per class.

Random Forest won on the 28 held-out dishes, with accuracy 0.929 and macro F1 0.914.

The Python pipeline is a clean-room rebuild. It reads only the cleaned CSVs, recomputes every feature in pandas, and trains scikit-learn models with the same grids. Tests parse its source to prove it never imports Spark code or reads Spark output.

## Model comparison and the disagreements

First we checked features. Python and Spark agree on every item and customer feature. The largest difference is 0.0000015 PKR in a cost sum, with no null mismatches.

Then predictions, on records neither model trained on:

| Task | Records | Spark vs Python |
|---|---|---|
| Customer segmentation | 9,899 holdout customers | 98.9% agree (adjusted Rand index 0.969) |
| Menu classification | 28 held-out dishes | 100% agree |

The 113 customers the two pipelines disagree on are almost all sitting on a boundary between two clusters. The median gap between their nearest and second-nearest centroid is 0.04, against 0.87 for customers they agree on. Different k-means initialisations settle those borderline customers differently, and that's all.

Against our rule-based "actual" segment, both pipelines score about 64%. That number looks bad until you see why: no cluster on either side maps to New or Frequent. "New" depends on signup date, which isn't a clustering input. It measures how well unsupervised clusters line up with a business rule, not supervised accuracy, and we say so.

## Recommendations and what-if

The recommendation engine turns all of this into 208 recommendations of nine types. Examples: promote this Hidden Opportunity, stock up before this forecast peak, cut prep on this wasteful dish, review this promotion, bundle these two items.

Every recommendation has at least two evidence lines with real computed values, plus an estimated impact. Priority is the impact's rank: top 10% Critical, next 20% High, next 40% Medium. The weak spot is that the impacts are in different units, so all six segment recommendations rank Critical simply because segment spend is a big number. The report tells readers to compare within a type.

The what-if simulator runs seven scenarios (price, discount, promotion frequency, remove item, reduce prep, demand change, wastage) as simple formulas over an item's current numbers. It uses the dish's measured price elasticity when there is one. Every result says "Simulated estimate, not an actual result", because it is one.

## The app, security and testing

The Streamlit app has ten pages in four groups: Overview, Intelligence, Decisions & Actions, and Platform. A filter bar persists across pages.

- **Security:** passwords are stored only as scrypt hashes. Four roles are enforced on every page, not just hidden from the menu. A Restaurant Manager only sees their own branch.
- **Audit log:** every login, export, what-if run and admin action is recorded.
- **Traceability:** every dashboard section ends with a small footer naming the file it came from and when that file was written, so nothing on screen is typed in by hand.

The test suite has 135 tests: schemas, data quality, cleaning, joins, features, every menu class and difficult case, the MLlib model, the analytics formulas, the forecaster, the Python pipeline's independence, recommendations, one hand-computed test per what-if scenario, and the app's login and role gating. Each of the SRS's difficult cases has a direct test, from the high-selling loss-maker to a Spark/Python disagreement.

## Performance

On a 4-core, 8 GB laptop VM:
- dataset generation takes 1–2 minutes;
- ingestion, cleaning and integration about 9;
- the eight analytics modules about 25;
- the forecaster about 5.5.

The MLlib classifier's cross-validation takes 28 minutes, almost all of it overhead on a 150-row problem. We cut shuffle partitions to 1 for that job after 16 partitions made loading 150 rows take over a minute. The app's first dashboard load reads the million-row fact table in about 10 seconds, then serves from cache in 1–3.

## Lessons learned

1. **A second pipeline is the best test you can write.** Reimplementing the features in pandas from the written definitions, and requiring the two sides to match to rounding error, checks what the numbers *mean*, not just that the code runs.
2. **Write the rules before the code.** The cleaning rules and classification thresholds in plain files made the decisions reviewable, and made "why is this dish a Volume Driver?" answerable.
3. **Fixed thresholds lie on skewed data.** Segment cutoffs, basket thresholds and wastage quartiles all had to become relative or data-driven. We document each change instead of silently tuning.
4. **Report the losses.** The forecaster's MAPE loss, the weak basket rules and the wastage model's low precision are all in the reports with explanations. They make the wins believable.
5. **Spark's defaults aren't your defaults.** Timestamp formats, shuffle partitions, ANSI division-by-zero errors and global Hadoop configs all needed explicit settings.

## Limitations

- The data is synthetic. We found what we planted, which proves the pipeline works but says nothing about real restaurants.
- The three-algorithm comparison was only done for menu classification. Wastage risk, forecasting and segmentation each use one algorithm.
- The menu classifier's test set is 28 dishes, and its label is built from its own inputs.
- Anomaly detection is statistical with no labelled truth; holidays get flagged alongside real problems.
- The app edits only restaurants and menu items. It has no Pricing & Promotions or Basket pages and no channel filter, and it isn't deployed to a public URL yet.

## Future enhancements

- Pricing, promotion and basket pages, and a channel filter.
- A day-level wastage model that uses the demand forecast as an input.
- Holiday and Ramadan-aware features in the forecaster.
- An anomaly review workflow.
- Scheduled pipeline runs and a hosted deployment.
- Most of all, a trial on a real restaurant's data.

---

*Source code, reports and the full build log: https://github.com/MuzammilAsif/DineIQ-DataScience*
