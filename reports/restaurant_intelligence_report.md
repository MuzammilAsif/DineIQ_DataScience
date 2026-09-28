# Restaurant Intelligence Report

One section per analytics module (`spark_jobs/07`-`14`). Each job rewrites only its own section. Thresholds come from `config/analytics_thresholds.yaml`.

<!-- section:07 -->
## 1. Customer segmentation and RFM

49,606 customers with at least one completed order by 2025-12-31 (`parquet_data/features/customer_features/`).

**RFM.** Recency, frequency and monetary value are each bucketed into quintiles with `ntile(5)` (5 is best: most recent, most frequent, highest spend). `rfm_code` concatenates the three and `rfm_score` sums them (3-15). Frequency has many ties at 1-2 orders, so customers with the same order count can land in adjacent quintiles.

RFM score distribution: 3: 2,488, 4: 4,057, 5: 2,826, 6: 3,574, 7: 4,920, 8: 5,029, 9: 5,194, 10: 4,592, 11: 3,835, 12: 3,287, 13: 2,788, 14: 3,269, 15: 3,747.

**KMeans.** Spark MLlib `KMeans`, k=6, fitted on the customers with crc32(customer_id) % 5 != 0 (the rest are held out for the Step 7 comparison and only assigned), on standardized recency, log(1 + frequency), log(1 + monetary), average order value, and promotion order share (the fraction of the customer's orders that used a promotion). The log is used because frequency and spend are heavily skewed. Clusters are labelled by ranking their means, each label used once, in this order: highest spend is High-Value Loyal; highest promo share (if above 0.4) is Promotion-Driven; highest recency is At-Risk; lowest tenure (days since first order) is New; highest remaining frequency is Frequent; the rest are Occasional. Fixed cutoffs were tried first and did not work: with most customers ordering once or twice, a mean recency over 90 days labelled four of six clusters At-Risk.

| cluster_id | segment | customers | recency | frequency | monetary | aov | promo_share | tenure |
|---|---|---|---|---|---|---|---|---|
| 0 | New | 10,872 | 69 | 1.2 | 5,942 | 4,906 | 0.01 | 93 |
| 1 | At-Risk | 8,443 | 273 | 1.7 | 10,470 | 6,169 | 0.03 | 297 |
| 2 | Promotion-Driven | 8,870 | 106 | 2.0 | 15,238 | 7,480 | 0.88 | 182 |
| 3 | High-Value Loyal | 6,502 | 22 | 12.3 | 107,760 | 8,750 | 0.12 | 323 |
| 4 | Frequent | 9,637 | 100 | 4.8 | 33,121 | 7,035 | 0.14 | 272 |
| 5 | Occasional | 5,282 | 101 | 1.4 | 19,027 | 13,643 | 0.09 | 133 |

| segment | customers |
|---|---|
| High-Value Loyal | 6,502 |
| Frequent | 9,637 |
| Promotion-Driven | 8,870 |
| At-Risk | 8,443 |
| New | 10,872 |
| Occasional | 5,282 |

Output: `parquet_data/customer_segments/` (customer_id, cluster_id, segment, R/F/M scores, rfm_code, rfm_score). The fitted pipeline is saved to `models/spark/customer_segmentation/`.
<!-- /section:07 -->

<!-- section:08 -->
## 2. Market-basket analysis

Spark MLlib `FPGrowth` on 179,283 completed orders (177,108 with two or more distinct items), minSupport 0.005, minConfidence 0.1. 586 frequent itemsets, 449 rules.

The highest lift is 1.48. Lift is how much more often two items appear together than they would by chance, so a lift near 1 means no real association. 4 of the top 20 rules reach the bundle threshold of 1.2. 

Top 20 rules by lift:

1. Customers who order Aloo Pakora also order Chicken Pakora 12% of the time (1.48x the base rate, in 0.6% of orders). Candidate bundle: Aloo Pakora + Chicken Pakora.
2. Customers who order Sweet Lassi also order Fresh Lime Soda 15% of the time (1.42x the base rate, in 0.6% of orders). Candidate bundle: Sweet Lassi + Fresh Lime Soda.
3. Customers who order Mango Shake also order Fresh Lime Soda 15% of the time (1.41x the base rate, in 0.8% of orders). Candidate bundle: Mango Shake + Fresh Lime Soda.
4. Customers who order Peach Iced Tea also order Fresh Lime Soda 13% of the time (1.26x the base rate, in 0.5% of orders). Candidate bundle: Peach Iced Tea + Fresh Lime Soda.
5. Customers who order Fruit Chaat also order Fresh Lime Soda 13% of the time (1.17x the base rate, in 0.6% of orders). Weak association, not a bundle candidate.
6. Customers who order Strawberry Shake also order Fresh Lime Soda 12% of the time (1.15x the base rate, in 0.9% of orders). Weak association, not a bundle candidate.
7. Customers who order Mango Shake also order Soft Drink (Regular) 20% of the time (1.14x the base rate, in 1.0% of orders). Weak association, not a bundle candidate.
8. Customers who order Mango Shake also order Mineral Water (500ml) 13% of the time (1.12x the base rate, in 0.7% of orders). Weak association, not a bundle candidate.
9. Customers who order Soft Drink (Regular) also order Fresh Lime Soda 12% of the time (1.12x the base rate, in 2.1% of orders). Weak association, not a bundle candidate.
10. Customers who order Fresh Lime Soda also order Soft Drink (Regular) 20% of the time (1.12x the base rate, in 2.1% of orders). Weak association, not a bundle candidate.
11. Customers who order Chicken Pakora also order Fresh Lime Soda 12% of the time (1.12x the base rate, in 0.9% of orders). Weak association, not a bundle candidate.
12. Customers who order Sweet Lassi also order Mineral Water (500ml) 13% of the time (1.12x the base rate, in 0.5% of orders). Weak association, not a bundle candidate.
13. Customers who order Sweet Lassi also order Soft Drink (Regular) 20% of the time (1.11x the base rate, in 0.8% of orders). Weak association, not a bundle candidate.
14. Customers who order Aloo Pakora also order Fresh Lime Soda 12% of the time (1.10x the base rate, in 0.6% of orders). Weak association, not a bundle candidate.
15. Customers who order Mutton Pulao also order Zinger Burger 18% of the time (1.10x the base rate, in 0.8% of orders). Weak association, not a bundle candidate.
16. Customers who order Peach Iced Tea also order Soft Drink (Regular) 19% of the time (1.09x the base rate, in 0.7% of orders). Weak association, not a bundle candidate.
17. Customers who order Tandoori Chicken (Half) also order Fresh Lime Soda 12% of the time (1.09x the base rate, in 0.7% of orders). Weak association, not a bundle candidate.
18. Customers who order Mutton Biryani also order Zinger Burger 18% of the time (1.08x the base rate, in 0.9% of orders). Weak association, not a bundle candidate.
19. Customers who order Peshawari Karahi also order Chicken Karahi (Half) 15% of the time (1.07x the base rate, in 0.9% of orders). Weak association, not a bundle candidate.
20. Customers who order Daal Makhni also order Chicken Karahi (Half) 15% of the time (1.07x the base rate, in 0.5% of orders). Weak association, not a bundle candidate.
<!-- /section:08 -->

<!-- section:09 -->
## 3. Wastage analysis and wastage-risk prediction

Wastage rate = wastage cost / (wastage cost + cost of goods sold). The rate is cost-based because wasted quantities are in kg, litres or pieces while sales are in portions. "Promotion active" means a promotion covered that item at that location on that day.

**Top 10 items by wastage cost**

| item_name | category_name | wastage_cost | cogs | wastage_rate |
|---|---|---|---|---|
| Mutton Karahi (Half) | Karahi & Handi | 5,974,610 | 24,561,563 | 19.6% |
| Chicken Karahi (Half) | Karahi & Handi | 5,376,754 | 22,984,174 | 19.0% |
| Chicken Handi | Karahi & Handi | 4,733,201 | 19,306,572 | 19.7% |
| Seekh Kabab | BBQ & Grills | 1,573,843 | 6,485,879 | 19.5% |
| Fried Fish Lahori | Seafood | 1,084,533 | 4,670,153 | 18.8% |
| Fresh Garden Salad | Salads | 1,013,431 | 1,773,318 | 36.4% |
| Jumbo Family Pizza | Pizza | 936,846 | 5,126,001 | 15.5% |
| Chicken Wings Bucket | Appetizers | 525,878 | 832,466 | 38.7% |
| Double Patty Burger | Burgers & Wraps | 415,279 | 2,044,518 | 16.9% |
| Chicken Tikka Pizza (Medium) | Pizza | 321,737 | 36,636,802 | 0.9% |

**By category**

| category_name | wastage_cost | cogs | wastage_rate |
|---|---|---|---|
| Salads | 1,459,145 | 9,585,590 | 13.2% |
| Karahi & Handi | 16,347,108 | 114,685,820 | 12.5% |
| Pasta | 1,147,726 | 17,373,704 | 6.2% |
| Appetizers | 1,141,306 | 18,570,680 | 5.8% |
| Seafood | 1,205,221 | 22,534,523 | 5.1% |
| Desserts | 602,748 | 11,620,531 | 4.9% |
| BBQ & Grills | 2,026,879 | 65,109,873 | 3.0% |
| Pizza | 2,449,860 | 80,873,996 | 2.9% |
| Burgers & Wraps | 1,265,279 | 42,283,809 | 2.9% |
| Sides & Breads | 398,662 | 42,631,156 | 0.9% |
| Soups | 53,646 | 8,345,085 | 0.6% |
| Hot Beverages | 67,424 | 11,682,959 | 0.6% |
| Cold Beverages & Shakes | 162,792 | 33,364,121 | 0.5% |
| Rice & Biryani | 193,735 | 44,045,628 | 0.4% |

**Locations, highest and lowest 5 by wastage rate**

| location_id | wastage_cost | cogs | wastage_rate |
|---|---|---|---|
| LOC0024 | 733,487 | 8,068,839 | 8.3% |
| LOC0025 | 1,129,618 | 14,629,612 | 7.2% |
| LOC0021 | 907,198 | 13,735,652 | 6.2% |
| LOC0013 | 1,364,543 | 22,216,759 | 5.8% |
| LOC0012 | 1,069,428 | 17,442,764 | 5.8% |
| LOC0015 | 977,586 | 20,650,580 | 4.5% |
| LOC0003 | 1,556,208 | 33,593,434 | 4.4% |
| LOC0001 | 1,014,878 | 22,131,439 | 4.4% |
| LOC0023 | 1,324,954 | 30,785,196 | 4.1% |
| LOC0004 | 1,233,745 | 30,522,678 | 3.9% |

**By day of week**

| dow | wastage_cost | cogs | wastage_rate |
|---|---|---|---|
| Sun | 3,975,195 | 88,083,982 | 4.3% |
| Mon | 4,161,616 | 60,836,086 | 6.4% |
| Tue | 4,130,538 | 60,343,787 | 6.4% |
| Wed | 4,289,459 | 64,224,509 | 6.3% |
| Thu | 3,875,372 | 66,575,875 | 5.5% |
| Fri | 4,072,818 | 87,889,740 | 4.4% |
| Sat | 4,016,533 | 94,753,495 | 4.1% |

**Promotion active vs not**

| promo_active | wastage_cost | cogs | wastage_rate |
|---|---|---|---|
| no | 15,386,243 | 343,813,999 | 4.3% |
| yes | 13,135,289 | 178,893,475 | 6.8% |

### Wastage-risk model

Grain: item x location x day from Inventory (rows with prepared quantity > 0), joined to Wastage. Label `high_wastage_risk` = wasted / prepared > 0.1. A top-quartile cut was not usable: 95% of rows have no waste, and the 75th percentile of the rest is exactly 1.0 (the whole preparation wasted).

Features: `prepared_quantity`, `recent_demand`, `prep_to_demand`, `day_of_week`, `month`, `is_weekend`, `promo_active`, `item_popularity`, `location_idx`, `category_idx`. `recent_demand` is the mean consumed quantity over the previous 7 days, excluding the day itself. `prep_to_demand` is prepared quantity over that mean. `item_popularity` comes from the `as_of_date=2025-09-30` item features, before the test period. Excluded as leakage: the day's wasted quantity, waste cost, wastage percentage, consumed and closing stock, and item-level wastage features. `tests/test_analytics.py` asserts that none of them reach the feature vector.

Time-based split: train before 2025-10-01 (929,436 rows, 4.3% positive), test from 2025-10-01 (318,056 rows, 4.2% positive). Spark MLlib `RandomForestClassifier` (numTrees=50, maxDepth=8) with balanced class weights. Step 5 already covers the three-algorithm comparison, so one algorithm is used here.

| accuracy | precision | recall | F1 | ROC AUC | PR AUC |
|---|---|---|---|---|---|
| 0.913 | 0.228 | 0.455 | 0.304 | 0.763 | 0.229 |

Confusion matrix (test): TP 6,044, FP 20,418, FN 7,234, TN 284,360. Always predicting "not high risk" would score 95.8% accuracy, so accuracy is not a useful measure here. PR AUC against the 4.2% base rate is the more honest one.

Feature importances: `item_popularity` 0.445, `category_idx` 0.396, `prepared_quantity` 0.069, `recent_demand` 0.045, `prep_to_demand` 0.024, `location_idx` 0.012, `promo_active` 0.005, `month` 0.003, `day_of_week` 0.001, `is_weekend` 0.001.

Item-level features (`item_popularity`, `category_idx`) carry 84% of the importance, so the model mostly learns which items are wasteful rather than which days are risky. Day-level signals (promotion, weekday, month) add little. It is useful for ranking item-location pairs to watch, not for day-by-day prep decisions.

Model version `v20260925_192941` in `models/spark/wastage_risk/`. Test-period predictions are in `parquet_data/wastage_risk/`.
<!-- /section:09 -->

<!-- section:10 -->
## 4. Price intelligence and price sensitivity

284 chain-wide price changes in Pricing_History. 120 are evaluable: a price move of at least 5%, a full 30-day window on both sides inside the order data, and sales before the change. Smaller changes are routine 2-5% inflation revisions, where demand noise swamps the price effect.

Demand is average daily quantity sold chain-wide, 30 days before vs 30 days from the change. To take out seasonality, the item's demand change is divided by the change in the rest of its category over the same windows (`control_change_pct`). Elasticity = adjusted % demand change / % price change. Where an item has several evaluable changes, the largest one is used.

A zero or positive elasticity (demand moved with the price) is **Inconclusive**: something other than price drove demand. The negative ones are split by the percentile of |elasticity|: above 0.67 is Highly, below 0.33 is Low, else Moderately Price Sensitive. Items without an evaluable change are **Not Evaluated**, and no class is forced on them.

Highly Price Sensitive: 13, Moderately Price Sensitive: 15, Low Price Sensitivity: 15, Inconclusive: 53, Not Evaluated: 60.

**Price-sensitivity evidence (negative elasticity), most sensitive first**

| item_name | change_date | old_price | new_price | price_change_pct | demand_before | demand_after | demand_change_pct | control_change_pct | elasticity | price_sensitivity |
|---|---|---|---|---|---|---|---|---|---|---|
| Double Patty Burger | 2025-02-01 | 1,350 | 1,440 | +6.7% | 5.7 | 2.2 | -60.6% | +0.3% | -9.11 | Highly Price Sensitive |
| Mutton Yakhni | 2025-03-15 | 550 | 590 | +7.3% | 13.2 | 5.9 | -55.4% | -24.7% | -5.61 | Highly Price Sensitive |
| Chicken Fried Rice | 2025-07-01 | 790 | 670 | -15.2% | 16.8 | 23.0 | +36.9% | -9.7% | -3.39 | Highly Price Sensitive |
| Chocolate Fudge Cake | 2025-02-01 | 650 | 690 | +6.2% | 13.3 | 10.5 | -21.1% | -3.7% | -2.93 | Highly Price Sensitive |
| Strawberry Shake | 2025-06-15 | 540 | 460 | -14.8% | 69.0 | 78.2 | +13.3% | -18.3% | -2.62 | Highly Price Sensitive |
| BBQ Chicken Pizza (Medium) | 2025-03-15 | 1,550 | 1,630 | +5.2% | 15.1 | 13.9 | -7.9% | +6.3% | -2.60 | Highly Price Sensitive |
| Club Sandwich | 2025-06-01 | 780 | 660 | -15.4% | 22.6 | 27.6 | +21.9% | -12.5% | -2.56 | Highly Price Sensitive |
| Four Cheese Pizza (Medium) | 2025-03-15 | 1,750 | 1,840 | +5.1% | 20.3 | 19.3 | -5.1% | +6.4% | -2.10 | Highly Price Sensitive |
| Roghni Naan | 2025-03-15 | 90 | 95 | +5.6% | 47.7 | 46.6 | -2.2% | +9.5% | -1.93 | Highly Price Sensitive |
| Palak Paneer | 2025-02-01 | 750 | 800 | +6.7% | 8.9 | 7.7 | -13.5% | -0.9% | -1.90 | Highly Price Sensitive |
| Classic Beef Burger | 2025-06-01 | 900 | 1,130 | +25.6% | 8.9 | 4.4 | -50.9% | -8.1% | -1.82 | Highly Price Sensitive |
| Masala Fries | 2025-03-15 | 350 | 370 | +5.7% | 27.5 | 27.1 | -1.5% | +9.2% | -1.71 | Highly Price Sensitive |
| Malai Boti | 2025-07-15 | 1,020 | 1,220 | +19.6% | 18.7 | 16.1 | -13.7% | +27.6% | -1.65 | Highly Price Sensitive |
| Seafood Pizza (Medium) | 2025-03-15 | 1,950 | 2,070 | +6.2% | 12.8 | 12.2 | -4.4% | +5.9% | -1.59 | Moderately Price Sensitive |
| Tiramisu | 2025-02-01 | 850 | 910 | +7.1% | 9.0 | 7.7 | -14.4% | -4.4% | -1.49 | Moderately Price Sensitive |
| Fajita Pizza (Medium) | 2025-07-01 | 1,600 | 1,920 | +20.0% | 24.1 | 16.7 | -30.6% | -1.5% | -1.48 | Moderately Price Sensitive |
| Dahi Bhallay | 2025-02-01 | 400 | 420 | +5.0% | 21.6 | 20.2 | -6.8% | +0.3% | -1.41 | Moderately Price Sensitive |
| Vegetable Biryani | 2025-03-15 | 500 | 530 | +6.0% | 20.8 | 22.2 | +6.7% | +15.7% | -1.29 | Moderately Price Sensitive |
| Mutton Chops | 2025-03-15 | 2,200 | 2,310 | +5.0% | 19.7 | 20.8 | +5.4% | +12.5% | -1.26 | Moderately Price Sensitive |
| Crispy Fish Bites | 2025-02-01 | 850 | 900 | +5.9% | 14.3 | 13.3 | -7.0% | -0.0% | -1.18 | Moderately Price Sensitive |
| Malai Boti Pizza (Medium) | 2025-03-15 | 1,600 | 1,710 | +6.9% | 11.0 | 10.7 | -2.4% | +5.8% | -1.13 | Moderately Price Sensitive |
| Kulfi | 2025-02-01 | 250 | 265 | +6.0% | 4.9 | 4.4 | -10.9% | -4.6% | -1.09 | Moderately Price Sensitive |
| Alfredo Pasta | 2025-07-01 | 1,190 | 1,410 | +18.5% | 21.9 | 16.3 | -25.4% | -6.7% | -1.09 | Moderately Price Sensitive |
| Kachumber Salad | 2025-02-01 | 250 | 265 | +6.0% | 8.2 | 7.3 | -11.0% | -4.8% | -1.07 | Moderately Price Sensitive |
| Finger Fish | 2025-06-15 | 1,150 | 1,420 | +23.5% | 5.9 | 3.9 | -33.9% | -12.7% | -1.04 | Moderately Price Sensitive |
| Oreo Shake | 2025-06-15 | 610 | 780 | +27.9% | 25.4 | 15.4 | -39.4% | -15.5% | -1.02 | Moderately Price Sensitive |
| Chicken White Karahi | 2025-02-01 | 1,550 | 1,650 | +6.5% | 15.3 | 14.1 | -7.4% | -0.9% | -1.02 | Moderately Price Sensitive |
| Chicken Boti | 2025-03-15 | 800 | 840 | +5.0% | 27.2 | 29.1 | +7.1% | +12.5% | -0.97 | Moderately Price Sensitive |
| Mutton Biryani | 2025-06-01 | 990 | 1,210 | +22.2% | 41.5 | 29.2 | -29.6% | -12.3% | -0.89 | Low Price Sensitivity |
| Nihari | 2025-02-01 | 1,210 | 1,280 | +5.8% | 80.6 | 76.2 | -5.4% | -0.3% | -0.87 | Low Price Sensitivity |
| Fresh Orange Juice | 2025-02-01 | 440 | 470 | +6.8% | 108.5 | 105.4 | -2.9% | +2.9% | -0.83 | Low Price Sensitivity |
| Cold Coffee | 2025-02-01 | 480 | 510 | +6.2% | 18.2 | 17.6 | -3.1% | +1.9% | -0.78 | Low Price Sensitivity |
| Hot Chocolate | 2025-09-01 | 540 | 570 | +5.6% | 19.2 | 13.1 | -31.5% | -28.5% | -0.74 | Low Price Sensitivity |
| Reshmi Kabab | 2025-03-15 | 900 | 960 | +6.7% | 13.4 | 14.5 | +7.7% | +12.2% | -0.61 | Low Price Sensitivity |
| Beef Pulao | 2025-03-15 | 750 | 790 | +5.3% | 14.7 | 16.4 | +11.5% | +15.3% | -0.60 | Low Price Sensitivity |
| Mutton Karahi (Half) | 2025-09-01 | 2,600 | 2,820 | +8.5% | 129.4 | 46.7 | -63.9% | -62.2% | -0.54 | Low Price Sensitivity |
| Mutton Raan | 2025-03-15 | 4,800 | 5,110 | +6.5% | 17.5 | 19.1 | +8.9% | +12.2% | -0.46 | Low Price Sensitivity |
| Plain Naan | 2025-09-01 | 60 | 65 | +8.3% | 320.0 | 200.2 | -37.4% | -35.0% | -0.45 | Low Price Sensitivity |
| Pepperoni Pizza (Medium) | 2025-03-15 | 1,650 | 1,760 | +6.7% | 14.1 | 14.6 | +3.5% | +5.5% | -0.28 | Low Price Sensitivity |
| Gajar Halwa | 2025-02-01 | 470 | 500 | +6.4% | 89.5 | 84.8 | -5.3% | -4.4% | -0.14 | Low Price Sensitivity |
| Fish Burger | 2025-02-01 | 800 | 850 | +6.2% | 13.7 | 13.5 | -1.7% | -1.0% | -0.11 | Low Price Sensitivity |
| Chicken Karahi (Half) | 2025-02-01 | 1,450 | 1,530 | +5.5% | 93.8 | 92.7 | -1.2% | -1.1% | -0.02 | Low Price Sensitivity |
| Seekh Kabab | 2025-03-15 | 850 | 900 | +5.9% | 54.1 | 60.6 | +12.0% | +12.0% | -0.01 | Low Price Sensitivity |

**Inconclusive changes**

| item_name | change_date | old_price | new_price | price_change_pct | demand_before | demand_after | demand_change_pct | control_change_pct | elasticity | price_sensitivity |
|---|---|---|---|---|---|---|---|---|---|---|
| Chickpea Chaat | 2025-07-01 | 390 | 410 | +5.1% | 5.2 | 10.1 | +93.0% | -4.1% | +19.73 | Inconclusive |
| Haleem | 2025-11-01 | 710 | 750 | +5.6% | 37.6 | 85.2 | +126.6% | +27.3% | +13.86 | Inconclusive |
| Sweet Lassi | 2025-04-01 | 310 | 330 | +6.5% | 25.7 | 67.3 | +162.2% | +39.6% | +13.61 | Inconclusive |
| Fried Fish Lahori | 2025-11-01 | 1,340 | 1,410 | +5.2% | 17.8 | 36.9 | +107.5% | +22.2% | +13.38 | Inconclusive |
| Jumbo Family Pizza | 2025-05-01 | 3,360 | 3,090 | -8.0% | 36.4 | 3.2 | -91.1% | -19.3% | +11.07 | Inconclusive |
| Kashmiri Chai | 2025-11-01 | 300 | 320 | +6.7% | 18.7 | 41.4 | +121.4% | +31.5% | +10.25 | Inconclusive |
| Chicken Wings Bucket | 2025-05-01 | 1,710 | 1,570 | -8.2% | 2.4 | 0.6 | -74.6% | -11.0% | +8.73 | Inconclusive |
| Russian Salad | 2025-09-01 | 540 | 570 | +5.6% | 8.1 | 5.8 | -28.7% | -51.5% | +8.47 | Inconclusive |
| Karak Chai | 2025-10-01 | 250 | 235 | -6.0% | 50.1 | 29.6 | -40.9% | +18.6% | +8.36 | Inconclusive |
| Falooda | 2025-02-01 | 550 | 580 | +5.5% | 3.8 | 5.1 | +33.3% | -5.5% | +7.53 | Inconclusive |
| BBQ Platter for 4 | 2025-05-01 | 5,770 | 5,310 | -8.0% | 11.9 | 4.7 | -60.1% | -4.6% | +7.30 | Inconclusive |
| Grilled Salmon Steak | 2025-03-15 | 3,800 | 3,990 | +5.0% | 6.9 | 8.3 | +19.2% | -11.7% | +7.01 | Inconclusive |
| Brownie Sundae | 2025-05-01 | 730 | 670 | -8.2% | 3.8 | 1.6 | -59.1% | -9.0% | +6.70 | Inconclusive |
| Aloo Pakora | 2025-07-01 | 360 | 380 | +5.6% | 36.3 | 80.2 | +120.7% | +66.5% | +5.86 | Inconclusive |
| Korean Fried Chicken Burger | 2025-11-24 | 990 | 1,100 | +11.1% | 8.0 | 15.3 | +92.1% | +25.3% | +4.79 | Inconclusive |
| Chicken Pakora | 2025-07-01 | 570 | 600 | +5.3% | 58.3 | 119.9 | +105.8% | +64.5% | +4.77 | Inconclusive |
| Peach Iced Tea | 2025-05-06 | 320 | 350 | +9.4% | 35.2 | 60.6 | +72.2% | +21.0% | +4.50 | Inconclusive |
| Mint Tea | 2025-03-15 | 170 | 180 | +5.9% | 23.9 | 23.7 | -1.1% | -18.2% | +3.54 | Inconclusive |
| Prawn Karahi | 2025-03-15 | 2,400 | 2,550 | +6.2% | 5.3 | 5.8 | +8.8% | -10.4% | +3.42 | Inconclusive |
| Lotus Biscoff Shake | 2025-12-01 | 680 | 750 | +10.3% | 21.3 | 30.6 | +43.8% | +10.3% | +2.95 | Inconclusive |
| Kebab Crust Pizza (Medium) | 2025-06-10 | 1,660 | 1,850 | +11.4% | 34.0 | 40.6 | +19.4% | -10.2% | +2.88 | Inconclusive |
| Cream of Mushroom Soup | 2025-03-01 | 550 | 520 | -5.5% | 13.4 | 5.8 | -56.4% | -48.3% | +2.85 | Inconclusive |
| Doodh Patti Chai | 2025-03-15 | 180 | 190 | +5.6% | 55.1 | 51.4 | -6.7% | -19.3% | +2.80 | Inconclusive |
| Beef Bihari Boti | 2025-09-01 | 1,140 | 1,200 | +5.3% | 14.8 | 8.9 | -39.6% | -45.6% | +2.10 | Inconclusive |
| Spaghetti Bolognese | 2025-02-01 | 1,150 | 1,220 | +6.1% | 19.0 | 21.1 | +10.7% | -1.5% | +2.03 | Inconclusive |
| Pink Sauce Pasta | 2025-11-10 | 1,080 | 1,200 | +11.1% | 6.6 | 8.4 | +27.3% | +4.9% | +1.92 | Inconclusive |
| Kabuli Pulao | 2025-03-15 | 900 | 950 | +5.6% | 37.4 | 46.6 | +24.6% | +13.8% | +1.70 | Inconclusive |
| Smash Burger | 2025-10-27 | 1,120 | 1,250 | +11.6% | 11.3 | 15.5 | +37.6% | +16.0% | +1.60 | Inconclusive |
| Caesar Salad | 2025-02-01 | 750 | 790 | +5.3% | 5.8 | 5.9 | +1.7% | -6.0% | +1.54 | Inconclusive |
| Baked Chicken Penne | 2025-02-01 | 1,150 | 1,230 | +7.0% | 4.5 | 4.9 | +9.7% | +0.5% | +1.31 | Inconclusive |
| Mineral Water (500ml) | 2025-02-01 | 100 | 105 | +5.0% | 81.7 | 87.6 | +7.3% | +0.7% | +1.31 | Inconclusive |
| Chocolate Shake | 2025-02-01 | 550 | 580 | +5.5% | 9.8 | 10.6 | +8.5% | +1.6% | +1.26 | Inconclusive |
| Calamari Rings | 2025-03-15 | 1,250 | 1,330 | +6.4% | 5.2 | 5.1 | -2.5% | -9.7% | +1.23 | Inconclusive |
| Paratha | 2025-03-15 | 100 | 105 | +5.0% | 42.1 | 48.4 | +15.0% | +8.5% | +1.19 | Inconclusive |
| Crispy Chicken Wrap | 2025-02-01 | 600 | 630 | +5.0% | 27.9 | 29.0 | +3.8% | -1.7% | +1.11 | Inconclusive |
| Truffle Mushroom Pasta | 2025-02-01 | 1,650 | 1,760 | +6.7% | 3.3 | 3.6 | +8.1% | +0.7% | +1.10 | Inconclusive |
| Butter Chicken | 2025-02-01 | 1,350 | 1,440 | +6.7% | 17.2 | 18.1 | +5.6% | -1.4% | +1.06 | Inconclusive |
| Vegetable Samosa | 2025-02-01 | 200 | 210 | +5.0% | 17.8 | 18.5 | +3.9% | -1.2% | +1.04 | Inconclusive |
| Quinoa Power Bowl | 2025-03-31 | 990 | 1,100 | +11.1% | 9.9 | 16.0 | +62.2% | +45.5% | +1.03 | Inconclusive |
| Greek Salad | 2025-02-01 | 780 | 820 | +5.1% | 6.0 | 5.9 | -1.7% | -5.8% | +0.84 | Inconclusive |
| Fruit Chaat | 2025-02-01 | 400 | 420 | +5.0% | 12.8 | 12.5 | -2.3% | -6.0% | +0.78 | Inconclusive |
| Vegetable Supreme Pizza (Medium) | 2025-03-15 | 1,300 | 1,380 | +6.2% | 17.9 | 19.5 | +9.3% | +5.0% | +0.67 | Inconclusive |
| Americano | 2025-09-01 | 390 | 410 | +5.1% | 35.0 | 25.4 | -27.3% | -28.9% | +0.46 | Inconclusive |
| Coleslaw | 2025-03-15 | 200 | 210 | +5.0% | 35.1 | 39.0 | +11.1% | +8.7% | +0.43 | Inconclusive |
| Dynamite Prawns | 2025-07-03 | 1,220 | 1,350 | +10.7% | 7.6 | 13.0 | +71.8% | +65.6% | +0.35 | Inconclusive |
| Steamed Rice | 2025-03-15 | 300 | 320 | +6.7% | 25.2 | 29.6 | +17.4% | +14.9% | +0.33 | Inconclusive |
| Papri Chaat | 2025-02-01 | 380 | 400 | +5.3% | 10.6 | 10.7 | +0.9% | -0.7% | +0.32 | Inconclusive |
| Grilled Chicken Salad | 2025-02-01 | 890 | 950 | +6.7% | 15.3 | 14.7 | -3.9% | -5.8% | +0.29 | Inconclusive |
| Grilled Chicken Burger | 2025-02-01 | 750 | 790 | +5.3% | 10.8 | 10.8 | +0.3% | -1.1% | +0.27 | Inconclusive |
| Mint Margarita | 2025-02-01 | 380 | 400 | +5.3% | 17.0 | 17.5 | +2.9% | +1.7% | +0.24 | Inconclusive |
| Garlic Bread | 2025-03-15 | 350 | 370 | +5.7% | 38.5 | 42.2 | +9.7% | +8.8% | +0.15 | Inconclusive |
| Grilled Fish Tikka | 2025-03-15 | 1,350 | 1,440 | +6.7% | 15.2 | 17.1 | +12.3% | +12.0% | +0.03 | Inconclusive |
| Blue Lagoon | 2025-02-01 | 420 | 450 | +7.1% | 8.8 | 9.0 | +1.9% | +1.7% | +0.03 | Inconclusive |

Caveats: the category control removes seasonality that the whole category shares, but not seasonality specific to one item, or promotions on that item alone. Some control items had their own price change on the same date, which dampens the adjustment. Treat these as rough elasticities.
<!-- /section:10 -->

<!-- section:11 -->
## 5. Promotion effectiveness and promotion traps

Each promotion is measured on its own items at its own locations (all orders, not only those that used the code) over three windows of equal length: the days immediately before, the promotion, and the days immediately after. Values below are changes during the promotion against the pre window. `post qty` compares the post window with the pre window.

A promotion is flagged as a trap if any of these hold: revenue or orders up but total margin down; customers up but margin per order down by more than 5%; wastage cost up by more than 10%; post-promotion quantity more than 5% below the pre-promotion baseline.

| promotion | type | days | flags | orders | revenue | margin | customers | margin/order | wastage | post qty |
|---|---|---|---|---|---|---|---|---|---|---|
| PROMO001 New Year Kickoff | PercentOff | 10 | not evaluated: pre window starts before the data |  |  |  |  |  |  |  |
| PROMO002 Winter Warmers | PercentOff | 36 | not evaluated: pre window starts before the data |  |  |  |  |  |  |  |
| PROMO003 Valentine Dinner Combo | ComboDeal | 7 | - | +39% | +49% | +45% | +38% | +4% | +10% | -4% |
| PROMO004 Ramadan Iftar Deal | FixedAmountOff | 29 | wastage_up | -4% | +15% | +12% | -2% | +17% | +55% | +1% |
| PROMO005 Eid Family Feast | ComboDeal | 7 | - | +78% | +107% | +87% | +77% | +5% | -3% | +2% |
| PROMO006 Spring Pizza Fest | BOGO | 16 | volume_up_margin_down, customers_up_margin_per_order_down, wastage_up, post_promo_drop | +17% | +19% | -21% | +17% | -32% | +13% | -15% |
| PROMO007 Summer Coolers | PercentOff | 61 | wastage_up | +22% | +34% | +25% | +17% | +3% | +103% | +38% |
| PROMO008 Mall Mania | PercentOff | 32 | wastage_up, post_promo_drop | +13% | +11% | +13% | +10% | -0% | +15% | -14% |
| PROMO009 Highway Traveler Deal | FixedAmountOff | 122 | wastage_up | +15% | +19% | +18% | +7% | +3% | +55% | -11% |
| PROMO010 Eid ul Adha BBQ Nights | PercentOff | 9 | wastage_up | +20% | +32% | +25% | +19% | +4% | +26% | -1% |
| PROMO011 Monsoon Munchies | FixedAmountOff | 31 | wastage_up | +37% | +61% | +56% | +35% | +14% | +10% | +96% |
| PROMO012 August BOGO Blast | BOGO | 31 | customers_up_margin_per_order_down, wastage_up | +87% | +95% | +51% | +71% | -19% | +448% | -3% |
| PROMO013 Independence Day 14% Off | PercentOff | 5 | wastage_up, post_promo_drop | +24% | +27% | +27% | +22% | +3% | +19% | -18% |
| PROMO014 Back to Work Lunch | FixedAmountOff | 30 | volume_up_margin_down, wastage_up, post_promo_drop | -12% | +1% | -4% | -8% | +8% | +35% | -32% |
| PROMO015 App Exclusive Fest | PercentOff | 31 | post_promo_drop | -16% | -15% | -15% | -12% | +0% | -49% | -14% |
| PROMO016 Pasta Week | PercentOff | 8 | customers_up_margin_per_order_down, wastage_up | +79% | +63% | +51% | +76% | -15% | +42% | +18% |
| PROMO017 Family Platter Month | ComboDeal | 20 | - | +30% | +88% | +61% | +29% | +23% | -5% | +31% |
| PROMO018 Black Friday Mega Sale | PercentOff | 4 | customers_up_margin_per_order_down, wastage_up | +103% | +89% | +74% | +100% | -15% | +35% | +23% |
| PROMO019 Winter Soup Season | PercentOff | 31 | wastage_up | +49% | +56% | +51% | +47% | +1% | +76% |  |
| PROMO020 Year End Celebration | FixedAmountOff | 12 | wastage_up | +35% | +38% | +37% | +31% | +1% | +64% |  |

15 of 20 promotions are flagged. 7 of them are flagged only for wastage: kitchens over-prepare for every promotion, so on its own that flag is a weak signal. The ones that also hurt margin or post-promotion demand are the real traps: PROMO006 Spring Pizza Fest, PROMO014 Back to Work Lunch, PROMO018 Black Friday Mega Sale, PROMO012 August BOGO Blast, PROMO016 Pasta Week, PROMO013 Independence Day 14% Off, PROMO008 Mall Mania, PROMO015 App Exclusive Fest.

**Most-flagged: PROMO006 Spring Pizza Fest** (2025-04-15 to 2025-04-30). Orders on its items went from 2,494 to 2,922 and customers from 2,328 to 2,717. Margin rate went from 48.6% to 32.3%, margin per order from 1,362 to 923, and wastage cost went from 109,191 to 123,905. Flags: volume_up_margin_down, customers_up_margin_per_order_down, wastage_up, post_promo_drop.

**Largest wastage rise among margin traps: PROMO012 August BOGO Blast** (2025-08-01 to 2025-08-31). Orders on its items went from 6,808 to 12,738 and customers from 5,639 to 9,654. Margin rate went from 60.9% to 47.0%, margin per order from 1,805 to 1,455, and wastage cost went from 1,065,456 to 5,839,932. Flags: customers_up_margin_per_order_down, wastage_up.

Caveats: promotions overlap (e.g. PROMO009 runs June to September), so a pre window can include another promotion. The windows also don't control for seasonality. Promotions whose pre window starts before 1 January 2025 can't be evaluated, and neither can the post window of promotions ending in the last weeks of December.
<!-- /section:11 -->

<!-- section:12 -->
## 6. Rating and sales anomalies

Rolling z-scores, flagged when |z| > 3.0. The baseline is the series' own recent past, excluding the current period: the previous 8 weeks for weekly ratings per item (at least 4), and the previous 28 days for daily sales per location and per item (at least 14). Identical ratings: one rating value is at least 80% of an item's week, with at least 10 ratings. High order value: total_amount above Q3 + 3.0 x IQR (27,365). Duplicate transactions: two or more orders with the same customer, minute and item/quantity list.

| anomaly_type | metric | flags |
|---|---|---|
| high_order_value | total_amount | 688 |
| identical_ratings | share_of_rating_2 | 2 |
| identical_ratings | share_of_rating_3 | 2 |
| identical_ratings | share_of_rating_4 | 15 |
| item_sales_z | daily_quantity | 894 |
| item_sales_z | daily_revenue | 869 |
| location_sales_z | daily_orders | 122 |
| location_sales_z | daily_revenue | 132 |
| rating_z | avg_rating | 238 |
| rating_z | rating_count | 307 |

**Rating anomalies (largest |z|)**

| entity | location_id | period_start | metric | value | baseline_mean | score |
|---|---|---|---|---|---|---|
| Double Patty Burger |  | 2025-03-31 | rating_count | 11.00 | 1.67 | +18.1 |
| Chapli Kabab Burger |  | 2025-05-26 | avg_rating | 2.75 | 3.94 | -12.3 |
| Vegetable Supreme Pizza (Medium) |  | 2025-01-27 | avg_rating | 3.00 | 3.47 | -11.2 |
| Fruit Chaat |  | 2025-02-17 | avg_rating | 2.50 | 4.11 | -10.8 |
| Reshmi Kabab |  | 2025-02-17 | avg_rating | 1.00 | 3.59 | -10.6 |
| Chicken Boti |  | 2025-01-27 | avg_rating | 3.12 | 3.66 | -10.2 |
| Crispy Fish Bites |  | 2025-12-22 | rating_count | 16.00 | 6.75 | +8.9 |
| Haleem |  | 2025-05-19 | avg_rating | 3.00 | 4.07 | -8.9 |

**Identical-rating weeks**

| entity | location_id | period_start | metric | value | baseline_mean | score |
|---|---|---|---|---|---|---|
| Chicken Fried Rice |  | 2025-08-25 | share_of_rating_4 | 0.82 |  | +17.0 |
| Doodh Patti Chai |  | 2025-01-06 | share_of_rating_4 | 0.81 |  | +16.0 |
| Peach Iced Tea |  | 2025-07-07 | share_of_rating_4 | 0.80 |  | +15.0 |
| Veg Spring Rolls |  | 2025-01-13 | share_of_rating_2 | 0.82 |  | +11.0 |
| Raita |  | 2025-01-06 | share_of_rating_4 | 0.82 |  | +11.0 |
| Onion Rings |  | 2025-05-05 | share_of_rating_4 | 0.82 |  | +11.0 |
| Americano |  | 2025-04-07 | share_of_rating_4 | 0.82 |  | +11.0 |
| Daal Makhni |  | 2025-01-27 | share_of_rating_4 | 0.90 |  | +10.0 |

**Location daily sales anomalies**

| entity | location_id | period_start | metric | value | baseline_mean | score |
|---|---|---|---|---|---|---|
| LOC0023 | LOC0023 | 2025-03-31 | daily_orders | 59.00 | 23.18 | +8.2 |
| LOC0023 | LOC0023 | 2025-11-06 | daily_revenue | 445,487.57 | 192,260.20 | +6.8 |
| LOC0023 | LOC0023 | 2025-08-02 | daily_revenue | 429,645.75 | 181,632.64 | +6.6 |
| LOC0008 | LOC0008 | 2025-08-14 | daily_orders | 68.00 | 26.11 | +6.5 |
| LOC0008 | LOC0008 | 2025-08-14 | daily_revenue | 535,685.33 | 192,670.27 | +6.2 |
| LOC0013 | LOC0013 | 2025-02-14 | daily_revenue | 404,745.95 | 142,732.50 | +6.2 |
| LOC0007 | LOC0007 | 2025-08-14 | daily_revenue | 420,707.65 | 162,872.73 | +5.7 |
| LOC0020 | LOC0020 | 2025-11-02 | daily_revenue | 349,003.72 | 139,563.47 | +5.7 |

**Item daily sales anomalies**

| entity | location_id | period_start | metric | value | baseline_mean | score |
|---|---|---|---|---|---|---|
| Double Patty Burger |  | 2025-03-31 | daily_quantity | 48.00 | 2.83 | +28.0 |
| Jumbo Family Pizza |  | 2025-03-31 | daily_quantity | 42.00 | 2.32 | +27.8 |
| Double Patty Burger |  | 2025-03-31 | daily_revenue | 57,960.00 | 4,069.57 | +23.2 |
| Jumbo Family Pizza |  | 2025-03-31 | daily_revenue | 118,380.00 | 7,733.18 | +23.2 |
| Brownie Sundae |  | 2025-03-31 | daily_quantity | 15.00 | 2.06 | +13.9 |
| BBQ Platter for 4 |  | 2025-03-01 | daily_quantity | 29.00 | 2.76 | +12.9 |
| BBQ Platter for 4 |  | 2025-03-01 | daily_revenue | 152,449.81 | 15,180.00 | +12.3 |
| Brownie Sundae |  | 2025-03-31 | daily_revenue | 9,122.50 | 1,550.62 | +10.8 |

**Dates with the most sales anomalies**

| period_start | flags | promotions starting within 1 day |
|---|---|---|
| 2025-02-14 | 134 |  |
| 2025-08-14 | 102 |  |
| 2025-08-02 | 97 | PROMO012 August BOGO Blast |
| 2025-08-01 | 80 | PROMO012 August BOGO Blast |
| 2025-11-29 | 56 |  |
| 2025-11-01 | 51 | PROMO017 Family Platter Month |
| 2025-03-31 | 50 | PROMO005 Eid Family Feast |
| 2025-08-09 | 44 |  |
| 2025-11-28 | 38 | PROMO018 Black Friday Mega Sale |
| 2025-04-01 | 37 | PROMO005 Eid Family Feast |

The dates with the most flags are promotion launches (annotated above) and public holidays. In 2025, 14 Feb is Valentine's Day, 31 Mar is Eid ul Fitr and 14 Aug is Independence Day. These spikes are real demand changes, not data errors, so an operational alert on this output should exclude known event dates.

**Highest single-order values** (score = value / cutoff; 688 orders above the cutoff)

| entity | location_id | period_start | metric | value | baseline_mean | score |
|---|---|---|---|---|---|---|
| ORD00034783 | LOC0018 | 2025-03-10 | total_amount | 70,922.41 |  | +2.6 |
| ORD00132920 | LOC0018 | 2025-09-13 | total_amount | 54,056.00 |  | +2.0 |
| ORD00142491 | LOC0003 | 2025-10-04 | total_amount | 50,554.00 |  | +1.8 |
| ORD00027963 | LOC0023 | 2025-02-23 | total_amount | 50,209.00 |  | +1.8 |
| ORD00168413 | LOC0025 | 2025-11-25 | total_amount | 50,082.50 |  | +1.8 |

**Duplicate-looking transactions**

None flagged.
<!-- /section:12 -->

<!-- section:13 -->
## 7. Slow-moving dishes and multi-location intelligence

Snapshot `as_of_date=2025-12-31`. An item is slow-moving when its Step 4 demand percentile is below 0.33, its repeat-purchase percentile is below 0.33, and its sales trend is flat or declining: a monthly slope at most 1% of its average monthly quantity. New items (Insufficient History) have no demand percentile and are never flagged.

6 slow-moving items:

| item_name | category | total_quantity_sold | demand_percentile | repeat_purchase_rate | relative_trend | average_rating | profit_percentage |
|---|---|---|---|---|---|---|---|
| Baked Chicken Penne | Hidden Opportunity | 1,793 | 0.03 | 0.063 | +0.8% | 3.82 | 66.0 |
| Calamari Rings | Low Performer | 1,924 | 0.06 | 0.055 | +1.0% | 3.85 | 57.8 |
| Loaded Nachos | Hidden Opportunity | 2,022 | 0.07 | 0.074 | +0.9% | 4.04 | 83.9 |
| Finger Fish | Low Performer | 2,187 | 0.08 | 0.087 | -3.0% | 3.61 | 62.7 |
| Classic Beef Burger | Low Performer | 2,513 | 0.12 | 0.094 | -2.8% | 3.88 | 61.2 |
| Kulfi | Hidden Opportunity | 3,934 | 0.26 | 0.082 | -1.7% | 3.77 | 69.0 |

**Location summary** (Step 3 location features; average rating from Ratings; category counts and mismatch share from Step 4's per-location classification, `parquet_data/menu_classification_by_location/`). Mismatch share is the fraction of items sold at a location whose category there differs from their chain-wide category.

| location_name | city | location_type | location_revenue | location_profit | location_avg_order_value | location_repeat_customer_rate | location_wastage_rate | avg_rating | Profit Driver | Low Performer | category_mismatch_share |
|---|---|---|---|---|---|---|---|---|---|---|---|
| DineIQ Karachi Dolmen Mall Clifton | Karachi | Mall | 78,497,203 | 44,903,769 | 6,728 | 26.3% | 2.0% | 3.35 | 11 | 49 | 15% |
| DineIQ Faisalabad Lyallpur Galleria | Faisalabad | Mall | 77,683,747 | 44,524,998 | 7,367 | 26.7% | 2.5% | 3.57 | 9 | 44 | 17% |
| DineIQ Quetta Jinnah Road | Quetta | Downtown | 70,423,225 | 39,638,029 | 6,745 | 26.3% | 1.9% | 3.67 | 13 | 51 | 14% |
| DineIQ Karachi Saddar | Karachi | Downtown | 70,069,749 | 39,547,071 | 6,562 | 25.9% | 1.8% | 3.71 | 11 | 48 | 18% |
| DineIQ Lahore MM Alam Road | Lahore | Downtown | 58,433,823 | 32,333,923 | 6,374 | 25.3% | 2.5% | 4.12 | 9 | 43 | 17% |
| DineIQ Karachi Gulshan-e-Iqbal | Karachi | Suburban | 55,079,497 | 31,058,151 | 6,525 | 25.0% | 2.1% | 3.56 | 11 | 49 | 16% |
| DineIQ Rawalpindi Bahria Town Phase 7 | Rawalpindi | Suburban | 54,704,621 | 30,200,381 | 6,258 | 25.6% | 2.2% | 4.06 | 12 | 44 | 19% |
| DineIQ Islamabad F-7 Markaz | Islamabad | Urban | 51,253,423 | 29,036,665 | 6,619 | 23.5% | 2.7% | 4.01 | 13 | 44 | 18% |
| DineIQ Karachi Clifton Block 5 | Karachi | Urban | 50,070,146 | 27,938,708 | 6,725 | 24.9% | 2.0% | 3.76 | 10 | 44 | 16% |
| DineIQ Lahore Gulberg III | Lahore | Urban | 49,968,615 | 28,122,982 | 6,625 | 23.7% | 2.6% | 3.71 | 15 | 47 | 15% |
| DineIQ Peshawar University Road | Peshawar | Urban | 49,557,799 | 28,110,189 | 6,845 | 25.0% | 2.4% | 3.63 | 10 | 44 | 20% |
| DineIQ Lahore Emporium Mall Johar Town | Lahore | Mall | 48,212,692 | 27,387,558 | 7,048 | 23.8% | 2.4% | 3.68 | 11 | 45 | 15% |
| DineIQ Rawalpindi Saddar | Rawalpindi | Downtown | 47,572,160 | 26,921,580 | 6,640 | 24.8% | 2.1% | 3.83 | 12 | 49 | 16% |
| DineIQ Hyderabad Auto Bhan Road | Hyderabad | Urban | 47,511,470 | 26,974,964 | 6,916 | 23.8% | 2.4% | 3.87 | 10 | 43 | 19% |
| DineIQ Karachi DHA Phase 6 | Karachi | Suburban | 45,922,692 | 25,732,361 | 6,614 | 24.8% | 2.4% | 3.53 | 12 | 44 | 19% |
| DineIQ Islamabad Centaurus Mall | Islamabad | Mall | 44,839,457 | 25,541,427 | 6,702 | 23.9% | 2.4% | 3.92 | 11 | 51 | 15% |
| DineIQ Faisalabad D Ground | Faisalabad | Urban | 42,190,092 | 24,044,569 | 6,435 | 23.3% | 2.5% | 3.36 | 9 | 45 | 21% |
| DineIQ Islamabad Blue Area | Islamabad | Downtown | 39,418,148 | 21,975,384 | 6,122 | 22.7% | 2.7% | 3.76 | 10 | 45 | 18% |
| DineIQ Multan Bosan Road | Multan | Urban | 37,750,345 | 21,247,474 | 6,539 | 23.4% | 2.6% | 3.47 | 13 | 53 | 15% |
| DineIQ M-1 Motorway Service Area | Nowshera | Highway | 33,817,291 | 19,187,679 | 7,395 | 22.6% | 3.3% | 3.57 | 10 | 46 | 19% |
| DineIQ Lahore Johar Town | Lahore | Suburban | 33,449,078 | 18,827,066 | 6,584 | 22.6% | 2.5% | 3.74 | 11 | 44 | 17% |
| DineIQ Super Highway Toll Plaza | Karachi | Highway | 32,281,506 | 18,234,582 | 7,413 | 21.8% | 2.7% | 3.88 | 13 | 46 | 22% |
| DineIQ Peshawar Hayatabad Phase 3 | Peshawar | Suburban | 31,569,462 | 17,833,809 | 6,576 | 22.2% | 2.9% | 3.30 | 12 | 45 | 19% |
| DineIQ Lahore DHA Phase 5 | Lahore | Suburban | 31,330,356 | 17,644,643 | 6,332 | 23.0% | 2.7% | 3.46 | 12 | 50 | 14% |
| DineIQ M-2 Motorway Service Area | Bhera | Highway | 18,823,769 | 10,754,930 | 7,251 | 21.1% | 3.9% | 3.43 | 11 | 40 | 16% |

Revenue spread between the busiest and quietest location: 4.2x. Highest wastage rate: DineIQ M-2 Motorway Service Area (3.9%). Lowest average rating: DineIQ Peshawar Hayatabad Phase 3 (3.30).
<!-- /section:13 -->

<!-- section:14 -->
## 8. Ordering channels and churn risk

Completed orders per channel, except cancellation rate, which uses all orders. Margin % is (line revenue - line cost) over net sales (subtotal - discount). Peak hours are 12:00-14:00 and 19:00-22:00, and the weekend is Fri-Sun, as in Step 3.

| order_channel | orders | order_share | avg_basket_size | avg_order_value | discount_order_share | discount_rate | margin_pct | cancellation_rate | peak_hour_share | weekend_share | busiest_hour |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Dine-in | 71,622 | 39.9% | 6.07 | 9,720 | 19.2% | 2.7% | 55.6% | 1.9% | 55.7% | 51.7% | 20 |
| App | 36,810 | 20.5% | 5.10 | 6,913 | 17.4% | 2.8% | 55.4% | 6.0% | 56.3% | 52.0% | 20 |
| Takeaway | 31,433 | 17.5% | 4.76 | 6,407 | 16.7% | 2.8% | 55.5% | 3.0% | 55.8% | 51.3% | 20 |
| ThirdPartyDelivery | 26,967 | 15.0% | 4.74 | 6,331 | 16.4% | 2.8% | 55.4% | 8.1% | 56.1% | 51.9% | 20 |
| Website | 12,492 | 7.0% | 5.05 | 6,867 | 17.4% | 2.8% | 55.3% | 6.1% | 55.4% | 52.0% | 20 |

Margin is within 0.3 points across channels, so no channel is meaningfully more profitable per sale; the differences are in basket size, order value and cancellations. Highest cancellation rate: ThirdPartyDelivery (8.1%). Largest baskets and order values: Dine-in.

### Churn risk

Rule-based, as of 2025-12-31. The average gap between a customer's orders (customers with 2+ orders) is 57.4 days. A customer is at risk when recency exceeds 115 days (the multiplier in config times that gap) and they placed fewer orders in the last 90 days than in the 90 days before.

7,893 of 49,606 customers (15.9%) are at risk.

At-risk customers by segment (module 07):

| segment | customers | at_risk | at_risk_share |
|---|---|---|---|
| Promotion-Driven | 8,870 | 2,094 | 23.6% |
| New | 10,872 | 2,492 | 22.9% |
| Frequent | 9,637 | 1,884 | 19.5% |
| Occasional | 5,282 | 1,023 | 19.4% |
| At-Risk | 8,443 | 313 | 3.7% |
| High-Value Loyal | 6,502 | 87 | 1.3% |

Customers who have already lapsed completely (no orders in either quarter) are not flagged, because the rule needs a decline between the two quarters. They show up in the At-Risk segment and in low recency scores instead.
<!-- /section:14 -->

<!-- section:99 -->
## Limitations

- Nothing from the Step 6 scope was cut. Every module is built and run.
- Segmentation labels come from ranking cluster means, not from ground truth. Fixed cutoffs (e.g. recency > 90 days) were tried first and labelled four of six clusters At-Risk, because most customers order only once or twice.
- Market-basket associations are weak: the highest lift is below 1.5, and 0.01 / 0.3 thresholds gave no rules at all, so they were lowered to 0.005 / 0.1. The strongest pairs (pakoras together, summer drinks together) are probably seasonal items bought in the same months rather than true product affinity.
- The wastage-risk label uses a fixed threshold (wasted / prepared > 10%) rather than a top quartile, because 95% of item-days have no waste.
- Price elasticities are before/after comparisons with a same-category control. They don't control for item-specific seasonality or promotions. Changes under 5% are not evaluated.
- Promotion windows can overlap other promotions and aren't seasonally adjusted. The wastage flag fires for most promotions because kitchens over-prepare for all of them.
- Anomaly detection is purely statistical. Holiday and promotion-launch spikes are flagged along with anything unusual, and there is no labelled set to measure precision against.
- Churn risk is a rule, not a model, and can't flag customers who had already stopped ordering before the previous quarter.
<!-- /section:99 -->
