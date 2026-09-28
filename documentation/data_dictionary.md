# DineIQ Analytics — Data Dictionary

Raw dataset produced by `data_generator/generate_dataset.py` into `raw_data/` (one CSV per table, 13 tables).

## Conventions

| Item | Convention |
|---|---|
| Business | Casual-dining chain with 25 locations across Pakistan |
| Currency | PKR. All prices, costs, discounts, taxes and totals are in PKR with 2 decimals |
| Transaction period | 2025-01-01 to 2025-12-31 (12 months) |
| Dates | `YYYY-MM-DD`; timestamps `YYYY-MM-DD HH:MM:SS` (local time) |
| Booleans | `True` / `False` |
| Nulls | Empty CSV field |
| IDs | Prefix + zero-padded number, e.g. `LOC0001`, `ITEM0001`, `ORD00000001` |

The raw CSVs contain deliberately injected data-quality problems (see the last section). The column rules below describe the valid data. Any row that breaks them is either an intended edge case described here, or an injected error recorded in `reports/dirty_injection_log.csv`.

## Relationships

```
Menu_Categories 1─* Menu_Items 1─* Pricing_History *─0..1 Restaurants
Restaurants 1─* Orders *─1 Customers *─0..1 Restaurants (home location)
Orders 1─* Order_Items *─1 Menu_Items
Orders *─0..1 Promotions 1─* Promotion_Items *─1 Menu_Items
                         1─* Promotion_Locations *─1 Restaurants
Ratings: *─1 Customers, *─1 Restaurants, *─0..1 Menu_Items, *─1 Orders
Inventory / Wastage: *─1 Menu_Items, *─1 Restaurants (daily grain)
```

## Restaurants

One row per restaurant location.

| Column | Type | Nullable | Description |
|---|---|---|---|
| location_id | string, PK | no | `LOC0001`–`LOC0025` |
| name | string | no | Branch name |
| city | string | no | City |
| region | string | no | Province / territory: Sindh, Punjab, Islamabad Capital Territory, Khyber Pakhtunkhwa, Balochistan |
| address | string | no | Street address |
| location_type | enum | no | Urban / Suburban / Mall / Highway / Downtown |
| seating_capacity | int | no | Number of seats |
| opening_date | date | no | All locations opened before the transaction period |
| latitude | float | no | WGS84 |
| longitude | float | no | WGS84 |

## Menu_Categories

| Column | Type | Nullable | Description |
|---|---|---|---|
| category_id | string, PK | no | `CAT01`–`CAT14` |
| category_name | string | no | e.g. Appetizers, BBQ & Grills, Desserts, Cold Beverages & Shakes |
| description | string | no | Short description |

## Menu_Items

| Column | Type | Nullable | Description |
|---|---|---|---|
| item_id | string, PK | no | `ITEM0001`–`ITEM0160` |
| item_name | string | no | Unique item name |
| category_id | string, FK → Menu_Categories | no | |
| base_price | decimal | no | Current reference price: the latest chain-wide price in Pricing_History |
| base_cost | decimal | no | Current reference cost per portion |
| description | string | no | |
| is_active | bool | no | `False` for the 4 items discontinued during 2025 |
| launch_date | date | no | 10 items launched during 2025. The 6 launched from October onward have too little history for forecasting |
| is_seasonal | bool | no | |
| season | enum | no | Summer / Winter / Monsoon / AllYear (`AllYear` when `is_seasonal` is False) |

## Pricing_History

Price and cost of an item over time. A row with no `location_id` is the chain-wide price. A row with a `location_id` overrides the chain-wide price at that location for its date range.

| Column | Type | Nullable | Description |
|---|---|---|---|
| price_id | string, PK | no | |
| item_id | string, FK → Menu_Items | no | |
| location_id | string, FK → Restaurants | yes | Null = applies to all locations |
| price | decimal | no | Menu price per portion |
| cost | decimal | no | Cost per portion |
| effective_start_date | date | no | First day the price applies |
| effective_end_date | date | yes | Last day the price applies; null = still current |
| change_reason | string | no | menu revision, inflation, seasonal, menu repricing, competitive pricing, promotion-adjustment, introductory price, introductory offer ended, location premium |

For any item and location, the date ranges of the chain-wide rows do not overlap, and neither do the ranges of that location's override rows. Every item has at least 2 chain-wide price points.

## Customers

Customers are anonymized, so there are no names or contact details.

| Column | Type | Nullable | Description |
|---|---|---|---|
| customer_id | string, PK | no | `CUST0000001` onward |
| signup_date | date | no | Between 2021 and 2025-12-20 |
| age_group | enum | no | 18-24 / 25-34 / 35-44 / 45-54 / 55+ |
| gender | enum | no | Male / Female / Undisclosed |
| home_location_id | string, FK → Restaurants | yes | Null when the customer has no usual branch |
| preferred_channel | enum | no | Same values as `Orders.order_channel` |
| loyalty_member | bool | no | |

Some customers signed up but never ordered (about 7%).

## Promotions

| Column | Type | Nullable | Description |
|---|---|---|---|
| promotion_id | string, PK | no | `PROMO001`–`PROMO020` |
| promotion_name | string | no | |
| promotion_type | enum | no | PercentOff / FixedAmountOff / BOGO / ComboDeal |
| discount_value | decimal | no | PercentOff and ComboDeal: percent off eligible lines. FixedAmountOff: PKR off the eligible items in the order. BOGO: always 1 (one free unit per unit bought) |
| start_date | date | no | |
| end_date | date | no | Inclusive |
| min_order_value | decimal | yes | Minimum order subtotal in PKR; null = no minimum |

How the discount is applied:
- **PercentOff:** `discount_value`% off every eligible line.
- **FixedAmountOff:** `min(discount_value, eligible subtotal)`, split across the eligible lines in proportion to their gross amounts.
- **BOGO:** `floor(quantity / 2)` units free on each eligible line.
- **ComboDeal:** `discount_value`% off eligible lines, only when the order has at least 2 eligible lines.

## Promotion_Items (bridge)

| Column | Type | Nullable | Description |
|---|---|---|---|
| promotion_id | string, FK → Promotions | no | |
| item_id | string, FK → Menu_Items | no | Item eligible for the promotion |

## Promotion_Locations (bridge)

| Column | Type | Nullable | Description |
|---|---|---|---|
| promotion_id | string, FK → Promotions | no | |
| location_id | string, FK → Restaurants | no | Location where the promotion runs |

## Orders

| Column | Type | Nullable | Description |
|---|---|---|---|
| order_id | string, PK | no | Assigned in chronological order |
| customer_id | string, FK → Customers | no | |
| location_id | string, FK → Restaurants | no | |
| order_datetime | timestamp | no | |
| order_channel | enum | no | Dine-in / Takeaway / Website / App / ThirdPartyDelivery |
| order_status | enum | no | Completed / Cancelled (about 4% cancelled, higher on delivery channels) |
| promotion_id | string, FK → Promotions | yes | Set only when the promotion actually reduced the order |
| subtotal | decimal | no | Sum of `quantity × unit_price` over the order's lines |
| discount_amount | decimal | no | Sum of line discounts |
| tax_amount | decimal | no | Provincial sales tax (15–16%) on `subtotal − discount_amount` |
| total_amount | decimal | no | `subtotal − discount_amount + tax_amount` |
| payment_method | enum | no | Cash / Card / Wallet / Online |

Cancelled orders keep their lines and amounts. They don't count toward inventory consumption and never get ratings.

## Order_Items

| Column | Type | Nullable | Description |
|---|---|---|---|
| order_item_id | string, PK | no | |
| order_id | string, FK → Orders | no | |
| item_id | string, FK → Menu_Items | no | Each item appears at most once per order |
| quantity | int | no | ≥ 1 |
| unit_price | decimal | no | Price in effect on the order date at that location (snapshot from Pricing_History) |
| unit_cost | decimal | no | Cost in effect on the order date (snapshot) |
| discount_amount | decimal | no | Promotion discount on this line |
| line_total | decimal | no | `quantity × unit_price − discount_amount` |

## Ratings

| Column | Type | Nullable | Description |
|---|---|---|---|
| rating_id | string, PK | no | |
| customer_id | string, FK → Customers | no | Customer who placed the rated order |
| item_id | string, FK → Menu_Items | yes | Null = overall rating of the location visit (about 15%) |
| location_id | string, FK → Restaurants | no | |
| order_id | string, FK → Orders | no | Always a completed order. An item rating always refers to an item in that order |
| rating_value | int | no | 1–5 |
| rating_date | date | no | On or up to a few days after the order date |
| review_text | string | yes | Short optional comment (about 35% of ratings) |

## Inventory

Daily snapshot per item per location. There is a row for every day an item is on the menu at a location. It starts at the item's launch date and ends at its discontinuation date. Each location carries about 90% of the menu.

| Column | Type | Nullable | Description |
|---|---|---|---|
| inventory_id | string, PK | no | |
| item_id | string, FK → Menu_Items | no | |
| location_id | string, FK → Restaurants | no | |
| date | date | no | |
| opening_stock | decimal | no | Equals the previous day's `closing_stock` |
| received_stock | decimal | no | Deliveries received that day |
| prepared_quantity | decimal | no | Quantity prepared for sale. Always ≥ `consumed_stock` |
| consumed_stock | decimal | no | Quantity sold in completed orders |
| closing_stock | decimal | no | `opening + received − consumed − wasted`, where wasted is the matching Wastage row (0 if there is none) |
| unit | string | no | pcs / kg / liters. Fixed per item, set by its category |

The units per portion are: pcs = 1, BBQ & Grills 0.30 kg, Karahi & Handi 0.50 kg, Rice & Biryani 0.40 kg, Seafood 0.35 kg, Soups 0.30 L, Hot Beverages 0.25 L, Cold Beverages & Shakes 0.35 L.

## Wastage

At most one row per item, location and date, only on days with waste.

| Column | Type | Nullable | Description |
|---|---|---|---|
| wastage_id | string, PK | no | |
| item_id | string, FK → Menu_Items | no | |
| location_id | string, FK → Restaurants | no | |
| date | date | no | |
| quantity_wasted | decimal | no | Never more than that day's `prepared_quantity − consumed_stock` in Inventory |
| unit | string | no | Same unit as the item's Inventory rows |
| cost_of_waste | decimal | no | `quantity_wasted / portion size × unit cost in effect that day` |
| reason | enum | no | Overproduction / Expired / Spoilage / CustomerReturn / PrepError |

## Injected data-quality issues (raw_data only)

The generator injects errors at low rates. Each rule uses its own rows within a table, so a row carries at most one injected problem. Every changed row is listed in `reports/dirty_injection_log.csv`: table, rule, primary key, column, original value, injected value. Per-rule counts are in `reports/dirty_injection_summary.csv`.

| Rule | Tables / columns |
|---|---|
| missing_value | Orders.customer_id, Order_Items.item_id, Ratings.rating_value, Wastage.item_id |
| duplicate_row | Orders, Order_Items (exact copies, adjacent to the original) |
| invalid_price (zero or negative) | Menu_Items.base_price, Pricing_History.price, Order_Items.unit_price |
| negative_quantity | Order_Items.quantity |
| invalid_date (out of range or malformed) | Customers.signup_date, Orders.order_datetime, Ratings.rating_date, Wastage.date |
| rating_out_of_range | Ratings.rating_value |
| orphan_foreign_key | Customers.home_location_id, Orders.customer_id, Orders.location_id, Order_Items.order_id, Order_Items.item_id, Ratings.location_id, Ratings.customer_id, Inventory.item_id, Wastage.location_id |
| wastage_exceeds_prepared | Wastage.quantity_wasted |
| discount_exceeds_subtotal | Orders.discount_amount |
| inconsistent_unit | Inventory.unit, Wastage.unit (e.g. `kgs`, `KG`, `ltr`, `pieces`) |

Cancelled orders are real business data, not an injected error.

Because the corruption is cell-level, the other columns on a corrupted row still hold their original values. For example, an Order_Items row with a negative quantity keeps its original `line_total`, and an Orders row with an inflated discount keeps its original `total_amount`. Checks that compare related columns will therefore flag those rows as well.
