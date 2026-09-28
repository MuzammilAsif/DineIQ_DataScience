# Data Quality Rules

Cleaning rules applied by `spark_jobs/02_clean.py`. Written before implementation, per the SRS. Every corrected or removed row is logged to `reports/cleaning_log.csv` (table, row id, issue, action taken).

| Issue | Action |
|---|---|
| Missing value in a non-critical field (e.g. `review_text`, `home_location_id`) | Keep as null, no action |
| Missing `customer_id` on an Order | Quarantine the row |
| Missing `item_id` on an Order_Item | Quarantine the row |
| Missing `item_id` on a Wastage row | Quarantine the row (same reasoning as Order_Items: the row is meaningless without it) |
| Missing `rating_value` on a Rating | Quarantine the row (the row carries no information without it) |
| Duplicate Orders (exact duplicate `order_id`) | Drop duplicates, keep first, log count |
| Duplicate Order_Items (exact duplicate `order_id` + `item_id` row) | Drop duplicates, keep first, log count |
| Invalid menu price (<= 0) | If a valid price exists in Pricing_History for that item/date, correct it and log as corrected; otherwise quarantine |
| Negative quantity | Quarantine (don't guess the sign) |
| Invalid date (outside dataset's valid range 2021-01-01 to 2025-12-31, or malformed) | Quarantine |
| Invalid rating (outside 1-5) | Quarantine |
| Invalid restaurant/location reference (orphan FK) | Quarantine |
| Impossible wastage quantity (`quantity_wasted` > that day's `prepared_quantity - consumed_stock`) | Quarantine, log the magnitude of the violation |
| Incorrect discount (`discount_amount` > `subtotal`) | Quarantine (don't silently cap it) |
| Cancelled transaction | Keep, tag `order_status = Cancelled` (already the raw value), exclude from revenue/demand aggregates downstream, include in cancellation-rate metrics |
| Inconsistent unit string (e.g. `kgs` vs `kg`) | Normalize via a fixed mapping table, log the normalization — this is a fix, not a quarantine |
| Orphan FK not covered above (e.g. `Order_Items.order_id` -> Orders, `Menu_Items.category_id` -> Menu_Categories, `Ratings.*`, `Inventory.*`, `Pricing_History.*`) | Quarantine the child row |

## Notes

- A row carries at most one injected problem (per the Step 1 generator), but cleaning still checks all rules independently and applies the first rule that matches, in the table order above, so a row is never double-quarantined under two reasons.
- Quarantined rows are written to `processed_data/quarantine/<table>.csv` with an added `quarantine_reason` column. They are never silently dropped.
- Unit normalization mapping (Inventory.unit, Wastage.unit):

  | Raw value | Normalized |
  |---|---|
  | pcs, pieces, piece, pc, Pcs, PCS | pcs |
  | kg, kgs, KG, Kg, kilogram, kilograms | kg |
  | liters, liter, litre, litres, ltr, L, l, Liters | liters |

- "Valid date range" for a date/timestamp column is the dataset's transaction period, 2025-01-01 to 2025-12-31, except `Customers.signup_date` which is valid from 2021-01-01 to 2025-12-31 (per the data dictionary).
