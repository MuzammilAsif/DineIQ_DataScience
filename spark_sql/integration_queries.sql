-- Integration joins for Step 2 (SRS Step 6 — Data Integration).
-- Run against the cleaned tables loaded from processed_data/ and registered as temp
-- views with these names: orders, order_items, customers, restaurants, promotions,
-- menu_items, menu_categories, pricing_history, ratings, inventory, wastage.
-- Parsed and executed by spark_jobs/03_integrate_and_store.py (split on "-- @name:").

-- @name: orders_customers
SELECT o.*, c.age_group, c.gender, c.loyalty_member, c.signup_date, c.home_location_id
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;

-- @name: orders_order_items
SELECT oi.*, o.order_datetime, o.order_status, o.customer_id, o.location_id
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id;

-- @name: order_items_menu_items
SELECT oi.*, mi.item_name, mi.category_id, mi.is_active, mi.is_seasonal, mi.season
FROM order_items oi
JOIN menu_items mi ON oi.item_id = mi.item_id;

-- @name: menu_items_categories
SELECT mi.*, mc.category_name, mc.description AS category_description
FROM menu_items mi
JOIN menu_categories mc ON mi.category_id = mc.category_id;

-- @name: orders_restaurants
SELECT o.*, r.name AS location_name, r.city, r.region, r.location_type
FROM orders o
JOIN restaurants r ON o.location_id = r.location_id;

-- @name: orders_promotions
SELECT o.*, p.promotion_name, p.promotion_type, p.discount_value
FROM orders o
JOIN promotions p ON o.promotion_id = p.promotion_id;

-- @name: menu_items_pricing_history
SELECT mi.item_id, mi.item_name, ph.location_id, ph.price, ph.cost,
       ph.effective_start_date, ph.effective_end_date, ph.change_reason
FROM menu_items mi
JOIN pricing_history ph ON mi.item_id = ph.item_id;

-- @name: menu_items_ratings
SELECT mi.item_id, mi.item_name, r.rating_id, r.rating_value, r.rating_date, r.location_id
FROM menu_items mi
JOIN ratings r ON mi.item_id = r.item_id;

-- @name: menu_items_inventory
SELECT mi.item_id, mi.item_name, inv.location_id, inv.date, inv.opening_stock,
       inv.received_stock, inv.prepared_quantity, inv.consumed_stock, inv.closing_stock, inv.unit
FROM menu_items mi
JOIN inventory inv ON mi.item_id = inv.item_id;

-- @name: menu_items_wastage
SELECT mi.item_id, mi.item_name, w.location_id, w.date, w.quantity_wasted, w.unit,
       w.cost_of_waste, w.reason
FROM menu_items mi
JOIN wastage w ON mi.item_id = w.item_id;

-- @name: fact_order_line
-- Base fact table: order-line grain (one row per Order_Items line), left-joined
-- outward to its order, customer, location, menu item and category. Left join from
-- order_items so the fact row count always equals the cleaned Order_Items count.
SELECT
    oi.order_item_id,
    oi.order_id,
    oi.item_id,
    oi.quantity,
    oi.unit_price,
    oi.unit_cost,
    oi.discount_amount AS line_discount_amount,
    oi.line_total,
    o.order_datetime,
    YEAR(o.order_datetime)  AS order_year,
    MONTH(o.order_datetime) AS order_month,
    o.order_channel,
    o.order_status,
    o.payment_method,
    o.promotion_id,
    o.customer_id,
    c.age_group,
    c.gender,
    c.loyalty_member,
    o.location_id,
    r.name   AS location_name,
    r.city,
    r.region,
    r.location_type,
    mi.item_name,
    mi.category_id,
    mc.category_name
FROM order_items oi
LEFT JOIN orders o           ON oi.order_id = o.order_id
LEFT JOIN customers c        ON o.customer_id = c.customer_id
LEFT JOIN restaurants r      ON o.location_id = r.location_id
LEFT JOIN menu_items mi      ON oi.item_id = mi.item_id
LEFT JOIN menu_categories mc ON mi.category_id = mc.category_id;
