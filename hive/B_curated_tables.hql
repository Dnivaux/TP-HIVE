-- ============================================================
-- PARTIE B — Curated : tables nettoyées au format Parquet
-- Suffixe _accurated sur chaque table
-- ============================================================

USE hive_tp;

-- Table customers_accurated (Parquet)
CREATE TABLE IF NOT EXISTS customers_accurated
STORED AS PARQUET AS
SELECT
    customer_id,
    full_name,
    city,
    country
FROM customers_raw
WHERE customer_id IS NOT NULL
  AND full_name IS NOT NULL;

-- Table products_accurated (Parquet)
CREATE TABLE IF NOT EXISTS products_accurated
STORED AS PARQUET AS
SELECT
    product_id,
    category,
    brand,
    unit_price
FROM products_raw
WHERE product_id IS NOT NULL
  AND unit_price IS NOT NULL
  AND unit_price > 0;

-- Table orders_accurated (Parquet)
-- On calcule order_date à partir du timestamp pour faciliter les jointures
CREATE TABLE IF NOT EXISTS orders_accurated
STORED AS PARQUET AS
SELECT
    order_id,
    customer_id,
    order_ts,
    TO_DATE(order_ts) AS order_date,
    status
FROM orders_raw
WHERE order_id IS NOT NULL
  AND customer_id IS NOT NULL;

-- Table orders_items_accurated (Parquet)
CREATE TABLE IF NOT EXISTS orders_items_accurated
STORED AS PARQUET AS
SELECT
    order_id,
    product_id,
    quantity
FROM orders_items_raw
WHERE order_id IS NOT NULL
  AND product_id IS NOT NULL
  AND quantity > 0;

-- ============================================================
-- Vérifications
-- ============================================================
SELECT COUNT(*) FROM customers_accurated;
SELECT COUNT(*) FROM products_accurated;
SELECT COUNT(*) FROM orders_accurated;
SELECT COUNT(*) FROM orders_items_accurated;

SELECT * FROM orders_accurated LIMIT 10;
