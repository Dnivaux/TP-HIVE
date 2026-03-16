-- ============================================================
-- PARTIE A — Ingestion brute en tables EXTERNAL (raw)
-- ============================================================

-- A1) Créer la base hive_tp
CREATE DATABASE IF NOT EXISTS hive_tp
LOCATION '/data/hive_tp';

USE hive_tp;

-- A2) Créer les tables EXTERNAL pointant vers les CSV dans HDFS

-- Table customers_raw
CREATE EXTERNAL TABLE IF NOT EXISTS customers_raw (
    customer_id INT,
    full_name   STRING,
    city        STRING,
    country     STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/hive_tp/raw/customers/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Table products_raw
CREATE EXTERNAL TABLE IF NOT EXISTS products_raw (
    product_id  INT,
    category    STRING,
    brand       STRING,
    unit_price  DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/hive_tp/raw/products/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Table orders_raw
CREATE EXTERNAL TABLE IF NOT EXISTS orders_raw (
    order_id    INT,
    customer_id INT,
    order_ts    STRING,
    status      STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/hive_tp/raw/orders/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Table orders_items_raw
CREATE EXTERNAL TABLE IF NOT EXISTS orders_items_raw (
    order_id   INT,
    product_id INT,
    quantity   INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/hive_tp/raw/orders_items/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ============================================================
-- Vérifications
-- ============================================================
SELECT COUNT(*) FROM customers_raw;
SELECT * FROM orders_raw LIMIT 10;
