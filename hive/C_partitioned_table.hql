-- ============================================================
-- PARTIE C — Partitionnement par jour
-- Table orders_partitioned partitionnée sur order_date
-- Alimentée depuis orders_accurated
-- ============================================================

USE hive_tp;

-- Activer le partitionnement dynamique
SET hive.exec.dynamic.partition        = true;
SET hive.exec.dynamic.partition.mode   = nonstrict;
SET hive.exec.max.dynamic.partitions   = 1000;

-- Créer la table partitionnée
CREATE TABLE IF NOT EXISTS orders_partitioned (
    order_id    INT,
    customer_id INT,
    order_ts    STRING,
    status      STRING
)
PARTITIONED BY (order_date STRING)
STORED AS PARQUET;

-- Charger la donnée depuis orders_accurated
INSERT INTO TABLE orders_partitioned
PARTITION (order_date)
SELECT
    order_id,
    customer_id,
    order_ts,
    status,
    order_date
FROM orders_accurated;

-- ============================================================
-- Lister les partitions
-- ============================================================
SHOW PARTITIONS orders_partitioned;

-- Vérifier une partition spécifique
SELECT * FROM orders_partitioned
WHERE order_date = '2025-01-10';
