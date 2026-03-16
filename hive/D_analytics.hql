-- ============================================================
-- PARTIE D — Requêtes analytiques
-- ============================================================

USE hive_tp;

-- ============================================================
-- D1) Chiffre d'affaires par jour
-- ============================================================
SELECT
    o.order_date,
    ROUND(SUM(oi.quantity * p.unit_price), 2) AS ca_jour
FROM orders_accurated     o
JOIN orders_items_accurated oi ON o.order_id    = oi.order_id
JOIN products_accurated     p  ON oi.product_id = p.product_id
WHERE o.status = 'PAID'
GROUP BY o.order_date
ORDER BY o.order_date;

-- ============================================================
-- D2) Top 5 clients par chiffre d'affaires
-- ============================================================
SELECT
    c.customer_id,
    c.full_name,
    c.city,
    c.country,
    ROUND(SUM(oi.quantity * p.unit_price), 2) AS ca_total
FROM orders_accurated       o
JOIN customers_accurated    c  ON o.customer_id = c.customer_id
JOIN orders_items_accurated oi ON o.order_id    = oi.order_id
JOIN products_accurated     p  ON oi.product_id = p.product_id
WHERE o.status = 'PAID'
GROUP BY c.customer_id, c.full_name, c.city, c.country
ORDER BY ca_total DESC
LIMIT 5;

-- ============================================================
-- D3) Fenêtres : running total (cumul du CA) par jour
-- ============================================================
SELECT
    order_date,
    ca_jour,
    ROUND(
        SUM(ca_jour) OVER (
            ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2
    ) AS ca_cumule
FROM (
    SELECT
        o.order_date,
        ROUND(SUM(oi.quantity * p.unit_price), 2) AS ca_jour
    FROM orders_accurated       o
    JOIN orders_items_accurated oi ON o.order_id    = oi.order_id
    JOIN products_accurated     p  ON oi.product_id = p.product_id
    WHERE o.status = 'PAID'
    GROUP BY o.order_date
) daily_ca
ORDER BY order_date;

-- ============================================================
-- D4) Ranking : top catégories par mois
-- ============================================================
SELECT
    mois,
    category,
    ca_mensuel,
    RANK() OVER (PARTITION BY mois ORDER BY ca_mensuel DESC) AS rang
FROM (
    SELECT
        SUBSTR(o.order_date, 1, 7)      AS mois,
        p.category,
        ROUND(SUM(oi.quantity * p.unit_price), 2) AS ca_mensuel
    FROM orders_accurated       o
    JOIN orders_items_accurated oi ON o.order_id    = oi.order_id
    JOIN products_accurated     p  ON oi.product_id = p.product_id
    WHERE o.status = 'PAID'
    GROUP BY SUBSTR(o.order_date, 1, 7), p.category
) monthly_cat
ORDER BY mois, rang;
