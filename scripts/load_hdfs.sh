#!/bin/bash
# ============================================================
# Script : Charger les CSV dans HDFS
# Usage  : Exécuter depuis le container hive-server
#           docker exec -it hive-server bash scripts/load_hdfs.sh
# ============================================================

set -euo pipefail

DATA_DIR="/hive-tp/data"
HDFS_BASE="/data/hive_tp/raw"

echo "==> Création des répertoires HDFS..."
hdfs dfs -mkdir -p "${HDFS_BASE}/customers"
hdfs dfs -mkdir -p "${HDFS_BASE}/products"
hdfs dfs -mkdir -p "${HDFS_BASE}/orders"
hdfs dfs -mkdir -p "${HDFS_BASE}/orders_items"

echo "==> Chargement des CSV dans HDFS..."
hdfs dfs -put -f "${DATA_DIR}/customers.csv"    "${HDFS_BASE}/customers/"
hdfs dfs -put -f "${DATA_DIR}/products.csv"     "${HDFS_BASE}/products/"
hdfs dfs -put -f "${DATA_DIR}/orders.csv"       "${HDFS_BASE}/orders/"
hdfs dfs -put -f "${DATA_DIR}/orders_items.csv" "${HDFS_BASE}/orders_items/"

echo "==> Vérification des fichiers HDFS :"
hdfs dfs -ls "${HDFS_BASE}/"
echo ""
echo "[OK] Données chargées dans HDFS."
