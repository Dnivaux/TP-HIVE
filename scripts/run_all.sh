#!/bin/bash
# ============================================================
# Script : Exécuter le TP HIVE complet
# Usage  : Exécuter depuis le container hive-server APRÈS avoir
#           lancé le cluster avec docker-compose up -d
#
#  Sur la machine hôte :
#    docker exec -it hive-server bash /hive-tp/scripts/run_all.sh
# ============================================================

set -euo pipefail

HQL_DIR="/hive-tp/hive"
BEELINE="beeline -u jdbc:hive2://localhost:10000 -n root --silent=true"

run_hql() {
    local step=$1
    local file=$2
    echo ""
    echo "=========================================="
    echo " ${step}"
    echo "=========================================="
    ${BEELINE} -f "${file}"
}

# Charger les données dans HDFS
echo "==> Chargement des CSVs dans HDFS..."
bash /hive-tp/scripts/load_hdfs.sh

# Exécuter les parties dans l'ordre
run_hql "Partie A — Tables EXTERNAL (raw)" "${HQL_DIR}/A_raw_tables.hql"
run_hql "Partie B — Tables curated (Parquet)" "${HQL_DIR}/B_curated_tables.hql"
run_hql "Partie C — Table partitionnée" "${HQL_DIR}/C_partitioned_table.hql"
run_hql "Partie D — Requêtes analytiques" "${HQL_DIR}/D_analytics.hql"

echo ""
echo "[OK] TP HIVE terminé avec succès."
