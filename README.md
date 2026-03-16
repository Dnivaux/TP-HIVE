# TP HIVE — EFREI DataScience

TP d'initiation à Apache Hive sur cluster Hadoop/Docker.
Basé sur l'image Docker : [Marcel-Jan/docker-hadoop-spark](https://github.com/Marcel-Jan/docker-hadoop-spark)

---

## Structure du projet

```
.
├── data/                       # Données source (CSV)
│   ├── customers.csv
│   ├── orders.csv
│   ├── orders_items.csv
│   └── products.csv
├── hive/                       # Scripts HiveQL
│   ├── A_raw_tables.hql        # Partie A : tables EXTERNAL (raw)
│   ├── B_curated_tables.hql    # Partie B : tables curated Parquet
│   ├── C_partitioned_table.hql # Partie C : table partitionnée par jour
│   └── D_analytics.hql         # Partie D : requêtes analytiques
├── scripts/
│   ├── load_hdfs.sh            # Chargement des CSV dans HDFS
│   └── run_all.sh              # Exécution complète du TP
├── docker-compose.yml          # Cluster Hadoop + Hive
├── hadoop-hive.env             # Variables d'environnement
└── README.md
```

---

## Schéma des données

| Table            | Colonnes |
|------------------|----------|
| `customers.csv`  | customer_id, full_name, city, country |
| `products.csv`   | product_id, category, brand, unit_price |
| `orders.csv`     | order_id, customer_id, order_ts, status |
| `orders_items.csv` | order_id, product_id, quantity |

---

## Démarrage rapide

### 1. Démarrer le cluster

```bash
docker-compose up -d
```

Attendre ~30 secondes que tous les services soient prêts.

Vérifier que le cluster est actif : http://localhost:9870 (NameNode UI)

### 2. Démarrer le serveur Hive

```bash
docker exec -d hive-server hiveserver2
```

Attendre ~10 secondes que HiveServer2 soit prêt.

### 3. Exécuter le TP complet

```bash
docker exec -it hive-server bash /hive-tp/scripts/run_all.sh
```

Ce script :
1. Charge les CSV dans HDFS
2. Exécute les parties A, B, C et D dans l'ordre

---

## Exécution manuelle étape par étape

### Étape 0 — Charger les CSV dans HDFS

```bash
docker exec -it hive-server bash /hive-tp/scripts/load_hdfs.sh
```

### Étape A — Tables EXTERNAL (raw)

```bash
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -n root \
  -f /hive-tp/hive/A_raw_tables.hql
```

### Étape B — Tables curated (Parquet)

```bash
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -n root \
  -f /hive-tp/hive/B_curated_tables.hql
```

### Étape C — Table partitionnée par jour

```bash
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -n root \
  -f /hive-tp/hive/C_partitioned_table.hql
```

### Étape D — Requêtes analytiques

```bash
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -n root \
  -f /hive-tp/hive/D_analytics.hql
```

---

## Détail des parties

### Partie A — Ingestion brute (tables EXTERNAL)

Création de la base `hive_tp` et des tables EXTERNAL pointant vers les CSV dans HDFS :

| Table HiveQL        | Fichier source        | Chemin HDFS                           |
|---------------------|-----------------------|---------------------------------------|
| `customers_raw`     | customers.csv         | /data/hive_tp/raw/customers/          |
| `products_raw`      | products.csv          | /data/hive_tp/raw/products/           |
| `orders_raw`        | orders.csv            | /data/hive_tp/raw/orders/             |
| `orders_items_raw`  | orders_items.csv      | /data/hive_tp/raw/orders_items/       |

### Partie B — Tables curated (Parquet)

Nettoyage et conversion en Parquet (suffixe `_accurated`) :

- `customers_accurated` — filtre les lignes avec `customer_id` nul
- `products_accurated` — filtre les prix nuls ou négatifs
- `orders_accurated` — ajoute la colonne `order_date` (date extraite du timestamp)
- `orders_items_accurated` — filtre les quantités invalides

### Partie C — Partitionnement

Table `orders_partitioned` partitionnée par `order_date` (format `YYYY-MM-DD`), alimentée depuis `orders_accurated`.

```sql
SHOW PARTITIONS orders_partitioned;
```

### Partie D — Requêtes analytiques

| Requête | Description |
|---------|-------------|
| **D1** | Chiffre d'affaires par jour |
| **D2** | Top 5 clients par CA total (commandes PAID) |
| **D3** | Running total (cumul du CA) par jour via window function |
| **D4** | Ranking des catégories par mois via RANK() OVER (PARTITION BY mois) |

---

## Arrêter le cluster

```bash
docker-compose down
```

Pour supprimer aussi les volumes (données HDFS et metastore) :

```bash
docker-compose down -v
```
