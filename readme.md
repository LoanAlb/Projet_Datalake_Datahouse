# Plateforme Data Lakehouse — Vélib

## Contexte métier

**Problème** : Le rééquilibrage des stations Vélib (camions déplaçant les vélos) est coûteux et souvent réactif. Certaines stations se vident (gares le matin), d'autres débordent.

**Objectif** : Fournir un outil de pilotage à l'équipe logistique pour passer d'un rééquilibrage **réactif** à un rééquilibrage **anticipé**, en identifiant les stations critiques et les tendances horaires.

## Architecture

```
API Vélib (OpenData)
      │
      ▼
  ingest_velib.py
      │
      ├──► MinIO [velib-raw]       ← JSON brut
      ├──► MinIO [velib-staging]   ← données nettoyées
      └──► MinIO [velib-curated]   ← données aplaties
                │
                ▼
        load_postgres.py
                │
                ▼
          PostgreSQL (velib_db)
          ├── stations
          ├── station_status (historisé)
          └── vues analytiques (KPIs)
                │
                ▼
        ┌───────┴───────┐
        │               │
    Metabase         n8n
   (dashboard)   (automatisation)
                        │
                        ▼
                    Telegram
                  (alertes + bot)
```

## Stack technique

| Service    | Port      | Usage                        |
|------------|-----------|------------------------------|
| PostgreSQL | 5432      | Base structurée + analytique |
| MinIO      | 9000/9001 | Data Lake (raw/staging/curated) |
| n8n        | 5678      | Orchestration + Telegram     |
| Metabase   | 3000      | Dashboard                    |
| ngrok      | —         | Webhook HTTPS pour Telegram  |

## Lancement

```bash
# 1. Démarrer l'infra
docker-compose up -d

# 2. Installer les dépendances Python
pip install -r requirements.txt

# 3. Ingestion API → MinIO (3 zones)
python scripts/ingest_velib.py

# 4. Chargement MinIO curated → PostgreSQL
python scripts/load_postgres.py

# 5. Lancer ngrok pour les webhooks Telegram
ngrok http 5678
```

## Organisation MinIO

| Bucket          | Contenu                                    |
|-----------------|--------------------------------------------|
| `velib-raw`     | JSON brut de l'API (station_information + station_status) |
| `velib-staging` | Données nettoyées (champs utiles extraits) |
| `velib-curated` | Données jointes et aplaties, prêtes pour PostgreSQL |

Structure : `YYYY/MM/DD/<fichier>_<timestamp>.json`

## KPIs

| KPI | Vue SQL | Justification métier |
|-----|---------|---------------------|
| Taux d'occupation par station | `v_station_occupancy` | Identifier les stations vides/pleines |
| Stations critiques (<10% / >90%) | `v_critical_stations` | Déclencher les interventions terrain |
| Tendance horaire | `v_hourly_trend` | Anticiper les flux matin/soir |
| Durée en état critique | `v_critical_duration` | Prioriser les stations problématiques |
| Ratio mécanique/électrique | `v_bike_type_ratio` | Adapter la flotte |
| Résumé global | `v_global_summary` | Vue d'ensemble pour Telegram |

## Dashboard Metabase

Accessible sur `http://localhost:3000`

| Carte | Visualisation | Source |
|-------|--------------|--------|
| Résumé global (chiffres clés) | Numérique | `v_global_summary` |
| Stations critiques | Tableau | `v_critical_stations` |
| Tendance horaire | Courbe | `v_hourly_trend` |
| Ratio mécanique/électrique | Histogramme empilé | `v_bike_type_ratio` |
| Stations en état critique | Histogramme | `v_critical_duration` |
| Carte géographique | Carte (pin map) | `v_station_occupancy` |

## Workflows n8n

| Workflow | Déclencheur | Action |
|----------|------------|--------|
| Alerte Stations Critiques | Cron toutes les 5 min | Query `v_critical_stations` → alerte Telegram |
| Bot Telegram Vélib | Webhook Telegram | Répond aux commandes `/kpi`, `/top_critiques`, `/station` |
| Rapport Quotidien | Cron chaque matin 8h | Résumé global → message Telegram |

Les workflows sont importables depuis le dossier `n8n-workflows/`.

## Commandes Telegram

| Commande | Action |
|----------|--------|
| `/kpi` | Résumé global du jour |
| `/top_critiques` | Top 10 stations nécessitant intervention |
| `/station <nom>` | État temps réel d'une station |

## Connexion PostgreSQL

```
Host: localhost (ou velib_postgres depuis les conteneurs)
Port: 5432
Database: velib_db
User: velib
Password: velib
```

## Connexion MinIO

```
URL: http://localhost:9001
User: minioadmin
Password: minioadmin
```

## Connexion Metabase

```
URL: http://localhost:3000
Email: admin@admin.admin
Mot de passe: Admin123!
```
