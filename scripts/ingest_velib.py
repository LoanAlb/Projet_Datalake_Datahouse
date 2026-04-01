"""
Ingestion Vélib API → MinIO (raw / staging / curated)
- raw     : JSON brut de l'API
- staging : données nettoyées (stations info + status séparés)
- curated : données aplaties prêtes pour PostgreSQL
"""

import json
import os
from datetime import datetime, timezone

import requests
from minio import Minio
from minio.error import S3Error
import io

# --- Config ---
VELIB_STATION_INFO_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
VELIB_STATION_STATUS_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

RAW_BUCKET = "velib-raw"
STAGING_BUCKET = "velib-staging"
CURATED_BUCKET = "velib-curated"

now = datetime.now(timezone.utc)
ts = now.strftime("%Y%m%dT%H%M%S")
date_prefix = now.strftime("%Y/%m/%d")


def get_minio_client():
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)


def upload_json(client, bucket, key, data):
    payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
    client.put_object(bucket, key, io.BytesIO(payload), len(payload), content_type="application/json")
    print(f"  ✔ {bucket}/{key} ({len(payload)} bytes)")


def fetch_api(url):
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def ingest_raw(client):
    """Zone RAW : JSON brut tel que reçu de l'API."""
    print("\n[RAW] Récupération des données brutes...")
    info = fetch_api(VELIB_STATION_INFO_URL)
    status = fetch_api(VELIB_STATION_STATUS_URL)

    upload_json(client, RAW_BUCKET, f"{date_prefix}/station_information_{ts}.json", info)
    upload_json(client, RAW_BUCKET, f"{date_prefix}/station_status_{ts}.json", status)
    return info, status


def ingest_staging(client, info_raw, status_raw):
    """Zone STAGING : extraction des listes, nettoyage minimal."""
    print("\n[STAGING] Nettoyage et extraction...")
    stations = info_raw.get("data", {}).get("stations", [])
    statuses = status_raw.get("data", {}).get("stations", [])

    # Nettoyage : ne garder que les champs utiles
    clean_stations = []
    for s in stations:
        clean_stations.append({
            "station_id": s.get("station_id"),
            "name": s.get("name", "").strip(),
            "capacity": s.get("capacity", 0),
            "lat": s.get("lat"),
            "lon": s.get("lon"),
            "commune": s.get("nom_arrondissement_communes", ""),
            "code_insee": s.get("code_insee_commune", ""),
        })

    clean_statuses = []
    for s in statuses:
        mechanical = 0
        ebike = 0
        for bike in s.get("num_bikes_available_types", []):
            mechanical += bike.get("mechanical", 0)
            ebike += bike.get("ebike", 0)

        clean_statuses.append({
            "station_id": s.get("station_id"),
            "num_bikes_available": s.get("num_bikes_available", 0),
            "num_docks_available": s.get("num_docks_available", 0),
            "mechanical_available": mechanical,
            "ebike_available": ebike,
            "is_installed": bool(s.get("is_installed")),
            "is_renting": bool(s.get("is_renting")),
            "is_returning": bool(s.get("is_returning")),
        })

    upload_json(client, STAGING_BUCKET, f"{date_prefix}/stations_{ts}.json", clean_stations)
    upload_json(client, STAGING_BUCKET, f"{date_prefix}/statuses_{ts}.json", clean_statuses)
    return clean_stations, clean_statuses


def ingest_curated(client, clean_stations, clean_statuses):
    """Zone CURATED : données jointes et aplaties, prêtes pour PostgreSQL."""
    print("\n[CURATED] Jointure et aplatissement...")
    station_map = {s["station_id"]: s for s in clean_stations}
    snapshot_ts = now.isoformat()

    curated = []
    for st in clean_statuses:
        sid = st["station_id"]
        info = station_map.get(sid, {})
        capacity = info.get("capacity", 0)
        bikes = st["num_bikes_available"]

        curated.append({
            **info,
            **st,
            "snapshot_ts": snapshot_ts,
            "occupancy_pct": round((bikes / capacity) * 100, 1) if capacity > 0 else 0,
        })

    upload_json(client, CURATED_BUCKET, f"{date_prefix}/velib_curated_{ts}.json", curated)
    return curated


def main():
    print(f"=== Ingestion Vélib — {now.isoformat()} ===")
    client = get_minio_client()

    info_raw, status_raw = ingest_raw(client)
    clean_stations, clean_statuses = ingest_staging(client, info_raw, status_raw)
    curated = ingest_curated(client, clean_stations, clean_statuses)

    print(f"\n✅ Terminé : {len(curated)} stations ingérées dans les 3 zones.")
    return curated


if __name__ == "__main__":
    main()
