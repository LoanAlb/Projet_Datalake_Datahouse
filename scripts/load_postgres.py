"""
Chargement des données curated (MinIO) → PostgreSQL
- Upsert des stations (infos statiques)
- Insert des snapshots de status (historisation)
"""

import json
import os
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from minio import Minio

# --- Config ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
CURATED_BUCKET = "velib-curated"

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "velib_db")
PG_USER = os.getenv("PG_USER", "velib")
PG_PASSWORD = os.getenv("PG_PASSWORD", "velib")


def get_minio_client():
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)


def get_latest_curated(client):
    """Récupère le fichier curated le plus récent."""
    objects = list(client.list_objects(CURATED_BUCKET, recursive=True))
    if not objects:
        raise FileNotFoundError("Aucun fichier curated trouvé dans MinIO")

    latest = sorted(objects, key=lambda o: o.object_name)[-1]
    print(f"📂 Fichier curated : {latest.object_name}")

    response = client.get_object(CURATED_BUCKET, latest.object_name)
    data = json.loads(response.read().decode("utf-8"))
    response.close()
    response.release_conn()
    return data


def load_to_postgres(data):
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    cur = conn.cursor()

    # 1. Créer un run d'ingestion
    cur.execute(
        "INSERT INTO ingestion_runs (source, status, records_received) VALUES (%s, %s, %s) RETURNING run_id",
        ("velib-curated-load", "running", len(data))
    )
    run_id = cur.fetchone()[0]
    print(f"🔄 Run #{run_id} — {len(data)} enregistrements")

    # 2. Upsert stations
    stations = set()
    station_rows = []
    for row in data:
        sid = row.get("station_id")
        if sid and sid not in stations:
            stations.add(sid)
            station_rows.append((
                sid,
                row.get("name", ""),
                row.get("capacity", 0),
                row.get("lat"),
                row.get("lon"),
                row.get("commune", ""),
                row.get("code_insee", ""),
            ))

    if station_rows:
        execute_values(cur, """
            INSERT INTO stations (station_id, name, capacity, lat, lon, commune, code_insee)
            VALUES %s
            ON CONFLICT (station_id) DO UPDATE SET
                name = EXCLUDED.name,
                capacity = EXCLUDED.capacity,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                commune = EXCLUDED.commune,
                code_insee = EXCLUDED.code_insee,
                last_updated_at = NOW()
        """, station_rows)
        print(f"  ✔ {len(station_rows)} stations upsertées")

    # 3. Insert status snapshots
    status_rows = []
    for row in data:
        status_rows.append((
            row.get("station_id"),
            row.get("snapshot_ts"),
            row.get("num_bikes_available", 0),
            row.get("num_docks_available", 0),
            row.get("mechanical_available", 0),
            row.get("ebike_available", 0),
            row.get("is_installed", False),
            row.get("is_renting", False),
            row.get("is_returning", False),
            run_id,
        ))

    if status_rows:
        execute_values(cur, """
            INSERT INTO station_status
                (station_id, snapshot_ts, num_bikes_available, num_docks_available,
                 mechanical_available, ebike_available, is_installed, is_renting, is_returning, run_id)
            VALUES %s
        """, status_rows)
        print(f"  ✔ {len(status_rows)} snapshots insérés")

    # 4. Finaliser le run
    cur.execute(
        "UPDATE ingestion_runs SET status = 'success', finished_at = NOW(), records_inserted = %s WHERE run_id = %s",
        (len(status_rows), run_id)
    )

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Chargement terminé (run #{run_id})")


def main():
    print(f"=== Chargement Curated → PostgreSQL — {datetime.now(timezone.utc).isoformat()} ===")
    client = get_minio_client()
    data = get_latest_curated(client)
    load_to_postgres(data)


if __name__ == "__main__":
    main()
