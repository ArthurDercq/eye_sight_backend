"""
Service de gestion intelligente des records personnels.

Ce service utilise une table dédiée pour stocker les records et ne les recalcule
que lorsque c'est nécessaire (nouvelle activité ou initialisation).
"""
import pandas as pd
from datetime import datetime
from db.connection import get_conn
from services.kpi_service import calculate_records as calculate_records_full, find_best_segment
from services.activity_service import get_streams_for_activity


DISTANCE_MAPPING = {
    "5k": 5.0,
    "10k": 10.0,
    "semi": 21.0975,
    "30k": 30.0,
    "marathon": 42.195
}

SPORT_MAPPING = {
    "TrailRun": "Trail",
    "Run": "Run",
    "Ride": "Bike",
    "Swim": "Swim"
}


def get_records_from_db():
    """
    Récupère les records depuis la base de données.

    Returns:
        dict: Records au même format que calculate_records()
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT distance_key, distance_km, time_seconds, pace_seconds_per_km,
                       activity_id, activity_name, activity_date, start_km, end_km
                FROM records
                ORDER BY distance_key
            """)
            rows = cur.fetchall()

    records = {}
    for row in rows:
        # Extract values from dict (RealDictCursor returns dicts)
        distance_key = row['distance_key']
        distance_km = row['distance_km']
        time_seconds = int(row['time_seconds'])
        pace_seconds_per_km = float(row['pace_seconds_per_km'])
        activity_id = row['activity_id']
        activity_name = row['activity_name']
        activity_date = row['activity_date']
        start_km = row['start_km']
        end_km = row['end_km']

        # Formater le temps
        hours = time_seconds // 3600
        minutes = (time_seconds % 3600) // 60
        seconds = time_seconds % 60

        if hours > 0:
            time_formatted = f"{hours}:{minutes:02d}:{seconds:02d}"
        else:
            time_formatted = f"{minutes}:{seconds:02d}"

        # Formater l'allure
        pace_minutes = int(pace_seconds_per_km // 60)
        pace_seconds = int(pace_seconds_per_km % 60)
        pace_formatted = f"{pace_minutes}:{pace_seconds:02d}"

        records[distance_key] = {
            "time": time_formatted,
            "pace": pace_formatted,
            "date": activity_date.strftime("%Y-%m-%d") if activity_date else None,
            "activity_id": activity_id,
            "activity_name": activity_name,
            "activity_url": f"https://www.strava.com/activities/{activity_id}",
            "distance": distance_km,
            "start_km": float(start_km) if start_km else None,
            "end_km": float(end_km) if end_km else None
        }

    # Ajouter les distances manquantes avec None
    for key in DISTANCE_MAPPING.keys():
        if key not in records:
            records[key] = None

    return records


def initialize_records():
    """
    Initialise les records en calculant tous les records depuis zéro
    et en les sauvegardant dans la base de données.

    À appeler une seule fois pour un nouvel utilisateur.
    """
    print("🔄 Initialisation des records...")

    # Calculer tous les records
    records = calculate_records_full()

    # Sauvegarder dans la base de données
    with get_conn() as conn:
        with conn.cursor() as cur:
            for distance_key, record in records.items():
                if record is None:
                    continue

                # Calculer les secondes
                time_parts = record['time'].split(':')
                if len(time_parts) == 3:  # H:MM:SS
                    time_seconds = int(time_parts[0]) * 3600 + int(time_parts[1]) * 60 + int(time_parts[2])
                else:  # MM:SS
                    time_seconds = int(time_parts[0]) * 60 + int(time_parts[1])

                pace_parts = record['pace'].split(':')
                pace_seconds_per_km = int(pace_parts[0]) * 60 + int(pace_parts[1])

                # Convert all values to Python native types
                start_km_value = None if record['start_km'] is None else float(record['start_km'])
                end_km_value = None if record['end_km'] is None else float(record['end_km'])

                cur.execute("""
                    INSERT INTO records
                    (distance_key, distance_km, time_seconds, pace_seconds_per_km,
                     activity_id, activity_name, activity_date, start_km, end_km, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (distance_key)
                    DO UPDATE SET
                        time_seconds = EXCLUDED.time_seconds,
                        pace_seconds_per_km = EXCLUDED.pace_seconds_per_km,
                        activity_id = EXCLUDED.activity_id,
                        activity_name = EXCLUDED.activity_name,
                        activity_date = EXCLUDED.activity_date,
                        start_km = EXCLUDED.start_km,
                        end_km = EXCLUDED.end_km,
                        updated_at = NOW()
                """, (
                    distance_key,
                    float(record['distance']),
                    int(time_seconds),
                    float(pace_seconds_per_km),
                    str(record['activity_id']),
                    str(record['activity_name']),
                    record['date'],
                    start_km_value,
                    end_km_value
                ))

        conn.commit()

    print("✅ Records initialisés avec succès !")
    return records


def check_and_update_record_with_activity(activity_id, activity_data):
    """
    Vérifie si une nouvelle activité bat un des records existants.
    Si oui, met à jour la base de données.

    Args:
        activity_id: ID de l'activité à vérifier
        activity_data: Dict avec les données de l'activité (distance, moving_time, sport_type, etc.)

    Returns:
        list: Liste des records battus (clés de distance)
    """
    # Normaliser le sport
    sport_type = SPORT_MAPPING.get(activity_data.get('sport_type'), activity_data.get('sport_type'))

    # Ne traiter que Run et Trail
    if sport_type not in ['Run', 'Trail']:
        return []

    distance = activity_data.get('distance', 0)
    if distance < 5.0:  # Trop courte pour battre un record
        return []

    # Récupérer les streams de l'activité
    streams = get_streams_for_activity(str(activity_id))
    if not streams or len(streams) == 0:
        return []

    streams_df = pd.DataFrame(streams)
    if 'distance_m' not in streams_df.columns or 'time_s' not in streams_df.columns:
        return []

    streams_df = streams_df.dropna(subset=['distance_m', 'time_s'])
    streams_df = streams_df.sort_values('time_s').reset_index(drop=True)

    if len(streams_df) < 2:
        return []

    # Récupérer les records actuels de la DB
    current_records = get_records_from_db()

    broken_records = []

    # Vérifier chaque distance
    for distance_key, target_km in DISTANCE_MAPPING.items():
        # Skip si l'activité n'est pas assez longue
        if distance < target_km:
            continue

        target_meters = target_km * 1000

        # Trouver le meilleur segment pour cette distance dans l'activité
        best_segment = find_best_segment(streams_df, target_meters)

        if best_segment is None:
            continue

        new_time = best_segment['duration']

        # Comparer avec le record actuel
        current_record = current_records.get(distance_key)

        is_new_record = False
        if current_record is None:
            # Pas de record existant, c'est un nouveau record
            is_new_record = True
        else:
            # Convertir le temps actuel en secondes
            time_parts = current_record['time'].split(':')
            if len(time_parts) == 3:
                current_time = int(time_parts[0]) * 3600 + int(time_parts[1]) * 60 + int(time_parts[2])
            else:
                current_time = int(time_parts[0]) * 60 + int(time_parts[1])

            if new_time < current_time:
                is_new_record = True

        if is_new_record:
            # Mettre à jour le record dans la DB
            pace_seconds_per_km = new_time / target_km

            # Convert all values to Python native types
            start_km_value = float(best_segment['start_distance_km'])
            end_km_value = float(best_segment['end_distance_km'])

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO records
                        (distance_key, distance_km, time_seconds, pace_seconds_per_km,
                         activity_id, activity_name, activity_date, start_km, end_km, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (distance_key)
                        DO UPDATE SET
                            distance_km = EXCLUDED.distance_km,
                            time_seconds = EXCLUDED.time_seconds,
                            pace_seconds_per_km = EXCLUDED.pace_seconds_per_km,
                            activity_id = EXCLUDED.activity_id,
                            activity_name = EXCLUDED.activity_name,
                            activity_date = EXCLUDED.activity_date,
                            start_km = EXCLUDED.start_km,
                            end_km = EXCLUDED.end_km,
                            updated_at = NOW()
                    """, (
                        distance_key,
                        float(target_km),
                        int(new_time),
                        float(pace_seconds_per_km),
                        str(activity_id),
                        str(activity_data.get('name', '')),
                        activity_data.get('start_date'),
                        start_km_value,
                        end_km_value
                    ))
                conn.commit()

            broken_records.append(distance_key)
            print(f"🎉 Nouveau record sur {distance_key} : {int(new_time//60)}:{int(new_time%60):02d}")

    return broken_records


def ensure_records_initialized():
    """
    Vérifie si les records sont initialisés dans la DB.
    Si non, les initialise.

    Returns:
        bool: True si initialisés, False sinon
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as count FROM records")
            result = cur.fetchone()
            count = result['count'] if result else 0

    if count == 0:
        print("⚠️ Aucun record trouvé en base, initialisation...")
        initialize_records()
        return True

    return False
