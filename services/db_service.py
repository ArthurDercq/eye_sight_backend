
import pandas as pd
from psycopg2 import connect, sql
from psycopg2.extras import execute_values
import pandas as pd
import json
from strava.clean_data import *
from strava.fetch_strava import *
from strava.params import *






def normalize_sport_type(sport):
    mapping = {
        "TrailRun": "Trail",
        "Ride": "Bike"
    }
    return mapping.get(sport, sport)

def store_df_in_postgresql(df, host, database, user, password, port):

    # Connexion à la DB
    conn = connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )
    cur = conn.cursor()

    table_name = TABLE_NAME

    # Création de la table (si elle n'existe pas)
    create_table_query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {} (
        id BIGSERIAL PRIMARY KEY,
        name VARCHAR(255),
        distance FLOAT,
        moving_time FLOAT,
        elapsed_time FLOAT,
        moving_time_hms TEXT,
        elapsed_time_hms TEXT,
        average_speed FLOAT,
        speed_minutes_per_km FLOAT,
        speed_minutes_per_km_hms TEXT,
        total_elevation_gain FLOAT,
        sport_type VARCHAR(255),
        start_date TIMESTAMP,
        start_date_local TIMESTAMP,
        timezone VARCHAR(50),
        achievement_count INTEGER,
        kudos_count INTEGER,
        gear_id VARCHAR(255),
        start_latlng VARCHAR(50),
        end_latlng VARCHAR(50),
        max_speed FLOAT,
        average_cadence FLOAT,
        average_temp FLOAT,
        has_heartrate BOOLEAN,
        average_heartrate FLOAT,
        max_heartrate FLOAT,
        elev_high FLOAT,
        elev_low FLOAT,
        pr_count INTEGER,
        has_kudoed BOOLEAN,
        average_watts FLOAT,
        kilojoules FLOAT,
        map JSONB
    );
    """).format(sql.Identifier(table_name))

    cur.execute(create_table_query)

    # Préparer les données
    values = [
        (
        row['id'], row['name'], row['distance'], row['moving_time'], row['elapsed_time'],
        row["moving_time_hms"], row["elapsed_time_hms"], row['average_speed'],
        row['speed_minutes_per_km'], row['speed_minutes_per_km_hms'], row['total_elevation_gain'],
        normalize_sport_type(row['sport_type']), row['start_date'], row['start_date_local'],
        row['timezone'], row['achievement_count'], row['kudos_count'], row['gear_id'],
        str(row['start_latlng']), str(row['end_latlng']), row['max_speed'], row['average_cadence'],
        row['average_temp'], row['has_heartrate'], row['average_heartrate'], row['max_heartrate'],
        row['elev_high'], row['elev_low'], row['pr_count'], row['has_kudoed'],
        row['average_watts'], row['kilojoules'], json.dumps(row['map'])
        )
        for _, row in df.iterrows()
    ]

    # Colonnes à insérer
    columns = (
        'id','name', 'distance', 'moving_time', 'elapsed_time','moving_time_hms',
        'elapsed_time_hms', 'average_speed', 'speed_minutes_per_km','speed_minutes_per_km_hms',
        'total_elevation_gain', 'sport_type', 'start_date', 'start_date_local', 'timezone',
        'achievement_count', 'kudos_count', 'gear_id', 'start_latlng', 'end_latlng','max_speed',
        'average_cadence','average_temp', 'has_heartrate', 'average_heartrate', 'max_heartrate',
        'elev_high', 'elev_low', 'pr_count', 'has_kudoed', 'average_watts','kilojoules', 'map'
    )

    for col in columns:
        if col not in df.columns:
            print(f"[DEBUG] Colonne manquante ajoutée: {col}")
            df[col] = None

    insert_query = sql.SQL("""
        INSERT INTO {} ({})
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """).format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(map(sql.Identifier, columns))
    )

    # Insertion en bulk
    execute_values(cur, insert_query.as_string(conn), values)

    conn.commit()
    cur.close()

    print("Données importées dans PostgreSQL ✅")

def store_df_streams_in_postgresql(df_streams, host, database, user, password, port, table_name="streams"):
    """
    Stocke un DataFrame de streams Strava dans une table PostgreSQL.
    """
    conn = connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )
    cur = conn.cursor()

    # Création de la table si elle n'existe pas
    create_table_query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {} (
        activity_id VARCHAR(50),
        lat FLOAT,
        lon FLOAT,
        altitude FLOAT,
        distance_m FLOAT,
        time_s FLOAT
    );
    """).format(sql.Identifier(table_name))
    cur.execute(create_table_query)

    # Préparer les données à insérer
    values = [
        (
            row['activity_id'],
            row['lat'],
            row['lon'],
            row['altitude'],
            row['distance_m'],
            row['time_s']
        )
        for _, row in df_streams.iterrows()
    ]

    columns = ('activity_id', 'lat', 'lon', 'altitude', 'distance_m', 'time_s')

    insert_query = sql.SQL("""
        INSERT INTO {} ({})
        VALUES %s
        ON CONFLICT DO NOTHING
    """).format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(map(sql.Identifier, columns))
    )

    execute_values(cur, insert_query.as_string(conn), values)
    conn.commit()
    cur.close()
    print("Streams importés dans PostgreSQL ✅")


# Fonction pour nettoyer les données

def convert_minutes_to_hms(minutes):

    if minutes is None or not isinstance(minutes, (int, float)):
        return "00:00:00"
    if minutes < 0:
        minutes = abs(minutes)

    total_seconds = int(minutes * 60)
    h = total_seconds // 3600
    remainder = total_seconds % 3600
    m = remainder // 60
    s = remainder % 60
    return f"{h:02}:{m:02}:{s:02}"

def format_pace(speed_kmh):

    if pd.isna(speed_kmh) or speed_kmh == 0:
        return None
    return 60 / speed_kmh  # retourne un float (minutes par km)

def clean_data(df):

    # Copier le DataFrame pour ne pas modifier l'original
    activities_df_cleaned = df.copy()

    # Liste des colonnes à supprimer
    columns_to_drop = [
    'resource_state', 'athlete', 'type', 'workout_type', 'utc_offset',
    'location_city', 'location_state', 'location_country', 'comment_count',
    'athlete_count', 'photo_count', 'trainer', 'commute', 'manual',
    'private', 'visibility', 'flagged', 'device_watts', 'heartrate_opt_out',
    'display_hide_heartrate_option', 'upload_id', 'upload_id_str', 'external_id',
    'from_accepted_tag', 'total_photo_count'
    ]
    # Suppression des colonnes non pertinentes du DataFrame
    activities_df_cleaned.drop(columns=columns_to_drop, errors='ignore', inplace=True)
    print("Colonnes ✅")

    # Conversion de la colonne 'distance' de mètres en kilomètres
    activities_df_cleaned['distance'] = activities_df_cleaned['distance'] / 1000
    print("Distance convertie ✅")


    # Conversion des colonnes 'moving_time' et 'elapsed_time' de secondes en minutes
    activities_df_cleaned['moving_time'] = activities_df_cleaned['moving_time'] / 60
    activities_df_cleaned['elapsed_time'] = activities_df_cleaned['elapsed_time'] / 60
    print("temps de secondes en minutes ✅")


    # Conversion de la colonne 'average_speed' de mètres par seconde en kilomètres par heure
    activities_df_cleaned['average_speed'] = activities_df_cleaned['average_speed'] * 3.6
    activities_df_cleaned['max_speed'] = activities_df_cleaned['max_speed'] * 3.6
    print("m/s en km/h ✅")


    # Ajouter une nouvelle colonne 'minutes_per_km' qui convertit 'average_speed' en minutes par kilomètre
    activities_df_cleaned['speed_minutes_per_km'] = activities_df_cleaned['average_speed'].apply(format_pace)
    # Colonne pour affichage format mm:ss
    activities_df_cleaned['speed_minutes_per_km_hms'] = activities_df_cleaned['speed_minutes_per_km'] \
    .apply(lambda x: f"{int(x)}:{int(round((x % 1) * 60)):02d}" if pd.notnull(x) else None)
    print("min/km colonne ✅")

    # Ajouter une nouvelle colonne avec le format HH:MM:SS pour 'moving_time' et 'elapsed_time'
    activities_df_cleaned['moving_time_hms'] = activities_df_cleaned['moving_time'].apply(convert_minutes_to_hms)
    activities_df_cleaned['elapsed_time_hms'] = activities_df_cleaned['elapsed_time'].apply(convert_minutes_to_hms)
    print("Format temps HH:MM:SS ✅")

    # Sérialisation du champ map
    activities_df_cleaned["map"] = activities_df_cleaned["map"].apply(json.dumps)

    required_columns = [
    'id','name', 'distance', 'moving_time', 'elapsed_time','moving_time_hms', 'elapsed_time_hms',
        'total_elevation_gain',
        'sport_type', 'start_date', 'start_date_local', 'timezone',
        'achievement_count', 'kudos_count', 'gear_id', 'start_latlng', 'end_latlng',
        'average_speed', 'speed_minutes_per_km','speed_minutes_per_km_hms', 'max_speed', 'average_cadence',
        'average_temp', 'has_heartrate', 'average_heartrate', 'max_heartrate',
        'elev_high', 'elev_low', 'pr_count', 'has_kudoed', 'average_watts',
        'kilojoules', 'map'
]

# Ajouter les colonnes manquantes avec None
    for col in required_columns:
        if col not in activities_df_cleaned.columns:
            activities_df_cleaned[col] = None

    print("Les données ont été nettoyées avec succès ✅")


    return activities_df_cleaned
