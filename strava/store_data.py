
import pandas as pd
from psycopg2 import connect, sql
from psycopg2.extras import execute_values
import json
from strava.clean_data import *
from strava.fetch_strava import *
from strava.params import *
import numpy as np


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
    DEPRECATED: Utilisez store_df_streams_in_postgresql_optimized à la place.
    Fonction gardée pour compatibilité mais redirige vers la version optimisée.
    """
    return store_df_streams_in_postgresql_optimized(
        df_streams, host, database, user, password, port, table_name
    )







def _to_python_value(x):
    """Convertit un élément numpy/pandas en type Python JSON/psycopg2-friendly."""
    if x is None:
        return None
    # pandas NA / numpy nan
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    # numpy scalars -> python scalars
    if isinstance(x, (np.floating, np.float64, np.float32)):
        return float(x)
    if isinstance(x, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(x)
    if isinstance(x, (np.bool_ , bool)):
        return bool(x)
    # pandas Timestamp -> isoformat string
    if isinstance(x, pd.Timestamp):
        return x.isoformat()
    # numpy string types
    if isinstance(x, (np.str_,)):
        return str(x)
    # bytes -> decode
    if isinstance(x, (bytes, bytearray)):
        return x.decode()
    # fallback (already python native)
    return x


def _safe_convert_activity_id(x):
    """Conversion sécurisée de activity_id en string."""
    try:
        if pd.isna(x) or x is None:
            return None
        return str(int(float(x)))
    except (ValueError, TypeError, OverflowError):
        return str(x) if x is not None else None

def store_df_streams_in_postgresql_optimized(
    df_streams,
    host, database, user, password, port,
    table_name="streams",
    debug_preview: int = 0
):
    """
    Version optimisée pour stocker un DataFrame de streams Strava dans PostgreSQL.
    Convertit les numpy types en types Python natifs et gère les conflits proprement.
    """
    if df_streams.empty:
        print("Aucune ligne à insérer dans les streams.")
        return 0

    conn = connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )

    try:
        with conn:
            with conn.cursor() as cur:
                # Vérifier si la table existe et sa structure
                cur.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = %s AND table_schema = 'public'
                    ORDER BY ordinal_position;
                """, (table_name,))

                existing_columns = cur.fetchall()
                table_exists = len(existing_columns) > 0

                if table_exists:
                    print(f"Table {table_name} existe déjà avec {len(existing_columns)} colonnes")
                    # Vérifier s'il y a une clé primaire
                    cur.execute("""
                        SELECT COUNT(*) FROM information_schema.table_constraints
                        WHERE table_name = %s AND constraint_type = 'PRIMARY KEY'
                    """, (table_name,))
                    has_pk = cur.fetchone()[0] > 0

                    if not has_pk:
                        print(f"Ajout d'une clé primaire composite à la table {table_name}...")
                        # Ajouter une clé primaire composite seulement si pas de doublons
                        cur.execute(sql.SQL("""
                            SELECT
                                COUNT(*) as total,
                                COUNT(DISTINCT (activity_id, time_s)) as distinct_pairs
                            FROM {}
                        """).format(sql.Identifier(table_name)))
                        result = cur.fetchone()
                        total, distinct = result[0], result[1]

                        if total == distinct:
                            try:
                                cur.execute(sql.SQL("""
                                    ALTER TABLE {}
                                    ADD CONSTRAINT {}_pk
                                    PRIMARY KEY (activity_id, time_s)
                                """).format(
                                    sql.Identifier(table_name),
                                    sql.Identifier(table_name)
                                ))
                                print("Clé primaire ajoutée avec succès ✅")
                            except Exception as e:
                                print(f"Impossible d'ajouter la clé primaire: {e}")
                        else:
                            print(f"ATTENTION: {total - distinct} doublons détectés, nettoyage recommandé")
                else:
                    # Créer la table avec clé primaire
                    create_table_query = sql.SQL("""
                    CREATE TABLE {table} (
                        activity_id VARCHAR(50) NOT NULL,
                        lat DOUBLE PRECISION,
                        lon DOUBLE PRECISION,
                        altitude DOUBLE PRECISION,
                        distance_m DOUBLE PRECISION,
                        time_s DOUBLE PRECISION NOT NULL,
                        heartrate INTEGER,
                        cadence INTEGER,
                        velocity_smooth DOUBLE PRECISION,
                        temp INTEGER,
                        power INTEGER,
                        grade_smooth DOUBLE PRECISION,
                        PRIMARY KEY (activity_id, time_s)
                    );
                    """).format(table=sql.Identifier(table_name))
                    cur.execute(create_table_query)
                    print(f"Table {table_name} créée avec clé primaire ✅")

                # Validation des colonnes
                expected_cols = ['activity_id', 'lat', 'lon', 'altitude', 'distance_m', 'time_s',
                                 'heartrate', 'cadence', 'velocity_smooth', 'temp', 'power', 'grade_smooth']

                # Only check for required columns (activity_id and time_s must exist)
                required_cols = ['activity_id', 'time_s']
                missing_required = [c for c in required_cols if c not in df_streams.columns]
                if missing_required:
                    raise ValueError(f"Colonnes requises manquantes dans df_streams: {missing_required}")

                # Add missing optional columns with None values
                for col in expected_cols:
                    if col not in df_streams.columns:
                        print(f"[INFO] Colonne optionnelle '{col}' manquante, ajout avec valeurs NULL")

                # Nettoyage et conversion des données
                df = df_streams.copy()

                # Add missing columns with None if they don't exist
                for col in expected_cols:
                    if col not in df.columns:
                        df[col] = None

                df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

                # Conversion sécurisée de activity_id
                df['activity_id'] = df['activity_id'].apply(_safe_convert_activity_id)

                # Supprimer les lignes avec activity_id ou time_s manquants (requis pour PK)
                initial_count = len(df)
                df = df.dropna(subset=['activity_id', 'time_s'])
                final_count = len(df)

                if initial_count != final_count:
                    print(f"ATTENTION: {initial_count - final_count} lignes supprimées (activity_id ou time_s manquants)")

                if df.empty:
                    print("Aucune ligne valide à insérer après nettoyage.")
                    return 0

                # Préparer les données pour insertion
                values = []
                for _, row in df.iterrows():
                    tup = tuple(_to_python_value(row[col]) for col in expected_cols)
                    values.append(tup)

                if debug_preview and values:
                    print(f"Preview des {min(debug_preview, len(values))} premières lignes:")
                    for v in values[:debug_preview]:
                        print(v)

                # Insertion avec gestion des conflits - méthode robuste
                # Vérifier si on a une clé primaire pour utiliser ON CONFLICT
                cur.execute("""
                    SELECT COUNT(*) FROM information_schema.table_constraints
                    WHERE table_name = %s AND constraint_type = 'PRIMARY KEY'
                """, (table_name,))
                has_pk_now = cur.fetchone()[0] > 0

                if has_pk_now:
                    # Avec clé primaire, utilisation normale d'ON CONFLICT
                    insert_query = sql.SQL("""
                        INSERT INTO {table} ({cols})
                        VALUES %s
                        ON CONFLICT (activity_id, time_s) DO NOTHING
                    """).format(
                        table=sql.Identifier(table_name),
                        cols=sql.SQL(', ').join(map(sql.Identifier, expected_cols))
                    )
                    execute_values(cur, insert_query.as_string(conn), values, template=None, page_size=1000)
                else:
                    # Sans clé primaire, insertion par lots avec vérification
                    print("⚠️  Pas de clé primaire, insertion sécurisée par lots")
                    inserted_count = 0

                    for value_batch in [values[i:i+100] for i in range(0, len(values), 100)]:
                        # Créer une table temporaire pour ce batch
                        temp_table = f"{table_name}_temp_{id(value_batch) % 10000}"

                        # Créer table temporaire
                        cur.execute(sql.SQL("""
                            CREATE TEMP TABLE {} AS
                            SELECT * FROM {} WHERE FALSE
                        """).format(
                            sql.Identifier(temp_table),
                            sql.Identifier(table_name)
                        ))

                        # Insérer dans la table temporaire
                        temp_insert = sql.SQL("""
                            INSERT INTO {} ({})
                            VALUES %s
                        """).format(
                            sql.Identifier(temp_table),
                            sql.SQL(', ').join(map(sql.Identifier, expected_cols))
                        )
                        execute_values(cur, temp_insert.as_string(conn), value_batch)

                        # Insérer seulement les nouvelles données
                        cur.execute(sql.SQL("""
                            INSERT INTO {main_table} ({cols})
                            SELECT {cols} FROM {temp_table} t
                            WHERE NOT EXISTS (
                                SELECT 1 FROM {main_table} m
                                WHERE m.activity_id = t.activity_id
                                AND m.time_s = t.time_s
                            )
                        """).format(
                            main_table=sql.Identifier(table_name),
                            temp_table=sql.Identifier(temp_table),
                            cols=sql.SQL(', ').join(map(sql.Identifier, expected_cols))
                        ))

                        inserted_count += cur.rowcount

                        # Supprimer la table temporaire
                        cur.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(temp_table)))

                    print(f"📊 {inserted_count} nouvelles lignes insérées (sur {len(values)} soumises)")

                # Compter les nouvelles insertions
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                total_after = cur.fetchone()[0]

                print(f"✅ Insertion terminée. Total dans {table_name}: {total_after} lignes")
                return len(values)

    except Exception as e:
        print(f"❌ Erreur lors du stockage des streams: {e}")
        raise
    finally:
        conn.close()


# Alias pour rétrocompatibilité
store_df_streams_in_postgresql_test = store_df_streams_in_postgresql_optimized
