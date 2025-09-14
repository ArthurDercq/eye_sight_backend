import pandas as pd
import json
import polyline
from db.connection import get_conn
import numpy as np
import datetime

def get_all_activities():
    """Récupère toutes les activités dans un DataFrame et rend les données JSON-compliant."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM activites ORDER BY start_date DESC;")
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

    df = pd.DataFrame(rows, columns=colnames)

    # Remplacer tous les NaN par None
    df = df.where(pd.notnull(df), None)
    # Remplacer tous les NaN, inf, -inf par None
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

    # Convertir les datetime en string ISO
    for col in ["start_date", "start_date_local"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x.isoformat() if x is not None else None)

    # Convertir float64 et int64 numpy en types Python natifs
    for col in df.select_dtypes(include=["float64", "int64"]).columns:
        df[col] = df[col].apply(lambda x: None if x is None else float(x) if isinstance(x, (float, np.floating)) else int(x) if isinstance(x, (int, np.integer)) else x)

    # Les colonnes bool et object sont déjà JSON-safe
    return df

def get_last_activity(sport_type=None):
    df = get_all_activities()
    if df.empty:
        return None

    if sport_type:
        df = df[df["sport_type"] == sport_type]

    if df.empty:
        return None

    #dernier_serie = df.sort_values(by="start_date", ascending=False).iloc[0]
    dernier = df.sort_values(by="start_date", ascending=False).iloc[0] #c'est une serie car une seule paire de crochets

    # Extraire la polyline
    map_json = json.loads(dernier.get("map", "{}"))
    polyline_str = map_json.get("summary_polyline")
    coords = polyline.decode(polyline_str) if polyline_str else []

    return {
    "type": dernier.get("sport_type"),
    "distance_km": round(dernier.get("distance", 0), 2),
    "duree_minutes": dernier.get("moving_time"),
    "duree_hms": dernier.get("moving_time_hms"),
    "allure_min_per_km": dernier.get("speed_minutes_per_km_hms"),
    "vitesse_kmh": round(dernier.get("average_speed", 0), 2),
    "denivele_m": round(dernier.get("total_elevation_gain", 0)),
    "bpm_moyen": dernier.get("average_heartrate"),
    "polyline_coords": coords
}

def get_recent_activities(weeks: int = 12, sport_types=None):
    """
    Récupère les activités des dernières `weeks` depuis la BDD.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            query = """
                SELECT *
                FROM activites
                WHERE start_date >= %s
                ORDER BY start_date DESC;
            """
            start_date = (datetime.datetime.now() - pd.Timedelta(weeks=weeks*7, unit="D")).isoformat()
            cur.execute(query, (start_date,))
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

    df = pd.DataFrame(rows, columns=colnames)

    # Nettoyer les NaN / inf pour JSON
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

    # Convertir datetime en string ISO
    for col in ["start_date", "start_date_local"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x.isoformat() if x else None)

    # Filtrer par sport si demandé
    if sport_types:
        df = df[df["sport_type"].isin(sport_types)]

    return df

def aggregate_weekly(df: pd.DataFrame, value_col: str = "moving_time"):
    """
    Agrège les données par semaine en additionnant la colonne `value_col`.
    """
    if df.empty:
        return pd.DataFrame(columns=["period", value_col])

    df = df.copy()
    df["start_date"] = pd.to_datetime(df["start_date"])

    # garder seulement les colonnes numériques utiles
    numeric_cols = [value_col]
    if "distance" in df.columns:
        numeric_cols.append("distance")
    if "total_elevation_gain" in df.columns:
        numeric_cols.append("total_elevation_gain")
    if "average_speed" in df.columns:
        numeric_cols.append("average_speed")

    df_numeric = df[["start_date"] + numeric_cols]

    # Grouper par semaine
    weekly = df_numeric.groupby(pd.Grouper(key="start_date", freq="W-MON", label="left"))[numeric_cols].sum().reset_index()

    # Renommer start_date -> period pour plus de clarté
    weekly = weekly.rename(columns={"start_date": "period"})

    # Rendre JSON-safe
    weekly = weekly.replace({np.nan: None})

    return weekly


def minutes_to_hms(minutes):
    """Convertit des minutes en format HH:MM:SS."""
    total_seconds = int(minutes * 60)
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


SPORT_MAPPING = {
    "TrailRun": "Trail",
    "Run": "Run",
    "Ride": "Bike",
    "Swim": "Swim"
}
