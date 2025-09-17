import pandas as pd
import json
import polyline
from db.connection import *
import numpy as np
from datetime import timedelta, datetime

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
    # Convertir tous les types numpy.* en types Python natifs
    dernier = dernier.apply(
    lambda x: x.item() if isinstance(x, (np.generic,)) else x)

    # Extraire la polyline
    map_json = json.loads(dernier.get("map", "{}"))
    polyline_str = map_json.get("summary_polyline")
    coords = polyline.decode(polyline_str) if polyline_str else []

    return {
    "id": dernier.get("id"),
    "name": dernier.get("name"),
    "date": dernier.get("start_date"),
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

def get_last_activity_streams(sport_type=None):
    """
    Récupère les streams de la dernière activité (optionnellement filtrée par sport_type),
    en utilisant get_last_activity() pour récupérer son ID.
    """
    last = get_last_activity(sport_type)
    if not last:
        return {"message": "Aucune activité trouvée."}

    activity_id = str(last["id"])

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM streams WHERE activity_id = %s ORDER BY time_s ASC;",
                (activity_id,)
            )
            streams = cur.fetchall()  # Liste de dicts grâce à RealDictCursor

    return {
        "activity_id": activity_id,
        "streams": streams or []  # Liste vide si aucun stream
    }


def get_last_activities(n=40, sport_type: list[str] = None):
    df = get_all_activities()
    if df.empty:
        return []

    if sport_type:
        df = df[df["sport_type"].isin(sport_type)]

    if df.empty:
        return []

    df_sorted = df.sort_values(by="start_date", ascending=False).head(n)

    activities = []
    for _, row in df_sorted.iterrows():
        map_json = json.loads(row.get("map", "{}"))
        polyline_str = map_json.get("summary_polyline")
        coords = polyline.decode(polyline_str) if polyline_str else []

        activities.append({
            "id": row.get("id"),
            "name": row.get("name"),
            "date": row.get("start_date"),
            "type": row.get("sport_type"),
            "distance_km": round(row.get("distance", 0), 2),
            "duree_minutes": row.get("moving_time"),
            "duree_hms": row.get("moving_time_hms"),
            "allure_min_per_km": row.get("speed_minutes_per_km_hms"),
            "vitesse_kmh": round(row.get("average_speed", 0), 2),
            "denivele_m": round(row.get("total_elevation_gain", 0)),
            "bpm_moyen": row.get("average_heartrate"),
            "polyline_coords": coords
        })

    return activities

def get_streams_for_activity(activity_id):
    """
    Récupère les données de streams (altitude, distance, etc.) pour une activité donnée.
    Utilise ton get_engine() pour accéder à la BDD.
    """

    # s'assurer que c'est une string
    activity_id = str(activity_id)

    engine = get_engine()
    query = """
        SELECT distance_m, altitude, time_s, lat, lon
        FROM streams
        WHERE activity_id = %s
        ORDER BY time_s
    """
    df = pd.read_sql(query, engine, params=(activity_id,))

    if df.empty:
        return []

    # Conversion en JSON-ready (liste de points)
    return df.to_dict(orient="records")

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
            start_date = (datetime.now() - pd.Timedelta(weeks=weeks*7, unit="D")).isoformat()
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
    Agrège les données par semaine :
    - somme pour les colonnes cumulatives
    - moyenne pondérée par moving_time pour les colonnes moyennes
    """
    if df.empty:
        return pd.DataFrame(columns=["period", value_col])

    df = df.copy()
    df["start_date"] = pd.to_datetime(df["start_date"])

    # Colonnes à sommer
    sum_cols = [value_col]
    if "distance" in df.columns:
        sum_cols.append("distance")
    if "total_elevation_gain" in df.columns:
        sum_cols.append("total_elevation_gain")

    # Colonnes à moyenner (pondérées par moving_time)
    mean_cols = []
    if "average_speed" in df.columns:
        mean_cols.append("average_speed")
    if "speed_minutes_per_km" in df.columns:
        mean_cols.append("speed_minutes_per_km")
    if "average_hearthrate" in df.columns:
        mean_cols.append("average_hearthrate")

    # Grouper par semaine
    grouped = df.groupby(pd.Grouper(key="start_date", freq="W-MON", label="left"))

    # Agrégation des colonnes sommables
    weekly_sum = grouped[sum_cols].sum() if sum_cols else pd.DataFrame()

    # Agrégation pondérée des colonnes moyennes
    weighted_means = {}
    for col in mean_cols:
        def safe_weighted_mean(g):
            total_time = g["moving_time"].sum()
            if total_time == 0:
                return None
            return (g[col] * g["moving_time"]).sum() / total_time

        weighted_means[col] = grouped.apply(safe_weighted_mean)

    # Combiner résultats
    weekly = weekly_sum.copy()
    for col, series in weighted_means.items():
        weekly[col] = series

    weekly = weekly.reset_index().rename(columns={"start_date": "period"})

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

def get_weekly_daily_barchart(df: pd.DataFrame, week_offset: int = 0):
    """
    Prépare les données pour un bar chart empilé :
    - x = jours de la semaine (Lundi..Dimanche)
    - y = somme de moving_time
    - couleur = sport_type
    - week_offset: 0 = cette semaine, 1 = semaine dernière, etc.
    """
    if df.empty:
        return {
            "week": start_week.strftime("%d-%m-%Y"),
            "labels": ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"],
            "datasets": [],
            "message": "Aucune activité"
        }

    df = df.copy()
    df["start_date"] = pd.to_datetime(df["start_date"])

    # Calcul des bornes de la semaine demandée
    today = datetime.now()
    # Trouver le lundi de cette semaine
    this_monday = today - timedelta(days=today.weekday())
    # Appliquer le décalage de semaines
    start_week = this_monday - timedelta(weeks=week_offset)
    end_week = start_week + timedelta(days=7)

    # Filtrer les activités dans cette semaine
    mask = (df["start_date"] >= start_week) & (df["start_date"] < end_week)
    df_week = df.loc[mask]

    jours = ["Lundi","Mardi","Mercredi","Jeudi","Vendredi","Samedi","Dimanche"]

    if df_week.empty:
        return {
            "week": start_week.strftime("%d-%m-%Y"),
            "labels": jours,
            "datasets": [],
            "message": "Aucune activité sur cette semaine"
        }

    # Ajouter jour de la semaine
    df_week["weekday"] = df_week["start_date"].dt.weekday.apply(lambda i: jours[i])

    # Grouper par jour et sport
    grouped = df_week.groupby(["weekday", "sport_type"])["moving_time"].sum().reset_index()

    # Pivot table : jour x sport
    pivoted = grouped.pivot(index="weekday", columns="sport_type", values="moving_time").fillna(0)
    pivoted = pivoted.reindex(jours, fill_value=0)  # forcer l’ordre

    # Format JSON pour le frontend (chart empilé)
    datasets = []
    for sport in pivoted.columns:
        datasets.append({
            "label": sport,
            "data": pivoted[sport].tolist()
        })

    return {
        "week": start_week.strftime("%d-%m-%Y"),
        "labels": pivoted.index.tolist(),
        "datasets": datasets
    }
