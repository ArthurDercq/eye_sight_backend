import pandas as pd
import json
import polyline
from db.connection import get_conn


def get_all_activities():
    """Récupère toutes les activités dans un DataFrame."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM activites ORDER BY start_date DESC;")
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

    return pd.DataFrame(rows, columns=colnames)

def get_last_activity(sport_type=None):
    df = get_all_activities()
    if df.empty:
        return None

    if sport_type:
        df = df[df["sport_type"] == sport_type]

    if df.empty:
        return None

    #dernier_serie = df.sort_values(by="start_date", ascending=False).iloc[0]
    dernier = df.sort_values(by="start_date", ascending=False).iloc[0] #c'est une serie car une seule paire de corchets

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

def filter_activities(df: pd.DataFrame, sport_type: str = None, date_start: str = None) -> pd.DataFrame:
    """Applique les filtres usuels (sport + dates)."""
    if sport_type:
        df = df[df["sport_type"] == sport_type]

    if date_start:
        df = df[df["start_date"] >= pd.to_datetime(date_start)]

    return df



def _aggregate_data(df, value_col, freq, periods, sport_types=None):
    """
    Agrège les données par période (jour, semaine, mois).

    :param df: DataFrame avec au minimum 'start_date'
    :param value_col: colonne principale à agréger (ex: 'moving_time', 'distance')
    :param freq: fréquence pandas ('D' = jour, 'W-MON' = semaine commençant lundi, 'M' = mois)
    :param periods: nombre de périodes à afficher
    :param sport_types: liste des sports à filtrer (optionnel)
    """
    df = df.copy()
    df["start_date"] = pd.to_datetime(df["start_date"])

    if sport_types:
        df = df[df["sport_type"].isin(sport_types)]

    # colonnes disponibles dynamiquement
    agg_dict = {}
    if value_col in df.columns:
        agg_dict[value_col] = "sum"
    if "total_elevation_gain" in df.columns:
        agg_dict["total_elevation_gain"] = "sum"
    if "distance" in df.columns and value_col != "distance":
        agg_dict["distance"] = "sum"

    # Grouper par période
    df["period"] = df["start_date"].dt.to_period(freq).apply(lambda r: r.start_time)
    aggregated = df.groupby("period").agg(agg_dict).reset_index()

    # Créer toutes les périodes complètes
    all_periods = pd.date_range(
        start=df["period"].min(),
        end=df["period"].max(),
        freq=freq
    )
    aggregated = (
        aggregated.set_index("period")
        .reindex(all_periods, fill_value=0)
        .reset_index()
        .rename(columns={"index": "period"})
    )

    # Limiter au nombre demandé
    aggregated = aggregated.sort_values("period", ascending=False).head(periods).sort_values("period")

    return aggregated


def prepare_weekly_data(df, value_col="moving_time", weeks=12, sport_types=None):
    return _aggregate_data(df, value_col, freq="W-MON", periods=weeks, sport_types=sport_types)


def prepare_monthly_data(df, value_col="moving_time", months=12, sport_types=None):
    return _aggregate_data(df, value_col, freq="M", periods=months, sport_types=sport_types)


def prepare_daily_data(df, value_col="moving_time", days=30, sport_types=None):
    return _aggregate_data(df, value_col, freq="D", periods=days, sport_types=sport_types)


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

def aggregate_distance(df=None, period="W", sport_filter=None):
    """
    Agrège les distances parcourues selon la période et le type de sport.

    :param df: DataFrame avec toutes les activités (si None, sera récupéré depuis DB)
    :param period: "D"=jour, "W"=semaine, "M"=mois, "Y"=année
    :param sport_filter: liste de sports à inclure (ex: ["Run","Trail"]) ou None pour tous
    :return: DataFrame avec colonnes ['period', 'Run', 'Trail', 'Run&Trail', 'Bike', 'Swim']
    """
    if df is None:
        df = get_all_activities()
    if df.empty:
        return pd.DataFrame(columns=['period', 'Run', 'Trail', 'Run&Trail', 'Bike', 'Swim'])

    df = df.copy()
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["sport_type"] = df["sport_type"].map(lambda s: SPORT_MAPPING.get(s, s))

    # Ajouter colonne période
    df["period"] = df["start_date"].dt.to_period(period).apply(lambda r: r.start_time)

    # Filtrer sports si nécessaire
    if sport_filter:
        df = df[df["sport_type"].isin(sport_filter)]

    # Agrégation
    agg = (
        df.groupby(["period","sport_type"])["distance"]
        .sum()
        .reset_index()
        .pivot(index="period", columns="sport_type", values="distance")
        .fillna(0)
    )

    # Ajouter colonnes combinées
    agg["Run&Trail"] = agg.get("Run", 0) + agg.get("Trail", 0)

    # S'assurer que toutes les colonnes sont présentes
    for col in ["Run","Trail","Run&Trail","Bike","Swim"]:
        if col not in agg.columns:
            agg[col] = 0

    # Tri chronologique
    agg = agg.reset_index().sort_values("period")

    return agg


def prepare_weekly_avg_speed_by_sport(df, weeks=12, sport_types=None):
    """
    Calcule la vitesse moyenne par semaine selon le type de sport.

    :param df: DataFrame avec colonnes 'start_date', 'distance' (km), 'moving_time' (minutes), 'sport_type'
    :param weeks: nombre de semaines à afficher
    :param sport_types: liste de sports à inclure
    :return: DataFrame avec colonnes ['period', sport1, sport2, ...] et vitesses adaptées
    """
    df = df.copy()
    df["start_date"] = pd.to_datetime(df["start_date"])

    if sport_types:
        df = df[df["sport_type"].isin(sport_types)]

    # Créer la période hebdomadaire
    df["period"] = df["start_date"].dt.to_period("W-MON").apply(lambda r: r.start_time)

    # Fonction pour calculer la vitesse selon le sport
    def compute_speed(sub):
        sport = sub["sport_type"].iloc[0]
        if sport in ["Run","Trail"]:
            # minutes/km
            return (sub["moving_time"].sum() / sub["distance"].sum()) if sub["distance"].sum() > 0 else 0
        elif sport == "Bike":
            # km/h
            return (sub["distance"].sum() / (sub["moving_time"].sum() / 60)) if sub["moving_time"].sum() > 0 else 0
        elif sport == "Swim":
            # minutes/100 m
            return (sub["moving_time"].sum() / (sub["distance"].sum() * 10)) if sub["distance"].sum() > 0 else 0
        else:
            return 0

    # Grouper par période et sport_type
    weekly = df.groupby(["period","sport_type"]).apply(compute_speed).reset_index(name="avg_speed")

    # Pivot pour avoir colonnes par sport
    weekly_pivot = weekly.pivot(index="period", columns="sport_type", values="avg_speed").fillna(0)

    # Assurer toutes les colonnes existantes
    for col in ["Run","Trail","Bike","Swim"]:
        if col not in weekly_pivot.columns:
            weekly_pivot[col] = 0

    # Ajouter Run&Trail
    weekly_pivot["Run&Trail"] = weekly_pivot["Run"] + weekly_pivot["Trail"]

    # Reindex toutes les semaines même vides
    all_weeks = pd.date_range(start=df["period"].min(), end=df["period"].max(), freq="W-MON")
    weekly_pivot = weekly_pivot.reindex(all_weeks, fill_value=0).reset_index().rename(columns={"index": "period"})

    # Limiter au nombre de semaines demandé
    weekly_pivot = weekly_pivot.sort_values("period", ascending=False).head(weeks).sort_values("period")

    return weekly_pivot
