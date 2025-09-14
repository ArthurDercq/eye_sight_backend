import pandas as pd
from services.activity_service import get_all_activities


SPORT_MAPPING = {
    "TrailRun": "Trail",
    "Run": "Run",
    "Ride": "Bike",
    "Swim": "Swim"
}

def prepare_kpis(start_date=None, end_date=None):
    """
    Calcule les KPIs globaux pour les activités de l'utilisateur.

    :param start_date: datetime ou str (YYYY-MM-DD), filtre la période
    :param end_date: datetime ou str (YYYY-MM-DD), filtre la période
    :return: dict avec les KPIs
    """
    df = get_all_activities()
    if df.empty:
        return {
            "total_km_run": 0,
            "total_km_trail": 0,
            "total_km_run_trail": 0,
            "total_km_bike": 0,
            "total_km_swim": 0,
            "total_hours": 0
        }

    # Normaliser les dates et sports
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["sport_type"] = df["sport_type"].map(lambda s: SPORT_MAPPING.get(s, s))

    # Filtrage par période
    if start_date:
        start_date = pd.to_datetime(start_date)
        df = df[df["start_date"] >= start_date]
    if end_date:
        end_date = pd.to_datetime(end_date)
        df = df[df["start_date"] <= end_date]

    # Calcul des KPIs
    total_km_run = df[df["sport_type"] == "Run"]["distance"].sum()
    total_km_trail = df[df["sport_type"] == "Trail"]["distance"].sum()
    total_km_run_trail = total_km_run + total_km_trail
    total_km_bike = df[df["sport_type"] == "Bike"]["distance"].sum()
    total_km_swim = df[df["sport_type"] == "Swim"]["distance"].sum()

    total_dplus_run = df[df["sport_type"] == "Run"]["total_elevation_gain"].sum()
    total_dplus_trail = df[df["sport_type"] == "Trail"]["total_elevation_gain"].sum()
    total_dplus_run_trail = total_km_run + total_km_trail
    total_dplus_bike = df[df["sport_type"] == "Bike"]["total_elevation_gain"].sum()

    # Total heures de sport (elapsed_time en secondes → heures)
    total_hours = df["elapsed_time"].sum() / 60 if "elapsed_time" in df.columns else 0

    return {
        "total_km_run": round(total_km_run, 2),
        "total_km_trail": round(total_km_trail, 2),
        "total_km_run_trail": round(total_km_run_trail, 2),
        "total_km_bike": round(total_km_bike, 2),
        "total_km_swim": round(total_km_swim, 2),
        "total_hours": round(total_hours, 2),
        "total_dplus_run": round(total_dplus_run, 2),
        "total_dplus_trail": round(total_dplus_trail, 2),
        "total_dplus_run_trail": round(total_dplus_run_trail, 2),
        "total_dplus_bike": round(total_dplus_bike, 2)

    }
