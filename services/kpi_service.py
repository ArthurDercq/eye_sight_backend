import pandas as pd
from services.activity_service import get_all_activities
from datetime import datetime, timedelta


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
            "total_km_run": None,
            "total_km_trail": None,
            "total_km_run_trail": None,
            "total_km_bike": None,
            "total_km_swim": None,
            "total_hours": None,
            "total_dplus_run": None,
            "total_dplus_trail": None,
            "total_dplus_run_trail": None,
            "total_dplus_bike": None
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
        # Add 1 day to include all activities on end_date (up to 23:59:59)
        end_date = end_date + pd.Timedelta(days=1)
        df = df[df["start_date"] < end_date]

    # Calcul des KPIs
    total_km_run = df[df["sport_type"] == "Run"]["distance"].sum()
    total_km_trail = df[df["sport_type"] == "Trail"]["distance"].sum()
    total_km_run_trail = total_km_run + total_km_trail
    total_km_bike = df[df["sport_type"] == "Bike"]["distance"].sum()
    total_km_swim = df[df["sport_type"] == "Swim"]["distance"].sum()

    total_dplus_run = df[df["sport_type"] == "Run"]["total_elevation_gain"].sum()
    total_dplus_trail = df[df["sport_type"] == "Trail"]["total_elevation_gain"].sum()
    total_dplus_run_trail = total_dplus_run + total_dplus_trail
    total_dplus_bike = df[df["sport_type"] == "Bike"]["total_elevation_gain"].sum()

    # Total heures de sport (elapsed_time en secondes → heures)
    total_hours = df["elapsed_time"].sum() / 60 if "elapsed_time" in df.columns else 0

    #Nombre d'activités par type de sport
    activity_counts = df["sport_type"].value_counts().to_dict()

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
        "total_dplus_bike": round(total_dplus_bike, 2),
        "nombre d'activités par sport": activity_counts
    }


def calculate_streak():
    """
    Calcule la série d'activités hebdomadaires consécutives.
    Conditions pour qu'une semaine compte:
    - Au moins 1 activité
    - Au moins 5 km courus (Run ou Trail)

    Retourne:
    - streak_weeks: nombre de semaines consécutives
    - total_activities: nombre total d'activités dans la streak
    """
    df = get_all_activities()
    if df.empty:
        return {"streak_weeks": 0, "total_activities": 0}

    # Normaliser les dates et sports
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["sport_type"] = df["sport_type"].map(lambda s: SPORT_MAPPING.get(s, s))

    # Trier par date décroissante (du plus récent au plus ancien)
    df = df.sort_values("start_date", ascending=False)

    # Calculer le numéro de semaine ISO (année, semaine)
    df["year"] = df["start_date"].dt.isocalendar().year
    df["week"] = df["start_date"].dt.isocalendar().week
    df["year_week"] = df["year"].astype(str) + "-W" + df["week"].astype(str).str.zfill(2)

    # Obtenir la semaine actuelle
    now = datetime.now()
    current_year = now.isocalendar().year
    current_week = now.isocalendar().week
    current_year_week = f"{current_year}-W{str(current_week).zfill(2)}"

    # Grouper par semaine
    weekly_groups = df.groupby("year_week").apply(
        lambda group: {
            "year": group["year"].iloc[0],
            "week": group["week"].iloc[0],
            "activity_count": len(group),
            "run_trail_distance": group[group["sport_type"].isin(["Run", "Trail"])]["distance"].sum(),
            "activities": group.index.tolist()
        },
        include_groups=False
    ).to_dict()

    # Générer la liste de toutes les semaines depuis la plus récente
    streak_weeks = 0
    total_activities = 0

    # Commencer à partir de la semaine actuelle
    check_year = current_year
    check_week = current_week

    while True:
        year_week_key = f"{check_year}-W{str(check_week).zfill(2)}"

        if year_week_key in weekly_groups:
            week_data = weekly_groups[year_week_key]
            # Vérifier les conditions: au moins 1 activité ET au moins 5 km Run/Trail
            if week_data["activity_count"] >= 1 and week_data["run_trail_distance"] >= 5:
                streak_weeks += 1
                total_activities += week_data["activity_count"]
            else:
                # La semaine ne valide pas les conditions, on arrête la streak
                break
        else:
            # Pas d'activité cette semaine, on arrête la streak
            break

        # Passer à la semaine précédente
        check_week -= 1
        if check_week < 1:
            # Passer à l'année précédente
            check_year -= 1
            # Obtenir le nombre de semaines dans l'année précédente
            last_day_of_year = datetime(check_year, 12, 31)
            check_week = last_day_of_year.isocalendar().week

    return {
        "streak_weeks": streak_weeks,
        "total_activities": total_activities
    }
