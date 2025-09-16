from services.activity_service import *
import pandas as pd
from typing import List


def get_calendar_heatmap_data(df, value_col="distance"):
    """
    Prépare les données pour un calendrier / heatmap.
    Renvoie un dict JSON avec chaque jour et la valeur associée.

    :param df: DataFrame avec 'start_date' et la colonne à agréger (ex: distance, moving_time)
    :param value_col: colonne à sommer pour le heatmap
    """
    df = df.copy()
    df["start_date"] = pd.to_datetime(df["start_date"]).dt.tz_localize(None)

    # Créer une colonne "date" sans l'heure
    df["date"] = df["start_date"].dt.date

    # Agréger par jour
    df_daily = df.groupby("date")[value_col].sum().reset_index()

    # Préparer le JSON pour le front
    data = []
    for _, row in df_daily.iterrows():
        data.append({
            "date": row["date"].strftime("%Y-%m-%d"),
            value_col: row[value_col]
        })

    # Retourner le JSON complet
    return {"value_col": value_col, "data": data}


def get_repartition_run_data(df_filtered, sport_type):
    """
    Prépare les données pour un barplot horizontal du nombre d'activités
    par catégorie de distance pour un ou plusieurs sports donnés, à envoyer au front.

    :param df_filtered: DataFrame filtré par date
    :param sport_type: str ou liste de str des sports à inclure
    """
    # S'assurer que sport_type est une liste
    if isinstance(sport_type, str):
        sport_type = [sport_type]

    # Filtrer par sport_type
    df_sport = df_filtered[df_filtered["sport_type"].isin(sport_type)].copy()

    if df_sport.empty:
        return {
            "labels": [],
            "values": [],
            "message": "Aucune activité",
            "sport_type": sport_type
        }

    # Définir les catégories de distance
    def categorie_distance(d):
        if d < 10:
            return "Court (<10 km)"
        elif 10 <= d <= 20:
            return "Moyen (10-20 km)"
        else:
            return "Long (>20 km)"

    df_sport["categorie_distance"] = df_sport["distance"].apply(categorie_distance)

    # Compter le nombre d'activités par catégorie
    df_count = df_sport.groupby("categorie_distance").size().reindex(
        ["Court (<10 km)", "Moyen (10-20 km)", "Long (>20 km)"], fill_value=0
    )

    return {
        "labels": df_count.index.tolist(),
        "values": df_count.values.tolist(),
        "sport_type": sport_type
    }


def get_poster_elev_profile(n=40, sport_type: List[str] = None):
    activities = get_last_activities(n=n, sport_type=sport_type)
    poster_data = []

    for act in activities:
        activity_id = act["id"]
        streams = get_streams_for_activity(activity_id)
        if not streams:
            continue

        streams_simple = [
            {"distance_m": p["distance_m"], "altitude": p["altitude"], "time_s": p["time_s"]}
            for p in streams
        ]

        poster_data.append({
            "activity_id": activity_id,
            "type": act["type"],
            "distance_km": act["distance_km"],
            "duree_minutes": act["duree_minutes"],
            "allure_min_per_km": act["allure_min_per_km"],
            "denivele_m": act["denivele_m"],
            "stream_points": streams_simple
        })

    return poster_data
