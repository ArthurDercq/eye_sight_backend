from services.activity_service import minutes_to_hms
import pandas as pd

SPORT_COLORS = {
    "Run": "#6466EA",       # orange
    "Trail": "#373AF1",  # même que Run
    "Bike": "#8B5CFC",      # bleu
    "Swim": "#D942F7",      # vert
    "Workout" : "#633C8F"   # bleu clair
}


def get_hours_bar_data(weekly_df, value_col="moving_time"):
    """
    Prépare les données pour un bar chart de type 'heures de sport'.
    Renvoie un dict JSON prêt à être consommé par le frontend.

    :param weekly_df: DataFrame contenant au moins 'week' et la colonne à agréger
    :param value_col: colonne contenant le temps en minutes
    """
    data = []
    for _, row in weekly_df.iterrows():
        entry = {
            "week": row["week"].strftime("%Y-%m-%d"),
            "value_minutes": row[value_col],
            "value_hms": minutes_to_hms(row[value_col])
        }
        data.append(entry)

    result = {
        "value_label": "Heures de sport",
        "data": data
    }
    return result



def get_weekly_intensity_data(df, week_start, week_end):
    """
    Prépare les données pour un stacked bar chart d'intensité hebdomadaire.
    Renvoie un dict JSON pour le frontend.

    :param df: DataFrame avec au moins 'start_date', 'sport_type', 'elapsed_time'
    :param week_start: datetime début de semaine
    :param week_end: datetime fin de semaine
    """
    df = df.copy()
    df["start_date"] = pd.to_datetime(df["start_date"]).dt.tz_localize(None)
    df_week = df[(df["start_date"] >= week_start) & (df["start_date"] < week_end + pd.Timedelta(days=1))]

    # Ordre fixe des jours
    days_order = ["lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"]

    if df_week.empty:
        return {"message": "Aucune activité cette semaine", "data": []}

    df_week["day"] = df_week["start_date"].dt.day_name(locale="fr_FR").str.lower()

    # Agrégation par jour et sport
    df_grouped = (
        df_week.groupby(["day", "sport_type"])["elapsed_time"]
        .sum()
        .reset_index()
    )

    # Pivot pour barres empilées, remplir 0 si aucune donnée
    df_pivot = df_grouped.pivot(index="day", columns="sport_type", values="elapsed_time").fillna(0)
    df_pivot = df_pivot.reindex(days_order, fill_value=0)

    # Préparer JSON
    data = []
    for day in days_order:
        day_entry = {"day": day}
        for sport in df_pivot.columns:
            day_entry[sport] = df_pivot.loc[day, sport]
            day_entry[f"{sport}_hms"] = minutes_to_hms(df_pivot.loc[day, sport])
            day_entry[f"{sport}_color"] = SPORT_COLORS.get(sport, "gray")
        data.append(day_entry)

    return {"week_start": week_start.strftime("%Y-%m-%d"),
            "week_end": week_end.strftime("%Y-%m-%d"),
            "data": data}



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
