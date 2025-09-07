import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import matplotlib.ticker as mticker


def _prepare_weekly_data(df, value_col, weeks, sport_types=None):
    """
    Prépare les données agrégées par semaine.

    :param df: DataFrame contenant au minimum 'start_date', 'sport_type', 'distance', 'moving_time', 'total_elevation_gain'
    :param value_col: colonne à agréger ('moving_time', 'distance')
    :param weeks: nombre de semaines à afficher
    :param sport_types: liste des sports à filtrer ou None pour tous
    """
    df = df.copy()
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["week"] = df["start_date"].dt.to_period("W").apply(lambda r: r.start_time)

    if sport_types:
        df = df[df["sport_type"].isin(sport_types)]

    weekly = (
        df.groupby("week")
        .agg({
            value_col: "sum",
            "total_elevation_gain": "sum"
        })
        .reset_index()
        .sort_values("week", ascending=False)
        .head(weeks)
        .sort_values("week")  # on remet en ordre chronologique
    )
    return weekly

def _minutes_to_hms(minutes):
    """Convertit des minutes en format HH:MM:SS."""
    total_seconds = int(minutes * 60)
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def _plot_bar_with_dplus(weekly_df, value_label, color="skyblue"):
    """
    Trace un graphique barres + courbe D+.
    """
    fig, ax1 = plt.subplots(figsize=(8, 4))

    # Barres
    bars = ax1.bar(weekly_df["week"], weekly_df.iloc[:, 1], color=color, width=5, label=value_label)
    ax1.bar_label(bars, padding=3, fontsize=8, color="black") # Ajouter les valeurs au-dessus des barres

    ax1.set_ylabel(value_label, fontsize=10)
    ax1.tick_params(axis="y", labelsize=8)


    # Courbe D+ sur axe secondaire
    ax2 = ax1.twinx()
    ax2.plot(weekly_df["week"], weekly_df["total_elevation_gain"], color="lightcoral", linewidth = 2, alpha=0.4)
    ax2.get_yaxis().set_visible(False)  # cache ticks et labels

    for label in ax2.get_xticklabels() + ax2.get_yticklabels():
        label.set_color("#5C6A7D")
        label.set_fontsize(8)


    # Couleur des axes (spines)
    for spine in ax1.spines.values():
        spine.set_color("#5C6A7D")

    # Couleur + taille des ticks
    ax1.tick_params(axis="both", colors="#5C6A7D", labelsize=8)

    # Couleur des labels (valeurs sur les ticks)
    for label in ax1.get_xticklabels() + ax1.get_yticklabels():
        label.set_color("#5C6A7D")
        label.set_fontsize(8)


    # Formatage de l'axe X → une étiquette par semaine
    ax1.set_xticks(weekly_df["week"])
    ax1.set_xticklabels(weekly_df["week"].dt.strftime("%d %b %Y"), rotation=45, ha="right", fontsize=8)

    ax1.set_ylabel(value_label, fontsize=8, color="#5C6A7D")

    # Garder X et Y, enlever seulement top et right
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    fig.autofmt_xdate()
    fig.tight_layout()
    return fig

def plot_hours_bar(weekly_df, value_label="Heures de sport", color="#B4B3E5"):
    fig, ax = plt.subplots(figsize=(10, 4))

    # Extraire les valeurs en minutes
    values_in_minutes = weekly_df.iloc[:, 1]

    # Barres principales
    bars = ax.bar(
        weekly_df["week"],
        values_in_minutes / 60,  # on garde l'axe Y en heures
        color=color,
        width=5
    )

    # Valeurs au-dessus des barres en HH:MM:SS
    labels = [_minutes_to_hms(m) for m in values_in_minutes]
    ax.bar_label(bars, labels=labels, padding=3) #, fontsize=8, color="black"
    # Style

    # Couleur des axes (spines)
    for spine in ax.spines.values():
        spine.set_color("#5C6A7D")

    # Couleur + taille des ticks
    ax.tick_params(axis="both", colors="#5C6A7D", labelsize=8)

    # Couleur des labels (valeurs sur les ticks)
    for label in ax.get_xticklabels() + ax.get_yticklabels():
        label.set_color("#5C6A7D")
        label.set_fontsize(8)

    ax.set_xlabel("",fontsize=8, color="#5C6A7D")
    # Axe Y (en heures)
    ax.set_ylabel(value_label, fontsize=8, color="#5C6A7D")
    ax.tick_params(axis="y") #, labelsize=8

    # Axe X → semaine
    ax.set_xticks(weekly_df["week"])
    ax.set_xticklabels(
        weekly_df["week"].dt.strftime("%d %b %Y"),
        rotation=45, ha="right"
    ) #, fontsize=8

    # Cadre : enlever top et right
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.tight_layout()
    return fig

def plot_heartrate(df, value_label="Fréquence cardiaque en bpm", color="skyblue"):

    fig, ax = plt.subplots(figsize=(10, 4))

    # X = positions (0, 1, 2...)
    x_pos = range(len(df))

    # Courbe
    ax.plot(x_pos, df["average_heartrate"], marker='o', color=color)

    # Axe Y
    ax.set_ylabel(value_label) #, fontsize=10
    ax.tick_params(axis="y") #, labelsize=8

    # Axe X : afficher les dates
    ax.set_xticks(x_pos)
    ax.set_xticklabels(
        df["start_date"].dt.strftime("%d %b"),
        rotation=45, ha="right"
    ) #, fontsize=8

    # Cadre : enlever top et right
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.tight_layout()
    return fig

def plot_hours_per_week(df, weeks=10):
    weekly_df = _prepare_weekly_data(df, "moving_time", weeks, sport_types=["Run", "Trail", 'Bike', 'Swim'])
    return plot_hours_bar(weekly_df, "Heures de sport", color="#B4B3E5")

def plot_run_trail_km_per_week(df, weeks=10):
    weekly_df = _prepare_weekly_data(df, "distance", weeks, sport_types=["Run", "Trail"])
    return _plot_bar_with_dplus(weekly_df, "Run & Trail (kms) ", color="#373AF1")

def plot_bike_km_per_week(df, weeks=10):
    weekly_df = _prepare_weekly_data(df, "distance", weeks, sport_types=["Bike"])
    return _plot_bar_with_dplus(weekly_df, "Vélo (kms)", color="#8A5CFC")

def plot_swim_km_per_week(df, weeks=10):
    weekly_df = _prepare_weekly_data(df, "distance", weeks, sport_types=["Swim"])
    return _plot_bar_with_dplus(weekly_df, "Natation (kms)", color="#D942F7")

def run_week_progress(df, objectif_km=50):
    """
    Affiche une barre de progression pour le Run/Trail de la semaine en cours.

    Args:
        df (pd.DataFrame): DataFrame avec colonnes 'start_date', 'sport_type', 'distance_km'
        objectif_km (float): objectif en km
    """
    # Filtrer pour Run et Trail
    df_run = df[df['sport_type'].isin(['Run', 'Trail'])].copy()

    # Date du début de la semaine (lundi)
    today = pd.Timestamp(datetime.today())
    start_week = today - pd.Timedelta(days=today.weekday())
    end_week = start_week + pd.Timedelta(days=6)

    # Filtrer les activités de la semaine
    df_week = df_run[(df_run['start_date'] >= start_week) & (df_run['start_date'] <= end_week)]

    # Total km cette semaine
    km_total = df_week['distance'].sum()

    # Calcul de la progression
    progression = min(km_total / objectif_km, 1.0)  # max 100%

    return progression, km_total, start_week.strftime('%d/%m/%Y'), end_week.strftime('%d/%m/%Y'), objectif_km


# 🎨 Palette couleurs graphique
SPORT_COLORS = {
    "Run": "#6466EA",       # orange
    "Trail": "#373AF1",  # même que Run
    "Bike": "#8B5CFC",      # bleu
    "Swim": "#D942F7",      # vert
    "Workout" : "#633C8F"   # bleu clair
}

def plot_weekly_intensity(df, week_start, week_end):

    df["start_date"] = pd.to_datetime(df["start_date"]).dt.tz_localize(None)
    df_week = df[(df["start_date"] >= week_start) & (df["start_date"] < week_end + pd.Timedelta(days=1))].copy()

    if df_week.empty:
        # --- Figure vide avec message central ---
        fig, ax = plt.subplots(figsize=(7, 3))
        ax.text(0.5, 0.5, "Aucune activité cette semaine",
                ha="center", va="center", fontsize=12, color="gray")
        ax.axis("off")
        return fig

    df_week["day"] = df_week["start_date"].dt.day_name(locale="fr_FR").str.lower()  # "lundi", "mardi", etc.
    # Ordre fixe des jours (toujours lundi→dimanche)
    days_order = ["lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"]


    # Agrégation par jour + sport
    df_grouped = (
        df_week.groupby(["day","sport_type"])["elapsed_time"]
        .sum()
        .reset_index()
    )
    # Pivot pour barres empilées
    df_pivot = df_grouped.pivot(index="day", columns="sport_type", values="elapsed_time").fillna(0)
    df_pivot = df_pivot.reindex(days_order)

    # --- Graphique ---
    fig, ax = plt.subplots(figsize=(7,3))

    df_pivot.plot(
        kind="bar",
        stacked=True,
        ax=ax,
        color=[SPORT_COLORS.get(s, "gray") for s in df_pivot.columns]
    )

    # Style

    # Couleur des axes (spines)
    for spine in ax.spines.values():
        spine.set_color("#5C6A7D")

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Couleur + taille des ticks
    ax.tick_params(axis="both", colors="#5C6A7D", labelsize=8)

    # Couleur des labels (valeurs sur les ticks)
    for label in ax.get_xticklabels() + ax.get_yticklabels():
        label.set_color("#5C6A7D")
        label.set_fontsize(8)

    ax.set_xticklabels([d.capitalize() for d in days_order], rotation=45, ha="right")


    ax.set_ylabel("Minutes", fontsize=8, color="#5C6A7D")
    ax.set_xlabel("")

    ax.legend(frameon=False, bbox_to_anchor=(1,1))
    #ax.yaxis.set_major_locator(mticker.MultipleLocator(50))

    return fig



def plot_repartition_run(df_filtered, sport_type):
    """
    Retourne une figure matplotlib d'un barplot horizontal du nombre d'activités
    par catégorie de distance pour un sport donné.

    Parameters:
        df_filtered (pd.DataFrame): DataFrame filtré sur la semaine et le sport sélectionné
        sport_type (str): 'Run' ou 'Trail'

    Returns:
        matplotlib.figure.Figure
    """

    # Filtrer le sport choisi
    df_sport = df_filtered[df_filtered["sport_type"] == sport_type].copy()

    if df_sport.empty:
        return None  # pas de données

    # Définir la catégorie de distance
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

    # Créer la figure
    fig, ax = plt.subplots(figsize=(6,4))
    ax.barh(df_count.index, df_count.values, color='#6466EA',height=0.2)

    # Ajouter les valeurs à côté des barres
    for i, v in enumerate(df_count.values):
        ax.text(v + 0.1, i, str(v), va='center')



    # Couleur + taille des ticks
    ax.tick_params(axis="both", colors="#5C6A7D", labelsize=8)
    # Couleur des labels (valeurs sur les ticks)
    for label in ax.get_xticklabels() + ax.get_yticklabels():
        label.set_color("#5C6A7D")
        label.set_fontsize(8)


    ax.set_xlabel("Nombre d'activités",fontsize=8, color="#5C6A7D")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_visible(False)


    # Axe X de 0 au nombre max d'activités
    max_activites = df_count.values.max()
    ax.set_xlim(0, max_activites + 1)
    ax.set_xticks(range(0, max_activites + 1, 1))
    plt.tight_layout()

    ax.tick_params(left=False, bottom=False, labelleft=True, labelbottom=False)


    return fig
