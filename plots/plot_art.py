import json
import polyline
import matplotlib.pyplot as plt
import io
import pandas as pd
import math
import contextily as ctx
import geopandas as gpd
from shapely.geometry import LineString
from sqlalchemy import create_engine
from strava.params import DB_URI



def create_latest_activity_poster(df):
    """
    Crée une affiche minimaliste (statique) de la dernière activité.

    Args:
        df (pd.DataFrame): DataFrame contenant au minimum :
                           - 'map' (dict ou JSON string avec 'summary_polyline')
                           - 'sport_type' (str)
                           - 'start_date' (datetime)
                           - 'distance' (float, en km)
                           - 'elapsed_time_hms' (formatée)
        save_path (str): chemin d'export de l'image finale (PNG).
    """

    plt.rcParams["figure.dpi"] = 300   # rendu à l'écran

    if df.empty:
        print("Df empty")
        return None

    # Trier par date décroissante
    df_sorted = df.sort_values('start_date', ascending=False)
    latest_activity = df_sorted.iloc[0]

    latest_activity_date = latest_activity["start_date"]
    latest_activity_date = pd.to_datetime(latest_activity_date)
    latest_activity_date_str = latest_activity_date.strftime("%d/%m/%Y")


    # Charger map
    map_data = latest_activity['map']
    if isinstance(map_data, str):
        try:
            map_data = json.loads(map_data)
        except Exception as e:
            print(f"Erreur JSON dans map : {e}")
            return None

    # Extraire polyline
    polyline_str = map_data.get("summary_polyline")
    if not polyline_str:
        print("Pas de polyline disponible.")
        return None

    coords = polyline.decode(polyline_str)
    lats, lons = zip(*coords)

    # Distance, temps et d+
    distance = latest_activity.get("distance", None)
    elapsed_time = latest_activity.get("elapsed_time_hms", None)
    dplus = latest_activity.get("total_elevation_gain", None)


    # --- 🎨 Création affiche ---
    fig, ax = plt.subplots(figsize=(8, 10))

    # Trace minimaliste
    ax.plot(lons, lats, color="white", linewidth=2)

    # Supprimer axes
    ax.set_axis_off()
    ax.set_facecolor("black")
    fig.patch.set_facecolor('black')


    # Nom de l'activité en haut
    fig.text(
        0.5, 0.95,              # 95% de la hauteur de la figure
        latest_activity["name"], # nom de l'activité
        ha="center",
        va="top",
        color="white",
        fontsize=20,
        family="monospace"
    )

    # Ajouter les datas principales
    if distance and elapsed_time and dplus:
        fig.text(
            0.5, -0.05,
            f"{distance:.1f} km | {elapsed_time} | {dplus:.0f} m D+",
            ha="center", va="top",
            color="white", fontsize=16, family="monospace"
        )

    # Date en dessous
    if latest_activity_date_str :
        fig.text(
            0.5, 0.015,  # légèrement plus bas
            latest_activity_date_str,    # formatée en "JJ/MM/AAAA"
            ha="center",
            va="bottom",
            color="white",
            fontsize=9,  # plus petite
            family="monospace"
        )


    return fig


def create_latest_activity_poster_test(df, sport_type, zoom_out=0.2):
    """
    Crée une affiche minimaliste (statique) de la dernière activité avec fond map noir/blanc.
    zoom_out : marge ajoutée autour du tracé (0.2 = 20%)
    """

    df_sport = df[df['sport_type'] == sport_type]

    if df_sport.empty:
        print("Aucune activité Trail trouvée.")
        return None

    # Trier par date décroissante
    df_sorted = df_sport.sort_values('start_date', ascending=False)
    latest_activity = df_sorted.iloc[0]

    latest_activity_date = pd.to_datetime(latest_activity["start_date"])
    latest_activity_date_str = latest_activity_date.strftime("%d/%m/%Y")

    # Charger map
    map_data = latest_activity['map']
    if isinstance(map_data, str):
        try:
            map_data = json.loads(map_data)
        except Exception as e:
            print(f"Erreur JSON dans map : {e}")
            return None

    # Extraire polyline
    polyline_str = map_data.get("summary_polyline")
    if not polyline_str:
        print("Pas de polyline disponible.")
        return None

    coords = polyline.decode(polyline_str)

    # Convertir coords (lat, lon) → (lon, lat)
    line_coords = [(lon, lat) for lat, lon in coords]

    # GeoDataFrame
    gdf = gpd.GeoDataFrame(geometry=[LineString(line_coords)], crs="EPSG:4326")
    gdf = gdf.to_crs(epsg=3857)

    # Distance et temps
    distance = latest_activity.get("distance", None)
    elapsed_time = latest_activity.get("elapsed_time_hms", None)
    dplus = latest_activity.get("total_elevation_gain", None)

    # --- 🎨 Création affiche ---
    fig, ax = plt.subplots(figsize=(8, 10))

    # Plot polyline
    gdf.plot(ax=ax, color="white", linewidth=1)

    # --- 🔎 Gestion du dézoom ---
    xmin, ymin, xmax, ymax = gdf.total_bounds
    xmargin = (xmax - xmin) * zoom_out
    ymargin = (ymax - ymin) * zoom_out
    ax.set_xlim(xmin - xmargin, xmax + xmargin)
    ax.set_ylim(ymin - ymargin, ymax + ymargin)

    # Ajouter fond noir & blanc
    ctx.add_basemap(ax, source=ctx.providers.CartoDB.DarkMatter, alpha=1)
    ax.set_axis_off()

    # Nom de l'activité en haut
    fig.text(
        0.5, 0.95,
        latest_activity["name"],
        ha="center", va="top",
        color="white", fontsize=20, family="monospace"
    )

    # Ligne principale : distance, temps, D+
    if distance is not None and elapsed_time is not None and dplus is not None:
        fig.text(
            0.5, 0.03,
            f"{distance:.1f} km | {elapsed_time} | {dplus:.0f} m D+",
            ha="center", va="bottom",
            color="white", fontsize=16, family="monospace"
        )

    # Date en dessous
    fig.text(
        0.5, 0.015,
        latest_activity_date_str,
        ha="center", va="bottom",
        color="white", fontsize=9, family="monospace"
    )

    # Export
    plt.savefig("affiche_trail_ctx.png", dpi=300, bbox_inches="tight", facecolor="black")

    return fig


def plot_mini_maps_grid(df, year, sport):

    """
    Crée un grid avec toutes les traces des activités

    Args:
        - df (pd.DataFrame): DataFrame avec une colonne 'map' (JSON string) et 'start_date' (datetime).
        - l'année concernée (améliorer pour pouvoir prendre plusieurs années ?)
        - une liste de sport à afficher

    Returns:
        un grid artistique
    """


    plt.rcParams["figure.dpi"] = 300   # rendu à l'écran

    # --- Filtrage sur 2025 et Run/Trail ---
    df_filtered = df[
        (pd.to_datetime(df["start_date"]).dt.year == year) &
        (df["sport_type"].isin(sport))
    ].copy()

    # --- Supprimer activités sans map ou polyline ---
    df_filtered = df_filtered[
        df_filtered["map"].apply(lambda x: bool(json.loads(x).get("summary_polyline")))
    ]

    # Trier du plus ancien au plus récent
    df_filtered = df_filtered.sort_values("start_date", ascending=True)

    n = len(df_filtered)
    if n == 0:
        print("Aucune activité Run ou Trail en 2025 avec polyline")
        return None

    # --- Calcul grille dynamique ---
    n_cols = math.ceil(math.sqrt(n))
    n_rows = math.ceil(n / n_cols)

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(n_cols, n_rows), gridspec_kw={"wspace": 0.5, "hspace": 0.5})
    axes = axes.flatten()

    for i, (_, activity) in enumerate(df_filtered.iterrows()):
        try:
            map_json = json.loads(activity["map"])
            polyline_str = map_json.get("summary_polyline")
            coords = polyline.decode(polyline_str)
            lats, lons = zip(*coords)

            axes[i].plot(lons, lats, color="black", linewidth=0.3)
            axes[i].axis("off")
            axes[i].set_aspect("equal")
        except Exception as e:
            print(f"Erreur activité {activity['activity_id']}: {e}")

    # Cases vides (si grille > n)
    for j in range(i+1, len(axes)):
        axes[j].axis("off")

    return fig



def get_latest_run_trail_activity_ids(db_uri, table_name="activites", n=100):
    """
    Récupère les n derniers activity_id des activités Run et Trail depuis la table activites.
    """
    engine = create_engine(db_uri)
    query = f"""
        SELECT id
        FROM {table_name}
        WHERE sport_type IN ('Run', 'Trail')
        ORDER BY start_date DESC
        LIMIT {n}
    """
    df_ids = pd.read_sql(query, engine)
    # Convertir en string pour la requête sur streams
    return [str(i) for i in df_ids["id"].tolist()]


def fetch_streams_for_activity_ids(activity_ids, db_uri, table_name="streams"):
    """
    Récupère tous les streams pour une liste d'activity_id depuis PostgreSQL.
    Retourne un DataFrame avec toutes les données.
    """
    engine = create_engine(db_uri)
    # Crée une requête SQL avec IN (...)
    placeholders = ','.join(['%s'] * len(activity_ids))
    query = f"""
        SELECT lat, lon, altitude, distance_m, time_s, activity_id
        FROM {table_name}
        WHERE activity_id IN ({placeholders})
        ORDER BY activity_id, time_s
    """
    df_streams = pd.read_sql(query, engine, params=tuple(activity_ids))
    return df_streams




def plot_trail_profiles_df(df_streams, n_cols=5):
    """
    Trace les profils de dénivelé en version minimaliste artistique à partir du DataFrame des streams.

    Args:
        df_streams (pd.DataFrame): doit contenir 'activity_id', 'distance_m', 'altitude'
        n_cols (int): nombre de colonnes dans la grille
    """
    activity_ids = df_streams["activity_id"].unique()
    n_activities = len(activity_ids)
    if n_activities == 0:
        print("Aucune donnée à tracer.")
        return

    n_rows = math.ceil(n_activities / n_cols)
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(4*n_cols, 2.5*n_rows), gridspec_kw={"wspace": 0.5, "hspace": 0.5})
    axes = axes.flatten()

    for i, activity_id in enumerate(activity_ids):
        data = df_streams[df_streams["activity_id"] == activity_id]
        ax = axes[i]
        ax.plot(data["distance_m"] / 1000, data["altitude"], color="black", linewidth=1)
        #ax.fill_between(data["distance_m"] / 1000, data["altitude"], alpha=0.2, color="gray")
        ax.axis("off")

    # Supprimer axes vides
    for j in range(n_activities, len(axes)):
        fig.delaxes(axes[j])

    return fig


def plot_trail_profiles_db(n):

    # 1. Récupère les IDs d'activités choisis
    activity_ids = get_latest_run_trail_activity_ids(DB_URI, n=n)

    # 2. Récupère tous les streams liés à ces IDs
    df_streams = fetch_streams_for_activity_ids(activity_ids, DB_URI)

    fig = plot_trail_profiles_df(df_streams)

    return fig
