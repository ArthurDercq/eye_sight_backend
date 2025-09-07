from __future__ import annotations
import json
import polyline
import folium
import matplotlib.pyplot as plt

def create_latest_activity_map(df):
    """
    Crée une carte Folium de la dernière activité selon start_date.

    Args:
        df (pd.DataFrame): DataFrame avec une colonne 'map' (JSON string) et 'start_date' (datetime).

    Returns:
        folium.Map ou None
    """

    # Trier le DataFrame par date décroissante (la plus récente en premier)
    df_sorted = df.sort_values('start_date', ascending=False)

    # Prendre la première ligne (dernière activité)
    latest_activity = df_sorted.iloc[0]

    # Charger le JSON de la colonne map
    map_json = json.loads(latest_activity['map'])

    # Extraire la polyline
    polyline_str = map_json.get('summary_polyline')
    if not polyline_str:
        print("Pas de polyline disponible pour la dernière activité.")
        return None

    # Décoder la polyline
    coords = polyline.decode(polyline_str)

    # Créer la carte centrée sur le premier point
    m = folium.Map(location=coords[0], zoom_start=13)

    # Ajouter la trace
    folium.PolyLine(coords, color='#6466EA', weight=5).add_to(m)

    return m





def plot_mini_map(df):

    """
    Crée une carte Folium de la dernière activité selon start_date.

    Args:
        df (pd.DataFrame): DataFrame avec une colonne 'map' (JSON string) et 'start_date' (datetime).

    Returns:
        la trace sur fond transparent d'une activité
    """

    # Trier le DataFrame par date décroissante (la plus récente en premier)
    df_sorted = df.sort_values('start_date', ascending=False)

    # Prendre la première ligne (dernière activité)
    latest_activity = df_sorted.iloc[0]

    # Charger le JSON de la colonne map
    map_json = json.loads(latest_activity['map'])

    # Extraire la polyline
    polyline_str = map_json.get('summary_polyline')
    if not polyline_str:
        print("Pas de polyline disponible pour la dernière activité.")
        return None
    # Décoder la polyline
    coords = polyline.decode(polyline_str)  # renvoie une liste de tuples (lat, lon)

    # Séparer latitude et longitude
    lats, lons = zip(*coords)

    # Tracer
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.plot(lons, lats, color="#6466EA", linewidth=2)
    ax.axis('off')           # cacher axes et ticks
    ax.set_aspect('equal')   # garder les proportions


    return fig
