import math
import matplotlib.pyplot as plt
from strava.store_data import *
from strava.clean_data import *
from strava.fetch_strava import *
from strava.params import *



def get_ids(df, sport_stype, nb_acti):
    """
    Récupère les ids et les extrait sous forme de liste.

    Args:
        df: ma df générale
        sport_type (string): pour quel sport je veux afficher mes traces

    Returns:
        liste: activity_id
    """

    df_selected_sport = df[df["sport_type"] == sport_stype].sort_values("start_date", ascending=False)

    activity_ids = df_selected_sport["id"].head(nb_acti).tolist()

    return activity_ids

def get_strava_headers():
    payload = {
    'client_id': STRAVA_CLIENT_ID,
    'client_secret': STRAVA_CLIENT_SECRET,
    'refresh_token': STRAVA_REFRESH_TOKEN,
    'grant_type': "refresh_token",
    'f': 'json'
    }
    res = requests.post(AUTH_URL, data=payload, verify=False)
    access_token = res.json()['access_token']
    headers = {"Authorization": f"Bearer {access_token}"}

    return headers


def fetch_trail_streams(activity_ids, headers):
    """
    Récupère les streams (distance, altitude) pour une liste d'activités.

    Args:
        activity_ids (list): liste d'IDs d'activités Strava
        headers (dict): headers Strava API (Bearer token)

    Returns:
        dict: {activity_id: {"distance": [...], "altitude": [...]}}
    """
    streams_dict = {}
    for activity_id in activity_ids:
        url = f"https://www.strava.com/api/v3/activities/{activity_id}/streams"
        params = {"keys": "altitude,distance", "key_by_type": "true"}
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        streams = resp.json()

        distance = streams.get("distance", {}).get("data", [])  # km
        altitude = streams.get("altitude", {}).get("data", [])  # m

        if distance and altitude:
            streams_dict[activity_id] = {"distance": distance, "altitude": altitude}

    return streams_dict


def plot_trail_profiles(streams_dict,df, n_cols=5):
    """
    Trace les profils de dénivelé en version minimaliste artistique à partir des données déjà récupérées.

    Args:
        streams_dict (dict): {activity_id: {"distance": [...], "altitude": [...]} }
        n_cols (int): nombre de colonnes dans la grille
    """
    activity_ids = list(streams_dict.keys())
    n_activities = len(activity_ids)
    if n_activities == 0:
        print("Aucune donnée à tracer.")
        return

    n_rows = math.ceil(n_activities / n_cols)
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(4*n_cols, 2.5*n_rows), gridspec_kw={"wspace": 0.5, "hspace": 0.5})
    axes = axes.flatten()

    for i, activity_id in enumerate(activity_ids):
        data = streams_dict[activity_id]
        ax = axes[i]
        ax.plot(data["distance"], data["altitude"], color="black", linewidth=1)
        #ax.fill_between(data["distance"], data["altitude"], alpha=0.2, color="gray")
        ax.axis("off")

        # --- Ajouter la date en semi-transparence
        date = df.loc[df["id"] == activity_id, "start_date"].iloc[0]
        ax.text(
            0.5, -0.1, date.strftime("%d-%m-%Y"),
            fontsize=8, color="black", alpha=0.3, ha="center", va="bottom",transform=ax.transAxes
        )

    # Supprimer axes vides
    for j in range(n_activities, len(axes)):
        fig.delaxes(axes[j])

    return fig
