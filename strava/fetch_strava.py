import requests
import pandas as pd
from pandas import Timestamp
from datetime import datetime
from strava.params import *
import time
from sqlalchemy import create_engine


def get_strava_header():
    payload = {
        'client_id': STRAVA_CLIENT_ID,
        'client_secret': STRAVA_CLIENT_SECRET,
        'refresh_token': STRAVA_REFRESH_TOKEN,
        'grant_type': "refresh_token",
        'f': 'json'
    }
    res = requests.post(AUTH_URL, data=payload, verify=False)
    access_token = res.json()['access_token']
    header = {'Authorization': 'Bearer ' + access_token}
    return header

# Fonction pour récupérer les données depuis l'API Strava
def fetch_strava_data(after_date = None, return_header=False):

    header = get_strava_header()


    # Si une date est fournie, la convertir en timestamp Unix pour Strava
    after_timestamp = None
    if after_date:
        after_timestamp = int(after_date.timestamp())
        print(f"⏩ Récupération des activités après {after_date} (timestamp={after_timestamp})")


    # Liste pour stocker toutes les activités
    all_activities = []

    # Boucle pour récupérer toutes les pages d'activités
    page = 1
    while True:
        params = {'per_page': 200, 'page': page}
        if after_timestamp:
            params['after'] = after_timestamp

        activities = requests.get(ACTIVITES_URL, headers=header, params=params).json()

        # Si aucune activité n'est retournée, on arrête la boucle
        if not activities:
            break

        # Ajout des activités à la liste
        all_activities.extend(activities)

        print(f"📄 Page {page}…")
        # Incrémentation du numéro de page
        page += 1

    # Conversion en DataFrame pandas
    activities_df = pd.DataFrame(all_activities)


    print("Données récupérées de l'API Strava ✅")

    # Retour conditionnel
    if return_header:
        return activities_df, header
    else:
        return activities_df





def get_all_activity_ids_from_db(db_uri, table_name):
    """
    Récupère tous les activity_id présents dans la base PostgreSQL.
    Retourne une liste de strings.
    """
    engine = create_engine(db_uri)
    with engine.connect() as conn:
        df = pd.read_sql(f"SELECT id FROM {table_name}", conn)

    print("Ids récupérés ✅")
    return df["id"].astype(str).tolist()


def fetch_stream(activity_id, header):

    #Récupère les streams (altitude, distance, latlng, time) d'une activité

    url = f"https://www.strava.com/api/v3/activities/{activity_id}/streams"
    params = {"keys": "latlng,altitude,distance,time", "key_by_type": "true"}
    resp = requests.get(url, headers=header, params=params)
    resp.raise_for_status()
    streams = resp.json()

    latlng = streams.get("latlng", {}).get("data", [])
    altitude = streams.get("altitude", {}).get("data", [])
    distance = streams.get("distance", {}).get("data", [])
    time = streams.get("time", {}).get("data", [])

    # Construction DataFrame
    df_stream = pd.DataFrame({
        "activity_id": activity_id,
        "lat": [pt[0] for pt in latlng] if latlng else None,
        "lon": [pt[1] for pt in latlng] if latlng else None,
        "altitude": altitude,
        "distance_m": distance,
        "time_s": time
    })
    print(f"Stream de l'activité {activity_id} récupéré ✅")

    return df_stream


def fetch_multiple_streams_df(activity_ids, header, max_per_15min=590):
    dfs = []
    count = 0
    no_stream_ids = []
    for i, activity_id in enumerate(activity_ids):
        if count >= max_per_15min:
            print("⏸ Pause 15 minutes pour respecter la limite Strava…")
            time.sleep(15 * 60)
            count = 0
        try:
            df_stream = fetch_stream(activity_id, header)
            # Ignore si l'une des 4 colonnes est entièrement vide ou NaN
            cols = ["altitude", "distance_m", "lat", "lon"]
            if df_stream.empty or any(df_stream[col].isna().all() or df_stream[col].isnull().all() for col in cols):
                no_stream_ids.append(activity_id)
            else:
                dfs.append(df_stream)
            count += 1
        except Exception as e:
            print(f"Erreur pour l'activité {activity_id}: {e}")
            no_stream_ids.append(activity_id)
    if dfs:
        result = pd.concat(dfs, ignore_index=True)
    else:
        result = pd.DataFrame()
    print(f"{len(no_stream_ids)} activités sans stream (ignorées).")
    return result
